// Copyright (c) 2026 Austin Han <austinhan1024@gmail.com>
//
// This file is part of MultiGraph.
//
// Use of this software is governed by the Business Source License 1.1
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date (2030-01-01), in accordance with the Business Source
// License, use of this software will be governed by the Apache License 2.0.
//
// SPDX-License-Identifier: BUSL-1.1

//! Non-transactional admin read/write operations for `RocksStorage`.
//!
//! These methods are for bulk loads and admin tooling **only**.  They bypass
//! OCC conflict detection; use `GraphContext` for all write paths that
//! require conflict safety.
//!
//! # Property codec
//!
//! Format: `count:u16 | (key_len:u16 | key:UTF-8 | tag:u8 | value_bytes)*`
//!
//! Tags: `0`=Bool(1B) `1`=Int32(4B) `2`=Int64(8B) `3`=Float32(4B)
//!       `4`=Float64(8B) `5`=String(len:u16 + UTF-8) `6`=Uuid(16B) `7`=Null(0B)

use std::collections::HashSet;

use rocksdb::{Direction as ScanDir, IteratorMode, ReadOptions, WriteBatchWithTransaction};

use crate::store::rocks::encoding::{
    decode_edge_key_in, decode_edge_key_out, edge_scan_prefix, encode_edge_key_in, encode_edge_key_out,
    encode_vertex_key, prefix_upper_bound, EdgeValue, VertexValue, CF_EDGES_IN, CF_EDGES_OUT, CF_VERTICES,
};
use crate::store::rocks::store::RocksStorage;
use crate::types::gvalue::{Primitive, Property};
use crate::types::prop_key::PropKey;
use crate::types::{CanonicalEdgeKey, CanonicalKey, Direction, FullEdge, FullVertex, LabelId, StoreError, VertexKey};

#[allow(dead_code)]
type EdgeKeyDecoder = fn(&[u8]) -> Option<CanonicalEdgeKey>;

// ── Property codec ────────────────────────────────────────────────────────────

/// Serialize a property list to the binary format described in the module comment.
pub(super) fn encode_props(props: &[Property]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(props.len() as u16).to_be_bytes());
    for prop in props {
        let kb = prop.key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u16).to_be_bytes());
        buf.extend_from_slice(kb);
        match &prop.value {
            Primitive::Bool(b) => {
                buf.push(0);
                buf.push(*b as u8);
            }
            Primitive::Int32(n) => {
                buf.push(1);
                buf.extend_from_slice(&n.to_be_bytes());
            }
            Primitive::Int64(n) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_be_bytes());
            }
            Primitive::Float32(f) => {
                buf.push(3);
                buf.extend_from_slice(&f.to_bits().to_be_bytes());
            }
            Primitive::Float64(f) => {
                buf.push(4);
                buf.extend_from_slice(&f.to_bits().to_be_bytes());
            }
            Primitive::String(s) => {
                buf.push(5);
                let sb = s.as_bytes();
                buf.extend_from_slice(&(sb.len() as u16).to_be_bytes());
                buf.extend_from_slice(sb);
            }
            Primitive::Uuid(u) => {
                buf.push(6);
                buf.extend_from_slice(&u.to_be_bytes());
            }
            Primitive::Null => {
                buf.push(7);
            }
        }
    }
    buf
}

/// Deserialize a property blob produced by `encode_props`.  Returns `None` on
/// any structural error so callers can surface a `StoreError::Other`.
pub(super) fn decode_props(blob: &[u8], owner: CanonicalKey) -> Option<Vec<Property>> {
    if blob.len() < 2 {
        return None;
    }
    let count = u16::from_be_bytes(blob[0..2].try_into().ok()?) as usize;
    let mut pos = 2;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 2 > blob.len() {
            return None;
        }
        let klen = u16::from_be_bytes(blob[pos..pos + 2].try_into().ok()?) as usize;
        pos += 2;
        if pos + klen > blob.len() {
            return None;
        }
        let key: PropKey = smol_str::SmolStr::new(std::str::from_utf8(&blob[pos..pos + klen]).ok()?);
        pos += klen;
        if pos >= blob.len() {
            return None;
        }
        let tag = blob[pos];
        pos += 1;
        let val = match tag {
            0 => {
                if pos >= blob.len() {
                    return None;
                }
                let b = blob[pos] != 0;
                pos += 1;
                Primitive::Bool(b)
            }
            1 => {
                if pos + 4 > blob.len() {
                    return None;
                }
                let n = i32::from_be_bytes(blob[pos..pos + 4].try_into().ok()?);
                pos += 4;
                Primitive::Int32(n)
            }
            2 => {
                if pos + 8 > blob.len() {
                    return None;
                }
                let n = i64::from_be_bytes(blob[pos..pos + 8].try_into().ok()?);
                pos += 8;
                Primitive::Int64(n)
            }
            3 => {
                if pos + 4 > blob.len() {
                    return None;
                }
                let bits = u32::from_be_bytes(blob[pos..pos + 4].try_into().ok()?);
                pos += 4;
                Primitive::Float32(f32::from_bits(bits))
            }
            4 => {
                if pos + 8 > blob.len() {
                    return None;
                }
                let bits = u64::from_be_bytes(blob[pos..pos + 8].try_into().ok()?);
                pos += 8;
                Primitive::Float64(f64::from_bits(bits))
            }
            5 => {
                if pos + 2 > blob.len() {
                    return None;
                }
                let slen = u16::from_be_bytes(blob[pos..pos + 2].try_into().ok()?) as usize;
                pos += 2;
                if pos + slen > blob.len() {
                    return None;
                }
                let s = std::str::from_utf8(&blob[pos..pos + slen]).ok()?;
                pos += slen;
                Primitive::String(smol_str::SmolStr::new(s))
            }
            6 => {
                if pos + 16 > blob.len() {
                    return None;
                }
                let u = u128::from_be_bytes(blob[pos..pos + 16].try_into().ok()?);
                pos += 16;
                Primitive::Uuid(u)
            }
            7 => Primitive::Null,
            _ => return None,
        };
        out.push(Property { owner, key, value: val });
    }
    Some(out)
}

// ── Element builders ──────────────────────────────────────────────────────────

/// Decode a `VertexValue` from storage into a `FullVertex`.
pub(super) fn build_full_vertex(id: VertexKey, vv: &VertexValue) -> Result<FullVertex, StoreError> {
    let owner = CanonicalKey::Vertex(id);
    let props = decode_props(&vv.property_blob, owner)
        .ok_or_else(|| StoreError::Other("corrupt vertex property blob".into()))?;
    Ok(FullVertex { id, label_id: vv.label_id, props })
}

/// Decode an `EdgeValue` from storage into an `FullEdge`.
pub(super) fn build_full_edge(cek: CanonicalEdgeKey, ev: &EdgeValue) -> Result<FullEdge, StoreError> {
    let owner = CanonicalKey::Edge(cek);
    let props =
        decode_props(&ev.property_blob, owner).ok_or_else(|| StoreError::Other("corrupt edge property blob".into()))?;
    Ok(FullEdge { src_id: cek.src_id, label_id: cek.label_id, rank: cek.rank, dst_id: cek.dst_id, props })
}

// ── Admin reads / writes ──────────────────────────────────────────────────────
// These methods are used in tests and admin tooling.  They are pub(crate) but
// only called from #[cfg(test)] blocks, so clippy flags them as dead code during
// non-test compilation.  The suppression is intentional.
#[allow(dead_code)]
impl RocksStorage {
    pub(crate) fn get_vertex(&self, key: VertexKey) -> Result<Option<FullVertex>, StoreError> {
        let cf = self.db.cf_handle(CF_VERTICES).ok_or_else(|| StoreError::Other("missing CF: vertices".into()))?;
        match self.db.get_cf(&cf, encode_vertex_key(key)).map_err(|e| StoreError::Other(e.to_string()))? {
            None => Ok(None),
            Some(raw) => {
                let vv = VertexValue::decode(&raw).ok_or_else(|| StoreError::Other("corrupt vertex value".into()))?;
                Ok(Some(build_full_vertex(key, &vv)?))
            }
        }
    }

    pub(crate) fn get_vertices(&self, keys: &[VertexKey]) -> Result<Vec<FullVertex>, StoreError> {
        let cf = self.db.cf_handle(CF_VERTICES).ok_or_else(|| StoreError::Other("missing CF: vertices".into()))?;
        let mut result = Vec::with_capacity(keys.len());
        for &key in keys {
            match self.db.get_cf(&cf, encode_vertex_key(key)).map_err(|e| StoreError::Other(e.to_string()))? {
                None => {}
                Some(raw) => {
                    let vv =
                        VertexValue::decode(&raw).ok_or_else(|| StoreError::Other("corrupt vertex value".into()))?;
                    result.push(build_full_vertex(key, &vv)?);
                }
            }
        }
        Ok(result)
    }

    pub(crate) fn get_edge(&self, key: CanonicalEdgeKey) -> Result<Option<FullEdge>, StoreError> {
        let cf = self.db.cf_handle(CF_EDGES_OUT).ok_or_else(|| StoreError::Other("missing CF: edges_out".into()))?;
        match self.db.get_cf(&cf, encode_edge_key_out(key)).map_err(|e| StoreError::Other(e.to_string()))? {
            None => Ok(None),
            Some(raw) => Ok(Some(build_full_edge(key, &EdgeValue::decode(&raw))?)),
        }
    }

    pub(crate) fn get_edges(
        &self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<FullEdge>, StoreError> {
        let (cf_name, decode_fn): (&str, EdgeKeyDecoder) = match direction {
            Direction::OUT => (CF_EDGES_OUT, decode_edge_key_out),
            Direction::IN => (CF_EDGES_IN, decode_edge_key_in),
        };
        let cf = self.db.cf_handle(cf_name).ok_or_else(|| StoreError::Other(format!("missing CF: {cf_name}")))?;

        let prefix = edge_scan_prefix(vertex, label);
        let mut read_opts = ReadOptions::default();
        if let Some(upper) = prefix_upper_bound(&prefix) {
            read_opts.set_iterate_upper_bound(upper);
        }

        let dst_set: Option<HashSet<VertexKey>> = dst.map(|k| k.iter().copied().collect());
        let iter = self.db.iterator_cf_opt(&cf, read_opts, IteratorMode::From(&prefix, ScanDir::Forward));

        let mut result = Vec::new();
        for item in iter {
            let (key_bytes, val_bytes) = item.map_err(|e| StoreError::Other(e.to_string()))?;
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            let cek = decode_fn(&key_bytes).ok_or_else(|| StoreError::Other("corrupt edge key".into()))?;
            if let Some(ref set) = dst_set {
                let remote = match direction {
                    Direction::OUT => cek.dst_id,
                    Direction::IN => cek.src_id,
                };
                if !set.contains(&remote) {
                    continue;
                }
            }
            result.push(build_full_edge(cek, &EdgeValue::decode(&val_bytes))?);
        }
        Ok(result)
    }

    // ── Admin writes ──────────────────────────────────────────────────────────
    // All write methods use `WriteBatchWithTransaction::<true>` (TRANSACTION=true).
    // `OptimisticTransactionDB::write()` requires this type; using the plain
    // `WriteBatch` (TRANSACTION=false) is a compile-time type mismatch.

    pub(crate) fn insert_vertices(&mut self, vertices: &[FullVertex]) -> Result<(), StoreError> {
        let cf = self.db.cf_handle(CF_VERTICES).ok_or_else(|| StoreError::Other("missing CF: vertices".into()))?;
        let mut batch = WriteBatchWithTransaction::<true>::default();
        for vv in vertices {
            let val = VertexValue { label_id: vv.label_id, property_blob: encode_props(&vv.props) };
            batch.put_cf(&cf, encode_vertex_key(vv.id), val.encode());
        }
        self.db.write(batch).map_err(|e| StoreError::Other(e.to_string()))
    }

    pub(crate) fn insert_edges(&mut self, edges: &[FullEdge]) -> Result<(), StoreError> {
        let cf_out =
            self.db.cf_handle(CF_EDGES_OUT).ok_or_else(|| StoreError::Other("missing CF: edges_out".into()))?;
        let cf_in = self.db.cf_handle(CF_EDGES_IN).ok_or_else(|| StoreError::Other("missing CF: edges_in".into()))?;
        let mut batch = WriteBatchWithTransaction::<true>::default();
        for ev in edges {
            let cek = ev.canonical_key();
            let bytes = EdgeValue { property_blob: encode_props(&ev.props) }.encode().to_vec();
            batch.put_cf(&cf_out, encode_edge_key_out(cek), &bytes);
            batch.put_cf(&cf_in, encode_edge_key_in(cek), &bytes);
        }
        self.db.write(batch).map_err(|e| StoreError::Other(e.to_string()))
    }

    pub(crate) fn delete_vertices(&mut self, keys: &[VertexKey]) -> Result<(), StoreError> {
        let cf = self.db.cf_handle(CF_VERTICES).ok_or_else(|| StoreError::Other("missing CF: vertices".into()))?;
        let mut batch = WriteBatchWithTransaction::<true>::default();
        for &key in keys {
            batch.delete_cf(&cf, encode_vertex_key(key));
        }
        self.db.write(batch).map_err(|e| StoreError::Other(e.to_string()))
    }

    pub(crate) fn delete_edges(&mut self, keys: &[CanonicalEdgeKey]) -> Result<(), StoreError> {
        let cf_out =
            self.db.cf_handle(CF_EDGES_OUT).ok_or_else(|| StoreError::Other("missing CF: edges_out".into()))?;
        let cf_in = self.db.cf_handle(CF_EDGES_IN).ok_or_else(|| StoreError::Other("missing CF: edges_in".into()))?;
        let mut batch = WriteBatchWithTransaction::<true>::default();
        for &key in keys {
            batch.delete_cf(&cf_out, encode_edge_key_out(key));
            batch.delete_cf(&cf_in, encode_edge_key_in(key));
        }
        self.db.write(batch).map_err(|e| StoreError::Other(e.to_string()))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use smol_str::SmolStr;

    use crate::store::rocks::store::RocksStorage;
    use crate::types::full_element::{FullEdge, FullVertex};
    use crate::types::gvalue::{Primitive, Property};
    use crate::types::{CanonicalEdgeKey, CanonicalKey, Direction};

    fn open_temp_store() -> (RocksStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksStorage::open(dir.path()).unwrap();
        (store, dir)
    }

    fn make_vertex(id: u64, label_id: u16, props: Vec<(SmolStr, Primitive)>) -> FullVertex {
        let owner = CanonicalKey::Vertex(id);
        FullVertex {
            id,
            label_id,
            props: props.into_iter().map(|(k, v)| Property { owner, key: k, value: v }).collect(),
        }
    }

    fn make_edge(cek: CanonicalEdgeKey, props: Vec<(SmolStr, Primitive)>) -> FullEdge {
        let owner = CanonicalKey::Edge(cek);
        FullEdge {
            src_id: cek.src_id,
            label_id: cek.label_id,
            rank: cek.rank,
            dst_id: cek.dst_id,
            props: props.into_iter().map(|(k, v)| Property { owner, key: k, value: v }).collect(),
        }
    }

    fn cek(src: u64, label: u16, dst: u64) -> CanonicalEdgeKey {
        CanonicalEdgeKey { src_id: src, label_id: label, rank: 0, dst_id: dst }
    }

    #[test]
    fn insert_and_get_single_vertex() {
        let (mut store, _dir) = open_temp_store();
        let v = make_vertex(
            1,
            3,
            vec![
                (SmolStr::new("name"), Primitive::String(SmolStr::new("Alice"))),
                (SmolStr::new("age"), Primitive::Int32(30)),
            ],
        );
        store.insert_vertices(&[v]).unwrap();
        let fv = store.get_vertex(1).unwrap().unwrap();
        assert_eq!(fv.id, 1);
        assert_eq!(fv.label_id, 3);
        assert_eq!(fv.props.len(), 2);
        assert_eq!(fv.props[0].key, SmolStr::new("name"));
        assert_eq!(fv.props[0].value, Primitive::String(SmolStr::new("Alice")));
        assert_eq!(fv.props[0].owner, CanonicalKey::Vertex(1));
        assert_eq!(fv.props[1].value, Primitive::Int32(30));
    }

    #[test]
    fn get_vertex_not_found_returns_none() {
        let (store, _dir) = open_temp_store();
        assert!(store.get_vertex(999).unwrap().is_none());
    }

    #[test]
    fn insert_vertex_with_no_props() {
        let (mut store, _dir) = open_temp_store();
        store.insert_vertices(&[make_vertex(42, 1, vec![])]).unwrap();
        let fv = store.get_vertex(42).unwrap().unwrap();
        assert_eq!(fv.label_id, 1);
        assert!(fv.props.is_empty());
    }

    #[test]
    fn insert_vertex_overwrite_updates_value() {
        let (mut store, _dir) = open_temp_store();
        store.insert_vertices(&[make_vertex(1, 1, vec![(SmolStr::new("age"), Primitive::Int32(20))])]).unwrap();
        store.insert_vertices(&[make_vertex(1, 2, vec![(SmolStr::new("age"), Primitive::Int32(99))])]).unwrap();
        let fv = store.get_vertex(1).unwrap().unwrap();
        assert_eq!(fv.label_id, 2);
        assert_eq!(fv.props[0].value, Primitive::Int32(99));
    }

    #[test]
    fn get_vertices_returns_all_inserted() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_vertices(&[make_vertex(1, 1, vec![]), make_vertex(2, 1, vec![]), make_vertex(3, 2, vec![])])
            .unwrap();
        let results = store.get_vertices(&[1, 2, 3]).unwrap();
        assert_eq!(results.len(), 3);
        let mut ids: Vec<u64> = results.iter().map(|v| v.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn get_vertices_silently_omits_missing_keys() {
        let (mut store, _dir) = open_temp_store();
        store.insert_vertices(&[make_vertex(10, 1, vec![])]).unwrap();
        let results = store.get_vertices(&[10, 20, 30]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 10);
    }

    #[test]
    fn get_vertices_all_missing_returns_empty() {
        let (store, _dir) = open_temp_store();
        assert!(store.get_vertices(&[1, 2, 3]).unwrap().is_empty());
    }

    #[test]
    fn insert_edge_readable_out() {
        let (mut store, _dir) = open_temp_store();
        let k = cek(1, 5, 2);
        store.insert_edges(&[make_edge(k, vec![(SmolStr::new("weight"), Primitive::Float64(1.5))])]).unwrap();
        let edges = store.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 1);
        let fe = &edges[0];
        assert_eq!(fe.src_id, 1);
        assert_eq!(fe.dst_id, 2);
        assert_eq!(fe.label_id, 5);
        assert_eq!(fe.props[0].value, Primitive::Float64(1.5));
        assert_eq!(fe.props[0].owner, CanonicalKey::Edge(k));
    }

    #[test]
    fn insert_edge_readable_in() {
        let (mut store, _dir) = open_temp_store();
        store.insert_edges(&[make_edge(cek(1, 5, 2), vec![])]).unwrap();
        let edges = store.get_edges(2, Direction::IN, None, None).unwrap();
        assert_eq!(edges.len(), 1);
        let fe = &edges[0];
        assert_eq!(fe.src_id, 1);
        assert_eq!(fe.dst_id, 2);
        assert_eq!(fe.label_id, 5);
    }

    #[test]
    fn get_edges_filter_by_label() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_edges(&[
                make_edge(cek(1, 1, 10), vec![]),
                make_edge(cek(1, 2, 20), vec![]),
                make_edge(cek(1, 1, 30), vec![]),
            ])
            .unwrap();
        let label1 = store.get_edges(1, Direction::OUT, Some(1), None).unwrap();
        assert_eq!(label1.len(), 2);
        assert!(label1.iter().all(|e| e.label_id == 1));
        let label2 = store.get_edges(1, Direction::OUT, Some(2), None).unwrap();
        assert_eq!(label2.len(), 1);
        assert_eq!(label2[0].dst_id, 20);
    }

    #[test]
    fn get_edges_filter_by_dst() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_edges(&[
                make_edge(cek(1, 1, 10), vec![]),
                make_edge(cek(1, 1, 20), vec![]),
                make_edge(cek(1, 1, 30), vec![]),
            ])
            .unwrap();
        let result = store.get_edges(1, Direction::OUT, None, Some(&[10, 30])).unwrap();
        assert_eq!(result.len(), 2);
        let mut dst_ids: Vec<u64> = result.iter().map(|e| e.dst_id).collect();
        dst_ids.sort_unstable();
        assert_eq!(dst_ids, vec![10, 30]);
    }

    #[test]
    fn get_edges_no_match_returns_empty() {
        let (store, _dir) = open_temp_store();
        assert!(store.get_edges(99, Direction::OUT, None, None).unwrap().is_empty());
        assert!(store.get_edges(99, Direction::IN, None, None).unwrap().is_empty());
    }

    #[test]
    fn get_edges_multiple_from_same_source() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_edges(&[
                make_edge(cek(1, 1, 10), vec![]),
                make_edge(cek(1, 1, 20), vec![]),
                make_edge(cek(1, 1, 30), vec![]),
                make_edge(cek(2, 1, 10), vec![]),
            ])
            .unwrap();
        let edges = store.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 3);
        assert!(edges.iter().all(|e| e.src_id == 1));
    }
}
