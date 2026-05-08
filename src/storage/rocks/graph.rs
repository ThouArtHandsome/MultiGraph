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

//! `GraphReader + GraphWriter + GraphStorage` impls for `RocksStorage`.

use std::collections::HashSet;
use std::sync::{Arc, Weak};

use rocksdb::{Direction as ScanDir, IteratorMode, ReadOptions, WriteBatch};
use smol_str::SmolStr;

use crate::storage::rocks::encoding::{
    decode_edge_key, encode_edge_key, encode_vertex_key, EdgeValue, VertexValue,
    CF_EDGES_IN, CF_EDGES_OUT, CF_VERTICES,
};
use crate::storage::rocks::store::RocksStorage;
use crate::storage::graph_store::{GraphReader, GraphStorage, GraphWriter};
use crate::types::gvalue::{Primitive, Property};
use crate::types::prop_key::PropKey;
use crate::types::{
    Direction, EdgeKey, FullEdge, FullElement, FullVertex, LabelId, StorageError, VertexKey,
};

// ── Property codec ────────────────────────────────────────────────────────────
//
// Format: count:u16 | (key_len:u16 | key:UTF-8 | tag:u8 | value_bytes)*
//
// Tags: 0=Bool(1B)  1=Int32(4B)  2=Int64(8B)  3=Float32(4B)  4=Float64(8B)
//       5=String(len:u16 + UTF-8)  6=Uuid(16B)  7=Null(0B)

fn encode_props(props: &[Property]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(props.len() as u16).to_be_bytes());
    for prop in props {
        let kb = prop.key.0.as_bytes();
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

fn decode_props(blob: &[u8]) -> Option<Vec<(PropKey, Primitive)>> {
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
        let key = PropKey::new(std::str::from_utf8(&blob[pos..pos + klen]).ok()?);
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
                Primitive::String(SmolStr::new(s))
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
        out.push((key, val));
    }
    Some(out)
}

// ── Element builders ──────────────────────────────────────────────────────────
//
// Properties are returned with dead `Weak::new()` back-pointers.
// The cache / transaction layer is responsible for re-wrapping elements in a
// live `Arc<FullElement>` when valid `Property::owner` references are needed.

fn build_full_vertex(id: VertexKey, vv: &VertexValue) -> Result<Arc<FullVertex>, StorageError> {
    let raw = decode_props(&vv.property_blob)
        .ok_or_else(|| StorageError::Other("corrupt vertex property blob".into()))?;
    let props = raw
        .into_iter()
        .map(|(k, v)| Property { owner: Weak::<FullElement>::new(), key: k, value: v })
        .collect();
    Ok(Arc::new(FullVertex { id, label: SmolStr::new(&vv.label), props }))
}

fn build_full_edge(key: EdgeKey, ev: &EdgeValue) -> Result<Arc<FullEdge>, StorageError> {
    let raw = decode_props(&ev.property_blob)
        .ok_or_else(|| StorageError::Other("corrupt edge property blob".into()))?;
    let props = raw
        .into_iter()
        .map(|(k, v)| Property { owner: Weak::<FullElement>::new(), key: k, value: v })
        .collect();
    Ok(Arc::new(FullEdge { key, props }))
}

// ── Edge scan helpers ─────────────────────────────────────────────────────────

fn edge_scan_prefix(vertex: VertexKey, direction: Direction, label: Option<LabelId>) -> Vec<u8> {
    let dir_byte: u8 = match direction {
        Direction::OUT => 0x00,
        Direction::IN => 0x01,
    };
    let mut prefix = Vec::with_capacity(11);
    prefix.extend_from_slice(&vertex.to_be_bytes());
    prefix.push(dir_byte);
    if let Some(lbl) = label {
        prefix.extend_from_slice(&lbl.to_be_bytes());
    }
    prefix
}

/// Exclusive upper bound for a lexicographic prefix scan.
/// Returns `None` when all bytes are `0xFF` (no finite upper bound).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for byte in upper.iter_mut().rev() {
        if *byte < 0xFF {
            *byte += 1;
            return Some(upper);
        }
        *byte = 0x00;
    }
    None
}

// ── GraphReader ───────────────────────────────────────────────────────────────

impl GraphReader for RocksStorage {
    fn get_edge(&self, key: EdgeKey) -> Result<Option<Arc<FullEdge>>, StorageError> {
        let cf = self
            .db
            .cf_handle(CF_EDGES_OUT)
            .ok_or_else(|| StorageError::Other("missing CF: edges_out".into()))?;
        let canonical = key.canonical();
        match self
            .db
            .get_cf(&cf, encode_edge_key(canonical))
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            None => Ok(None),
            Some(raw) => {
                let ev = EdgeValue::decode(&raw);
                Ok(Some(build_full_edge(canonical, &ev)?))
            }
        }
    }

    fn get_vertex(&self, key: VertexKey) -> Result<Option<Arc<FullVertex>>, StorageError> {
        let cf = self
            .db
            .cf_handle(CF_VERTICES)
            .ok_or_else(|| StorageError::Other("missing CF: vertices".into()))?;
        match self
            .db
            .get_cf(&cf, encode_vertex_key(key))
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            None => Ok(None),
            Some(raw) => {
                let vv = VertexValue::decode(&raw)
                    .ok_or_else(|| StorageError::Other("corrupt vertex value".into()))?;
                Ok(Some(build_full_vertex(key, &vv)?))
            }
        }
    }

    fn get_vertices(&self, keys: &[VertexKey]) -> Result<Vec<Arc<FullVertex>>, StorageError> {
        let cf = self
            .db
            .cf_handle(CF_VERTICES)
            .ok_or_else(|| StorageError::Other("missing CF: vertices".into()))?;
        let mut result = Vec::with_capacity(keys.len());
        for &key in keys {
            match self
                .db
                .get_cf(&cf, encode_vertex_key(key))
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                None => {}
                Some(raw) => {
                    let vv = VertexValue::decode(&raw)
                        .ok_or_else(|| StorageError::Other("corrupt vertex value".into()))?;
                    result.push(build_full_vertex(key, &vv)?);
                }
            }
        }
        Ok(result)
    }

    fn get_edges(
        &self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<Arc<FullEdge>>, StorageError> {
        let cf_name = match direction {
            Direction::OUT => CF_EDGES_OUT,
            Direction::IN => CF_EDGES_IN,
        };
        let cf = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| StorageError::Other(format!("missing CF: {cf_name}")))?;

        let prefix = edge_scan_prefix(vertex, direction, label);

        let mut read_opts = ReadOptions::default();
        if let Some(upper) = prefix_upper_bound(&prefix) {
            read_opts.set_iterate_upper_bound(upper);
        }

        let dst_set: Option<HashSet<VertexKey>> =
            dst.map(|keys| keys.iter().copied().collect());

        let iter = self
            .db
            .iterator_cf_opt(&cf, read_opts, IteratorMode::From(&prefix, ScanDir::Forward));

        let mut result = Vec::new();
        for item in iter {
            let (key_bytes, val_bytes) =
                item.map_err(|e| StorageError::Other(e.to_string()))?;
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            let edge_key = decode_edge_key(&key_bytes)
                .ok_or_else(|| StorageError::Other("corrupt edge key".into()))?;
            if let Some(ref set) = dst_set {
                if !set.contains(&edge_key.secondary_id) {
                    continue;
                }
            }
            let ev = EdgeValue::decode(&val_bytes);
            result.push(build_full_edge(edge_key, &ev)?);
        }

        Ok(result)
    }
}

// ── GraphWriter ───────────────────────────────────────────────────────────────

impl GraphWriter for RocksStorage {
    fn insert_vertices(&mut self, vertices: &[FullVertex]) -> Result<(), StorageError> {
        let cf = self
            .db
            .cf_handle(CF_VERTICES)
            .ok_or_else(|| StorageError::Other("missing CF: vertices".into()))?;
        let mut batch = WriteBatch::default();
        for fv in vertices {
            let vv = VertexValue {
                label: fv.label.to_string(),
                property_blob: encode_props(&fv.props),
            };
            batch.put_cf(&cf, encode_vertex_key(fv.id), vv.encode());
        }
        self.db.write(batch).map_err(|e| StorageError::Other(e.to_string()))
    }

    fn delete_vertex(&mut self, key: VertexKey) -> Result<(), StorageError> {
        let cf = self
            .db
            .cf_handle(CF_VERTICES)
            .ok_or_else(|| StorageError::Other("missing CF: vertices".into()))?;
        self.db
            .delete_cf(&cf, encode_vertex_key(key))
            .map_err(|e| StorageError::Other(e.to_string()))
    }

    fn delete_edge(&mut self, key: EdgeKey) -> Result<(), StorageError> {
        let cf_out = self
            .db
            .cf_handle(CF_EDGES_OUT)
            .ok_or_else(|| StorageError::Other("missing CF: edges_out".into()))?;
        let cf_in = self
            .db
            .cf_handle(CF_EDGES_IN)
            .ok_or_else(|| StorageError::Other("missing CF: edges_in".into()))?;
        let canonical = key.canonical();
        let mut batch = WriteBatch::default();
        batch.delete_cf(&cf_out, encode_edge_key(canonical));
        batch.delete_cf(&cf_in, encode_edge_key(canonical.flip()));
        self.db.write(batch).map_err(|e| StorageError::Other(e.to_string()))
    }

    fn insert_edges(&mut self, edges: &[FullEdge]) -> Result<(), StorageError> {
        let cf_out = self
            .db
            .cf_handle(CF_EDGES_OUT)
            .ok_or_else(|| StorageError::Other("missing CF: edges_out".into()))?;
        let cf_in = self
            .db
            .cf_handle(CF_EDGES_IN)
            .ok_or_else(|| StorageError::Other("missing CF: edges_in".into()))?;
        let mut batch = WriteBatch::default();
        for fe in edges {
            let ev_bytes =
                EdgeValue { property_blob: encode_props(&fe.props) }.encode().to_vec();
            // Canonical Out-direction key → edges_out; flipped In-direction key → edges_in.
            let out_key = fe.key.canonical();
            batch.put_cf(&cf_out, encode_edge_key(out_key), &ev_bytes);
            batch.put_cf(&cf_in, encode_edge_key(out_key.flip()), &ev_bytes);
        }
        self.db.write(batch).map_err(|e| StorageError::Other(e.to_string()))
    }
}

impl GraphStorage for RocksStorage {}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use smol_str::SmolStr;

    use crate::storage::graph_store::{GraphReader, GraphWriter};
    use crate::storage::rocks::store::RocksStorage;
    use crate::types::gvalue::{Primitive, Property};
    use crate::types::prop_key::PropKey;
    use crate::types::{Direction, EdgeKey, FullEdge, FullElement, FullVertex};

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn open_temp_store() -> (RocksStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksStorage::open(dir.path()).unwrap();
        (store, dir)
    }

    fn make_vertex(id: u64, label: &str, props: Vec<(PropKey, Primitive)>) -> FullVertex {
        FullVertex {
            id,
            label: SmolStr::new(label),
            props: props
                .into_iter()
                .map(|(k, v)| Property { owner: Weak::<FullElement>::new(), key: k, value: v })
                .collect(),
        }
    }

    fn make_edge(key: EdgeKey, props: Vec<(PropKey, Primitive)>) -> FullEdge {
        FullEdge {
            key,
            props: props
                .into_iter()
                .map(|(k, v)| Property { owner: Weak::<FullElement>::new(), key: k, value: v })
                .collect(),
        }
    }

    // ── insert_vertices / get_vertex ──────────────────────────────────────────

    #[test]
    fn insert_and_get_single_vertex() {
        let (mut store, _dir) = open_temp_store();
        let v = make_vertex(
            1,
            "person",
            vec![
                (PropKey::new("name"), Primitive::String(SmolStr::new("Alice"))),
                (PropKey::new("age"), Primitive::Int32(30)),
            ],
        );
        store.insert_vertices(&[v]).unwrap();

        let fv = store.get_vertex(1).unwrap().unwrap();
        assert_eq!(fv.id, 1);
        assert_eq!(fv.label.as_str(), "person");
        assert_eq!(fv.props.len(), 2);
        assert_eq!(fv.props[0].key, PropKey::new("name"));
        assert_eq!(fv.props[0].value, Primitive::String(SmolStr::new("Alice")));
        assert_eq!(fv.props[1].key, PropKey::new("age"));
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
        store.insert_vertices(&[make_vertex(42, "empty", vec![])]).unwrap();

        let fv = store.get_vertex(42).unwrap().unwrap();
        assert_eq!(fv.label.as_str(), "empty");
        assert!(fv.props.is_empty());
    }

    #[test]
    fn insert_vertex_overwrite_updates_value() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_vertices(&[make_vertex(1, "person", vec![(PropKey::new("age"), Primitive::Int32(20))])])
            .unwrap();

        store
            .insert_vertices(&[make_vertex(1, "admin", vec![(PropKey::new("age"), Primitive::Int32(99))])])
            .unwrap();

        let fv = store.get_vertex(1).unwrap().unwrap();
        assert_eq!(fv.label.as_str(), "admin");
        assert_eq!(fv.props[0].value, Primitive::Int32(99));
    }

    // ── get_vertices ──────────────────────────────────────────────────────────

    #[test]
    fn get_vertices_returns_all_inserted() {
        let (mut store, _dir) = open_temp_store();
        let vertices = vec![
            make_vertex(1, "person", vec![(PropKey::new("name"), Primitive::String(SmolStr::new("Alice")))]),
            make_vertex(2, "person", vec![(PropKey::new("name"), Primitive::String(SmolStr::new("Bob")))]),
            make_vertex(3, "company", vec![(PropKey::new("name"), Primitive::String(SmolStr::new("Acme")))]),
        ];
        store.insert_vertices(&vertices).unwrap();

        let results = store.get_vertices(&[1, 2, 3]).unwrap();
        assert_eq!(results.len(), 3);

        let mut ids: Vec<u64> = results.iter().map(|v| v.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn get_vertices_silently_omits_missing_keys() {
        let (mut store, _dir) = open_temp_store();
        store.insert_vertices(&[make_vertex(10, "node", vec![])]).unwrap();

        let results = store.get_vertices(&[10, 20, 30]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 10);
    }

    #[test]
    fn get_vertices_all_missing_returns_empty() {
        let (store, _dir) = open_temp_store();
        assert!(store.get_vertices(&[1, 2, 3]).unwrap().is_empty());
    }

    // ── insert_edges / get_edges ──────────────────────────────────────────────

    #[test]
    fn insert_edge_readable_in_out_direction() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_edges(&[make_edge(
                EdgeKey::out_e(1, 5, 2, 0),
                vec![(PropKey::new("weight"), Primitive::Float64(1.5))],
            )])
            .unwrap();

        let edges = store.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 1);
        let fe = &edges[0];
        assert_eq!(fe.key.primary_id, 1);
        assert_eq!(fe.key.secondary_id, 2);
        assert_eq!(fe.key.label_id, 5);
        assert_eq!(fe.key.direction, Direction::OUT);
        assert_eq!(fe.props.len(), 1);
        assert_eq!(fe.props[0].key, PropKey::new("weight"));
        assert_eq!(fe.props[0].value, Primitive::Float64(1.5));
    }

    #[test]
    fn insert_edge_readable_in_in_direction() {
        let (mut store, _dir) = open_temp_store();
        // Edge: src=1 → dst=2, label=5
        store
            .insert_edges(&[make_edge(EdgeKey::out_e(1, 5, 2, 0), vec![])])
            .unwrap();

        // Query from dst vertex with Direction::IN
        let edges = store.get_edges(2, Direction::IN, None, None).unwrap();
        assert_eq!(edges.len(), 1);
        let fe = &edges[0];
        assert_eq!(fe.key.primary_id, 2);   // dst is primary for IN view
        assert_eq!(fe.key.secondary_id, 1); // src is secondary for IN view
        assert_eq!(fe.key.label_id, 5);
        assert_eq!(fe.key.direction, Direction::IN);
    }

    #[test]
    fn insert_edge_supplied_as_in_key_writes_both_directions() {
        let (mut store, _dir) = open_temp_store();
        // in_e(src=1, label=5, dst=2) — caller supplies an IN-direction key
        store
            .insert_edges(&[make_edge(EdgeKey::in_e(1, 5, 2, 0), vec![])])
            .unwrap();

        // Both OUT and IN scans must find exactly one edge
        let out_edges = store.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(out_edges.len(), 1);
        assert_eq!(out_edges[0].key, EdgeKey::out_e(1, 5, 2, 0));

        let in_edges = store.get_edges(2, Direction::IN, None, None).unwrap();
        assert_eq!(in_edges.len(), 1);
        assert_eq!(in_edges[0].key, EdgeKey::in_e(1, 5, 2, 0));
    }

    #[test]
    fn get_edges_filter_by_label() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_edges(&[
                make_edge(EdgeKey::out_e(1, 1, 10, 0), vec![]),
                make_edge(EdgeKey::out_e(1, 2, 20, 0), vec![]),
                make_edge(EdgeKey::out_e(1, 1, 30, 0), vec![]),
            ])
            .unwrap();

        let label1 = store.get_edges(1, Direction::OUT, Some(1), None).unwrap();
        assert_eq!(label1.len(), 2);
        assert!(label1.iter().all(|e| e.key.label_id == 1));

        let label2 = store.get_edges(1, Direction::OUT, Some(2), None).unwrap();
        assert_eq!(label2.len(), 1);
        assert_eq!(label2[0].key.secondary_id, 20);
    }

    #[test]
    fn get_edges_filter_by_dst() {
        let (mut store, _dir) = open_temp_store();
        store
            .insert_edges(&[
                make_edge(EdgeKey::out_e(1, 1, 10, 0), vec![]),
                make_edge(EdgeKey::out_e(1, 1, 20, 0), vec![]),
                make_edge(EdgeKey::out_e(1, 1, 30, 0), vec![]),
            ])
            .unwrap();

        let result = store.get_edges(1, Direction::OUT, None, Some(&[10, 30])).unwrap();
        assert_eq!(result.len(), 2);

        let mut dst_ids: Vec<u64> = result.iter().map(|e| e.key.secondary_id).collect();
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
                make_edge(EdgeKey::out_e(1, 1, 10, 0), vec![]),
                make_edge(EdgeKey::out_e(1, 1, 20, 0), vec![]),
                make_edge(EdgeKey::out_e(1, 1, 30, 0), vec![]),
                make_edge(EdgeKey::out_e(2, 1, 10, 0), vec![]), // different src
            ])
            .unwrap();

        // Only edges from src=1
        let edges = store.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 3);
        assert!(edges.iter().all(|e| e.key.primary_id == 1));
    }
}
