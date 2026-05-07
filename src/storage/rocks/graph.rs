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
    decode_edge_key, encode_edge_key, EdgeValue, VertexKey as RocksVKey, VertexValue, CF_EDGES_IN,
    CF_EDGES_OUT, CF_VERTICES,
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
    fn get_vertex(&self, key: VertexKey) -> Result<Option<Arc<FullVertex>>, StorageError> {
        let cf = self
            .db
            .cf_handle(CF_VERTICES)
            .ok_or_else(|| StorageError::Other("missing CF: vertices".into()))?;
        match self
            .db
            .get_cf(&cf, RocksVKey(key).encode())
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
                .get_cf(&cf, RocksVKey(key).encode())
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
            batch.put_cf(&cf, RocksVKey(fv.id).encode(), vv.encode());
        }
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
