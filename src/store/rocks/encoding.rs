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

//! On-disk byte layout for RocksDB keys and values.
//!
//! All multi-byte integers are big-endian so that lexicographic byte order
//! matches numeric order, enabling efficient range scans.
//!
//! # Column families
//!
//! | CF name      | Key layout                               | Value layout              |
//! |--------------|------------------------------------------|---------------------------|
//! | `vertices`   | `[ VertexId:u64 ]`                       | `[ label_id:u16 \| props ]`|
//! | `edges_out`  | `[ SrcId:u64 \| LabelId:u16 \| Rank:u16 \| DstId:u64 ]` | `[ props ]` |
//! | `edges_in`   | `[ DstId:u64 \| LabelId:u16 \| Rank:u16 \| SrcId:u64 ]` | `[ props ]` |
//!
//! Edge properties are duplicated across `edges_out` and `edges_in`.
//! The direction byte present in previous versions has been removed; each CF
//! encodes direction implicitly via its key layout.
//!
//! # Prefix scan lengths
//!
//! | Prefix          | Bytes | Enables                              |
//! |-----------------|-------|--------------------------------------|
//! | `vertex_id`     | 8     | all incident edges (`bothE`)          |
//! | `vertex_id \| label_id` | 10 | `outE(label)` / `inE(label)`    |

use crate::types::{CanonicalEdgeKey, LabelId, Rank, VertexKey};

// ── Scan helpers ──────────────────────────────────────────────────────────────

/// Build the prefix for an edge CF scan:
/// `vertex_id` (8 B), optionally followed by `label_id` (2 B).
pub fn edge_scan_prefix(vertex: VertexKey, label: Option<LabelId>) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(10);
    prefix.extend_from_slice(&vertex.to_be_bytes());
    if let Some(lbl) = label {
        prefix.extend_from_slice(&lbl.to_be_bytes());
    }
    prefix
}

/// Compute the exclusive upper-bound for a prefix scan by incrementing the
/// last non-`0xFF` byte.  Returns `None` when all bytes are `0xFF` (scan to
/// end of CF instead).
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
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

// ── Column-family name constants ──────────────────────────────────────────────

pub const CF_VERTICES: &str = "vertices";
pub const CF_EDGES_OUT: &str = "edges_out";
pub const CF_EDGES_IN: &str = "edges_in";

// ── Size constants ────────────────────────────────────────────────────────────

pub const VERTEX_KEY_SIZE: usize = 8;
/// Edge key: 8 (vertex) + 2 (label) + 2 (rank) + 8 (vertex) = 20 bytes.
/// No direction byte — each CF encodes direction implicitly.
pub const EDGE_KEY_SIZE: usize = 20;

// ── VertexKey encoding ────────────────────────────────────────────────────────

pub fn encode_vertex_key(key: VertexKey) -> [u8; VERTEX_KEY_SIZE] {
    key.to_be_bytes()
}

#[allow(dead_code)]
pub fn decode_vertex_key(bytes: &[u8]) -> Option<VertexKey> {
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

// ── Edge key encoding ─────────────────────────────────────────────────────────
//
// edges_out layout:  [ SrcId:u64 | LabelId:u16 | Rank:u16 | DstId:u64 ]
// edges_in  layout:  [ DstId:u64 | LabelId:u16 | Rank:u16 | SrcId:u64 ]
//
// Both are encoded with the same physical byte format; only the semantic
// meaning of the first and last u64 differs by CF.

fn encode_edge_key_raw(a: VertexKey, label: LabelId, rank: Rank, b: VertexKey) -> [u8; EDGE_KEY_SIZE] {
    let mut buf = [0u8; EDGE_KEY_SIZE];
    buf[0..8].copy_from_slice(&a.to_be_bytes());
    buf[8..10].copy_from_slice(&label.to_be_bytes());
    buf[10..12].copy_from_slice(&rank.to_be_bytes());
    buf[12..20].copy_from_slice(&b.to_be_bytes());
    buf
}

/// Encode a `CanonicalEdgeKey` for the `edges_out` CF: `[ src | label | rank | dst ]`.
pub fn encode_edge_key_out(k: CanonicalEdgeKey) -> [u8; EDGE_KEY_SIZE] {
    encode_edge_key_raw(k.src_id, k.label_id, k.rank, k.dst_id)
}

/// Encode a `CanonicalEdgeKey` for the `edges_in` CF: `[ dst | label | rank | src ]`.
pub fn encode_edge_key_in(k: CanonicalEdgeKey) -> [u8; EDGE_KEY_SIZE] {
    encode_edge_key_raw(k.dst_id, k.label_id, k.rank, k.src_id)
}

/// Decode a 20-byte `edges_out` key: `[ src | label | rank | dst ]`.
pub fn decode_edge_key_out(bytes: &[u8]) -> Option<CanonicalEdgeKey> {
    if bytes.len() < EDGE_KEY_SIZE {
        return None;
    }
    Some(CanonicalEdgeKey {
        src_id: u64::from_be_bytes(bytes[0..8].try_into().ok()?),
        label_id: u16::from_be_bytes(bytes[8..10].try_into().ok()?) as LabelId,
        rank: u16::from_be_bytes(bytes[10..12].try_into().ok()?) as Rank,
        dst_id: u64::from_be_bytes(bytes[12..20].try_into().ok()?),
    })
}

/// Decode a 20-byte `edges_in` key: `[ dst | label | rank | src ]` → canonical.
pub fn decode_edge_key_in(bytes: &[u8]) -> Option<CanonicalEdgeKey> {
    if bytes.len() < EDGE_KEY_SIZE {
        return None;
    }
    Some(CanonicalEdgeKey {
        src_id: u64::from_be_bytes(bytes[12..20].try_into().ok()?),
        label_id: u16::from_be_bytes(bytes[8..10].try_into().ok()?) as LabelId,
        rank: u16::from_be_bytes(bytes[10..12].try_into().ok()?) as Rank,
        dst_id: u64::from_be_bytes(bytes[0..8].try_into().ok()?),
    })
}

// ── VertexValue ───────────────────────────────────────────────────────────────

/// `[ label_id:u16 | property_blob ]` — value in the `vertices` CF.
///
/// The label string is NOT stored here; `label_id` is resolved to a string
/// via the process-wide `Schema` when needed.
#[derive(Debug, Clone)]
pub struct VertexValue {
    pub label_id: LabelId,
    pub property_blob: Vec<u8>,
}

impl VertexValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.property_blob.len());
        buf.extend_from_slice(&self.label_id.to_be_bytes());
        buf.extend_from_slice(&self.property_blob);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }
        let label_id = u16::from_be_bytes(bytes[0..2].try_into().ok()?);
        let property_blob = bytes[2..].to_vec();
        Some(Self { label_id, property_blob })
    }
}

// ── EdgeValue ─────────────────────────────────────────────────────────────────

/// `[ property_blob ]` — value in both `edges_out` and `edges_in` CFs.
#[derive(Debug, Clone)]
pub struct EdgeValue {
    pub property_blob: Vec<u8>,
}

impl EdgeValue {
    pub fn encode(&self) -> &[u8] {
        &self.property_blob
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self { property_blob: bytes.to_vec() }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use smol_str::SmolStr;
    use std::sync::RwLock;

    use super::{
        decode_edge_key_in, decode_edge_key_out, decode_vertex_key, encode_edge_key_in, encode_edge_key_out,
        encode_vertex_key, EdgeValue, VertexValue,
    };
    use crate::types::full_element::{FullEdge, FullVertex};
    use crate::types::{CanonicalEdgeKey, CanonicalKey, Primitive, PropKey, Property, StoreError};

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn encode_props(props: &[(PropKey, Primitive)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(props.len() as u16).to_be_bytes());
        for (key, val) in props {
            let kb = key.as_bytes();
            buf.extend_from_slice(&(kb.len() as u16).to_be_bytes());
            buf.extend_from_slice(kb);
            match val {
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

    fn decode_props(blob: &[u8]) -> Vec<(PropKey, Primitive)> {
        let mut pos = 0;
        let count = u16::from_be_bytes(blob[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let klen = u16::from_be_bytes(blob[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let key: PropKey = SmolStr::new(std::str::from_utf8(&blob[pos..pos + klen]).unwrap());
            pos += klen;
            let tag = blob[pos];
            pos += 1;
            let val = match tag {
                0 => {
                    let b = blob[pos] != 0;
                    pos += 1;
                    Primitive::Bool(b)
                }
                1 => {
                    let n = i32::from_be_bytes(blob[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    Primitive::Int32(n)
                }
                2 => {
                    let n = i64::from_be_bytes(blob[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    Primitive::Int64(n)
                }
                3 => {
                    let bits = u32::from_be_bytes(blob[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    Primitive::Float32(f32::from_bits(bits))
                }
                4 => {
                    let bits = u64::from_be_bytes(blob[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    Primitive::Float64(f64::from_bits(bits))
                }
                5 => {
                    let slen = u16::from_be_bytes(blob[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    let s = std::str::from_utf8(&blob[pos..pos + slen]).unwrap();
                    pos += slen;
                    Primitive::String(SmolStr::new(s))
                }
                6 => {
                    let u = u128::from_be_bytes(blob[pos..pos + 16].try_into().unwrap());
                    pos += 16;
                    Primitive::Uuid(u)
                }
                7 => Primitive::Null,
                t => panic!("unknown prop tag {t}"),
            };
            out.push((key, val));
        }
        out
    }

    fn make_vertex(id: u64, label_id: u16, raw: &[(PropKey, Primitive)]) -> FullVertex {
        let owner = CanonicalKey::Vertex(id);
        let props = raw.iter().map(|(k, v)| Property { owner, key: k.clone(), value: v.clone() }).collect();
        FullVertex { id, label_id, props: RwLock::new(props) }
    }

    fn make_edge(cek: CanonicalEdgeKey, raw: &[(PropKey, Primitive)]) -> FullEdge {
        let owner = CanonicalKey::Edge(cek);
        let props = raw.iter().map(|(k, v)| Property { owner, key: k.clone(), value: v.clone() }).collect();
        FullEdge {
            src_id: cek.src_id,
            label_id: cek.label_id,
            rank: cek.rank,
            dst_id: cek.dst_id,
            props: RwLock::new(props),
        }
    }

    // ── VertexKey ─────────────────────────────────────────────────────────────

    #[test]
    fn vertex_key_encode_decode() {
        let id: u64 = 42;
        assert_eq!(decode_vertex_key(&encode_vertex_key(id)).unwrap(), id);
    }

    #[test]
    fn vertex_key_decode_bad_length() {
        assert!(decode_vertex_key(&[0u8; 4]).is_none());
        assert!(decode_vertex_key(&[]).is_none());
    }

    // ── EdgeKey (20 bytes, no direction byte) ─────────────────────────────────

    #[test]
    fn edge_key_out_encode_decode() {
        let k = CanonicalEdgeKey { src_id: 100, label_id: 3, rank: 0, dst_id: 200 };
        let encoded = encode_edge_key_out(k);
        assert_eq!(encoded.len(), 20);
        let decoded = decode_edge_key_out(&encoded).unwrap();
        assert_eq!(decoded, k);
    }

    #[test]
    fn edge_key_in_encode_decode() {
        let k = CanonicalEdgeKey { src_id: 100, label_id: 5, rank: 2, dst_id: 200 };
        let in_bytes = encode_edge_key_in(k);
        assert_eq!(in_bytes.len(), 20);
        assert_eq!(u64::from_be_bytes(in_bytes[0..8].try_into().unwrap()), 200u64);
        let decoded = decode_edge_key_in(&in_bytes).unwrap();
        assert_eq!(decoded, k);
    }

    #[test]
    fn edge_key_out_in_roundtrip() {
        let k = CanonicalEdgeKey { src_id: 1, label_id: 7, rank: 3, dst_id: 99 };
        assert_eq!(decode_edge_key_out(&encode_edge_key_out(k)).unwrap(), k);
        assert_eq!(decode_edge_key_in(&encode_edge_key_in(k)).unwrap(), k);
    }

    // ── VertexValue ───────────────────────────────────────────────────────────

    #[test]
    fn vertex_value_encode_decode() {
        let raw = vec![
            (SmolStr::new("name"), Primitive::String(SmolStr::new("Alice"))),
            (SmolStr::new("age"), Primitive::Int32(30)),
        ];
        let vv = VertexValue { label_id: 7, property_blob: encode_props(&raw) };
        let bytes = vv.encode();
        let dec = VertexValue::decode(&bytes).unwrap();
        assert_eq!(dec.label_id, 7);
        let props = decode_props(&dec.property_blob);
        assert_eq!(props.len(), 2);
        assert_eq!(props[0].0, SmolStr::new("name"));
        assert_eq!(props[0].1, Primitive::String(SmolStr::new("Alice")));
        assert_eq!(props[1].1, Primitive::Int32(30));
    }

    #[test]
    fn vertex_value_decode_bad_length() {
        assert!(VertexValue::decode(&[0u8; 1]).is_none());
        assert!(VertexValue::decode(&[]).is_none());
    }

    // ── Full roundtrips ───────────────────────────────────────────────────────

    #[test]
    fn full_vertex_roundtrip() {
        let raw = vec![
            (SmolStr::new("name"), Primitive::String(SmolStr::new("Bob"))),
            (SmolStr::new("score"), Primitive::Float64(9.9)),
        ];
        let key_bytes = encode_vertex_key(42);
        let val_bytes = VertexValue { label_id: 1, property_blob: encode_props(&raw) }.encode();
        let id = decode_vertex_key(&key_bytes).unwrap();
        let vv = VertexValue::decode(&val_bytes).unwrap();
        assert_eq!(id, 42);
        assert_eq!(vv.label_id, 1);
        let dec_props = decode_props(&vv.property_blob);
        let fv = make_vertex(id, vv.label_id, &dec_props);
        assert_eq!(fv.id, 42);
        assert_eq!(fv.label_id, 1);
        let props_guard = fv.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(props_guard.len(), 2);
        assert_eq!(props_guard[0].key, SmolStr::new("name"));
        assert_eq!(props_guard[0].owner, CanonicalKey::Vertex(42));
    }

    #[test]
    fn full_edge_roundtrip() {
        let cek = CanonicalEdgeKey { src_id: 10, label_id: 7, rank: 0, dst_id: 20 };
        let raw = vec![
            (SmolStr::new("weight"), Primitive::Float64(std::f64::consts::PI)),
            (SmolStr::new("tag"), Primitive::String(SmolStr::new("friend"))),
        ];
        let key_bytes = encode_edge_key_out(cek);
        let val_bytes = EdgeValue { property_blob: encode_props(&raw) }.encode().to_vec();
        let dec_cek = decode_edge_key_out(&key_bytes).unwrap();
        let ev = EdgeValue::decode(&val_bytes);
        assert_eq!(dec_cek, cek);
        let dec_props = decode_props(&ev.property_blob);
        let fe = make_edge(dec_cek, &dec_props);
        assert_eq!(fe.src_id, 10);
        assert_eq!(fe.dst_id, 20);
        assert_eq!(fe.label_id, 7);
        let props_guard = fe.props.write().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(props_guard[0].owner, CanonicalKey::Edge(cek));
        assert_eq!(props_guard[1].value, Primitive::String(SmolStr::new("friend")));
    }

    #[test]
    fn all_primitive_types_roundtrip() {
        let raw: Vec<(PropKey, Primitive)> = vec![
            (SmolStr::new("bool"), Primitive::Bool(true)),
            (SmolStr::new("i32"), Primitive::Int32(-100)),
            (SmolStr::new("i64"), Primitive::Int64(i64::MAX)),
            (SmolStr::new("f32"), Primitive::Float32(f32::MIN_POSITIVE)),
            (SmolStr::new("f64"), Primitive::Float64(f64::MIN_POSITIVE)),
            (SmolStr::new("str"), Primitive::String(SmolStr::new("hello"))),
            (SmolStr::new("uuid"), Primitive::Uuid(u128::MAX)),
            (SmolStr::new("null"), Primitive::Null),
        ];
        let blob = encode_props(&raw);
        let dec = decode_props(&blob);
        assert_eq!(dec.len(), 8);
        for (i, (k, v)) in dec.iter().enumerate() {
            assert_eq!(k, &raw[i].0);
            assert_eq!(v, &raw[i].1);
        }
    }

    #[test]
    fn property_owner_is_canonical_key() {
        let cek = CanonicalEdgeKey { src_id: 5, label_id: 1, rank: 0, dst_id: 6 };
        let fe = make_edge(cek, &[(SmolStr::new("w"), Primitive::Float32(0.5))]);
        let props_guard = fe.props.write().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(props_guard[0].owner, CanonicalKey::Edge(cek));

        let fv = make_vertex(99, 2, &[(SmolStr::new("x"), Primitive::Int32(7))]);
        let props_guard = fv.props.write().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(props_guard[0].owner, CanonicalKey::Vertex(99));
    }
}
