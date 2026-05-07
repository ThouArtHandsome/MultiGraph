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
//! | CF name      | Key type   | Value type   |
//! |--------------|------------|--------------|
//! | `vertices`   | `VertexKey`| `VertexValue`|
//! | `edges_out`  | `EdgeKey`  | `EdgeValue`  |
//! | `edges_in`   | `EdgeKey`  | `EdgeValue`  |
//!
//! Edge properties are duplicated across `edges_out` and `edges_in` so that
//! `inE()` traversals can read properties without a cross-CF lookup.
//!
//! # Key layouts
//!
//! ```text
//! VertexKey (8 B)  : [ VertexId:u64                                                 ]
//! EdgeKey   (21 B) : [ primary:u64 | dir:u8 | label:u16 | secondary:u64 | rank:u16  ]
//! ```
//!
//! The `dir` byte (`0x00` = Out, `0x01` = In) is stored in both CFs so that
//! the key format is self-describing and consistent for range-scan prefixes:
//!
//! | Prefix bytes                        | Len  | Enables                          |
//! |-------------------------------------|------|----------------------------------|
//! | `primary`                           | 8 B  | all incident edges (`bothE`)     |
//! | `primary \| dir`                    | 9 B  | all edges in one direction       |
//! | `primary \| dir \| label`           | 11 B | `outE(label)` / `inE(label)`     |
//! | `primary \| dir \| label \| secondary` | 19 B | existence / parallel-edge lookup |
//!
//! # Value layouts
//!
//! ```text
//! VertexValue : [ label_len:u16 | label:UTF-8 | property_blob ]
//! EdgeValue   : [ property_blob ]
//! ```

use crate::types::{Direction, EdgeKey, LabelId, Rank};

// ── Column-family name constants ──────────────────────────────────────────────

pub const CF_VERTICES: &str = "vertices";
pub const CF_EDGES_OUT: &str = "edges_out";
pub const CF_EDGES_IN: &str = "edges_in";

// ── Size constants ────────────────────────────────────────────────────────────

pub const VERTEX_KEY_SIZE: usize = 8;
pub const EDGE_KEY_SIZE: usize = 21; // 8 + 1 + 2 + 8 + 2

const DIR_OUT: u8 = 0x00;
const DIR_IN: u8 = 0x01;

// ── Direction byte helpers ────────────────────────────────────────────────────

fn direction_to_byte(d: Direction) -> u8 {
    match d {
        Direction::OUT => DIR_OUT,
        Direction::IN => DIR_IN,
    }
}

fn direction_from_byte(b: u8) -> Option<Direction> {
    match b {
        DIR_OUT => Some(Direction::OUT),
        DIR_IN => Some(Direction::IN),
        _ => None,
    }
}

// ── VertexKey ─────────────────────────────────────────────────────────────────

/// `[ VertexKey:u64 ]` — key for the `vertices` CF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VertexKey(pub u64);

impl VertexKey {
    pub fn encode(self) -> [u8; VERTEX_KEY_SIZE] {
        self.0.to_be_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let arr: [u8; 8] = bytes.try_into().ok()?;
        Some(Self(u64::from_be_bytes(arr)))
    }
}

// ── EdgeKey encoding ──────────────────────────────────────────────────────────
//
// Uses `types::EdgeKey` directly — no separate encoding-layer key struct.
//
// `edges_out` stores Out-direction keys (`primary = src`).
// `edges_in`  stores In-direction keys  (`primary = dst`).

/// Encode a `types::EdgeKey` to the 21-byte on-disk representation.
pub fn encode_edge_key(k: EdgeKey) -> [u8; EDGE_KEY_SIZE] {
    let mut buf = [0u8; EDGE_KEY_SIZE];
    buf[0..8].copy_from_slice(&k.primary_id.to_be_bytes());
    buf[8] = direction_to_byte(k.direction);
    buf[9..11].copy_from_slice(&k.label_id.to_be_bytes());
    buf[11..19].copy_from_slice(&k.secondary_id.to_be_bytes());
    buf[19..21].copy_from_slice(&k.rank.to_be_bytes());
    buf
}

/// Decode a 21-byte on-disk key into a `types::EdgeKey`.
pub fn decode_edge_key(bytes: &[u8]) -> Option<EdgeKey> {
    if bytes.len() < EDGE_KEY_SIZE {
        return None;
    }
    Some(EdgeKey {
        primary_id: u64::from_be_bytes(bytes[0..8].try_into().ok()?),
        direction: direction_from_byte(bytes[8])?,
        label_id: u16::from_be_bytes(bytes[9..11].try_into().ok()?) as LabelId,
        secondary_id: u64::from_be_bytes(bytes[11..19].try_into().ok()?),
        rank: u16::from_be_bytes(bytes[19..21].try_into().ok()?) as Rank,
    })
}

// ── VertexValue ───────────────────────────────────────────────────────────────

/// `[ label_len:u16 | label:UTF-8 | property_blob ]` — value in the `vertices` CF.
#[derive(Debug, Clone)]
pub struct VertexValue {
    pub label: String,
    /// Serialised property list — format determined by the codec layer.
    pub property_blob: Vec<u8>,
}

impl VertexValue {
    pub fn encode(&self) -> Vec<u8> {
        let label_bytes = self.label.as_bytes();
        let label_len = label_bytes.len() as u16;
        let mut buf = Vec::with_capacity(2 + label_bytes.len() + self.property_blob.len());
        buf.extend_from_slice(&label_len.to_be_bytes());
        buf.extend_from_slice(label_bytes);
        buf.extend_from_slice(&self.property_blob);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }
        let label_len = u16::from_be_bytes(bytes[0..2].try_into().ok()?) as usize;
        if bytes.len() < 2 + label_len {
            return None;
        }
        let label = std::str::from_utf8(&bytes[2..2 + label_len]).ok()?.to_owned();
        let property_blob = bytes[2 + label_len..].to_vec();
        Some(Self { label, property_blob })
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
    use std::sync::Arc;

    use smol_str::SmolStr;

    use super::{decode_edge_key, encode_edge_key, EdgeValue, VertexKey, VertexValue};
    use crate::types::{
        element::{Direction, EdgeKey},
        full::{FullEdge, FullElement, FullVertex},
        gvalue::{GValue, Primitive, Property},
        prop_key::PropKey,
    };

    // ── Minimal property blob codec ───────────────────────────────────────────
    //
    // Format: count:u16 | (key_len:u16 | key:UTF-8 | tag:u8 | value_bytes)*
    //
    // Tags: 0=Bool  1=Int32  2=Int64  3=Float32  4=Float64
    //       5=String(len:u16|UTF-8)  6=Uuid(16 B)  7=Null

    fn encode_props(props: &[(PropKey, Primitive)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(props.len() as u16).to_be_bytes());
        for (key, val) in props {
            let kb = key.0.as_bytes();
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
            let key = PropKey::new(std::str::from_utf8(&blob[pos..pos + klen]).unwrap());
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

    // ── FullElement builders using Arc::new_cyclic ────────────────────────────

    fn make_full_vertex(id: u64, label: &str, raw: &[(PropKey, Primitive)]) -> Arc<FullElement> {
        let label = SmolStr::new(label);
        let raw = raw.to_vec();
        Arc::new_cyclic(|weak| {
            let props = Arc::new(
                raw.iter()
                    .map(|(k, v)| Property { owner: weak.clone(), key: k.clone(), value: v.clone() })
                    .collect::<Vec<_>>(),
            );
            FullElement::Vertex(Arc::new(FullVertex { id, label, props }))
        })
    }

    fn make_full_edge(key: EdgeKey, raw: &[(PropKey, Primitive)]) -> Arc<FullElement> {
        let raw = raw.to_vec();
        Arc::new_cyclic(|weak| {
            let props = Arc::new(
                raw.iter()
                    .map(|(k, v)| Property { owner: weak.clone(), key: k.clone(), value: v.clone() })
                    .collect::<Vec<_>>(),
            );
            FullElement::Edge(Arc::new(FullEdge { key, props }))
        })
    }

    // ── VertexKey tests ───────────────────────────────────────────────────────

    #[test]
    fn vertex_key_encode_decode() {
        let original = VertexKey(42);
        let decoded = VertexKey::decode(&original.encode()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn vertex_key_decode_bad_length() {
        assert!(VertexKey::decode(&[0u8; 4]).is_none());
        assert!(VertexKey::decode(&[]).is_none());
    }

    // ── EdgeKey tests ─────────────────────────────────────────────────────────

    #[test]
    fn edge_key_encode_decode_out() {
        let k = EdgeKey::out_e(100, 3, 200, 0);
        assert_eq!(decode_edge_key(&encode_edge_key(k)).unwrap(), k);
        assert_eq!(decode_edge_key(&encode_edge_key(k)).unwrap().direction, Direction::OUT);
    }

    #[test]
    fn edge_key_encode_decode_in() {
        let k = EdgeKey::in_e(100, 5, 200, 2);
        assert_eq!(decode_edge_key(&encode_edge_key(k)).unwrap(), k);
        assert_eq!(decode_edge_key(&encode_edge_key(k)).unwrap().direction, Direction::IN);
    }

    #[test]
    fn edge_key_flip_roundtrip() {
        let out_k = EdgeKey::out_e(1, 2, 3, 4);
        let in_k = out_k.flip();
        assert_eq!(decode_edge_key(&encode_edge_key(out_k)).unwrap(), out_k);
        assert_eq!(decode_edge_key(&encode_edge_key(in_k)).unwrap(), in_k);
        // Flipping the decoded In key must recover the Out key.
        assert_eq!(decode_edge_key(&encode_edge_key(in_k)).unwrap().flip(), out_k);
    }

    // ── Full vertex pipeline: FullVertex → key+value bytes → FullVertex → GValue

    #[test]
    fn full_vertex_roundtrip_to_gvalue() {
        let vid: u64 = 42;
        let raw_props = vec![
            (PropKey::new("name"), Primitive::String(SmolStr::new("Alice"))),
            (PropKey::new("age"), Primitive::Int32(30)),
            (PropKey::new("score"), Primitive::Float64(9.5)),
            (PropKey::new("active"), Primitive::Bool(true)),
        ];

        // Encode to key bytes + value bytes.
        let key_bytes = VertexKey(vid).encode();
        let val_bytes = VertexValue { label: "person".into(), property_blob: encode_props(&raw_props) }.encode();

        // Decode back.
        let dec_key = VertexKey::decode(&key_bytes).unwrap();
        let dec_vv = VertexValue::decode(&val_bytes).unwrap();
        assert_eq!(dec_key.0, vid);
        assert_eq!(dec_vv.label, "person");

        let dec_props = decode_props(&dec_vv.property_blob);
        assert_eq!(dec_props.len(), raw_props.len());
        for (i, (k, v)) in dec_props.iter().enumerate() {
            assert_eq!(k, &raw_props[i].0);
            assert_eq!(v, &raw_props[i].1);
        }

        // Reconstruct FullVertex.
        let full_elem = make_full_vertex(dec_key.0, &dec_vv.label, &dec_props);
        let FullElement::Vertex(ref fv) = *full_elem else { panic!("expected Vertex") };
        assert_eq!(fv.id, vid);
        assert_eq!(fv.label.as_str(), "person");
        assert_eq!(fv.props.len(), 4);
        assert_eq!(fv.props[0].key, PropKey::new("name"));
        assert_eq!(fv.props[0].value, Primitive::String(SmolStr::new("Alice")));
        assert_eq!(fv.props[1].value, Primitive::Int32(30));

        // GValue::Vertex wrapping the full element.
        let gv = GValue::Vertex(Arc::clone(fv));
        let GValue::Vertex(ref f) = gv else { panic!("expected Vertex") };
        assert_eq!(f.id, vid);
        assert_eq!(f.props.len(), 4);
        assert_eq!(f.props[1].key, PropKey::new("age"));
        assert_eq!(f.props[1].value, Primitive::Int32(30));
    }

    // ── Full edge pipeline: FullEdge → key+value bytes → FullEdge → GValue ───

    #[test]
    fn full_edge_roundtrip_to_gvalue() {
        let edge_key = EdgeKey::out_e(10, 7, 20, 0);
        let raw_props = vec![
            (PropKey::new("weight"), Primitive::Float64(3.14)),
            (PropKey::new("created"), Primitive::Int64(1_720_000_000)),
            (PropKey::new("tag"), Primitive::String(SmolStr::new("friend"))),
            (PropKey::new("active"), Primitive::Bool(false)),
            (PropKey::new("score"), Primitive::Null),
        ];

        // Encode.
        let key_bytes = encode_edge_key(edge_key);
        let val_bytes = EdgeValue { property_blob: encode_props(&raw_props) }.encode().to_vec();

        // Decode.
        let dec_key = decode_edge_key(&key_bytes).unwrap();
        let dec_ev = EdgeValue::decode(&val_bytes);
        assert_eq!(dec_key, edge_key);

        let dec_props = decode_props(&dec_ev.property_blob);
        assert_eq!(dec_props.len(), raw_props.len());
        for (i, (k, v)) in dec_props.iter().enumerate() {
            assert_eq!(k, &raw_props[i].0);
            assert_eq!(v, &raw_props[i].1);
        }

        // Reconstruct FullEdge.
        let full_elem = make_full_edge(dec_key, &dec_props);
        let FullElement::Edge(ref fe) = *full_elem else { panic!("expected Edge") };
        assert_eq!(fe.key, edge_key);
        assert_eq!(fe.props.len(), 5);
        assert!(matches!(fe.props[0].value, Primitive::Float64(f) if f.to_bits() == 3.14_f64.to_bits()));
        assert!(matches!(fe.props[3].value, Primitive::Bool(false)));
        assert!(matches!(fe.props[4].value, Primitive::Null));

        // GValue::Edge wrapping the full element.
        let ge = GValue::Edge(Arc::clone(fe));
        let GValue::Edge(ref f) = ge else { panic!("expected Edge") };
        assert_eq!(f.key.primary_id, 10);
        assert_eq!(f.props.len(), 5);
        assert_eq!(f.props[2].key, PropKey::new("tag"));
        assert_eq!(f.props[2].value, Primitive::String(SmolStr::new("friend")));
    }

    // ── All Primitive variants through the full vertex pipeline ───────────────

    #[test]
    fn vertex_all_primitive_types() {
        let raw_props = vec![
            (PropKey::new("bool_val"), Primitive::Bool(true)),
            (PropKey::new("int32_val"), Primitive::Int32(-100)),
            (PropKey::new("int64_val"), Primitive::Int64(i64::MAX)),
            (PropKey::new("f32_val"), Primitive::Float32(f32::MIN_POSITIVE)),
            (PropKey::new("f64_val"), Primitive::Float64(f64::MIN_POSITIVE)),
            (PropKey::new("str_val"), Primitive::String(SmolStr::new("hello"))),
            (PropKey::new("uuid_val"), Primitive::Uuid(u128::MAX)),
            (PropKey::new("null_val"), Primitive::Null),
        ];

        let val_bytes = VertexValue { label: "all_types".into(), property_blob: encode_props(&raw_props) }.encode();

        let dec_vv = VertexValue::decode(&val_bytes).unwrap();
        let dec_props = decode_props(&dec_vv.property_blob);
        assert_eq!(dec_props.len(), 8);
        for (i, (k, v)) in dec_props.iter().enumerate() {
            assert_eq!(k, &raw_props[i].0);
            assert_eq!(v, &raw_props[i].1);
        }

        // Build GValue::Vertex wrapping the full element.
        let full_elem = make_full_vertex(1, "all_types", &dec_props);
        let FullElement::Vertex(ref fv) = *full_elem else { panic!() };
        let gv = GValue::Vertex(Arc::clone(fv));
        let GValue::Vertex(ref f) = gv else { panic!() };
        assert_eq!(f.props[0].value, Primitive::Bool(true));
        assert_eq!(f.props[1].value, Primitive::Int32(-100));
        assert_eq!(f.props[2].value, Primitive::Int64(i64::MAX));
        assert_eq!(f.props[6].value, Primitive::Uuid(u128::MAX));
        assert_eq!(f.props[7].value, Primitive::Null);
    }

    // ── Property owner back-pointer resolves to enclosing FullElement ─────────

    #[test]
    fn prop_owner_weak_back_pointer_vertex() {
        let raw = vec![(PropKey::new("x"), Primitive::Int32(7))];
        let full_elem = make_full_vertex(99, "node", &raw);
        let FullElement::Vertex(ref fv) = *full_elem else { panic!() };
        let owner = fv.props[0].owner.upgrade().expect("owner must still be alive");
        assert!(Arc::ptr_eq(&owner, &full_elem));
    }

    #[test]
    fn prop_owner_weak_back_pointer_edge() {
        let key = EdgeKey::out_e(5, 1, 6, 0);
        let raw = vec![(PropKey::new("w"), Primitive::Float32(0.5))];
        let full_elem = make_full_edge(key, &raw);
        let FullElement::Edge(ref fe) = *full_elem else { panic!() };
        let owner = fe.props[0].owner.upgrade().expect("owner must still be alive");
        assert!(Arc::ptr_eq(&owner, &full_elem));
    }
}
