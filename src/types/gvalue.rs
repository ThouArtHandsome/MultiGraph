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

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use indexmap::IndexMap;
use smol_str::SmolStr;

use crate::types::keys::{CanonicalKey, EdgeKey, VertexKey};
use crate::types::prop_key::PropKey;

// ── Primitive ────────────────────────────────────────────────────────────────

/// A scalar value that can appear as a property value or standalone scalar.
#[derive(Debug, Clone)]
pub enum Primitive {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(SmolStr),
    Uuid(u128),
    Null,
}

impl PartialEq for Primitive {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int32(a), Self::Int32(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float32(a), Self::Float32(b)) => a.to_bits() == b.to_bits(),
            (Self::Float64(a), Self::Float64(b)) => a.to_bits() == b.to_bits(),
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Primitive {}

impl Hash for Primitive {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Bool(v) => v.hash(state),
            Self::Int32(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            Self::Float32(v) => v.to_bits().hash(state),
            Self::Float64(v) => v.to_bits().hash(state),
            Self::String(v) => v.hash(state),
            Self::Uuid(v) => v.hash(state),
            Self::Null => {}
        }
    }
}

// ── Property ─────────────────────────────────────────────────────────────────

/// A single property value together with its owning element.
///
/// `owner` identifies the vertex or edge this property belongs to.  The engine
/// uses `owner` to call mutation methods on the transaction (e.g. for `drop()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property {
    pub owner: CanonicalKey,
    pub key: PropKey,
    pub value: Primitive,
}

impl Hash for Property {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.owner.hash(state);
        self.key.hash(state);
        self.value.hash(state);
    }
}

// ── GValue ───────────────────────────────────────────────────────────────────

/// The universal in-memory value type flowing through a traversal pipeline.
///
/// `Vertex` wraps a `VertexKey`; `Edge` wraps an `EdgeKey` (direction-aware).
/// The engine calls `ctx.get_vertex(key)` / `ctx.get_edges(…)` to obtain
/// `&FullVertex` / `&FullEdge` references when it needs property data.
///
/// Both key types are `Copy` (8 / 24 bytes), so `GValue` is cheap to clone.
#[derive(Debug, Clone)]
pub enum GValue {
    /// A vertex identified by its store key.
    Vertex(VertexKey),
    /// A directed edge.  The `EdgeKey` preserves traversal direction (Out / In)
    /// for `path()` / `select()` identity.
    Edge(EdgeKey),
    /// A property travelling through the pipeline as a standalone element.
    Property(Property),
    Scalar(Primitive),
    List(Arc<Vec<GValue>>),
    Map(Arc<IndexMap<GValue, GValue>>),
    Path(Arc<Vec<GValue>>),
}

impl PartialEq for GValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Vertex(a), Self::Vertex(b)) => a == b,
            (Self::Edge(a), Self::Edge(b)) => a == b,
            (Self::Property(a), Self::Property(b)) => a == b,
            (Self::Scalar(a), Self::Scalar(b)) => a == b,
            (Self::List(a), Self::List(b)) => a == b,
            (Self::Map(a), Self::Map(b)) => a == b,
            (Self::Path(a), Self::Path(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for GValue {}

impl Hash for GValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Vertex(key) => key.hash(state),
            Self::Edge(key) => key.hash(state),
            Self::Property(p) => p.hash(state),
            Self::Scalar(p) => p.hash(state),
            Self::List(list) => {
                list.len().hash(state);
                for item in list.iter() {
                    item.hash(state);
                }
            }
            Self::Map(map) => {
                map.len().hash(state);
                for (k, v) in map.iter() {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Self::Path(path) => {
                path.len().hash(state);
                for item in path.iter() {
                    item.hash(state);
                }
            }
        }
    }
}
