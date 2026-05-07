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

use std::{
    hash::{Hash, Hasher},
    sync::{Arc, Weak},
};

use indexmap::IndexMap;
use smol_str::SmolStr;

use crate::types::{
    full::{FullEdge, FullElement, FullVertex},
    prop_key::PropKey,
};

// ── Primitive ────────────────────────────────────────────────────────────────

/// A scalar value that can appear as a property value or standalone scalar.
///
/// `f32`/`f64` do not implement `Eq`/`Hash` in std, so we implement them
/// manually using bitwise comparison (`to_bits()`).  Two NaN values with
/// identical bits compare equal — intentional for map-key use.
///
/// UUIDs are stored as raw `u128` to avoid a crate dependency.
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
/// Stored in `Vec<Property>` inside `FullVertex`/`FullEdge`.
///
/// `owner` is a `Weak<FullElement>` back-pointer so the property can reach
/// the ground-truth element without a cache look-up.  `Weak` (not `Arc`)
/// breaks the reference cycle: `Arc<FullElement>` → `Arc<FullVertex/FullEdge>`
/// → `Vec<Property>` → `Property` → back to `Arc<FullElement>`.
/// Construct via `Arc::new_cyclic`.
#[derive(Debug, Clone)]
pub struct Property {
    pub owner: Weak<FullElement>,
    pub key: PropKey,
    pub value: Primitive,
}

impl PartialEq for Property {
    fn eq(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.owner, &other.owner) && self.key == other.key && self.value == other.value
    }
}

impl Eq for Property {}

impl Hash for Property {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.owner.as_ptr().hash(state);
        self.key.hash(state);
        self.value.hash(state);
    }
}

// ── GValue ───────────────────────────────────────────────────────────────────

/// The universal in-memory value type flowing through a traversal pipeline.
///
/// `Vertex` and `Edge` hold a direct `Arc` to the authoritative cache entry,
/// giving zero-copy access to identity, label, and all properties.
///
/// `List` and `Map` are reference-counted for cheap clone.
///
/// `Eq` and `Hash` are implemented manually:
/// - `Vertex` uses `FullVertex::id` for identity; `Edge` uses `FullEdge::key`.
/// - Float variants in `Scalar`/`Property` use bit-pattern comparison.
#[derive(Debug, Clone)]
pub enum GValue {
    Vertex(Arc<FullVertex>),
    Edge(Arc<FullEdge>),
    /// A property value travelling through the pipeline as a standalone element.
    Property(Property),
    Scalar(Primitive),
    List(Arc<Vec<GValue>>),
    Map(Arc<IndexMap<GValue, GValue>>),
    Path(Arc<Vec<GValue>>),
}

impl PartialEq for GValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Vertex(a), Self::Vertex(b)) => a.id == b.id,
            (Self::Edge(a), Self::Edge(b)) => a.key == b.key,
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
            Self::Vertex(fv) => fv.id.hash(state),
            Self::Edge(fe) => fe.key.hash(state),
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
