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

use crate::types::gvalue::Property;
use crate::types::keys::{CanonicalEdgeKey, LabelId, Rank, VertexKey};
use std::sync::RwLock;

// ── FullVertex ────────────────────────────────────────────────────────────────

/// The ground-truth vertex record crossing the store ↔ context boundary.
///
/// Returned by `GraphTransaction::get_vertex` and stored inside `GraphContext`'s
/// overlay.  The traversal engine accesses properties directly via
/// `ctx.vertex(idx)` without copying or dereferencing an extra wrapper.
/// There is no `Existence` field — the store never returns tombstoned elements.
#[derive(Debug)]
pub struct FullVertex {
    pub id: VertexKey,
    pub label_id: LabelId,
    pub props: RwLock<Vec<Property>>, // Only this is mutable
}

// ── FullEdge ──────────────────────────────────────────────────────────────────

/// The ground-truth edge record crossing the store ↔ context boundary.
///
/// Always in canonical `Out` orientation.  The engine derives the directed
/// `EdgeKey` from `canonical_key()` plus the direction it requested.
///
/// TODO: haven't considered the property ordering implications of `Vec<Property>` yet.  If we need to support
#[derive(Debug)]
pub struct FullEdge {
    pub src_id: VertexKey,
    pub label_id: LabelId,
    pub rank: Rank,
    pub dst_id: VertexKey,
    pub props: RwLock<Vec<Property>>, // Only this is mutable
}

impl FullEdge {
    /// Extract the direction-free canonical key (same as the `edges_out` CF key).
    pub fn canonical_key(&self) -> CanonicalEdgeKey {
        CanonicalEdgeKey { src_id: self.src_id, label_id: self.label_id, rank: self.rank, dst_id: self.dst_id }
    }
}

impl PartialEq for FullVertex {
    fn eq(&self, other: &Self) -> bool {
        // Compare basic fields
        if self.id != other.id || self.label_id != other.label_id {
            return false;
        }

        // Lock both sides to compare properties
        let p1 = self.props.read().unwrap();
        let p2 = other.props.read().unwrap();
        *p1 == *p2
    }
}

impl Eq for FullVertex {}

impl PartialEq for FullEdge {
    fn eq(&self, other: &Self) -> bool {
        // Compare basic fields
        if self.src_id != other.src_id
            || self.label_id != other.label_id
            || self.rank != other.rank
            || self.dst_id != other.dst_id
        {
            return false;
        }

        // Lock both sides to compare properties
        let p1 = self.props.read().unwrap();
        let p2 = other.props.read().unwrap();
        *p1 == *p2
    }
}

impl Eq for FullEdge {}
