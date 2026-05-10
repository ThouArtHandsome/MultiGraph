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

// ── FullVertex ────────────────────────────────────────────────────────────────

/// The ground-truth vertex record crossing the store ↔ context boundary.
///
/// Returned by `GraphTransaction::get_vertex` and stored inside `GraphContext`'s
/// overlay.  The traversal engine accesses properties directly via
/// `ctx.vertex(idx)` without copying or dereferencing an extra wrapper.
/// There is no `Existence` field — the store never returns tombstoned elements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullVertex {
    pub id: VertexKey,
    pub label_id: LabelId,
    pub props: Vec<Property>,
}

// ── FullEdge ──────────────────────────────────────────────────────────────────

/// The ground-truth edge record crossing the store ↔ context boundary.
///
/// Always in canonical `Out` orientation.  The engine derives the directed
/// `EdgeKey` from `canonical_key()` plus the direction it requested.
/// 
/// TODO: haven't considered the property ordering implications of `Vec<Property>` yet.  If we need to support
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullEdge {
    pub src_id: VertexKey,
    pub label_id: LabelId,
    pub rank: Rank,
    pub dst_id: VertexKey,
    pub props: Vec<Property>,
}

impl FullEdge {
    /// Extract the direction-free canonical key (same as the `edges_out` CF key).
    pub fn canonical_key(&self) -> CanonicalEdgeKey {
        CanonicalEdgeKey { src_id: self.src_id, label_id: self.label_id, rank: self.rank, dst_id: self.dst_id }
    }
}
