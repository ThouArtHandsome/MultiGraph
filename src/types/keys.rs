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

use std::fmt::Display;

/// Unique identifier for a vertex.
pub type VertexKey = u64;

/// Numeric id for an edge label, mapped via the schema registry.
/// 12 bits are used semantically (max 4 096 distinct labels); stored as u16.
pub type LabelId = u16;

/// Disambiguates parallel edges sharing the same (src, label, dst) triple.
pub type Rank = u16;

// ── Direction ─────────────────────────────────────────────────────────────────

/// The traversal direction of an edge reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    OUT,
    IN,
}

// ── CanonicalEdgeKey ──────────────────────────────────────────────────────────

/// A direction-free edge identity in canonical `Out` orientation.
///
/// Used as the key type in the transaction's edge index and the dirty set.
/// Maps 1-to-1 with the `edges_out` CF key on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CanonicalEdgeKey {
    pub src_id: VertexKey,
    pub label_id: LabelId,
    pub rank: Rank,
    pub dst_id: VertexKey,
}

impl CanonicalEdgeKey {
    /// Build a directed `EdgeKey` for Out-direction traversal.
    pub fn out_key(self) -> EdgeKey {
        EdgeKey {
            primary_id: self.src_id,
            direction: Direction::OUT,
            label_id: self.label_id,
            secondary_id: self.dst_id,
            rank: self.rank,
        }
    }

    /// Build a directed `EdgeKey` for In-direction traversal.
    pub fn in_key(self) -> EdgeKey {
        EdgeKey {
            primary_id: self.dst_id,
            direction: Direction::IN,
            label_id: self.label_id,
            secondary_id: self.src_id,
            rank: self.rank,
        }
    }
}

impl Display for CanonicalEdgeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} -{}-> {})[rank={}]", self.src_id, self.label_id, self.dst_id, self.rank)
    }
}

// ── EdgeKey ───────────────────────────────────────────────────────────────────

/// A directed edge key carried by traversers.
///
/// `GValue::Edge` wraps an `EdgeKey` so that traversal direction (Out vs In)
/// is preserved for `path()` / `select()` identity.
/// Persistence always uses `CanonicalEdgeKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeKey {
    pub primary_id: VertexKey,
    pub direction: Direction,
    pub label_id: LabelId,
    pub secondary_id: VertexKey,
    pub rank: Rank,
}

impl EdgeKey {
    /// Canonical `Out`-direction key for `(src → dst)`.
    pub fn out_e(src: VertexKey, label: LabelId, dst: VertexKey, rank: Rank) -> Self {
        Self { primary_id: src, direction: Direction::OUT, label_id: label, secondary_id: dst, rank }
    }

    /// `IN`-direction key viewed from the destination.
    pub fn in_e(src: VertexKey, label: LabelId, dst: VertexKey, rank: Rank) -> Self {
        Self { primary_id: dst, direction: Direction::IN, label_id: label, secondary_id: src, rank }
    }

    /// Flip to the opposite direction (swaps `primary_id` ↔ `secondary_id`).
    pub fn flip(self) -> Self {
        Self {
            primary_id: self.secondary_id,
            direction: match self.direction {
                Direction::OUT => Direction::IN,
                Direction::IN => Direction::OUT,
            },
            label_id: self.label_id,
            secondary_id: self.primary_id,
            rank: self.rank,
        }
    }

    /// Return the canonical `Out`-direction form.
    pub fn canonical(self) -> Self {
        match self.direction {
            Direction::OUT => self,
            Direction::IN => self.flip(),
        }
    }

    /// Extract the direction-free `CanonicalEdgeKey`.
    pub fn canonical_edge_key(self) -> CanonicalEdgeKey {
        let out = self.canonical();
        CanonicalEdgeKey { src_id: out.primary_id, label_id: out.label_id, rank: out.rank, dst_id: out.secondary_id }
    }
}

// ── CanonicalKey ──────────────────────────────────────────────────────────────

/// Direction-free identity for any graph element.
///
/// Used in `Property.owner` and the transaction dirty set.  All variants are `Copy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanonicalKey {
    Vertex(VertexKey),
    Edge(CanonicalEdgeKey),
}
