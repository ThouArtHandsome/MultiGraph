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

/// Unique identifier for a vertex.
pub type VertexKey = u64;

/// Numeric id for an edge-label, assigned by the schema registry (max 65 535 distinct labels).
pub type LabelId = u16;

/// Disambiguates parallel edges sharing the same (primary, label, secondary) triple
/// (max 65 535 per pair).
pub type Rank = u16;

// ── Direction ─────────────────────────────────────────────────────────────────

/// The traversal direction of an edge reference.
///
/// An edge exists once in the graph but can be viewed from either endpoint:
/// `OUT` means the reference is anchored at the source vertex,
/// `In` means it is anchored at the destination vertex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    OUT,
    IN,
}

// ── EdgeKey ───────────────────────────────────────────────────────────────────

/// A directed edge key: identity + traversal direction in one struct.
///
/// | `direction` | `primary_id` | `secondary_id` |
/// |-------------|--------------|----------------|
/// | `Out`       | src          | dst            |
/// | `In`        | dst          | src            |
///
/// Using `Out`-direction as the canonical form when a direction-independent
/// identity is needed (e.g. dirty-cache keys, read-buffer keys).
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

    /// `IN`-direction key for the same physical edge viewed from the destination.
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

    /// Return the canonical `Out`-direction form of this key.
    pub fn canonical(self) -> Self {
        match self.direction {
            Direction::OUT => self,
            Direction::IN => self.flip(),
        }
    }
}

// ── ElementKey ────────────────────────────────────────────────────────────────

/// Identifies any graph element unambiguously.
///
/// Used as the key in dirty-cache and read-buffer maps.  Edge keys should be
/// in canonical (`Out`) form for those maps.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ElementKey {
    Vertex(VertexKey),
    Edge(EdgeKey),
}
