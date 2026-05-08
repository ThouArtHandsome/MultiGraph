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

use std::sync::Arc;

use crate::types::{Direction, EdgeKey, FullEdge, FullVertex, LabelId, StorageError, VertexKey};

/// Read-only access to the graph store.
pub trait GraphReader {
    /// Fetch a single vertex by its key; returns `None` if not found.
    fn get_vertex(&self, key: VertexKey) -> Result<Option<Arc<FullVertex>>, StorageError>;

    /// Fetch multiple vertices by their keys; missing keys are silently omitted from the result.
    fn get_vertices(&self, keys: &[VertexKey]) -> Result<Vec<Arc<FullVertex>>, StorageError>;

    /// Fetch a single edge by its canonical (`Out`-direction) key.
    /// Returns `None` if not found.
    fn get_edge(&self, key: EdgeKey) -> Result<Option<Arc<FullEdge>>, StorageError>;

    /// Fetch all edges incident to `vertex` in the given `direction`.
    ///
    /// - `direction`: `OUT` reads outgoing edges (vertex is `src`); `IN` reads incoming edges (vertex is `dst`).
    /// - `label`: when `Some`, restrict to edges with that label id.
    /// - `dst`: when `Some`, restrict to edges whose remote endpoint is in the slice.
    fn get_edges(
        &self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<Arc<FullEdge>>, StorageError>;
}

/// Write access to the graph store.
pub trait GraphWriter {
    /// Persist a batch of vertices; any pre-existing vertex with the same key is overwritten.
    fn insert_vertices(&mut self, vertices: &[FullVertex]) -> Result<(), StorageError>;

    /// Persist a batch of edges; any pre-existing edge with the same key is overwritten.
    ///
    /// Implementors **must** write each logical edge in both traversal directions so
    /// that `get_edges` with `Direction::OUT` and `Direction::IN` both work without
    /// cross-direction lookups:
    ///
    /// - the canonical `OUT` form (primary = src) is written to the outgoing index.
    /// - the flipped `IN` form (primary = dst) is written to the incoming index.
    ///
    /// Callers supply edges in any direction; the implementation is responsible for
    /// canonicalising and deriving the reverse entry.
    fn insert_edges(&mut self, edges: &[FullEdge]) -> Result<(), StorageError>;

    /// Delete a vertex by key.  A no-op if the vertex does not exist.
    fn delete_vertex(&mut self, key: VertexKey) -> Result<(), StorageError>;

    /// Delete an edge by its canonical (`Out`-direction) key.
    /// Removes entries from both the outgoing and incoming indices.
    /// A no-op if the edge does not exist.
    fn delete_edge(&mut self, key: EdgeKey) -> Result<(), StorageError>;
}

/// Combined read + write access; blanket-implemented for any type that satisfies both.
pub trait GraphStorage: GraphReader + GraphWriter {}
