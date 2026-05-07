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

use crate::types::{Direction, FullEdge, FullVertex, LabelId, StorageError, VertexKey};

/// Read-only access to the graph store.
pub trait GraphReader {
    /// Fetch a single vertex by its key; returns `None` if not found.
    fn get_vertex(&self, key: VertexKey) -> Result<Option<Arc<FullVertex>>, StorageError>;

    /// Fetch multiple vertices by their keys; missing keys are silently omitted from the result.
    fn get_vertices(&self, keys: &[VertexKey]) -> Result<Vec<Arc<FullVertex>>, StorageError>;

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
    fn insert_edges(&mut self, edges: &[FullEdge]) -> Result<(), StorageError>;
}

/// Combined read + write access; blanket-implemented for any type that satisfies both.
pub trait GraphStorage: GraphReader + GraphWriter {}
