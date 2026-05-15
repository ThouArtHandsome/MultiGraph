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

use crate::{
    graph::LogicalGraph,
    store::traits::GraphStore,
    types::{
        element::{Edge, Vertex},
        keys::VertexKey,
        EdgeKey, StoreError,
    },
};

/// Graph access interface passed through every `BasicStep::next` call.
///
/// Steps receive a `&mut dyn GraphCtx` rather than holding a stored reference,
/// which avoids `Rc<RefCell<…>>` and gives compile-time borrow guarantees.
/// Methods will be added here as step implementations are fleshed out
/// (e.g. `get_out_edges`, `get_property`).
pub trait GraphCtx {
    fn get_vertex(&mut self, key: VertexKey) -> Result<Option<Arc<Vertex>>, StoreError>;
    fn get_edge(&mut self, key: EdgeKey) -> Result<Option<Arc<Edge>>, StoreError>;
    fn get_outs(&mut self, vertex_key: VertexKey) -> Result<Vec<VertexKey>, StoreError>;
    fn get_out_edges(&mut self, vertex_key: VertexKey) -> Result<Vec<EdgeKey>, StoreError>;
    fn get_ins(&mut self, vertex_key: VertexKey) -> Result<Vec<VertexKey>, StoreError>;
    fn get_in_edges(&mut self, vertex_key: VertexKey) -> Result<Vec<EdgeKey>, StoreError>;
}

/// Zero-cost context used in unit tests where no real graph is needed.
pub struct NoopCtx;
impl GraphCtx for NoopCtx {
    fn get_vertex(&mut self, _key: VertexKey) -> Result<Option<Arc<Vertex>>, StoreError> {
        Ok(None)
    }
    fn get_edge(&mut self, _key: EdgeKey) -> Result<Option<Arc<Edge>>, StoreError> {
        Ok(None)
    }
    fn get_outs(&mut self, _vertex_key: VertexKey) -> Result<Vec<VertexKey>, StoreError> {
        Ok(vec![])
    }
    fn get_out_edges(&mut self, _vertex_key: VertexKey) -> Result<Vec<EdgeKey>, StoreError> {
        Ok(vec![])
    }
    fn get_ins(&mut self, _vertex_key: VertexKey) -> Result<Vec<VertexKey>, StoreError> {
        Ok(vec![])
    }
    fn get_in_edges(&mut self, _vertex_key: VertexKey) -> Result<Vec<EdgeKey>, StoreError> {
        Ok(vec![])
    }
}

impl<S: GraphStore> GraphCtx for LogicalGraph<S> {
    fn get_vertex(&mut self, key: VertexKey) -> Result<Option<Arc<Vertex>>, StoreError> {
        LogicalGraph::get_vertex(self, key)
    }
    fn get_edge(&mut self, key: EdgeKey) -> Result<Option<Arc<Edge>>, StoreError> {
        LogicalGraph::get_edge(self, key.canonical_edge_key())
    }
    fn get_outs(&mut self, vertex_key: VertexKey) -> Result<Vec<VertexKey>, StoreError> {
        let edges = self.get_edges(vertex_key, crate::types::Direction::OUT, None, None)?;
        Ok(edges.into_iter().map(|(ek, _)| ek.secondary_id).collect())
    }
    fn get_out_edges(&mut self, vertex_key: VertexKey) -> Result<Vec<EdgeKey>, StoreError> {
        let edges = self.get_edges(vertex_key, crate::types::Direction::OUT, None, None)?;
        Ok(edges.into_iter().map(|(ek, _)| ek).collect())
    }
    fn get_ins(&mut self, vertex_key: VertexKey) -> Result<Vec<VertexKey>, StoreError> {
        let edges = self.get_edges(vertex_key, crate::types::Direction::IN, None, None)?;
        Ok(edges.into_iter().map(|(ek, _)| ek.secondary_id).collect())
    }
    fn get_in_edges(&mut self, vertex_key: VertexKey) -> Result<Vec<EdgeKey>, StoreError> {
        let edges = self.get_edges(vertex_key, crate::types::Direction::IN, None, None)?;
        Ok(edges.into_iter().map(|(ek, _)| ek).collect())
    }
}
