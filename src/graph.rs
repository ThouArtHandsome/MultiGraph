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

//! Query-scoped logical graph — the ground truth for a single traversal.
//! # Role:
//! `LogicalGraph<S>` sits between the Gremlin traversal engine and the
//! persistent `GraphStore`.  The engine never touches the store directly;
//! it only ever calls methods on `LogicalGraph`.
//!
//!
//! ```text
//! Traversal Engine
//!   │  ctx.get_vertex(key)  → Result<Option<Arc<Vertex>>, StoreError>
//!   │  ctx.add_vertex(id, lbl) → Result<(VertexKey, Arc<Vertex>), StoreError>
//!   │  ctx.get_edges(…)     → Result<Vec<(EdgeKey, Arc<Edge>)>, StoreError>
//!   │  ctx.set_property(…)  → Result<(), StoreError>
//!   │  ctx.commit()
//!   ▼
//! LogicalGraph<S: GraphStore>
//!   vertices:      HashMap<VertexKey, Arc<Vertex>>    ← query-scoped overlay
//!   edges:         HashMap<CanonicalEdgeKey, Arc<Edge>>
//!   vertex_degree: HashMap<VertexKey, (u32, u32)>     ← degree tracking (out, in)
//!   dirty:         HashMap<CanonicalKey, Existence>
//!   store:         S::Txn                             ← flush-on-commit
//!   ▼
//! S::Txn: GraphTransaction         ← RocksDB / Distributed / Mock
//! ```
//!
//! # Read path
//!
//! On first access, `get_vertex` checks the local map.  If absent it calls
//! `store.get_vertex`, inserts the result, and returns an `Arc<Vertex>`.
//! Subsequent accesses in the same query are O(1) map lookups.
//!
//! # Write path
//!
//! Mutations update the in-memory overlay and mark the element `dirty`.  The
//! store is never written until `commit()`.  This means the engine sees its
//! own writes immediately (read-your-writes), regardless of store backend.
//!
//! # Commit
//!
//! `commit()` iterates `dirty` and calls `store.put_*` / `store.delete_*` for
//! each element, then calls `store.commit()`. The overlay is cleared so the
//! `LogicalGraph` can be reused for a retry on OCC conflict.
//!
//! # Graph Consistency
//!
//! `LogicalGraph` is solely responsible for graph-level integrity, while the
//! store layer acts as a dumb physical backend. It enforces invariants such as:
//! - Bidirectional edges: Committing an edge always emits writes for both `OUT` and `IN` indices.
//! - Dangling prevention: Creating an edge strictly verifies the existence of both vertices.
//! - Degree validation: A vertex cannot be dropped if its incident edge counts are non-zero.
//!
//! # In-place mutation
//!
//! Clean elements loaded from the store hold an `Arc<Vertex>` or `Arc<Edge>`.
//! Mutations acquire a write lock on the `RwLock` wrapping the properties and
//! modify them in place.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
};

use crate::{
    store::traits::{GraphStore, GraphTransaction},
    traversal::context::GraphCtx,
    types::{
        element::{Edge, Vertex},
        gvalue::{Primitive, Property},
        keys::{CanonicalEdgeKey, CanonicalKey, Direction, EdgeKey, LabelId, VertexKey},
        prop_key::PropKey,
        StoreError,
    },
};

// ── Existence ────────────────────────────────────────────────────────────────
//
/// Mutation kind for a dirty graph element within a `LogicalGraph`.
///
/// Only dirty elements appear in the `dirty` map; absence means `Clean`.
/// TODO:
///     1. how to handle delete -> add on the same element within a single query?
///     This is currently treated as `New`, but we might want to distinguish it
///     from a pure create for better conflict detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Existence {
    /// Props were mutated on an existing element.
    Modified,
    /// Only the vertex edge counts changed.
    CounterOnly,
    /// Props and vertex edge counts both changed.
    ModifiedWithCounter,
    /// Created in this query; not yet persisted.
    New,
    /// Deleted in this query.
    Tombstone,
}

impl Existence {
    fn merge(self, other: Existence) -> Existence {
        use Existence::*;
        match (self, other) {
            (Tombstone, _) | (_, Tombstone) => Tombstone,
            (New, _) | (_, New) => New,
            (ModifiedWithCounter, _) | (_, ModifiedWithCounter) => ModifiedWithCounter,
            (Modified, CounterOnly) | (CounterOnly, Modified) => ModifiedWithCounter,
            (Modified, Modified) => Modified,
            (CounterOnly, CounterOnly) => CounterOnly,
        }
    }
}

// ── LogicalGraph ──────────────────────────────────────────────────────────────
/// Query-scoped logical graph wrapping a store transaction.
///
/// Obtained by calling `LogicalGraph::new(store.begin())`. The engine uses this
/// as its sole interface to the graph.
pub struct LogicalGraph<S: GraphStore> {
    store: S::Txn,
    vertices: HashMap<VertexKey, Arc<Vertex>>,
    edges: HashMap<CanonicalEdgeKey, Arc<Edge>>,
    vertex_degree: HashMap<VertexKey, (u32, u32)>,
    dirty: HashMap<CanonicalKey, Existence>,
}

impl<S: GraphStore> LogicalGraph<S> {
    /// Create a new logical graph context wrapping the given transaction.
    pub fn new(store: S::Txn) -> Self {
        Self {
            store,
            vertices: HashMap::new(),
            edges: HashMap::new(),
            vertex_degree: HashMap::new(),
            dirty: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn get_vertex_degree_for_test(&mut self, key: VertexKey) -> Result<Option<(u32, u32)>, StoreError> {
        self.get_vertex_degree(key)
    }

    fn get_vertex_degree(&mut self, key: VertexKey) -> Result<Option<(u32, u32)>, StoreError> {
        if let Some(counts) = self.vertex_degree.get(&key) {
            return Ok(Some(*counts));
        }
        self.store.get_vertex_degree(key)?.map_or(Ok(None), |counts| {
            self.vertex_degree.insert(key, counts);
            Ok(Some(counts))
        })
    }

    fn mark_dirty(&mut self, key: CanonicalKey, state: Existence) {
        match self.dirty.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(state);
            }
            Entry::Occupied(mut entry) => {
                let combined = entry.get().merge(state);
                *entry.get_mut() = combined;
            }
        }
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Look up a vertex by key, loading from the store on first access.
    ///
    /// Returns `None` for absent or tombstoned vertices.
    // TODO:
    //      1. Consider adding a batch `get_vertices` method for bulk property retrieval.
    //      Currently, `get_vertex` serves dual purposes: fetching property data and checking
    //      for existence. A batch API would improve data fetching performance, but requires careful
    //      design to comfortably handle partial results where some keys might be missing.
    pub fn get_vertex(&mut self, key: VertexKey) -> Result<Option<Arc<Vertex>>, StoreError> {
        if !self.vertices.contains_key(&key) {
            match self.store.get_vertex(key)? {
                None => return Ok(None),
                Some(arc) => {
                    self.vertices.insert(key, arc);
                }
            }
        }
        if self.dirty.get(&CanonicalKey::Vertex(key)) == Some(&Existence::Tombstone) {
            return Ok(None);
        }
        Ok(self.vertices.get(&key).cloned())
    }

    /// Look up an edge by canonical key, loading from the store on first access.
    ///
    /// Returns `None` for absent or tombstoned edges.
    pub fn get_edge(&mut self, key: CanonicalEdgeKey) -> Result<Option<Arc<Edge>>, StoreError> {
        if !self.edges.contains_key(&key) {
            // Load the primary physical record (OUT) to populate the canonical edge.
            match self.store.get_edge(key, Direction::OUT)? {
                None => return Ok(None),
                Some(arc) => {
                    self.edges.insert(key, arc);
                }
            }
        }
        if self.dirty.get(&CanonicalKey::Edge(key)) == Some(&Existence::Tombstone) {
            return Ok(None);
        }
        Ok(self.edges.get(&key).cloned())
    }

    /// Scan edges incident to `vertex` in `direction`, merging committed data
    /// with the in-memory dirty overlay.  Tombstoned edges are filtered out.
    ///
    /// Returns `(EdgeKey, &Edge)` pairs — `EdgeKey` carries traversal direction.
    pub fn get_edges(
        &mut self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<(EdgeKey, Arc<Edge>)>, StoreError> {
        // Phase 1: populate overlay from store (mutable).
        let committed = self.store.get_edges(vertex, direction, label, dst)?;
        for arc in committed {
            let cek = arc.canonical_key();
            self.edges.entry(cek).or_insert(arc);
        }
        // Phase 2: collect from overlay (immutable, returns refs into self.edges).
        let dirty = &self.dirty;
        let mut result = Vec::new();
        for (&cek, arc) in &self.edges {
            if dirty.get(&CanonicalKey::Edge(cek)) == Some(&Existence::Tombstone) {
                continue;
            }
            if !edge_matches(arc, vertex, direction, label, dst) {
                continue;
            }
            let physical_key = match direction {
                Direction::OUT => cek.out_key(),
                Direction::IN => cek.in_key(),
            };
            result.push((physical_key, arc.clone()));
        }
        Ok(result)
    }

    // ── Mutations ─────────────────────────────────────────────────────────────

    /// Add a new vertex with explicit `id` and `label_id` to the overlay.
    ///
    /// Returns `Result<(VertexKey, Arc<Vertex>), StoreError>` — the returned
    /// Arc gives immediate read access within this context.
    /// Note:
    ///     1. the vertex existence check is performed against both the overlay and the store to prevent duplicates.
    pub fn add_vertex(&mut self, id: VertexKey, label_id: LabelId) -> Result<(VertexKey, Arc<Vertex>), StoreError> {
        if self.vertices.contains_key(&id) {
            return Err(StoreError::DuplicateVertex(id));
        }
        // Use get_vertex_degree for a more efficient existence check.
        if self.store.get_vertex_degree(id)?.is_some() {
            return Err(StoreError::DuplicateVertex(id));
        }

        let vertex = Arc::new(Vertex { id, label_id, props: RwLock::new(Vec::new()) });
        self.vertices.insert(id, vertex.clone());
        self.vertex_degree.insert(id, (0, 0));
        self.dirty.insert(CanonicalKey::Vertex(id), Existence::New);
        Ok((id, vertex))
    }

    /// Register a new directed edge identified by `cek`.
    ///
    /// Returns `(EdgeKey, Arc<Edge>)` in Out orientation.
    /// Returns `StoreError::DuplicateEdge` if the key already exists in the overlay or store.
    pub fn add_edge(&mut self, cek: CanonicalEdgeKey) -> Result<(EdgeKey, Arc<Edge>), StoreError> {
        if self.edges.contains_key(&cek) {
            return Err(StoreError::DuplicateEdge(cek));
        }
        // 1. try to retrieve edge from store to check for duplicates before allocation.
        if self.store.get_edge(cek, Direction::OUT)?.is_some() {
            return Err(StoreError::DuplicateEdge(cek));
        }

        // Ensure vertices exist and update their edge counters
        let (mut src_out, src_in) = self.get_vertex_degree(cek.src_id)?.ok_or(StoreError::NotFound)?;
        let (dst_out, mut dst_in) = self.get_vertex_degree(cek.dst_id)?.ok_or(StoreError::NotFound)?;

        src_out += 1;
        dst_in += 1;

        self.vertex_degree.insert(cek.src_id, (src_out, src_in));
        self.mark_dirty(CanonicalKey::Vertex(cek.src_id), Existence::CounterOnly);

        self.vertex_degree.insert(cek.dst_id, (dst_out, dst_in));
        self.mark_dirty(CanonicalKey::Vertex(cek.dst_id), Existence::CounterOnly);

        // 2. insert new edge into overlay and mark dirty.  The store is not touched until commit.
        self.edges.insert(
            cek,
            Arc::new(Edge {
                src_id: cek.src_id,
                label_id: cek.label_id,
                rank: cek.rank,
                dst_id: cek.dst_id,
                props: RwLock::new(Vec::new()),
            }),
        );
        self.dirty.insert(CanonicalKey::Edge(cek), Existence::New);
        Ok((cek.out_key(), self.edges[&cek].clone()))
    }

    /// Upsert a property on a vertex or edge.
    ///
    /// The element identified by `key` must have been previously loaded or
    /// created in this context.  Returns an error for unknown or tombstoned
    /// elements.
    pub fn set_property(&mut self, key: CanonicalKey, prop: PropKey, value: Primitive) -> Result<(), StoreError> {
        match key {
            CanonicalKey::Vertex(id) => {
                if self.dirty.get(&key) == Some(&Existence::Tombstone) {
                    return Err(StoreError::Tombstoned);
                }
                match self.vertices.get_mut(&id) {
                    None => return Err(StoreError::NotFound),
                    Some(arc) => {
                        // 1. Acquire a write lock on the properties
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;

                        // 2. Modify in place. No cloning happens!
                        upsert_prop(&mut props, key, prop, value);
                        // upsert_prop(&mut Arc::make_mut(arc).props, key, prop, value),
                    }
                }
                self.mark_dirty(key, Existence::Modified);
            }
            CanonicalKey::Edge(cek) => {
                if self.dirty.get(&key) == Some(&Existence::Tombstone) {
                    return Err(StoreError::Tombstoned);
                }
                match self.edges.get_mut(&cek) {
                    None => return Err(StoreError::NotFound),
                    Some(arc) => {
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;
                        upsert_prop(&mut props, key, prop, value);
                    }
                }
                self.mark_dirty(key, Existence::Modified);
            }
        }
        Ok(())
    }

    /// Remove a property from a vertex or edge.
    pub fn drop_property(&mut self, key: CanonicalKey, prop: &PropKey) -> Result<(), StoreError> {
        match key {
            CanonicalKey::Vertex(id) => {
                if self.dirty.get(&key) == Some(&Existence::Tombstone) {
                    return Err(StoreError::Tombstoned);
                }
                match self.vertices.get_mut(&id) {
                    None => return Err(StoreError::NotFound),
                    Some(arc) => {
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;
                        props.retain(|p| &p.key != prop);
                    }
                }
                self.mark_dirty(key, Existence::Modified);
            }
            CanonicalKey::Edge(cek) => {
                if self.dirty.get(&key) == Some(&Existence::Tombstone) {
                    return Err(StoreError::Tombstoned);
                }
                match self.edges.get_mut(&cek) {
                    None => return Err(StoreError::NotFound),
                    Some(arc) => {
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;
                        props.retain(|p| &p.key != prop);
                    }
                }
                self.mark_dirty(key, Existence::Modified);
            }
        }
        Ok(())
    }

    /// Mark a vertex or edge as deleted.
    ///
    /// Note:
    ///     1.The element must have been previously loaded or created in this context.
    pub fn drop_element(&mut self, key: CanonicalKey) -> Result<(), StoreError> {
        match key {
            CanonicalKey::Vertex(id) => {
                let (out_e, in_e) = self.get_vertex_degree(id)?.ok_or(StoreError::NotFound)?;
                if out_e > 0 || in_e > 0 {
                    return Err(StoreError::IncidentEdges);
                }
                self.dirty.insert(key, Existence::Tombstone);
            }
            CanonicalKey::Edge(cek) => {
                if !self.edges.contains_key(&cek) {
                    return Err(StoreError::NotFound);
                }
                if self.dirty.get(&key) != Some(&Existence::Tombstone) {
                    self.dirty.insert(key, Existence::Tombstone);
                    if let Some((mut out_e, in_e)) = self.get_vertex_degree(cek.src_id)? {
                        out_e = out_e.saturating_sub(1);
                        self.vertex_degree.insert(cek.src_id, (out_e, in_e));
                        self.mark_dirty(CanonicalKey::Vertex(cek.src_id), Existence::CounterOnly);
                    }
                    if let Some((out_e, mut in_e)) = self.get_vertex_degree(cek.dst_id)? {
                        in_e = in_e.saturating_sub(1);
                        self.vertex_degree.insert(cek.dst_id, (out_e, in_e));
                        self.mark_dirty(CanonicalKey::Vertex(cek.dst_id), Existence::CounterOnly);
                    }
                }
            }
        }
        Ok(())
    }

    // ── Transaction control ───────────────────────────────────────────────────

    /// Flush all dirty mutations to the store and commit atomically.
    ///
    /// On `StoreError::Conflict` the overlay is cleared so the context can be
    /// reused; the caller must rebuild traversal state from scratch.
    pub fn commit(&mut self) -> Result<(), StoreError> {
        // Collect first so the loop body can borrow self.vertices / self.edges
        // and self.store simultaneously without a conflict on self.dirty.
        let dirty: Vec<(CanonicalKey, Existence)> = self.dirty.iter().map(|(&k, &v)| (k, v)).collect();
        for (key, existence) in dirty {
            match (key, existence) {
                (CanonicalKey::Vertex(id), Existence::New) => {
                    let v = self.vertices.get(&id).expect("dirty vertex key not in vertices");
                    let props_guard = v.props.read().map_err(|_| StoreError::LockError)?;
                    self.store.put_vertex(id, v.label_id, &props_guard)?;
                    let (out_e, in_e) = self.vertex_degree[&id];
                    self.store.put_vertex_degree(id, out_e, in_e)?;
                }
                (CanonicalKey::Vertex(id), Existence::Modified) => {
                    let v = self.vertices.get(&id).expect("dirty vertex key not in vertices");
                    let props_guard = v.props.read().map_err(|_| StoreError::LockError)?;
                    self.store.put_vertex(id, v.label_id, &props_guard)?;
                }
                (CanonicalKey::Vertex(id), Existence::CounterOnly) => {
                    let (out_e, in_e) = self.vertex_degree[&id];
                    self.store.put_vertex_degree(id, out_e, in_e)?;
                }
                (CanonicalKey::Vertex(id), Existence::ModifiedWithCounter) => {
                    let v = self.vertices.get(&id).expect("dirty vertex key not in vertices");
                    let props_guard = v.props.read().map_err(|_| StoreError::LockError)?;
                    self.store.put_vertex(id, v.label_id, &props_guard)?;
                    let (out_e, in_e) = self.vertex_degree[&id];
                    self.store.put_vertex_degree(id, out_e, in_e)?;
                }
                (CanonicalKey::Vertex(id), Existence::Tombstone) => {
                    self.store.delete_vertex(id)?;
                    self.store.delete_vertex_degree(id)?;
                }
                (
                    CanonicalKey::Edge(cek),
                    Existence::New | Existence::Modified | Existence::CounterOnly | Existence::ModifiedWithCounter,
                ) => {
                    let e = self.edges.get(&cek).expect("dirty edge key not in edges");
                    let props_guard = e.props.read().map_err(|_| StoreError::LockError)?;
                    self.store.put_edge(cek, Direction::OUT, &props_guard)?;
                    self.store.put_edge(cek, Direction::IN, &props_guard)?;
                }
                (CanonicalKey::Edge(cek), Existence::Tombstone) => {
                    self.store.delete_edge(cek, Direction::OUT)?;
                    self.store.delete_edge(cek, Direction::IN)?;
                }
            }
        }
        self.store.commit()?;
        self.reset();
        Ok(())
    }

    /// Discard all pending mutations and reset the context.
    pub fn abort(&mut self) {
        self.store.abort();
        self.reset();
    }

    fn reset(&mut self) {
        self.dirty.clear();
        self.vertices.clear();
        self.edges.clear();
        self.vertex_degree.clear();
    }
}

impl<S: GraphStore> GraphCtx for LogicalGraph<S> {}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn upsert_prop(props: &mut Vec<Property>, owner: CanonicalKey, key: PropKey, value: Primitive) {
    if let Some(p) = props.iter_mut().find(|p| p.key == key) {
        p.value = value;
    } else {
        props.push(Property { owner, key, value });
    }
}

fn edge_matches(
    view: &Edge,
    vertex: VertexKey,
    direction: Direction,
    label: Option<LabelId>,
    dst: Option<&[VertexKey]>,
) -> bool {
    let primary = match direction {
        Direction::OUT => view.src_id,
        Direction::IN => view.dst_id,
    };
    if primary != vertex {
        return false;
    }
    if let Some(lbl) = label {
        if view.label_id != lbl {
            return false;
        }
    }
    if let Some(slice) = dst {
        let remote = match direction {
            Direction::OUT => view.dst_id,
            Direction::IN => view.src_id,
        };
        if !slice.contains(&remote) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use smol_str::SmolStr;

    use super::LogicalGraph;
    use crate::store::traits::GraphStore;

    use crate::{
        store::RocksStorage,
        types::{
            element::{Edge, Vertex},
            gvalue::Primitive,
            keys::{CanonicalEdgeKey, CanonicalKey, Direction},
            StoreError,
        },
    };

    fn open() -> (RocksStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksStorage::open(dir.path()).unwrap();
        (store, dir)
    }

    fn ctx(store: &RocksStorage) -> LogicalGraph<RocksStorage> {
        LogicalGraph::new(store.begin())
    }

    fn cek(src: u64, label: u16, dst: u64) -> CanonicalEdgeKey {
        CanonicalEdgeKey { src_id: src, label_id: label, rank: 0, dst_id: dst }
    }

    fn prop(v: &Vertex, key: &str) -> Option<Primitive> {
        let props_guard = v.props.read().unwrap();
        props_guard.iter().find(|p| p.key == key).map(|p| p.value.clone())
    }

    fn eprop(e: &Edge, key: &str) -> Option<Primitive> {
        let props_guard = e.props.read().unwrap();
        props_guard.iter().find(|p| p.key == key).map(|p| p.value.clone())
    }

    // ── add_vertex / get_vertex ───────────────────────────────────────────────

    #[test]
    fn add_vertex_visible_via_get_vertex() {
        let (store, _dir) = open();
        let mut c = ctx(&store);

        let (key, fv) = c.add_vertex(100, 1).unwrap();
        let result = c.get_vertex(key).unwrap();
        assert_eq!(result, Some(fv));
        assert_eq!(result.expect("vertex should exist").label_id, 1);
    }

    #[test]
    fn get_vertex_absent_returns_none() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        assert!(c.get_vertex(9999).unwrap().is_none());
    }

    #[test]
    fn get_vertex_returns_same_idx_on_repeated_calls() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, idx) = c.add_vertex(100, 2).unwrap();
        assert_eq!(c.get_vertex(key).unwrap(), Some(idx));
    }

    // ── add_edge / get_edge ───────────────────────────────────────────────────

    #[test]
    fn add_edge_visible_via_get_edge() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v2, _) = c.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        let (key, fe) = c.add_edge(k).unwrap();
        let result = c.get_edge(k).unwrap().unwrap();
        assert_eq!(k.out_key(), key);
        assert_eq!(result, fe);
        assert_eq!((result.src_id, result.label_id, result.dst_id), (v1, 5, v2));
    }

    #[test]
    fn add_duplicated_edge_should_fail() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v2, _) = c.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c.add_edge(k).unwrap();

        c.commit().unwrap();

        let mut c = ctx(&store);
        let result = c.add_edge(k);
        assert!(result.is_err());
    }

    #[test]
    fn add_duplicated_edge_in_mem_should_fail() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v2, _) = c.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c.add_edge(k).unwrap();

        let result = c.add_edge(k);
        assert!(result.is_err());
    }

    #[test]
    fn add_edge_vs_add_same_edge_handmade() {
        let (store, _dir) = open();
        let mut c0 = ctx(&store);
        let (v1, _) = c0.add_vertex(1, 1).unwrap();
        let (v2, _) = c0.add_vertex(2, 1).unwrap();
        c0.commit().unwrap();

        let mut c1 = ctx(&store);
        let mut c2 = ctx(&store);
        let k = cek(v1, 5, v2);

        c1.add_edge(k).unwrap();
        c2.add_edge(k).unwrap();

        c1.commit().unwrap();
        let result = c2.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }
    // ── set_property ─────────────────────────────────────────────────────────

    #[test]
    fn set_property_on_new_vertex_read_your_writes() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, fv) = c.add_vertex(100, 1).unwrap();

        c.set_property(CanonicalKey::Vertex(key), SmolStr::new("age"), Primitive::Int32(42)).unwrap();

        let v = c.get_vertex(key).unwrap().unwrap();
        let fv_props = v.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(fv_props.len(), 1);
        assert_eq!(fv_props[0].key, SmolStr::new("age"));
        assert_eq!(fv_props[0].value, Primitive::Int32(42));

        assert!(Arc::ptr_eq(&fv, &v), "get_vertex should return the same Arc as add_vertex");
        let fv_props = fv.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(fv_props.len(), 1);
        assert_eq!(fv_props[0].key, SmolStr::new("age"));
        assert_eq!(fv_props[0].value, Primitive::Int32(42));
    }

    #[test]
    fn set_property_upserts_existing_key() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(100, 1).unwrap();

        c.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(2)).unwrap();

        let v = c.get_vertex(key).unwrap().unwrap();
        let v_props = v.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(v_props.len(), 1);
        assert_eq!(v_props[0].value, Primitive::Int32(2));
    }

    #[test]
    fn set_property_on_edge_read_your_writes() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v2, _) = c.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c.add_edge(k).unwrap();

        c.set_property(CanonicalKey::Edge(k), SmolStr::new("w"), Primitive::Float64(1.5)).unwrap();

        let e = c.get_edge(k).unwrap().unwrap();
        let e_props = e.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(e_props.len(), 1);
        assert_eq!(e_props[0].value, Primitive::Float64(1.5));
    }

    #[test]
    fn set_vertex_property_vs_set_vertex_property_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (key, _) = c1.add_vertex(100, 1).unwrap();
        c1.commit().unwrap();

        // Two contexts load the same vertex, then concurrently update the same property key with different values.
        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        c2.get_vertex(key).unwrap();
        c3.get_vertex(key).unwrap();
        c2.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c3.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(2)).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
        let mut c4 = ctx(&store);
        let v = c4.get_vertex(key).unwrap().unwrap();
        let v_props = v.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(v_props.len(), 1);
        assert_eq!(v_props[0].value, Primitive::Int32(1));
    }

    #[test]
    fn set_edge_property_vs_set_edge_property_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c1.add_edge(k).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        c2.get_edge(k).unwrap();
        c3.get_edge(k).unwrap();
        c2.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c3.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(2)).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    // ── drop_property ─────────────────────────────────────────────────────────

    #[test]
    fn drop_property_removes_key() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(100, 1).unwrap();

        c.set_property(CanonicalKey::Vertex(key), SmolStr::new("a"), Primitive::Int32(1)).unwrap();
        c.set_property(CanonicalKey::Vertex(key), SmolStr::new("b"), Primitive::Int32(2)).unwrap();
        c.drop_property(CanonicalKey::Vertex(key), &SmolStr::new("a")).unwrap();

        let v = c.get_vertex(key).unwrap().unwrap();
        let v_props = v.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(v_props.len(), 1);
        assert_eq!(v_props[0].key, SmolStr::new("b"));
    }

    #[test]
    fn drop_property_on_missing_key_is_noop() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(100, 1).unwrap();
        c.drop_property(CanonicalKey::Vertex(key), &SmolStr::new("nonexistent")).unwrap();
        let v = c.get_vertex(key).unwrap().unwrap();
        let v_props = v.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert!(v_props.is_empty());
    }

    #[test]
    fn drop_vertex_property_vs_set_vertex_property_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (key, _) = c1.add_vertex(100, 1).unwrap();
        c1.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        let _ = c2.get_vertex(key).unwrap();
        let _ = c3.get_vertex(key).unwrap();
        c2.drop_property(CanonicalKey::Vertex(key), &SmolStr::new("x")).unwrap();
        c3.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(2)).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn set_vertex_property_vs_drop_vertex_property_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (key, _) = c1.add_vertex(100, 1).unwrap();
        c1.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        let _ = c2.get_vertex(key).unwrap();
        let _ = c3.get_vertex(key).unwrap();
        c2.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(2)).unwrap();
        c3.drop_property(CanonicalKey::Vertex(key), &SmolStr::new("x")).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn drop_edge_property_vs_set_edge_property_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c1.add_edge(k).unwrap();
        c1.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        let _ = c2.get_edge(k).unwrap();
        let _ = c3.get_edge(k).unwrap();
        c2.drop_property(CanonicalKey::Edge(k), &SmolStr::new("x")).unwrap();
        c3.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(2)).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn set_edge_property_vs_drop_edge_property_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c1.add_edge(k).unwrap();
        c1.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        let _ = c2.get_edge(k).unwrap();
        let _ = c3.get_edge(k).unwrap();
        c2.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(2)).unwrap();
        c3.drop_property(CanonicalKey::Edge(k), &SmolStr::new("x")).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    // ── drop_element ──────────────────────────────────────────────────────────

    #[test]
    fn tombstoned_vertex_invisible_to_get_vertex() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(100, 1).unwrap();
        let v = c.get_vertex(key).unwrap().unwrap();
        assert_eq!(v.id, key);
        assert_eq!(v.label_id, 1);
        c.drop_element(CanonicalKey::Vertex(key)).unwrap();
        assert!(c.get_vertex(key).unwrap().is_none());
    }

    #[test]
    fn tombstoned_edge_invisible_to_get_edge() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v2, _) = c.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c.add_edge(k).unwrap();
        let e = c.get_edge(k).unwrap().unwrap();
        assert_eq!(e.canonical_key(), k);
        c.drop_element(CanonicalKey::Edge(k)).unwrap();
        assert!(c.get_edge(k).unwrap().is_none());
    }

    #[test]
    fn drop_vertex_with_edges_errors() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v2, _) = c.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c.add_edge(k).unwrap();

        let err = c.drop_element(CanonicalKey::Vertex(v1));
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().to_string(), "cannot drop vertex with incident edges");

        c.commit().unwrap();

        let mut c2 = ctx(&store);
        let _e = c2.get_vertex(v1).unwrap().unwrap();
        let err = c2.drop_element(CanonicalKey::Vertex(v1));
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().to_string(), "cannot drop vertex with incident edges");
    }

    #[test]
    fn set_property_on_tombstoned_vertex_errors() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(100, 1).unwrap();
        c.drop_element(CanonicalKey::Vertex(key)).unwrap();
        let err = c.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(1));
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().to_string(), "element is tombstoned");
    }

    #[test]
    fn add_edge_vs_drop_edge_handmade() {
        let (store, _dir) = open();
        let mut c0 = ctx(&store);
        let (v1, _) = c0.add_vertex(1, 1).unwrap();
        let (v2, _) = c0.add_vertex(2, 1).unwrap();
        c0.commit().unwrap();

        let mut c1 = ctx(&store);
        let mut c2 = ctx(&store);
        let k = cek(v1, 5, v2);

        c1.add_edge(k).unwrap();
        c2.add_edge(k).unwrap();

        c1.commit().unwrap();
        c2.drop_element(CanonicalKey::Edge(k)).unwrap();
        let result = c2.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn drop_vertex_vs_add_edge_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 2).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);

        let k = cek(v1, 5, v2);
        let _ = c2.get_vertex(v1).unwrap().unwrap();
        let _ = c2.get_vertex(v2).unwrap().unwrap();
        let _ = c3.get_vertex(v1).unwrap().unwrap();

        c2.add_edge(k).unwrap();
        c3.drop_element(CanonicalKey::Vertex(v1)).unwrap();

        assert!(c3.commit().is_ok(), "c3 should commit successfully");

        let result = c2.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn add_edge_vs_drop_vertex_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 2).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);

        let k = cek(v1, 5, v2);
        let _ = c2.get_vertex(v1).unwrap().unwrap();
        let _ = c2.get_vertex(v2).unwrap().unwrap();
        let _ = c3.get_vertex(v1).unwrap().unwrap();

        c2.add_edge(k).unwrap();
        c3.drop_element(CanonicalKey::Vertex(v1)).unwrap();

        assert!(c2.commit().is_ok(), "c2 should commit successfully");

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn drop_dst_vertex_vs_add_edge_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 2).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);

        let k = cek(v1, 5, v2);
        let _ = c2.get_vertex(v1).unwrap().unwrap();
        let _ = c2.get_vertex(v2).unwrap().unwrap();
        let _ = c3.get_vertex(v2).unwrap().unwrap();

        c2.add_edge(k).unwrap();
        c3.drop_element(CanonicalKey::Vertex(v2)).unwrap();

        assert!(c3.commit().is_ok(), "c3 should commit successfully");

        let result = c2.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn add_edge_vs_drop_dst_vertex_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 2).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);

        let k = cek(v1, 5, v2);
        let _ = c2.get_vertex(v1).unwrap().unwrap();
        let _ = c2.get_vertex(v2).unwrap().unwrap();
        let _ = c3.get_vertex(v2).unwrap().unwrap();

        c2.add_edge(k).unwrap();
        c3.drop_element(CanonicalKey::Vertex(v2)).unwrap();

        assert!(c2.commit().is_ok(), "c2 should commit successfully");

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn set_edge_property_vs_drop_edge_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c1.add_edge(k).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        let _ = c2.get_edge(k).unwrap();
        let _ = c3.get_edge(k).unwrap();
        c2.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
        c3.drop_element(CanonicalKey::Edge(k)).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn drop_edge_vs_set_edge_property_handmade() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1, 1).unwrap();
        let (v2, _) = c1.add_vertex(2, 1).unwrap();
        let k = cek(v1, 5, v2);
        c1.add_edge(k).unwrap();
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        let _ = c2.get_edge(k).unwrap();
        let _ = c3.get_edge(k).unwrap();
        c2.drop_element(CanonicalKey::Edge(k)).unwrap();
        c3.set_property(CanonicalKey::Edge(k), SmolStr::new("x"), Primitive::Int32(1)).unwrap();

        c2.commit().unwrap();

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    // ── commit ────────────────────────────────────────────────────────────────

    #[test]
    fn commit_persists_vertex_to_store() {
        let (store, _dir) = open();
        let id = {
            let mut c = ctx(&store);
            let (key, _) = c.add_vertex(77, 7).unwrap();
            c.set_property(CanonicalKey::Vertex(key), SmolStr::new("name"), Primitive::String(SmolStr::new("Alice")))
                .unwrap();
            c.commit().unwrap();
            key
        };

        let fv = store.get_vertex(id).unwrap().unwrap();
        assert_eq!(fv.label_id, 7);
        let fv_props = fv.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(fv_props.len(), 1);
        assert_eq!(fv_props[0].value, Primitive::String(SmolStr::new("Alice")));
    }

    #[test]
    fn commit_persists_edge_to_store() {
        let (store, _dir) = open();
        let (v1, v2) = {
            let mut c0 = ctx(&store);
            let (v_1, _) = c0.add_vertex(1, 1).unwrap();
            let (v_2, _) = c0.add_vertex(2, 1).unwrap();
            c0.commit().unwrap();
            (v_1, v_2)
        };
        let k = cek(v1, 3, v2);
        {
            let mut c = ctx(&store);
            c.add_edge(k).unwrap();
            c.set_property(CanonicalKey::Edge(k), SmolStr::new("w"), Primitive::Int32(99)).unwrap();
            c.commit().unwrap();
        }

        let edges = store.get_edges(v1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        let e_props = e.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(e_props.len(), 1);
        assert_eq!(e_props[0].value, Primitive::Int32(99));
    }

    #[test]
    fn commit_persists_vertex_deletion() {
        let (store, _dir) = open();
        let id = {
            let mut c = ctx(&store);
            let (key, _) = c.add_vertex(100, 1).unwrap();
            c.commit().unwrap();
            key
        };
        assert!(store.get_vertex(id).unwrap().is_some());

        {
            let mut c = ctx(&store);
            let _ = c.get_vertex(id).unwrap();
            c.drop_element(CanonicalKey::Vertex(id)).unwrap();
            c.commit().unwrap();
        }
        assert!(store.get_vertex(id).unwrap().is_none());
    }

    #[test]
    fn commit_resets_overlay_for_reuse() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(100, 1).unwrap();
        c.commit().unwrap();
        // Overlay is cleared — the same key must now load from store, not the old overlay.
        let vertex = c.get_vertex(key).unwrap().unwrap();
        assert_eq!(vertex.label_id, 1);
    }

    // ── abort ─────────────────────────────────────────────────────────────────

    #[test]
    fn abort_discards_pending_writes() {
        let (store, _dir) = open();
        let id = {
            let mut c = ctx(&store);
            let (key, _) = c.add_vertex(100, 1).unwrap();
            c.abort();
            key
        };
        assert!(store.get_vertex(id).unwrap().is_none());
    }

    // ── get_edges ─────────────────────────────────────────────────────────────

    #[test]
    fn get_edges_returns_new_dirty_edges_before_commit() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v10, _) = c.add_vertex(10, 1).unwrap();
        let (v20, _) = c.add_vertex(20, 1).unwrap();
        c.add_edge(cek(v1, 1, v10)).unwrap();
        c.add_edge(cek(v1, 1, v20)).unwrap();

        let edges = c.get_edges(v1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn get_edges_filters_tombstoned_edges() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v10, _) = c.add_vertex(10, 1).unwrap();
        let (v20, _) = c.add_vertex(20, 1).unwrap();
        c.add_edge(cek(v1, 1, v10)).unwrap();
        c.add_edge(cek(v1, 1, v20)).unwrap();
        c.drop_element(CanonicalKey::Edge(cek(v1, 1, v10))).unwrap();

        let edges = c.get_edges(v1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn get_edges_direction_in_vs_out() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v2, _) = c.add_vertex(2, 1).unwrap();
        c.add_edge(cek(v1, 1, v2)).unwrap();

        let out = c.get_edges(v1, Direction::OUT, None, None).unwrap();
        let in_ = c.get_edges(v2, Direction::IN, None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(in_.len(), 1);
        // Vertex v1 has no incoming edges; vertex v2 has no outgoing.
        assert!(c.get_edges(v1, Direction::IN, None, None).unwrap().is_empty());
        assert!(c.get_edges(v2, Direction::OUT, None, None).unwrap().is_empty());
    }

    #[test]
    fn get_edges_label_filter() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v10, _) = c.add_vertex(10, 1).unwrap();
        let (v20, _) = c.add_vertex(20, 1).unwrap();
        let (v30, _) = c.add_vertex(30, 1).unwrap();
        c.add_edge(cek(v1, 1, v10)).unwrap();
        c.add_edge(cek(v1, 2, v20)).unwrap();
        c.add_edge(cek(v1, 1, v30)).unwrap();

        let label1 = c.get_edges(v1, Direction::OUT, Some(1), None).unwrap();
        assert_eq!(label1.len(), 2);
        assert!(label1.iter().all(|(ek, _)| ek.label_id == 1));

        let label2 = c.get_edges(v1, Direction::OUT, Some(2), None).unwrap();
        assert_eq!(label2.len(), 1);
    }

    #[test]
    fn get_edges_dst_filter() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (v1, _) = c.add_vertex(1, 1).unwrap();
        let (v10, _) = c.add_vertex(10, 1).unwrap();
        let (v20, _) = c.add_vertex(20, 1).unwrap();
        let (v30, _) = c.add_vertex(30, 1).unwrap();
        c.add_edge(cek(v1, 1, v10)).unwrap();
        c.add_edge(cek(v1, 1, v20)).unwrap();
        c.add_edge(cek(v1, 1, v30)).unwrap();

        let result = c.get_edges(v1, Direction::OUT, None, Some(&[v10, v30])).unwrap();
        assert_eq!(result.len(), 2);
        let mut secondaries: Vec<u64> = result.iter().map(|(ek, _)| ek.secondary_id).collect();
        secondaries.sort_unstable();
        let mut expected = vec![v10, v30];
        expected.sort_unstable();
        assert_eq!(secondaries, expected);
    }

    #[test]
    fn get_edges_merges_committed_and_dirty() {
        let (store, _dir) = open();

        // Commit one edge, then add another in a new context.
        let (v1, v10, v20) = {
            let mut c0 = ctx(&store);
            let (v_1, _) = c0.add_vertex(1, 1).unwrap();
            let (v_10, _) = c0.add_vertex(10, 1).unwrap();
            let (v_20, _) = c0.add_vertex(20, 1).unwrap();
            c0.commit().unwrap();
            (v_1, v_10, v_20)
        };

        let k1 = cek(v1, 1, v10);
        {
            let mut c = ctx(&store);
            c.add_edge(k1).unwrap();
            c.commit().unwrap();
        }

        let mut c = ctx(&store);
        c.add_edge(cek(v1, 1, v20)).unwrap();
        let edges = c.get_edges(v1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 2);
    }

    // ── Concurrency & Conflict Test Matrix ────────────────────────────────────
    //
    // This matrix documents the test coverage for Optimistic Concurrency Control
    // (OCC) conflicts. It shows which concurrent operations on the same or
    // related elements are tested to guarantee a `StoreError::Conflict` on `commit()`.
    // Both commit orders (Txn1 -> Txn2, and Txn2 -> Txn1) are tested for every cell
    // in `conflict_matrix`, alongside specific handmade tests.
    //
    // | Txn 1 \ Txn 2   | Add Edge       | Drop Edge      | Set Prop(E)    | Drop Prop(E)   | Set Prop(V)    | Drop Prop(V)   | Drop Vertex    |
    // |-----------------|----------------|----------------|----------------|----------------|----------------|----------------|----------------|
    // | Add Edge        | [1], [20]      | [2], [21]      | N/A            | N/A            | [3]            | [4]            | [5], [22..25]  |
    // | Drop Edge       | [2], [21]      | [6]            | [7], [26,27]   | [8]            | [9]            | [10]           | N/A            |
    // | Set Prop(E)     | N/A            | [7], [26,27]   | [11], [28]     | [12], [29,30]  | N/A            | N/A            | N/A            |
    // | Drop Prop(E)    | N/A            | [8]            | [12], [29,30]  | [13]           | N/A            | N/A            | N/A            |
    // | Set Prop(V)     | [3]            | [9]            | N/A            | N/A            | [14], [31]     | [15], [32,33]  | [16]           |
    // | Drop Prop(V)    | [4]            | [10]           | N/A            | N/A            | [15], [32,33]  | [17]           | [18]           |
    // | Drop Vertex     | [5], [22..25]  | N/A            | N/A            | N/A            | [16]           | [18]           | [19]           |
    //
    // ── Automated conflict_matrix tests:
    // [1]  add_edge_vs_add_edge
    // [2]  add_edge_vs_drop_edge
    // [3]  add_edge_vs_set_vertex_property
    // [4]  add_edge_vs_drop_vertex_property
    // [5]  add_edge_vs_drop_vertex
    // [6]  drop_edge_vs_drop_edge
    // [7]  drop_edge_vs_set_edge_property
    // [8]  drop_edge_vs_drop_edge_property
    // [9]  drop_edge_vs_set_vertex_property
    // [10] drop_edge_vs_drop_vertex_property
    // [11] set_edge_property_vs_set_edge_property
    // [12] set_edge_property_vs_drop_edge_property
    // [13] drop_edge_property_vs_drop_edge_property
    // [14] set_vertex_property_vs_set_vertex_property
    // [15] set_vertex_property_vs_drop_vertex_property
    // [16] set_vertex_property_vs_drop_vertex
    // [17] drop_vertex_property_vs_drop_vertex_property
    // [18] drop_vertex_property_vs_drop_vertex
    // [19] drop_vertex_vs_drop_vertex
    //
    // ── Handmade concurrent tests:
    // [20] add_edge_vs_add_edge_handmade
    // [21] add_edge_vs_drop_edge_handmade
    // [22] drop_vertex_vs_add_edge_handmade
    // [23] add_edge_vs_drop_vertex_handmade
    // [24] drop_dst_vertex_vs_add_edge_handmade
    // [25] add_edge_vs_drop_dst_vertex_handmade
    // [26] set_edge_property_vs_drop_edge_handmade
    // [27] drop_edge_vs_set_edge_property_handmade
    // [28] set_edge_property_vs_set_edge_property_handmade
    // [29] drop_edge_property_vs_set_edge_property_handmade
    // [30] set_edge_property_vs_drop_edge_property_handmade
    // [31] set_vertex_property_vs_set_vertex_property_handmade
    // [32] drop_vertex_property_vs_set_vertex_property_handmade
    // [33] set_vertex_property_vs_drop_vertex_property_handmade
    //
    // N/A: Combinations that don't conflict (mutate distinct elements without read dependencies)
    // or are impossible (e.g. dropping a vertex with an existing edge fails validation early).
    // ──────────────────────────────────────────────────────────────────────────

    mod conflict_matrix {
        use super::*;

        fn run_non_conflict<State: Copy, Setup, Op1, Op2>(setup: Setup, op1: Op1, op2: Op2)
        where
            Setup: Fn(&mut LogicalGraph<RocksStorage>) -> State,
            Op1: Fn(&mut LogicalGraph<RocksStorage>, State),
            Op2: Fn(&mut LogicalGraph<RocksStorage>, State),
        {
            // Order 1: Txn1 commits, Txn2 conflicts
            {
                let (store, _dir) = open();
                let mut c0 = ctx(&store);
                let state = setup(&mut c0);
                c0.commit().unwrap();

                let mut c1 = ctx(&store);
                let mut c2 = ctx(&store);

                op1(&mut c1, state);
                op2(&mut c2, state);

                c1.commit().unwrap();
                let res = c2.commit();
                assert!(res.is_ok(), "unexpected conflict in non-conflicting operations. Order 1 (Txn1 commits, Txn2 should succeed) failed with error: {:?}", res.err());
            }

            // Order 2: Txn2 commits, Txn1 conflicts
            {
                let (store, _dir) = open();
                let mut c0 = ctx(&store);
                let state = setup(&mut c0);
                c0.commit().unwrap();

                let mut c1 = ctx(&store);
                let mut c2 = ctx(&store);

                op1(&mut c1, state);
                op2(&mut c2, state);

                c2.commit().unwrap();
                let res = c1.commit();
                assert!(res.is_ok(), "unexpected conflict in non-conflicting operations. Order 2 (Txn2 commits, Txn1 should succeed) failed with error: {:?}", res.err());
            }
        }

        fn run_conflict<State: Copy, Setup, Op1, Op2>(setup: Setup, op1: Op1, op2: Op2)
        where
            Setup: Fn(&mut LogicalGraph<RocksStorage>) -> State,
            Op1: Fn(&mut LogicalGraph<RocksStorage>, State),
            Op2: Fn(&mut LogicalGraph<RocksStorage>, State),
        {
            // Order 1: Txn1 commits, Txn2 conflicts
            {
                let (store, _dir) = open();
                let mut c0 = ctx(&store);
                let state = setup(&mut c0);
                c0.commit().unwrap();

                let mut c1 = ctx(&store);
                let mut c2 = ctx(&store);

                op1(&mut c1, state);
                op2(&mut c2, state);

                c1.commit().unwrap();
                let res = c2.commit();
                assert!(
                    matches!(res, Err(StoreError::Conflict)),
                    "Order 1 (Txn1 commits, Txn2 conflicts) failed. Expected Conflict, got {:?}",
                    res
                );
            }

            // Order 2: Txn2 commits, Txn1 conflicts
            {
                let (store, _dir) = open();
                let mut c0 = ctx(&store);
                let state = setup(&mut c0);
                c0.commit().unwrap();

                let mut c1 = ctx(&store);
                let mut c2 = ctx(&store);

                op1(&mut c1, state);
                op2(&mut c2, state);

                c2.commit().unwrap();
                let res = c1.commit();
                assert!(
                    matches!(res, Err(StoreError::Conflict)),
                    "Order 2 (Txn2 commits, Txn1 conflicts) failed. Expected Conflict, got {:?}",
                    res
                );
            }
        }

        #[test]
        fn add_edge_vs_add_edge() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    (v1, v2)
                },
                |c, (v1, v2)| {
                    c.add_edge(cek(v1, 5, v2)).unwrap();
                },
                |c, (v1, v2)| {
                    c.add_edge(cek(v1, 5, v2)).unwrap();
                },
            );
        }

        #[test]
        fn add_edge_vs_add_edge_with_same_vertex() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let (v3, _) = c.add_vertex(3, 1).unwrap();
                    (v1, v2, v3)
                },
                |c, (v1, v2, _v3)| {
                    c.add_edge(cek(v1, 5, v2)).unwrap();
                },
                |c, (v1, _v2, v3)| {
                    c.add_edge(cek(v1, 5, v3)).unwrap();
                },
            );
        }

        #[test]
        fn add_edge_vs_drop_edge() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let (v3, _) = c.add_vertex(3, 1).unwrap();
                    let e1 = cek(v1, 5, v2);
                    c.add_edge(e1).unwrap();
                    (v1, e1, v3)
                },
                |c, (v1, _, v3)| {
                    c.add_edge(cek(v1, 6, v3)).unwrap();
                },
                |c, (_, e1, _)| {
                    c.get_edge(e1).unwrap();
                    c.drop_element(CanonicalKey::Edge(e1)).unwrap();
                },
            );
        }

        #[test]
        fn add_edge_vs_drop_edge_with_same_vertex() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let (v3, _) = c.add_vertex(3, 1).unwrap();
                    let e1 = cek(v1, 5, v2);
                    c.add_edge(e1).unwrap();
                    (v1, e1, v3)
                },
                |c, (v1, _, v3)| {
                    c.add_edge(cek(v1, 6, v3)).unwrap();
                },
                |c, (_, e1, _)| {
                    c.get_edge(e1).unwrap();
                    c.drop_element(CanonicalKey::Edge(e1)).unwrap();
                },
            );
        }

        #[test]
        fn add_edge_vs_set_vertex_property() {
            run_non_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    (v1, v2)
                },
                |c, (v1, v2)| {
                    c.add_edge(cek(v1, 5, v2)).unwrap();
                },
                |c, (v1, _)| {
                    c.get_vertex(v1).unwrap();
                    c.set_property(CanonicalKey::Vertex(v1), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                },
            );
        }

        #[test]
        fn add_edge_vs_drop_vertex_property() {
            run_non_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    c.set_property(CanonicalKey::Vertex(v1), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    (v1, v2)
                },
                |c, (v1, v2)| {
                    c.add_edge(cek(v1, 5, v2)).unwrap();
                },
                |c, (v1, _)| {
                    c.get_vertex(v1).unwrap();
                    c.drop_property(CanonicalKey::Vertex(v1), &SmolStr::new("x")).unwrap();
                },
            );
        }

        #[test]
        fn add_edge_vs_drop_vertex() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    (v1, v2)
                },
                |c, (v1, v2)| {
                    c.add_edge(cek(v1, 5, v2)).unwrap();
                },
                |c, (_, v2)| {
                    c.get_vertex(v2).unwrap();
                    c.drop_element(CanonicalKey::Vertex(v2)).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_vs_drop_edge() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    e
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_element(CanonicalKey::Edge(e)).unwrap();
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_element(CanonicalKey::Edge(e)).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_vs_drop_edge_with_same_vertex() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let (v3, _) = c.add_vertex(3, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    let e2 = cek(v1, 6, v3);
                    c.add_edge(e).unwrap();
                    c.add_edge(e2).unwrap();
                    (e, e2)
                },
                |c, (e1, _e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e1).unwrap();
                    c.drop_element(CanonicalKey::Edge(e1)).unwrap();
                },
                |c, (_e1, e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e2).unwrap();
                    c.drop_element(CanonicalKey::Edge(e2)).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_vs_set_edge_property() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    e
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_element(CanonicalKey::Edge(e)).unwrap();
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_vs_drop_edge_property() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    e
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_element(CanonicalKey::Edge(e)).unwrap();
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_property(CanonicalKey::Edge(e), &SmolStr::new("x")).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_vs_set_vertex_property() {
            run_non_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    (v1, e)
                },
                |c, (_, e)| {
                    c.get_edge(e).unwrap();
                    c.drop_element(CanonicalKey::Edge(e)).unwrap();
                },
                |c, (v1, _)| {
                    c.get_vertex(v1).unwrap();
                    c.set_property(CanonicalKey::Vertex(v1), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_vs_drop_vertex_property() {
            run_non_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    c.set_property(CanonicalKey::Vertex(v1), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    (v1, e)
                },
                |c, (_, e)| {
                    c.get_edge(e).unwrap();
                    c.drop_element(CanonicalKey::Edge(e)).unwrap();
                },
                |c, (v1, _)| {
                    c.get_vertex(v1).unwrap();
                    c.drop_property(CanonicalKey::Vertex(v1), &SmolStr::new("x")).unwrap();
                },
            );
        }

        #[test]
        fn set_edge_property_vs_set_edge_property() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    e
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(2)).unwrap();
                },
            );
        }

        #[test]
        fn set_edge_property_vs_set_edge_property_with_same_vertex() {
            run_non_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let (v3, _) = c.add_vertex(3, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    let e2 = cek(v1, 6, v3);
                    c.add_edge(e).unwrap();
                    c.add_edge(e2).unwrap();
                    (e, e2)
                },
                |c, (e1, _e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e1).unwrap();
                    c.set_property(CanonicalKey::Edge(e1), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                },
                |c, (_e1, e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e2).unwrap();
                    c.set_property(CanonicalKey::Edge(e2), SmolStr::new("y"), Primitive::Int32(2)).unwrap();
                },
            );
        }

        #[test]
        fn set_edge_property_vs_drop_edge_property() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    e
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(2)).unwrap();
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_property(CanonicalKey::Edge(e), &SmolStr::new("x")).unwrap();
                },
            );
        }

        #[test]
        fn set_edge_property_vs_drop_edge_property_with_same_vertex() {
            run_non_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let (v3, _) = c.add_vertex(3, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    let e2 = cek(v1, 6, v3);
                    c.add_edge(e).unwrap();
                    c.add_edge(e2).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    c.set_property(CanonicalKey::Edge(e2), SmolStr::new("y"), Primitive::Int32(2)).unwrap();
                    (e, e2)
                },
                |c, (e1, _e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e1).unwrap();
                    c.set_property(CanonicalKey::Edge(e1), SmolStr::new("x"), Primitive::Int32(2)).unwrap();
                },
                |c, (_e1, e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e2).unwrap();
                    c.drop_property(CanonicalKey::Edge(e2), &SmolStr::new("y")).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_property_vs_drop_edge_property() {
            run_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    c.add_edge(e).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    e
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_property(CanonicalKey::Edge(e), &SmolStr::new("x")).unwrap();
                },
                |c, e| {
                    c.get_edge(e).unwrap();
                    c.drop_property(CanonicalKey::Edge(e), &SmolStr::new("x")).unwrap();
                },
            );
        }

        #[test]
        fn drop_edge_property_vs_drop_edge_property_with_same_vertex() {
            run_non_conflict(
                |c| {
                    let (v1, _) = c.add_vertex(1, 1).unwrap();
                    let (v2, _) = c.add_vertex(2, 1).unwrap();
                    let (v3, _) = c.add_vertex(3, 1).unwrap();
                    let e = cek(v1, 5, v2);
                    let e2 = cek(v1, 6, v3);
                    c.add_edge(e).unwrap();
                    c.add_edge(e2).unwrap();
                    c.set_property(CanonicalKey::Edge(e), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    c.set_property(CanonicalKey::Edge(e2), SmolStr::new("y"), Primitive::Int32(2)).unwrap();
                    (e, e2)
                },
                |c, (e1, _e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e1).unwrap();
                    c.drop_property(CanonicalKey::Edge(e1), &SmolStr::new("x")).unwrap();
                },
                |c, (_e1, e2): (CanonicalEdgeKey, CanonicalEdgeKey)| {
                    c.get_edge(e2).unwrap();
                    c.drop_property(CanonicalKey::Edge(e2), &SmolStr::new("y")).unwrap();
                },
            );
        }

        #[test]
        fn set_vertex_property_vs_set_vertex_property() {
            run_conflict(
                |c| {
                    let (v, _) = c.add_vertex(100, 1).unwrap();
                    v
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.set_property(CanonicalKey::Vertex(v), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.set_property(CanonicalKey::Vertex(v), SmolStr::new("x"), Primitive::Int32(2)).unwrap();
                },
            );
        }

        #[test]
        fn set_vertex_property_vs_drop_vertex_property() {
            run_conflict(
                |c| {
                    let (v, _) = c.add_vertex(100, 1).unwrap();
                    c.set_property(CanonicalKey::Vertex(v), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    v
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.set_property(CanonicalKey::Vertex(v), SmolStr::new("x"), Primitive::Int32(2)).unwrap();
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_property(CanonicalKey::Vertex(v), &SmolStr::new("x")).unwrap();
                },
            );
        }

        #[test]
        fn set_vertex_property_vs_drop_vertex() {
            run_conflict(
                |c| {
                    let (v, _) = c.add_vertex(100, 1).unwrap();
                    v
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.set_property(CanonicalKey::Vertex(v), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_element(CanonicalKey::Vertex(v)).unwrap();
                },
            );
        }

        #[test]
        fn drop_vertex_property_vs_drop_vertex_property() {
            run_conflict(
                |c| {
                    let (v, _) = c.add_vertex(100, 1).unwrap();
                    c.set_property(CanonicalKey::Vertex(v), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    v
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_property(CanonicalKey::Vertex(v), &SmolStr::new("x")).unwrap();
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_property(CanonicalKey::Vertex(v), &SmolStr::new("x")).unwrap();
                },
            );
        }

        #[test]
        fn drop_vertex_property_vs_drop_vertex() {
            run_conflict(
                |c| {
                    let (v, _) = c.add_vertex(100, 1).unwrap();
                    c.set_property(CanonicalKey::Vertex(v), SmolStr::new("x"), Primitive::Int32(1)).unwrap();
                    v
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_property(CanonicalKey::Vertex(v), &SmolStr::new("x")).unwrap();
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_element(CanonicalKey::Vertex(v)).unwrap();
                },
            );
        }

        #[test]
        fn drop_vertex_vs_drop_vertex() {
            run_conflict(
                |c| {
                    let (v, _) = c.add_vertex(100, 1).unwrap();
                    v
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_element(CanonicalKey::Vertex(v)).unwrap();
                },
                |c, v| {
                    c.get_vertex(v).unwrap();
                    c.drop_element(CanonicalKey::Vertex(v)).unwrap();
                },
            );
        }
    }

    // ── Integration tests ─────────────────────────────────────────────────────

    #[test]
    fn sequential_contexts_accumulate_edges() {
        let (store, _dir) = open();

        // Build edges in separate contexts; each must see all previously committed edges.
        let hub = {
            let mut c = ctx(&store);
            let (key, _) = c.add_vertex(100, 1).unwrap();
            c.commit().unwrap();
            key
        };

        let spokes: Vec<u64> = (0..4)
            .map(|i| {
                let mut c = ctx(&store);
                let (key, _) = c.add_vertex(i, 1).unwrap();
                c.add_edge(cek(hub, 1, key)).unwrap();
                c.commit().unwrap();
                key
            })
            .collect();

        // A final context must see all 4 outgoing edges from hub.
        let mut c = ctx(&store);
        let out = c.get_edges(hub, Direction::OUT, Some(1), None).unwrap();
        assert_eq!(out.len(), 4);

        // check vertex counter is correct after multiple contexts
        let (out_e, in_e) = c.get_vertex_degree_for_test(hub).unwrap().unwrap();
        assert_eq!(out_e, 4);
        assert_eq!(in_e, 0);

        // The 4 edges must land at the 4 spoke vertices.
        let mut dst_ids: Vec<u64> = out.iter().map(|(ek, _)| ek.secondary_id).collect();
        dst_ids.sort_unstable();
        let mut expected = spokes.clone();
        expected.sort_unstable();
        assert_eq!(dst_ids, expected);

        // Each spoke has exactly one incoming edge from hub.
        for &spoke in &spokes {
            let in_edges = c.get_edges(spoke, Direction::IN, Some(1), None).unwrap();
            assert_eq!(in_edges.len(), 1);
            assert_eq!(in_edges[0].1.src_id, hub);
        }
    }

    #[test]
    fn two_concurrent_contexts_build_graph_fourth_reads_all() {
        let (store, _dir) = open();

        // ctx1 — person: Alice
        let mut c1 = ctx(&store);
        let alice = {
            let (key, _) = c1.add_vertex(101, 1).unwrap();
            c1.set_property(CanonicalKey::Vertex(key), SmolStr::new("name"), Primitive::String(SmolStr::new("Alice")))
                .unwrap();
            c1.set_property(CanonicalKey::Vertex(key), SmolStr::new("age"), Primitive::Int32(30)).unwrap();
            key
        };

        // ctx2 — person: Bob
        let mut c2 = ctx(&store);
        let bob = {
            let (key, _) = c2.add_vertex(102, 1).unwrap();
            c2.set_property(CanonicalKey::Vertex(key), SmolStr::new("name"), Primitive::String(SmolStr::new("Bob")))
                .unwrap();
            c2.set_property(CanonicalKey::Vertex(key), SmolStr::new("age"), Primitive::Int32(25)).unwrap();
            key
        };

        c2.commit().unwrap();
        c1.commit().unwrap(); // commit after c2 to test concurrent visibility of both contexts

        // ctx3 — city: London + two "lives_in" edges (label=2) from each person
        let london = {
            let mut c = ctx(&store);
            let (city_key, _) = c.add_vertex(201, 2).unwrap();
            c.set_property(
                CanonicalKey::Vertex(city_key),
                SmolStr::new("name"),
                Primitive::String(SmolStr::new("London")),
            )
            .unwrap();
            // Alice -> London
            let e1 = cek(alice, 2, city_key);
            c.add_edge(e1).unwrap();
            c.set_property(CanonicalKey::Edge(e1), SmolStr::new("since"), Primitive::Int32(2015)).unwrap();
            // Bob -> London
            let e2 = cek(bob, 2, city_key);
            c.add_edge(e2).unwrap();
            c.set_property(CanonicalKey::Edge(e2), SmolStr::new("since"), Primitive::Int32(2019)).unwrap();
            c.commit().unwrap();
            city_key
        };

        // ctx4 — read-only verification
        let mut c = ctx(&store);

        // Vertices survive across contexts.
        let alice_idx = c.get_vertex(alice).unwrap().unwrap();
        assert_eq!(alice_idx.label_id, 1);
        assert_eq!(prop(&alice_idx, "name"), Some(Primitive::String(SmolStr::new("Alice"))));
        assert_eq!(prop(&alice_idx, "age"), Some(Primitive::Int32(30)));
        let (alice_out_e, alice_in_e) = c.get_vertex_degree_for_test(alice).unwrap().unwrap();
        assert_eq!(alice_out_e, 1);
        assert_eq!(alice_in_e, 0);

        let bob_idx = c.get_vertex(bob).unwrap().unwrap();
        assert_eq!(bob_idx.label_id, 1);
        assert_eq!(prop(&bob_idx, "name"), Some(Primitive::String(SmolStr::new("Bob"))));
        let (bob_out_e, bob_in_e) = c.get_vertex_degree_for_test(bob).unwrap().unwrap();
        assert_eq!(bob_out_e, 1);
        assert_eq!(bob_in_e, 0);

        let london_idx = c.get_vertex(london).unwrap().unwrap();
        assert_eq!(london_idx.label_id, 2);
        assert_eq!(prop(&london_idx, "name"), Some(Primitive::String(SmolStr::new("London"))));
        let (london_out_e, london_in_e) = c.get_vertex_degree_for_test(london).unwrap().unwrap();
        assert_eq!(london_out_e, 0);
        assert_eq!(london_in_e, 2);

        // Both outgoing "lives_in" edges from Alice land at London.
        let alice_out = c.get_edges(alice, Direction::OUT, Some(2), None).unwrap();
        assert_eq!(alice_out.len(), 1);
        let (e_idx, fe) = &alice_out[0];
        assert_eq!(e_idx.secondary_id, london);
        assert_eq!(eprop(fe, "since"), Some(Primitive::Int32(2015)));

        // London has two incoming edges: one from Alice, one from Bob.
        let london_in = c.get_edges(london, Direction::IN, Some(2), None).unwrap();
        assert_eq!(london_in.len(), 2);
        let mut src_ids: Vec<u64> = london_in.iter().map(|(ek, _)| ek.secondary_id).collect();
        src_ids.sort_unstable();
        assert_eq!(src_ids, vec![alice.min(bob), alice.max(bob)]);
    }

    // Tests that operations depending on vertex counters (like adding an edge or dropping the vertex)
    // fail gracefully when the vertex is deleted by a concurrent transaction.
    #[test]
    fn concurrent_vertex_deletion_fails_dependent_operations() {
        let (store, _dir) = open();

        // step 1, insert a vertex and set properties, commit the transaction txn1
        let mut txn1 = ctx(&store);
        let (v1, _) = txn1.add_vertex(1, 1).unwrap();
        txn1.set_property(CanonicalKey::Vertex(v1), SmolStr::new("name"), Primitive::String(SmolStr::new("Alice")))
            .unwrap();
        txn1.commit().unwrap();

        // step 2, in a new Transaction txn2, get_vertex
        let mut txn2 = ctx(&store);
        assert!(txn2.get_vertex(v1).unwrap().is_some());

        // step 3, the vertex was deleted in another transaction, commit the deleting transaction which should succeed
        let mut txn3 = ctx(&store);
        txn3.drop_element(CanonicalKey::Vertex(v1)).unwrap();
        txn3.commit().unwrap();

        // As a result, adding an edge in txn2 using the deleted vertex should gracefully error out
        let err = txn2.add_edge(cek(v1, 5, 2));
        assert!(matches!(err, Err(StoreError::NotFound)));

        // As a result, dropping the deleted vertex in txn2 should gracefully error out
        let err = txn2.drop_element(CanonicalKey::Vertex(v1));
        assert!(matches!(err, Err(StoreError::NotFound)));

        // step 4, check that get_vertex in txn2 now returns None for the deleted vertex
        let counts = txn2.get_vertex_degree_for_test(v1).unwrap();
        assert!(counts.is_none());
    }
}
