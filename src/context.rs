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

//! Query-scoped graph context — the ground truth for a single traversal.
//!
//! # Role
//!
//! `GraphContext<S>` sits between the Gremlin traversal engine and the
//! persistent `GraphStore`.  The engine never touches the store directly;
//! it only ever calls methods on `GraphContext`.
//!
//! ```text
//! Traversal Engine
//!   │  ctx.get_vertex(key)  → Option<&FullVertex>
//!   │  ctx.add_vertex(lbl)  → (VertexKey, &FullVertex)
//!   │  ctx.get_edges(…)    → Vec<(EdgeKey, &FullEdge)>
//!   │  ctx.set_property(…)
//!   │  ctx.commit()
//!   ▼
//! GraphContext<S: GraphStore>
//!   vertices: HashMap<VertexKey, Arc<FullVertex>>   ← query-scoped overlay
//!   edges:    HashMap<CanonicalEdgeKey, Arc<FullEdge>>
//!   dirty:    HashMap<CanonicalKey, Existence>
//!   id_gen:   Arc<IdGen>
//!   store:    S::Txn              ← flush-on-commit
//!   ▼
//! S::Txn: GraphTransaction         ← RocksDB / Distributed / Mock
//! ```
//!
//! # Read path
//!
//! On first access, `get_vertex` checks the local map.  If absent it calls
//! `store.get_vertex`, inserts the result, and returns a `&FullVertex`.
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
//! `commit()` iterates `dirty` and calls `store.put_*` / `store.delete_*`
//! for each element, then calls `store.commit()`.  The overlay is cleared
//! so the `GraphContext` can be reused for a retry on OCC conflict.
//!
//! # CoW on mutation
//!
//! Clean vertices (loaded from store) hold an `Arc<FullVertex>` also held by
//! the process-wide cache.  `Arc::make_mut` triggers a clone the first time a
//! clean vertex is mutated, giving copy-on-write semantics without an upfront
//! allocation on the read path.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    store::{
        id_gen::IdGen,
        traits::{GraphStore, GraphTransaction},
    },
    types::{
        full_element::{FullEdge, FullVertex},
        gvalue::{Primitive, Property},
        keys::{CanonicalEdgeKey, CanonicalKey, Direction, EdgeKey, LabelId, VertexKey},
        prop_key::PropKey,
        StoreError,
    },
};

// ── Existence ─────────────────────────────────────────────────────────────────

/// Mutation kind for a dirty graph element within a `GraphContext`.
///
/// Only dirty elements appear in the `dirty` map; absence means `Clean`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Existence {
    /// Props were mutated (element pre-existed in storage).
    Modified,
    /// Created in this query; not yet persisted.
    New,
    /// Deleted in this query.
    Tombstone,
}

// ── GraphContext ──────────────────────────────────────────────────────────────

/// Query-scoped graph context wrapping a store transaction.
///
/// Obtained by calling `GraphContext::new(store.begin(), store.id_gen())`.
/// The engine uses this as its sole interface to the graph.
pub struct GraphContext<S: GraphStore> {
    store: S::Txn,
    vertices: HashMap<VertexKey, Arc<FullVertex>>,
    edges: HashMap<CanonicalEdgeKey, Arc<FullEdge>>,
    dirty: HashMap<CanonicalKey, Existence>,
    id_gen: Arc<IdGen>,
}

impl<S: GraphStore> GraphContext<S> {
    /// Create a new context wrapping the given transaction and ID allocator.
    pub fn new(store: S::Txn, id_gen: Arc<IdGen>) -> Self {
        Self { store, vertices: HashMap::new(), edges: HashMap::new(), dirty: HashMap::new(), id_gen }
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Look up a vertex by key, loading from the store on first access.
    ///
    /// Returns `None` for absent or tombstoned vertices.
    pub fn get_vertex(&mut self, key: VertexKey) -> Result<Option<Arc<FullVertex>>, StoreError> {
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
    pub fn get_edge(&mut self, key: CanonicalEdgeKey) -> Result<Option<Arc<FullEdge>>, StoreError> {
        if !self.edges.contains_key(&key) {
            match self.store.get_edge(key)? {
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
    /// Returns `(EdgeKey, &FullEdge)` pairs — `EdgeKey` carries traversal direction.
    pub fn get_edges(
        &mut self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<(EdgeKey, Arc<FullEdge>)>, StoreError> {
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

    /// Allocate a new vertex with `label_id` and add it to the overlay.
    ///
    /// Returns `(VertexKey, &FullVertex)` — the key is globally unique (from
    /// `id_gen`); the reference gives immediate read access within this context.
    /// TODO: VertexKey should be specified by the caller to have the chance to check for duplicates before allocation.
    pub fn add_vertex(&mut self, label_id: LabelId) -> (VertexKey, Arc<FullVertex>) {
        let id = self.id_gen.next_vertex_id();
        self.vertices.insert(id, Arc::new(FullVertex { id, label_id, props: RwLock::new(Vec::new()) }));
        self.dirty.insert(CanonicalKey::Vertex(id), Existence::New);
        (id, self.vertices[&id].clone())
    }

    /// Register a new directed edge identified by `cek`.
    ///
    /// Returns `(EdgeKey, &FullEdge)` in Out orientation.
    ///
    /// # Panics (debug)
    ///
    /// Asserts the key is not already in the overlay.
    pub fn add_edge(&mut self, cek: CanonicalEdgeKey) -> Result<(EdgeKey, Arc<FullEdge>), StoreError> {
        if self.edges.contains_key(&cek) {
            return Err(StoreError::DuplicateEdge(cek));
        }
        // 1. try to retrieve edge from store to check for duplicates before allocation.
        if let Some(_) = self.store.get_edge(cek)? {
            return Err(StoreError::DuplicateEdge(cek));
        }

        // 2. insert new edge into overlay and mark dirty.  The store is not touched until commit.
        self.edges.insert(
            cek,
            Arc::new(FullEdge {
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
                    return Err(StoreError::Other("element is tombstoned".into()));
                }
                match self.vertices.get_mut(&id) {
                    None => return Err(StoreError::Other(format!("vertex {id} not exist"))),
                    Some(arc) => {
                        // 1. Acquire a write lock on the properties
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;

                        // 2. Modify in place. No cloning happens!
                        upsert_prop(&mut props, key, prop, value);
                        // upsert_prop(&mut Arc::make_mut(arc).props, key, prop, value),
                    }
                }
                self.dirty.entry(key).or_insert(Existence::Modified);
            }
            CanonicalKey::Edge(cek) => {
                if self.dirty.get(&key) == Some(&Existence::Tombstone) {
                    return Err(StoreError::Other("element is tombstoned".into()));
                }
                match self.edges.get_mut(&cek) {
                    None => return Err(StoreError::Other("edge not loaded".into())),
                    Some(arc) => {
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;
                        upsert_prop(&mut props, key, prop, value);
                    }
                }
                self.dirty.entry(key).or_insert(Existence::Modified);
            }
        }
        Ok(())
    }

    /// Remove a property from a vertex or edge.
    pub fn drop_property(&mut self, key: CanonicalKey, prop: &PropKey) -> Result<(), StoreError> {
        match key {
            CanonicalKey::Vertex(id) => {
                if self.dirty.get(&key) == Some(&Existence::Tombstone) {
                    return Err(StoreError::Other("element is tombstoned".into()));
                }
                match self.vertices.get_mut(&id) {
                    None => return Err(StoreError::Other(format!("vertex {id} not loaded"))),
                    Some(arc) => {
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;
                        props.retain(|p| &p.key != prop);
                    }
                }
                self.dirty.entry(key).or_insert(Existence::Modified);
            }
            CanonicalKey::Edge(cek) => {
                if self.dirty.get(&key) == Some(&Existence::Tombstone) {
                    return Err(StoreError::Other("element is tombstoned".into()));
                }
                match self.edges.get_mut(&cek) {
                    None => return Err(StoreError::Other("edge not loaded".into())),
                    Some(arc) => {
                        let mut props = arc.props.write().map_err(|_| StoreError::LockError)?;
                        props.retain(|p| &p.key != prop);
                    }
                }
                self.dirty.entry(key).or_insert(Existence::Modified);
            }
        }
        Ok(())
    }

    /// Mark a vertex or edge as deleted.
    ///
    /// The element must have been previously loaded or created in this context.
    pub fn drop_element(&mut self, key: CanonicalKey) -> Result<(), StoreError> {
        match key {
            CanonicalKey::Vertex(id) => {
                if !self.vertices.contains_key(&id) {
                    return Err(StoreError::Other(format!("vertex {id} not loaded")));
                }
                self.dirty.insert(key, Existence::Tombstone);
            }
            CanonicalKey::Edge(cek) => {
                if !self.edges.contains_key(&cek) {
                    return Err(StoreError::Other("edge not loaded".into()));
                }
                self.dirty.insert(key, Existence::Tombstone);
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
                (CanonicalKey::Vertex(id), Existence::New | Existence::Modified) => {
                    let v = self.vertices.get(&id).expect("dirty vertex key not in vertices");
                    // 1. Acquire a read lock
                    let props_guard = v.props.read().map_err(|_| StoreError::LockError)?;

                    // 2. Pass the guard (it derefs to &Vec<Property>, which matches &[Property])
                    self.store.put_vertex(id, v.label_id, &props_guard)?;
                }
                (CanonicalKey::Vertex(id), Existence::Tombstone) => {
                    self.store.delete_vertex(id)?;
                }
                (CanonicalKey::Edge(cek), Existence::New | Existence::Modified) => {
                    let e = self.edges.get(&cek).expect("dirty edge key not in edges");
                    // 1. Acquire a read lock
                    let props_guard = e.props.read().map_err(|_| StoreError::LockError)?;

                    // 2. Pass the guard (it derefs to &Vec<Property>, which matches &[Property])
                    self.store.put_edge(cek, &props_guard)?;
                }
                (CanonicalKey::Edge(cek), Existence::Tombstone) => {
                    self.store.delete_edge(cek)?;
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
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn upsert_prop(props: &mut Vec<Property>, owner: CanonicalKey, key: PropKey, value: Primitive) {
    if let Some(p) = props.iter_mut().find(|p| p.key == key) {
        p.value = value;
    } else {
        props.push(Property { owner, key, value });
    }
}

fn edge_matches(
    view: &FullEdge,
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

    use super::GraphContext;
    use crate::store::traits::GraphStore;

    use crate::{
        store::RocksStorage,
        types::{
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

    fn ctx(store: &RocksStorage) -> GraphContext<RocksStorage> {
        GraphContext::new(store.begin(), store.id_gen())
    }

    fn cek(src: u64, label: u16, dst: u64) -> CanonicalEdgeKey {
        CanonicalEdgeKey { src_id: src, label_id: label, rank: 0, dst_id: dst }
    }

    // ── add_vertex / get_vertex ───────────────────────────────────────────────

    #[test]
    fn add_vertex_visible_via_get_vertex() {
        let (store, _dir) = open();
        let mut c = ctx(&store);

        let (key, fv) = c.add_vertex(1);
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
        let (key, idx) = c.add_vertex(2);
        assert_eq!(c.get_vertex(key).unwrap(), Some(idx));
    }

    // ── add_edge / get_edge ───────────────────────────────────────────────────

    #[test]
    fn add_edge_visible_via_get_edge() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let k = cek(1, 5, 2);
        let (key, fe) = c.add_edge(k).unwrap();
        let result = c.get_edge(k).unwrap().unwrap();
        assert_eq!(k.out_key(), key);
        assert_eq!(result, fe);
        assert_eq!((result.src_id, result.label_id, result.dst_id), (1, 5, 2));
    }

    #[test]
    fn add_duplicated_edge_should_fail() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let k = cek(1, 5, 2);
        let _ = c.add_edge(k);

        c.commit().unwrap();

        let mut c = ctx(&store);
        let result = c.add_edge(k);
        assert!(result.is_err());
    }

    #[test]
    fn concurrent_add_same_edge_should_fail() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let mut c2 = ctx(&store);
        let k = cek(1, 5, 2);

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
        let (key, fv) = c.add_vertex(1);

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
        let (key, _) = c.add_vertex(1);

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
        let k = cek(1, 5, 2);
        let _ = c.add_edge(k);

        c.set_property(CanonicalKey::Edge(k), SmolStr::new("w"), Primitive::Float64(1.5)).unwrap();

        let e = c.get_edge(k).unwrap().unwrap();
        let e_props = e.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert_eq!(e_props.len(), 1);
        assert_eq!(e_props[0].value, Primitive::Float64(1.5));
    }

    #[test]
    fn concurrent_set_property_should_conflict_on_commit() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (key, _) = c1.add_vertex(1);
        c1.commit().unwrap();

        // Two contexts load the same vertex, then concurrently update the same property key with different values.
        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);
        let _ = c2.get_vertex(key);
        let _ = c3.get_vertex(key);
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
    // ── drop_property ─────────────────────────────────────────────────────────

    #[test]
    fn drop_property_removes_key() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(1);

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
        let (key, _) = c.add_vertex(1);
        c.drop_property(CanonicalKey::Vertex(key), &SmolStr::new("nonexistent")).unwrap();
        let v = c.get_vertex(key).unwrap().unwrap();
        let v_props = v.props.read().map_err(|_| StoreError::LockError).unwrap();
        assert!(v_props.is_empty());
    }

    #[test]
    fn concurrent_drop_property_conflits_with_set_property() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (key, _) = c1.add_vertex(1);
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
    fn concurrent_set_property_conflits_with_drop_property() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (key, _) = c1.add_vertex(1);
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

    // ── drop_element ──────────────────────────────────────────────────────────

    #[test]
    fn tombstoned_vertex_invisible_to_get_vertex() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(1);
        c.drop_element(CanonicalKey::Vertex(key)).unwrap();
        assert!(c.get_vertex(key).unwrap().is_none());
    }

    #[test]
    fn tombstoned_edge_invisible_to_get_edge() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let k = cek(1, 5, 2);
        let _ = c.add_edge(k);
        c.drop_element(CanonicalKey::Edge(k)).unwrap();
        assert!(c.get_edge(k).unwrap().is_none());
    }

    #[test]
    fn set_property_on_tombstoned_vertex_errors() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let (key, _) = c.add_vertex(1);
        c.drop_element(CanonicalKey::Vertex(key)).unwrap();
        let err = c.set_property(CanonicalKey::Vertex(key), SmolStr::new("x"), Primitive::Int32(1));
        assert!(err.is_err());
    }

    #[test]
    fn concurrent_add_edge_then_drop_should_conflict_on_commit() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let mut c2 = ctx(&store);
        let k = cek(1, 5, 2);

        c1.add_edge(k).unwrap();
        c2.add_edge(k).unwrap();

        c1.commit().unwrap();
        c2.drop_element(CanonicalKey::Edge(k)).unwrap();
        let result = c2.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    fn concurrent_drop_vertex_then_add_edge_should_conflict_on_commit() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1);
        let (v2, _) = c1.add_vertex(2);
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);

        let k = cek(v1, 5, v2);
        let _ = c2.get_vertex(v1).unwrap().unwrap();
        let _ = c2.get_vertex(v2).unwrap().unwrap();
        let _ = c3.get_vertex(v1).unwrap().unwrap();

        let _ = c2.add_edge(k).unwrap();
        c3.drop_element(CanonicalKey::Vertex(v1)).unwrap();

        assert!(c3.commit().is_ok(), "c2 should commit successfully");

        let result = c2.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }

    #[test]
    #[ignore = "Wait for fixing the issue that the second committer (c3) does not detect the conflict and commits successfully, which should not happen."]
    fn concurrent_add_edge_then_drop_vertex_should_conflict_on_commit() {
        let (store, _dir) = open();
        let mut c1 = ctx(&store);
        let (v1, _) = c1.add_vertex(1);
        let (v2, _) = c1.add_vertex(2);
        c1.commit().unwrap();

        let mut c2 = ctx(&store);
        let mut c3 = ctx(&store);

        let k = cek(v1, 5, v2);
        let _ = c2.get_vertex(v1).unwrap();
        let _ = c2.get_vertex(v2).unwrap();
        let _ = c3.get_vertex(v1).unwrap();

        let _ = c2.add_edge(k).unwrap();
        c3.drop_element(CanonicalKey::Vertex(v1)).unwrap();

        assert!(c2.commit().is_ok(), "c2 should commit successfully");

        let result = c3.commit();
        assert!(matches!(result, Err(StoreError::Conflict)));
    }
    // ── commit ────────────────────────────────────────────────────────────────

    #[test]
    fn commit_persists_vertex_to_store() {
        let (store, _dir) = open();
        let id = {
            let mut c = ctx(&store);
            let (key, _) = c.add_vertex(7);
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
        let k = cek(10, 3, 20);
        {
            let mut c = ctx(&store);
            let _ = c.add_edge(k);
            c.set_property(CanonicalKey::Edge(k), SmolStr::new("w"), Primitive::Int32(99)).unwrap();
            c.commit().unwrap();
        }

        let edges = store.get_edges(10, Direction::OUT, None, None).unwrap();
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
            let (key, _) = c.add_vertex(1);
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
        let (key, _) = c.add_vertex(1);
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
            let (key, _) = c.add_vertex(1);
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
        let _ = c.add_edge(cek(1, 1, 10));
        let _ = c.add_edge(cek(1, 1, 20));

        let edges = c.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn get_edges_filters_tombstoned_edges() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let _ = c.add_edge(cek(1, 1, 10));
        let _ = c.add_edge(cek(1, 1, 20));
        c.drop_element(CanonicalKey::Edge(cek(1, 1, 10))).unwrap();

        let edges = c.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn get_edges_direction_in_vs_out() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let _ = c.add_edge(cek(1, 1, 2));

        let out = c.get_edges(1, Direction::OUT, None, None).unwrap();
        let in_ = c.get_edges(2, Direction::IN, None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(in_.len(), 1);
        // Vertex 1 has no incoming edges; vertex 2 has no outgoing.
        assert!(c.get_edges(1, Direction::IN, None, None).unwrap().is_empty());
        assert!(c.get_edges(2, Direction::OUT, None, None).unwrap().is_empty());
    }

    #[test]
    fn get_edges_label_filter() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let _ = c.add_edge(cek(1, 1, 10));
        let _ = c.add_edge(cek(1, 2, 20));
        let _ = c.add_edge(cek(1, 1, 30));

        let label1 = c.get_edges(1, Direction::OUT, Some(1), None).unwrap();
        assert_eq!(label1.len(), 2);
        assert!(label1.iter().all(|(ek, _)| ek.label_id == 1));

        let label2 = c.get_edges(1, Direction::OUT, Some(2), None).unwrap();
        assert_eq!(label2.len(), 1);
    }

    #[test]
    fn get_edges_dst_filter() {
        let (store, _dir) = open();
        let mut c = ctx(&store);
        let _ = c.add_edge(cek(1, 1, 10));
        let _ = c.add_edge(cek(1, 1, 20));
        let _ = c.add_edge(cek(1, 1, 30));

        let result = c.get_edges(1, Direction::OUT, None, Some(&[10, 30])).unwrap();
        assert_eq!(result.len(), 2);
        let mut secondaries: Vec<u64> = result.iter().map(|(ek, _)| ek.secondary_id).collect();
        secondaries.sort_unstable();
        assert_eq!(secondaries, vec![10, 30]);
    }

    #[test]
    fn get_edges_merges_committed_and_dirty() {
        let (store, _dir) = open();

        // Commit one edge, then add another in a new context.
        let k1 = cek(1, 1, 10);
        {
            let mut c = ctx(&store);
            let _ = c.add_edge(k1);
            c.commit().unwrap();
        }

        let mut c = ctx(&store);
        let _ = c.add_edge(cek(1, 1, 20));
        let edges = c.get_edges(1, Direction::OUT, None, None).unwrap();
        assert_eq!(edges.len(), 2);
    }

    // ── id_gen shared across contexts ─────────────────────────────────────────

    #[test]
    fn vertex_keys_unique_across_contexts() {
        let (store, _dir) = open();
        let id_gen = Arc::clone(&store.id_gen());
        let mut c1 = GraphContext::<RocksStorage>::new(store.begin(), Arc::clone(&id_gen));
        let mut c2 = GraphContext::<RocksStorage>::new(store.begin(), Arc::clone(&id_gen));
        let (k1, _) = c1.add_vertex(1);
        let (k2, _) = c2.add_vertex(1);
        assert_ne!(k1, k2);
    }
}
