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

//! Per-query transaction with a three-level read stack and delta-based writes.
//!
//! # Read path (fastest → slowest)
//!
//! ```text
//! dirty            ← mutations this query (overlaid on baseline)
//! read_buffer      ← committed baseline fetched this query (L1 cache)
//! SharedStoreCache ← process-wide LRU of committed elements (L2 cache)
//! RocksDB          ← authoritative on-disk store
//! ```
//!
//! # Write path
//!
//! All mutations are recorded in `dirty` as fine-grained `DirtyEntry` deltas
//! and never touch the store or cache until `commit` is called.
//!
//! # Commit
//!
//! `commit` flushes all dirty entries to the store in two phases:
//! 1. Load any missing committed baselines needed for `Existing`+mutations
//!    entries (requires only a read lock on the store).
//! 2. Write all changes under a write lock, then evict affected entries from
//!    `SharedStoreCache`.
//!
//! Note: individual store calls within a commit are not cross-entity atomic.
//! A crash between two calls can leave the store in a partially-written state.
//! Full write-batch atomicity is a planned future improvement.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use smol_str::SmolStr;

use crate::storage::cache::SharedStoreCache;
use crate::storage::graph_store::GraphStorage;
use crate::transaction::dirty_cache::{DirtyEntry, ExistenceMutation, PropMutation};
use crate::transaction::id_gen::IdGen;
use crate::transaction::overlay;
use crate::types::{
    Direction, EdgeKey, ElementKey, FullEdge, FullElement, FullVertex, LabelId, StorageError,
    VertexKey,
};
use crate::types::gvalue::Primitive;
use crate::types::prop_key::PropKey;

// ── Transaction ───────────────────────────────────────────────────────────────

/// Per-query transaction.
///
/// Parameterised over the storage backend `S` so tests can inject an
/// in-memory store without touching RocksDB.
pub struct Transaction<S: GraphStorage + Send + Sync> {
    /// Mutations applied during this query, keyed by canonical `ElementKey`.
    /// Edge keys are always in `Out`-direction canonical form.
    dirty: HashMap<ElementKey, DirtyEntry>,

    /// Committed baseline records fetched during this query.
    /// Acts as L1 in front of the process-wide `SharedStoreCache`.
    read_buffer: HashMap<ElementKey, Arc<FullElement>>,

    store: Arc<RwLock<S>>,
    cache: Arc<SharedStoreCache>,
    id_gen: Arc<IdGen>,
}

impl<S: GraphStorage + Send + Sync> Transaction<S> {
    pub fn new(store: Arc<RwLock<S>>, cache: Arc<SharedStoreCache>, id_gen: Arc<IdGen>) -> Self {
        Self {
            dirty: HashMap::new(),
            read_buffer: HashMap::new(),
            store,
            cache,
            id_gen,
        }
    }

    // ── Mutation methods ──────────────────────────────────────────────────────

    /// Create a new vertex with the given string label.
    /// Returns the newly assigned `VertexKey`.
    pub fn create_vertex(&mut self, label: impl Into<SmolStr>) -> VertexKey {
        let id = self.id_gen.next_vertex_id();
        self.dirty.insert(
            ElementKey::Vertex(id),
            DirtyEntry { existence: ExistenceMutation::NewVertex(label.into()), props: HashMap::new() },
        );
        id
    }

    /// Register a new edge.  `key` may be in any direction; it is
    /// canonicalised to `Out`-form before storage.
    pub fn create_edge(&mut self, key: EdgeKey) {
        let canonical = key.canonical();
        self.dirty.insert(
            ElementKey::Edge(canonical),
            DirtyEntry { existence: ExistenceMutation::NewEdge, props: HashMap::new() },
        );
    }

    /// Set a property on a vertex or edge.
    ///
    /// If the element has not been touched by this transaction before, an
    /// `Existing` dirty entry is created so the prop delta is recorded.
    /// Edge keys are canonicalised automatically.
    ///
    /// The caller is responsible for ensuring the element exists; this method
    /// records the delta unconditionally.
    pub fn set_property(
        &mut self,
        element: ElementKey,
        prop: PropKey,
        value: Primitive,
    ) {
        let ek = canonicalise_key(element);
        self.dirty
            .entry(ek)
            .or_insert_with(|| DirtyEntry {
                existence: ExistenceMutation::Existing,
                props: HashMap::new(),
            })
            .props
            .insert(prop, PropMutation::Set(value));
    }

    /// Remove a property from a vertex or edge.
    /// Edge keys are canonicalised automatically.
    pub fn remove_property(&mut self, element: ElementKey, prop: PropKey) {
        let ek = canonicalise_key(element);
        self.dirty
            .entry(ek)
            .or_insert_with(|| DirtyEntry {
                existence: ExistenceMutation::Existing,
                props: HashMap::new(),
            })
            .props
            .insert(prop, PropMutation::Removed);
    }

    /// Mark a vertex as deleted in this transaction.
    pub fn delete_vertex(&mut self, key: VertexKey) {
        self.dirty.insert(
            ElementKey::Vertex(key),
            DirtyEntry { existence: ExistenceMutation::Tombstone, props: HashMap::new() },
        );
    }

    /// Mark an edge as deleted in this transaction.
    /// The key is canonicalised to `Out`-form.
    pub fn delete_edge(&mut self, key: EdgeKey) {
        self.dirty.insert(
            ElementKey::Edge(key.canonical()),
            DirtyEntry { existence: ExistenceMutation::Tombstone, props: HashMap::new() },
        );
    }

    // ── Read methods ──────────────────────────────────────────────────────────

    /// Fetch a vertex, overlaying any dirty mutations on top of the committed
    /// baseline.  Returns `None` if the vertex does not exist or has been
    /// deleted in this transaction.
    pub fn get_vertex_view(
        &mut self,
        key: VertexKey,
    ) -> Result<Option<Arc<FullVertex>>, StorageError> {
        let ek = ElementKey::Vertex(key);

        if let Some(entry) = self.dirty.get(&ek).cloned() {
            return match entry.existence {
                ExistenceMutation::Tombstone => Ok(None),
                ExistenceMutation::NewVertex(ref label) => {
                    Ok(Some(Arc::new(overlay::build_new_vertex(key, label.clone(), &entry))))
                }
                ExistenceMutation::Existing => {
                    let base = self.fetch_vertex_baseline(key)?;
                    Ok(base.map(|fv| Arc::new(overlay::merge_vertex(&fv, &entry))))
                }
                ExistenceMutation::NewEdge => unreachable!("vertex key maps to edge entry"),
            };
        }

        self.fetch_vertex_baseline(key)
    }

    /// Fetch multiple vertices, overlaying dirty mutations.
    /// Missing or tombstoned vertices are silently omitted.
    pub fn get_vertices_view(
        &mut self,
        keys: &[VertexKey],
    ) -> Result<Vec<Arc<FullVertex>>, StorageError> {
        let mut result = Vec::with_capacity(keys.len());
        for &key in keys {
            if let Some(fv) = self.get_vertex_view(key)? {
                result.push(fv);
            }
        }
        Ok(result)
    }

    /// Fetch all edges incident to `vertex` in `direction`, overlaying dirty
    /// mutations (new edges added, tombstoned edges removed, prop deltas applied).
    ///
    /// `label` and `dst` are forwarded to the store scan and also applied when
    /// evaluating dirty new edges.
    pub fn get_edges_view(
        &mut self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<Arc<FullEdge>>, StorageError> {
        // ── Committed edges from store ────────────────────────────────────────
        let store_clone = Arc::clone(&self.store);
        let committed = store_clone.read().get_edges(vertex, direction, label, dst)?;

        // Key by canonical EdgeKey so dirty lookups align.
        // Store the Arc directly; the edge's own `key` carries the direction.
        let mut result_map: HashMap<EdgeKey, Arc<FullEdge>> = committed
            .into_iter()
            .map(|fe| (fe.key.canonical(), fe))
            .collect();

        // ── Overlay dirty mutations ───────────────────────────────────────────
        let dst_set: Option<HashSet<VertexKey>> = dst.map(|k| k.iter().copied().collect());

        // Collect matching dirty edge entries first to avoid borrow conflicts.
        let dirty_edges: Vec<(EdgeKey, DirtyEntry)> = self
            .dirty
            .iter()
            .filter_map(|(ek, entry)| {
                if let ElementKey::Edge(k) = ek { Some((*k, entry.clone())) } else { None }
            })
            .filter(|(k, _)| edge_matches(*k, vertex, direction, label, dst_set.as_ref()))
            .collect();

        for (canonical_key, entry) in dirty_edges {
            let view_key = match direction {
                Direction::OUT => canonical_key,
                Direction::IN => canonical_key.flip(),
            };

            match entry.existence {
                ExistenceMutation::Tombstone => {
                    result_map.remove(&canonical_key);
                }
                ExistenceMutation::NewEdge => {
                    let fe = Arc::new(overlay::build_new_edge(view_key, &entry));
                    result_map.insert(canonical_key, fe);
                }
                ExistenceMutation::Existing if !entry.props.is_empty() => {
                    if let Some(base_fe) = result_map.get(&canonical_key).cloned() {
                        let merged = overlay::merge_edge(&base_fe, &entry, base_fe.key);
                        result_map.insert(canonical_key, Arc::new(merged));
                    }
                    // If the edge wasn't in the committed result (filtered by
                    // label/dst), it doesn't belong in this result either.
                }
                _ => {}
            }
        }

        Ok(result_map.into_values().collect())
    }

    // ── Commit / rollback ─────────────────────────────────────────────────────

    /// Flush all dirty mutations to the store and invalidate affected cache
    /// entries, then reset the transaction to a clean state.
    ///
    /// The commit happens in two phases to avoid holding a write lock while
    /// reading baselines:
    /// 1. Load missing committed baselines under a read lock.
    /// 2. Write all changes under a write lock, then evict cache entries.
    pub fn commit(&mut self) -> Result<(), StorageError> {
        // ── Phase 1: pre-load baselines for Existing+mutations entries ────────
        //
        // Clone Arc so phase-1 reads don't borrow self.store in a way that
        // would conflict with self.read_buffer access.
        {
            let store_clone = Arc::clone(&self.store);
            let store_read = store_clone.read();

            for (ek, entry) in &self.dirty {
                if !matches!(entry.existence, ExistenceMutation::Existing)
                    || entry.props.is_empty()
                {
                    continue;
                }
                if self.read_buffer.contains_key(ek) {
                    continue;
                }
                let fetched: Option<Arc<FullElement>> = match ek {
                    ElementKey::Vertex(id) => store_read
                        .get_vertex(*id)?
                        .map(|fv| Arc::new(FullElement::Vertex(fv))),
                    ElementKey::Edge(k) => store_read
                        .get_edge(*k)?
                        .map(|fe| Arc::new(FullElement::Edge(fe))),
                };
                if let Some(elem) = fetched {
                    self.read_buffer.insert(ek.clone(), elem);
                }
            }
        }

        // ── Phase 2: write all dirty entries to store ─────────────────────────
        {
            let store_arc = Arc::clone(&self.store);
            let mut store = store_arc.write();

            for (ek, entry) in &self.dirty {
                match (&entry.existence, ek) {
                    // ── Tombstones ────────────────────────────────────────────
                    (ExistenceMutation::Tombstone, ElementKey::Vertex(id)) => {
                        store.delete_vertex(*id)?;
                    }
                    (ExistenceMutation::Tombstone, ElementKey::Edge(k)) => {
                        store.delete_edge(*k)?;
                    }

                    // ── New elements ──────────────────────────────────────────
                    (ExistenceMutation::NewVertex(label), ElementKey::Vertex(id)) => {
                        let fv = overlay::build_new_vertex(*id, label.clone(), entry);
                        store.insert_vertices(&[fv])?;
                    }
                    (ExistenceMutation::NewEdge, ElementKey::Edge(k)) => {
                        let fe = overlay::build_new_edge(*k, entry);
                        store.insert_edges(&[fe])?;
                    }

                    // ── Existing elements with prop mutations ─────────────────
                    (ExistenceMutation::Existing, _) if !entry.props.is_empty() => {
                        match (ek, self.read_buffer.get(ek)) {
                            (ElementKey::Vertex(_id), Some(elem)) => {
                                if let FullElement::Vertex(base) = elem.as_ref() {
                                    let fv = overlay::merge_vertex(base, entry);
                                    store.insert_vertices(&[fv])?;
                                }
                            }
                            (ElementKey::Edge(k), Some(elem)) => {
                                if let FullElement::Edge(base) = elem.as_ref() {
                                    // Commit always uses canonical (OUT) key.
                                    let fe = overlay::merge_edge(base, entry, *k);
                                    store.insert_edges(&[fe])?;
                                }
                            }
                            _ => {} // baseline missing — element may have been deleted
                        }
                    }

                    _ => {} // Existing with no prop changes — nothing to write
                }
            }
        }

        // ── Phase 3: evict dirty keys from shared cache ───────────────────────
        for ek in self.dirty.keys() {
            self.cache.remove(ek);
        }

        self.dirty.clear();
        self.read_buffer.clear();
        Ok(())
    }

    /// Discard all uncommitted mutations.
    pub fn rollback(&mut self) {
        self.dirty.clear();
        self.read_buffer.clear();
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Walk read_buffer → SharedStoreCache → store for a committed vertex.
    /// Populates read_buffer and cache on cache/store hits.
    fn fetch_vertex_baseline(
        &mut self,
        key: VertexKey,
    ) -> Result<Option<Arc<FullVertex>>, StorageError> {
        let ek = ElementKey::Vertex(key);

        // L1: read_buffer
        if let Some(elem) = self.read_buffer.get(&ek) {
            if let FullElement::Vertex(fv) = elem.as_ref() {
                return Ok(Some(Arc::clone(fv)));
            }
        }

        // L2: process-wide cache
        if let Some(elem) = self.cache.get(&ek) {
            if let FullElement::Vertex(fv) = elem.as_ref() {
                self.read_buffer.insert(ek, Arc::clone(&elem));
                return Ok(Some(Arc::clone(fv)));
            }
        }

        // L3: store
        let store_clone = Arc::clone(&self.store);
        let fv_opt = store_clone.read().get_vertex(key)?;
        if let Some(ref fv) = fv_opt {
            let elem = Arc::new(FullElement::Vertex(Arc::clone(fv)));
            self.cache.insert(ek.clone(), Arc::clone(&elem));
            self.read_buffer.insert(ek, elem);
        }
        Ok(fv_opt)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Canonicalise an `EdgeKey` inside an `ElementKey` to `Out`-direction form.
fn canonicalise_key(ek: ElementKey) -> ElementKey {
    match ek {
        ElementKey::Edge(k) => ElementKey::Edge(k.canonical()),
        other => other,
    }
}

/// Return `true` if the canonical OUT-form edge key `k` matches a
/// `get_edges`-style query `(vertex, direction, label, dst_set)`.
fn edge_matches(
    k: EdgeKey,           // canonical OUT form
    vertex: VertexKey,
    direction: Direction,
    label: Option<LabelId>,
    dst_set: Option<&HashSet<VertexKey>>,
) -> bool {
    // The primary vertex for this direction.
    let primary = match direction {
        Direction::OUT => k.primary_id,   // OUT: primary = src
        Direction::IN => k.secondary_id,  // IN:  secondary = dst (in OUT form)
    };
    if primary != vertex {
        return false;
    }
    if let Some(lbl) = label {
        if k.label_id != lbl {
            return false;
        }
    }
    if let Some(set) = dst_set {
        let remote = match direction {
            Direction::OUT => k.secondary_id,
            Direction::IN => k.primary_id,
        };
        if !set.contains(&remote) {
            return false;
        }
    }
    true
}

