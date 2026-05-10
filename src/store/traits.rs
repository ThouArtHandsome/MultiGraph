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

//! Store-layer trait contracts.
//!
//! # Layer structure
//!
//! ```text
//! Gremlin Traversal Engine
//!   │  talks only to GraphContext via inherent methods
//!   ▼
//! GraphContext<S: GraphStore>       ← query-scoped ground truth
//!   │  owns the element overlay (VertexIdx / EdgeIdx)
//!   │  merges committed + dirty state
//!   │  forwards to S::Txn on commit
//!   ▼
//! GraphTransaction                  ← store-layer contract
//!   reads:   get_vertex / get_edge / get_edges
//!   writes:  put_vertex / put_edge / delete_vertex / delete_edge
//!   control: commit / abort
//!
//! GraphStore
//!   begin()  → fresh GraphTransaction
//!   id_gen() → shared VertexKey allocator
//! ```
//!
//! The engine never imports `GraphTransaction` or `GraphStore` directly —
//! it only touches `GraphContext`.  Backend details (RocksDB CFs, OCC,
//! encoding) never cross the `GraphTransaction` boundary.

use std::sync::Arc;

use crate::store::id_gen::IdGen;
use crate::types::gvalue::Property;
use crate::types::{CanonicalEdgeKey, Direction, FullEdge, FullVertex, LabelId, StoreError, VertexKey};

// ── GraphTransaction ──────────────────────────────────────────────────────────

/// A single I/O transaction against the persistent graph store.
///
/// `GraphContext` is the only caller.  The engine never holds a
/// `GraphTransaction` directly — it always works through `GraphContext`.
///
/// # Read semantics
///
/// Reads return `Arc<Full...>` so the shared process-wide vertex cache can
/// hand out cheap clones without copying property data.  `GraphContext`
/// stores the Arcs in its overlay; on mutation it uses `Arc::make_mut` for
/// copy-on-write semantics.
///
/// # Write semantics
///
/// Writes are raw: `GraphContext` passes explicit keys (allocated via
/// `GraphStore::id_gen`) and full property lists.  The transaction stages
/// them and flushes atomically on `commit`.
pub trait GraphTransaction {
    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Fetch a committed vertex; `None` if absent.
    ///
    /// Implementations should register the key in an OCC read-set so that a
    /// concurrent write detected at commit time returns `StoreError::Conflict`.
    fn get_vertex(&mut self, key: VertexKey) -> Result<Option<Arc<FullVertex>>, StoreError>;

    /// Fetch a committed edge by canonical key; `None` if absent.
    fn get_edge(&mut self, key: CanonicalEdgeKey) -> Result<Option<Arc<FullEdge>>, StoreError>;

    /// Scan committed edges incident to `vertex` in `direction`.
    ///
    /// - `label`: restrict to edges with this label id when `Some`.
    /// - `dst`:   restrict to edges whose remote endpoint is in the slice when `Some`.
    fn get_edges(
        &mut self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<Arc<FullEdge>>, StoreError>;

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Upsert a vertex record with explicit key, label, and full property list.
    fn put_vertex(&mut self, key: VertexKey, label_id: LabelId, props: &[Property]) -> Result<(), StoreError>;

    /// Upsert an edge record (both physical CF entries) with its property list.
    fn put_edge(&mut self, key: CanonicalEdgeKey, props: &[Property]) -> Result<(), StoreError>;

    /// Delete a vertex record.
    fn delete_vertex(&mut self, key: VertexKey) -> Result<(), StoreError>;

    /// Delete an edge record (both physical CF entries).
    fn delete_edge(&mut self, key: CanonicalEdgeKey) -> Result<(), StoreError>;

    // ── Control ───────────────────────────────────────────────────────────────

    /// Flush all staged writes atomically.
    ///
    /// Returns `StoreError::Conflict` on OCC conflict; the caller must retry
    /// the entire traversal from scratch with a new `GraphContext`.
    fn commit(&mut self) -> Result<(), StoreError>;

    /// Discard all staged writes and reset the transaction.
    fn abort(&mut self);
}

// ── GraphStore ────────────────────────────────────────────────────────────────

/// A pluggable graph store backend.
///
/// Implementations include `RocksStorage` (local) and future distributed
/// backends.  The engine (and `GraphContext`) is generic over `S: GraphStore`
/// and never imports concrete backend types.
pub trait GraphStore {
    /// The concrete transaction type produced by this store.
    type Txn: GraphTransaction;

    /// Begin a fresh transaction.
    fn begin(&self) -> Self::Txn;

    /// Return the shared vertex-ID allocator.
    ///
    /// `GraphContext` uses this to assign keys to newly created vertices
    /// before staging them via `put_vertex`.
    fn id_gen(&self) -> Arc<IdGen>;
}
