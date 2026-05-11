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
//! ```
//!
//! The engine never imports `GraphTransaction` or `GraphStore` directly —
//! it only touches `GraphContext`.  Backend details (RocksDB CFs, OCC,
//! encoding) never cross the `GraphTransaction` boundary.

use std::sync::Arc;

use crate::types::{gvalue::Property, CanonicalEdgeKey, Direction, Edge, LabelId, StoreError, Vertex, VertexKey};

// ── GraphTransaction ──────────────────────────────────────────────────────────

/// A single I/O transaction against the persistent graph store.
///
/// `GraphContext` is the only caller.  The engine never holds a
/// `GraphTransaction` directly — it always works through `GraphContext`.
///
/// # Read semantics
///
/// Reads return `Arc<Full...>` so the engine can easily hold onto references.
/// `GraphContext` stores the Arcs in its overlay; on mutation it acquires a write
/// lock on the properties `RwLock` for in-place updates.
///
/// # Write semantics
///
/// Writes are purely physical: `GraphTransaction` writes exactly what it is told
/// and operates on individual records. It does not enforce graph consistency
/// (e.g., maintaining matching Out and In edge records, updating vertex edge
/// counts, or checking for dangling edges). That graph-level consistency is
/// strictly the responsibility of `GraphContext`.
pub trait GraphTransaction {
    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Fetch a committed vertex; `None` if absent.
    ///
    /// Implementations should register the key in an OCC read-set so that a
    /// concurrent write detected at commit time returns `StoreError::Conflict`.
    // TODO: Consider adding a batch `get_vertices` method to optimize bulk property retrieval.
    // Currently, `get_vertex` is used both for property loading and existence checking.
    // A batch API would be great for data fetching, but returning a partial result set makes it
    // inconvenient for strict existence checks. Needs further design consideration.
    fn get_vertex(&mut self, key: VertexKey) -> Result<Option<Arc<Vertex>>, StoreError>;

    /// Fetch a single committed edge record for the given direction; `None` if absent.
    fn get_edge(&mut self, key: CanonicalEdgeKey, direction: Direction) -> Result<Option<Arc<Edge>>, StoreError>;

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
    ) -> Result<Vec<Arc<Edge>>, StoreError>;

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Upsert a vertex record with explicit key, label, and full property list.
    fn put_vertex(
        &mut self,
        key: VertexKey,
        label_id: LabelId,
        out_e_cnt: u32,
        in_e_cnt: u32,
        props: &[Property],
    ) -> Result<(), StoreError>;

    /// Upsert a single edge record in the specified physical direction index.
    fn put_edge(&mut self, key: CanonicalEdgeKey, direction: Direction, props: &[Property]) -> Result<(), StoreError>;

    /// Delete a vertex record.
    fn delete_vertex(&mut self, key: VertexKey) -> Result<(), StoreError>;

    /// Delete a single edge record from the specified physical direction index.
    fn delete_edge(&mut self, key: CanonicalEdgeKey, direction: Direction) -> Result<(), StoreError>;

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
}
