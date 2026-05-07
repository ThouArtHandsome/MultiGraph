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

use std::collections::HashMap;
use std::sync::Arc;

use crate::transaction::dirty_cache::DirtyEntry;
use crate::types::{ElementKey, FullElement};

/// Per-query transaction cache.
///
/// Private to a single query — never shared between concurrent queries.
/// Committed atomically to RocksDB via a write batch; dropped on rollback.
///
/// # Two-layer structure
///
/// ```text
/// dirty        — mutations this query (overlaid on baseline at read time)
/// read_buffer  — committed baseline fetched this query
/// ```
///
/// On read, the engine looks in `dirty` first (for mutations), then
/// `read_buffer` (for the clean baseline), then the process-wide
/// `SharedStoreCache`, then RocksDB.  Fetched records are stored in
/// `read_buffer` so subsequent reads in the same query are free.
///
/// On commit, `dirty` is flushed as a RocksDB write batch and the
/// corresponding `SharedStoreCache` shards are invalidated.
#[derive(Debug, Default)]
pub struct Transaction {
    /// Mutations applied during this query, keyed by canonical `ElementKey`.
    /// Edge keys must be in `Out`-direction canonical form.
    pub dirty: HashMap<ElementKey, DirtyEntry>,

    /// Committed baseline records fetched during this query.
    /// Acts as L1 in front of the process-wide `SharedStoreCache`.
    pub read_buffer: HashMap<ElementKey, Arc<FullElement>>,
}
