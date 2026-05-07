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

use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::RwLock;

use crate::types::{ElementKey, FullElement};

// ── SharedStoreCache ──────────────────────────────────────────────────────────

/// Process-wide, sharded LRU cache for committed graph elements.
///
/// # Design
///
/// * Only holds committed, immutable `FullVertex`/`FullEdge` records —
///   in-flight mutations live in `Transaction::dirty` and are never inserted
///   here.
/// * Sharded to reduce lock contention under concurrent read-heavy queries.
/// * Each shard uses an `RwLock` so multiple queries can read the same shard
///   concurrently; a write lock is acquired only on insert or eviction.
/// * On commit, the engine calls `remove` for each element touched by
///   `Transaction::dirty`, ensuring stale entries are not served after flush.
/// * `Arc` pins a `FullElement` alive even after its LRU shard evicts it —
///   traversers that already hold a reference continue to see a consistent
///   snapshot until they drop it.
pub struct SharedStoreCache {
    shards: Vec<RwLock<LruCache<ElementKey, Arc<FullElement>>>>,
}

impl SharedStoreCache {
    /// Creates a cache with `shard_count` shards, each holding up to
    /// `shard_capacity` entries.
    pub fn new(shard_count: usize, shard_capacity: NonZeroUsize) -> Self {
        let shards = (0..shard_count).map(|_| RwLock::new(LruCache::new(shard_capacity))).collect();
        Self { shards }
    }

    fn shard_index(&self, key: &ElementKey) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.shards.len()
    }

    /// Look up an element.  Acquires a read lock; promotes the entry to MRU
    /// under a write lock only if found (LRU update requires exclusive access).
    pub fn get(&self, key: &ElementKey) -> Option<Arc<FullElement>> {
        let idx = self.shard_index(key);
        self.shards[idx].write().get(key).cloned()
    }

    /// Insert a committed element record.
    pub fn insert(&self, key: ElementKey, value: Arc<FullElement>) {
        let idx = self.shard_index(&key);
        self.shards[idx].write().put(key, value);
    }

    /// Evict a single entry (called by the commit path after flushing dirty data).
    pub fn remove(&self, key: &ElementKey) {
        let idx = self.shard_index(key);
        self.shards[idx].write().pop(key);
    }
}
