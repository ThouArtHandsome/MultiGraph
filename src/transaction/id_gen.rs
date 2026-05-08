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

use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::VertexKey;

/// Process-wide monotonic vertex-ID generator.
///
/// Shared across all concurrent transactions via `Arc<IdGen>`.  IDs are
/// allocated with `Relaxed` ordering — the generator only needs to guarantee
/// uniqueness, not happens-before with other memory operations.
///
/// In a distributed deployment this would be replaced by a range-lease or
/// Snowflake-style allocator; the interface is intentionally narrow so that
/// swap-out is mechanical.
pub struct IdGen {
    next: AtomicU64,
}

impl IdGen {
    /// Create a new generator whose first issued ID is `start`.
    pub fn new(start: VertexKey) -> Self {
        Self { next: AtomicU64::new(start) }
    }

    /// Return a unique `VertexKey` and advance the counter.
    pub fn next_vertex_id(&self) -> VertexKey {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}
