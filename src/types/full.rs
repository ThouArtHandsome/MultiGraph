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

use smol_str::SmolStr;

use crate::types::element::{EdgeKey, VertexKey};
use crate::types::gvalue::Property;

// ── FullVertex / FullEdge ─────────────────────────────────────────────────────

/// The single authoritative in-process copy of a vertex's committed state.
///
/// Lives in the `Transaction::read_buffer` (per-query) and/or the
/// `SharedStoreCache` (process-wide).  Never mutated after insertion.
#[derive(Debug, Clone)]
pub struct FullVertex {
    pub id: VertexKey,
    pub label: SmolStr,
    pub props: Vec<Property>,
}

/// The single authoritative in-process copy of an edge's committed state.
///
/// `key` is in canonical (`Out`) direction — the `In` view is derived on
/// demand via `EdgeKey::flip`.
#[derive(Debug, Clone)]
pub struct FullEdge {
    pub key: EdgeKey,
    pub props: Vec<Property>,
}

// ── FullElement ───────────────────────────────────────────────────────────────

/// Union of `FullVertex` and `FullEdge` for heterogeneous caches.
///
/// The `Transaction::read_buffer` and `SharedStoreCache` both store
/// `Arc<FullElement>` keyed by `ElementKey`.  The inner `Arc<FullVertex>` /
/// `Arc<FullEdge>` can be cloned cheaply and handed to `GValue` handles
/// without copying or keeping the whole `FullElement` alive indirectly.
#[derive(Debug, Clone)]
pub enum FullElement {
    Vertex(Arc<FullVertex>),
    Edge(Arc<FullEdge>),
}
