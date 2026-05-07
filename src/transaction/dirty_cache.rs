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

use crate::types::{LabelId, Primitive, PropKey};

// ── Existence mutation ────────────────────────────────────────────────────────

/// What happened to the element's existence within this transaction.
#[derive(Debug, Clone)]
pub enum ExistenceMutation {
    /// Element was created in this transaction; carries the assigned label.
    New(LabelId),
    /// Element already existed in storage before this transaction.
    Existing,
    /// Element has been deleted in this transaction.
    Tombstone,
}

// ── Property mutation ─────────────────────────────────────────────────────────

/// The change applied to a single property key within this transaction.
#[derive(Debug, Clone)]
pub enum PropMutation {
    /// Property was set (created or overwritten) to this value.
    Set(Primitive),
    /// Property was explicitly removed.
    Removed,
}

// ── DirtyEntry ────────────────────────────────────────────────────────────────

/// Uncommitted state for one graph element (vertex or edge).
///
/// Keyed by `ElementKey` in `Transaction::dirty`.  A single struct covers
/// both element types because their delta structure is identical.
#[derive(Debug, Clone)]
pub struct DirtyEntry {
    pub existence: ExistenceMutation,
    /// Delta map — only keys touched in this transaction are present.
    /// Merged with the `FullVertex`/`FullEdge` baseline at read time.
    pub props: HashMap<PropKey, PropMutation>,
}
