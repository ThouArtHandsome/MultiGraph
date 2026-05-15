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

use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::{engine::data_flow::group_id::GroupId, types::GValue};

/// The unit of work that flows between steps in a traversal pipeline.
///
/// * `labels` holds the `as(…)` labels active at the current step; the traversal engine uses them to build
///   label→position maps for `select()` and `path()` evaluation.  `None` when no labels are attached. Stack-allocates
///   up to 2 labels without heap allocation.
///
/// * `parent` is a back-pointer to the traverser at the previous step, forming a persistent tree (child → parent).
///   Following the chain and collecting `(value, labels)` pairs reconstructs the full traversal history. Allocated only
///   when path tracking is active.
#[derive(Debug, Clone)]
pub struct Traverser {
    /// The current value carried by this traverser.
    pub value: GValue,
    /// Back-pointer to the spawning traverser — `Some` only when path tracking is active.
    pub parent: Option<Arc<Traverser>>,
    // which group this traverser belongs to, only used data_flow engine to track group membership for grouping and
    // co-grouping steps
    pub group_id: GroupId,
    /// Labels assigned to the current step via `as(…)`.  `None` = no labels.
    pub labels: Option<SmallVec<[SmolStr; 2]>>,
}

impl Traverser {
    pub fn new(value: GValue) -> Self {
        Self { value, labels: None, parent: None, group_id: GroupId::noop() }
    }

    /// Collect the full traversal history as `(value, labels)` pairs,
    /// oldest entry first (including the current traverser).
    pub fn collect_path(&self) -> Vec<(GValue, Option<&SmallVec<[SmolStr; 2]>>)> {
        let mut entries: Vec<(GValue, Option<&SmallVec<[SmolStr; 2]>>)> =
            vec![(self.value.clone(), self.labels.as_ref())];
        let mut cur = self.parent.as_deref();
        while let Some(ancestor) = cur {
            entries.push((ancestor.value.clone(), ancestor.labels.as_ref()));
            cur = ancestor.parent.as_deref();
        }
        entries.reverse();
        entries
    }
}
