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

// hierarchical group identity
// carried by every traverser in the stream
// depth-1: single flat group id
// depth-2: (outer_id, inner_id)
// depth-N: Vec of ids, one per nesting level

use smallvec::{smallvec, SmallVec};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct GroupId {
    path: SmallVec<[u32; 4]>, // u32 per level, inline for depth <= 4
}

impl GroupId {
    pub fn new(root: u32) -> GroupId {
        GroupId { path: smallvec![root] }
    }

    pub fn child(&self, child_id: u32) -> GroupId {
        let mut path = self.path.clone();
        path.push(child_id);
        GroupId { path }
    }

    pub fn parent(&self) -> Option<GroupId> {
        if self.path.len() <= 1 {
            return None;
        }
        let mut path = self.path.clone();
        path.pop();
        Some(GroupId { path })
    }

    pub fn depth(&self) -> usize {
        self.path.len()
    }

    pub fn inner_id(&self) -> u32 {
        *self.path.last().unwrap()
    }

    pub fn noop() -> GroupId {
        GroupId { path: smallvec![] }
    }
}
