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

use crate::traversal::group_id::GroupId;
use crate::traversal::iterators::traits::Step;
use crate::traversal::traverser::Traverser;

pub struct WhereExitStep {
    pub origin: Option<Box<dyn Step>>,
    pub sub: Option<Box<dyn Step>>,
    origin_group: HashMap<GroupId, Traverser>,
    sub_group: HashMap<GroupId, bool>,
}

impl WhereExitStep {
    pub fn new() -> Self {
        Self { origin: None, sub: None, origin_group: HashMap::new(), sub_group: HashMap::new() }
    }
}

impl Step for WhereExitStep {
    fn next(&mut self) -> Option<Traverser> {
        loop {
            if let Some(t_org) = self.origin.as_mut().unwrap().next() {
                let gid = t_org.group_id.clone();
                self.origin_group.insert(gid.clone(), t_org.clone());

                // In a true synchronized DAG, we pull from the sub pipeline to evaluate criteria
                if let Some(t_sub) = self.sub.as_mut().unwrap().next() {
                    self.sub_group.insert(t_sub.group_id.clone(), true);
                }

                if let Some(&matched) = self.sub_group.get(&gid) {
                    if matched {
                        self.origin_group.remove(&gid);
                        return Some(t_org);
                    }
                }
            } else {
                return None;
            }
        }
    }

    fn add_upper(&mut self, upstream: Box<dyn Step>) {
        if self.origin.is_none() {
            self.origin = Some(upstream);
        } else if self.sub.is_none() {
            self.sub = Some(upstream);
        } else {
            panic!("WhereExitStep can only accept exactly two upstream iterators.");
        }
    }
}
