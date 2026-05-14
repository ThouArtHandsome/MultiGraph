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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use crate::graph::LogicalGraph;
use crate::store::RocksStorage;
use crate::traversal::Traverser;

/// The core contract for a synchronous pull-based iterator pipeline.
pub trait Step {
    fn next(&mut self) -> Option<Traverser>;

    /// Wires an upstream iterator into this step.
    fn add_upper(&mut self, _upstream: Box<dyn Step>) {
        panic!("add_upper not supported or implemented for this step");
    }
}

/// The contract for a multi-consumer broadcast iterator.
pub trait BroadcastStep {
    fn next_for_branch(&mut self, branch_id: usize) -> Option<Traverser>;
}

// ── Broadcast (Tee) Pattern ──────────────────────────────────────────────────

pub struct BroadcastIter {
    pub source: Option<Box<dyn Step>>,
    pub buffer: VecDeque<Traverser>,
    // how many items each consumer has read so far
    pub cursors: Vec<usize>,
}

impl BroadcastIter {
    pub fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self { source: None, buffer: VecDeque::new(), cursors: Vec::new() }))
    }

    pub fn add_upper(&mut self, source: Box<dyn Step>) {
        self.source = Some(source);
    }

    pub fn subscribe(rc: &Rc<RefCell<Self>>) -> ConsumerIter<Self> {
        let mut this = rc.borrow_mut();
        let id = this.cursors.len();
        this.cursors.push(0); // Initialize a new cursor at the current read point
        ConsumerIter { broadcast: rc.clone(), id }
    }
}

impl BroadcastStep for BroadcastIter {
    fn next_for_branch(&mut self, branch_id: usize) -> Option<Traverser> {
        let pos = self.cursors[branch_id];

        // if this consumer has caught up to buffer end, fetch from source
        if pos >= self.buffer.len() {
            match self.source.as_mut().unwrap().next() {
                Some(item) => self.buffer.push_back(item),
                None => return None,
            }
        }

        let item = self.buffer[pos].clone();
        self.cursors[branch_id] += 1;

        // evict items all consumers have passed
        let min_cursor = *self.cursors.iter().min().unwrap();
        for _ in 0..min_cursor {
            self.buffer.pop_front();
        }
        // adjust all cursors after eviction
        for c in &mut self.cursors {
            *c -= min_cursor;
        }

        Some(item)
    }
}

pub struct ConsumerIter<T: BroadcastStep> {
    pub broadcast: Rc<RefCell<T>>,
    pub id: usize, // which consumer am I
}

impl<T: BroadcastStep> Step for ConsumerIter<T> {
    fn next(&mut self) -> Option<Traverser> {
        self.broadcast.borrow_mut().next_for_branch(self.id)
    }
}

// ── Gather Pattern ───────────────────────────────────────────────────────────

pub struct GatherIter {
    pub sub_plans: Vec<Box<dyn Step>>,
    pub current_idx: usize,
    pub active_branches: usize,
}

impl GatherIter {
    pub fn new() -> Self {
        Self { sub_plans: Vec::new(), current_idx: 0, active_branches: 0 }
    }
}

impl Step for GatherIter {
    fn next(&mut self) -> Option<Traverser> {
        while self.active_branches > 0 {
            if let Some(t) = self.sub_plans[self.current_idx].next() {
                // Round-robin execution
                self.current_idx = (self.current_idx + 1) % self.sub_plans.len();
                return Some(t);
            } else {
                // Sub-plan exhausted
                self.sub_plans.remove(self.current_idx);
                self.active_branches -= 1;
                if self.active_branches > 0 {
                    self.current_idx %= self.active_branches;
                }
            }
        }
        None
    }

    fn add_upper(&mut self, upstream: Box<dyn Step>) {
        self.sub_plans.push(upstream);
        self.active_branches = self.sub_plans.len();
    }
}
