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
use crate::traversal::iterators::traits::Step;
use crate::traversal::traverser::Traverser;
use crate::types::{Direction, LabelId};

pub struct OutEStep {
    pub upstream: Option<Box<dyn Step>>,
    pub label_filter: Option<LabelId>,
    pub buffer: VecDeque<Traverser>,
    pub graph: Rc<RefCell<LogicalGraph<RocksStorage>>>,
}

impl OutEStep {
    pub fn new(label_filter: Option<LabelId>, graph: Rc<RefCell<LogicalGraph<RocksStorage>>>) -> Self {
        Self { upstream: None, label_filter, buffer: VecDeque::new(), graph }
    }
}

impl Step for OutEStep {
    fn next(&mut self) -> Option<Traverser> {
        loop {
            if let Some(t) = self.buffer.pop_front() {
                return Some(t);
            }

            // Pull from upstream, if graph query logic is implemented here
            let parent = self.upstream.as_mut().unwrap().next()?;

            let mut graph = self.graph.borrow_mut();

            // e.g. if let GValue::Vertex(vid) = parent.value { ... }
            // Mock functionality for layout demonstration
            self.buffer.push_back(parent);
        }
    }

    fn add_upper(&mut self, upstream: Box<dyn Step>) {
        self.upstream = Some(upstream);
    }
}
