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

use crate::graph::LogicalGraph;
use crate::store::RocksStorage;
use crate::traversal::iterators::traits::Step;
use crate::traversal::Traverser;
use crate::types::gvalue::Primitive;
use crate::types::prop_key::PropKey;
use std::cell::RefCell;
use std::rc::Rc;

pub struct HasPropertyStep {
    pub upstream: Option<Box<dyn Step>>,
    pub prop_key: PropKey,
    pub expected_value: Primitive,
    pub graph: Rc<RefCell<LogicalGraph<RocksStorage>>>,
}

impl HasPropertyStep {
    pub fn new(prop_key: PropKey, expected_value: Primitive, graph: Rc<RefCell<LogicalGraph<RocksStorage>>>) -> Self {
        Self { upstream: None, prop_key, expected_value, graph }
    }
}

impl Step for HasPropertyStep {
    fn next(&mut self) -> Option<Traverser> {
        loop {
            let t = self.upstream.as_mut().unwrap().next()?;

            let mut graph = self.graph.borrow_mut();
            // Lookup properties using graph interface here...
            return Some(t);
        }
    }

    fn add_upper(&mut self, upstream: Box<dyn Step>) {
        self.upstream = Some(upstream);
    }
}
