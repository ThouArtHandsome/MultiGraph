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

use crate::traversal::iterators::traits::{BroadcastIter, ConsumerIter, Step};
use std::cell::RefCell;
use std::rc::Rc;

pub struct WhereEnterStep;

impl WhereEnterStep {
    /// Returns a new BroadcastIter instance that can be dynamically subscribed to.
    pub fn new() -> Rc<RefCell<BroadcastIter>> {
        BroadcastIter::new()
    }
}
