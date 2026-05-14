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

use std::{cell::RefCell, rc::Rc};

use crate::traversal::{
    context::GraphCtx,
    step::physical::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
    traverser::Traverser,
};
use crate::types::LabelId;

struct Inner {
    upstream: Option<ConsumerIter>,
    label_filter: Option<LabelId>,
}

pub struct OutEStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl OutEStep {
    pub fn new(label_filter: Option<LabelId>) -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { upstream: None, label_filter }),
        })
    }
}

impl HasBroadcast for OutEStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for OutEStep {
    fn produce(&self, ctx: &dyn GraphCtx) -> Option<Vec<Traverser>> {
        let inner = self.inner.borrow();
        let item = inner.upstream.as_ref().unwrap().next(ctx)?;
        // expand item to outgoing edges via ctx here; stub passes single item through
        Some(vec![item])
    }
}

impl GremlinStep for OutEStep {
    fn add_upper(&self, upstream: ConsumerIter, _label: &str) {
        self.inner.borrow_mut().upstream = Some(upstream);
    }
}
