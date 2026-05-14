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

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use crate::traversal::{
    context::GraphCtx,
    step::physical::traits::{BroadcastState, ConsumerId, ConsumerIter, HasBroadcast, Produce, Pullable},
    Traverser,
};

struct Inner {
    items: VecDeque<Traverser>,
}

pub struct VecSourceStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl VecSourceStep {
    pub fn new(items: VecDeque<Traverser>) -> Rc<Self> {
        Rc::new(Self { broadcast: RefCell::new(BroadcastState::new()), inner: RefCell::new(Inner { items }) })
    }

    pub fn subscribe(rc: &Rc<Self>) -> ConsumerIter {
        ConsumerIter::new(rc.clone(), rc.register())
    }
}

impl HasBroadcast for VecSourceStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for VecSourceStep {
    fn produce(&self, _ctx: &dyn GraphCtx) -> Option<Vec<Traverser>> {
        let item = self.inner.borrow_mut().items.pop_front()?;
        Some(vec![item])
    }
}
