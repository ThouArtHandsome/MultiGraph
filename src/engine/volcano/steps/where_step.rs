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

use smallvec::{smallvec, SmallVec};

use crate::engine::{
    context::GraphCtx,
    traverser::Traverser,
    volcano::{
        builder::PhysicalPlan,
        steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
    },
};

struct Inner {
    upstream: Option<ConsumerIter>,
    physical_sub_plan: PhysicalPlan,
}

pub struct WhereStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl WhereStep {
    pub fn new(physical_sub_plan: PhysicalPlan) -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { upstream: None, physical_sub_plan }),
        })
    }
}

impl HasBroadcast for WhereStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for WhereStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Traverser; 4]>> {
        let mut inner = self.inner.borrow_mut();
        loop {
            let t = inner.upstream.as_ref().unwrap().next(ctx)?;

            let physical_sub_plan = &inner.physical_sub_plan;

            physical_sub_plan.reset();
            physical_sub_plan.inject(std::collections::VecDeque::from(vec![t.clone()]));

            // Sub pipeline evaluates properly — if sub-traversal yields at least one item, original goes through
            if physical_sub_plan.next(ctx).is_some() {
                return Some(smallvec![t]);
            }
        }
    }
}

impl GremlinStep for WhereStep {
    fn add_upper(&self, upstream: ConsumerIter) {
        self.inner.borrow_mut().upstream = Some(upstream);
    }

    fn reset(&self) {
        self.broadcast.borrow_mut().reset();
        let inner = self.inner.borrow();
        if let Some(up) = &inner.upstream {
            up.reset();
        }
        inner.physical_sub_plan.reset();
    }
}
