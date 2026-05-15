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

use smallvec::{smallvec, SmallVec};
use std::{cell::RefCell, rc::Rc};

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
    physical_plans: Vec<PhysicalPlan>,
    current_plan_idx: usize,
    current_input: Option<Traverser>,
}

pub struct UnionStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl UnionStep {
    pub fn new(physical_plans: Vec<PhysicalPlan>) -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { upstream: None, physical_plans, current_plan_idx: 0, current_input: None }),
        })
    }
}

impl HasBroadcast for UnionStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for UnionStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Traverser; 4]>> {
        let mut inner = self.inner.borrow_mut();
        loop {
            if inner.current_input.is_none() {
                let t = inner.upstream.as_ref().unwrap().next(ctx)?;
                inner.current_input = Some(t.clone());
                inner.current_plan_idx = 0;

                if !inner.physical_plans.is_empty() {
                    let p = &inner.physical_plans[0];
                    p.reset();
                    p.inject(std::collections::VecDeque::from(vec![t]));
                }
            }
            if inner.physical_plans.is_empty() {
                inner.current_input = None;
                continue;
            }

            let p = &inner.physical_plans[inner.current_plan_idx];
            if let Some(res) = p.next(ctx) {
                return Some(smallvec![res]);
            } else {
                inner.current_plan_idx += 1;
                if inner.current_plan_idx < inner.physical_plans.len() {
                    let next_p = &inner.physical_plans[inner.current_plan_idx];
                    next_p.reset();
                    next_p.inject(std::collections::VecDeque::from(vec![inner
                        .current_input
                        .as_ref()
                        .unwrap()
                        .clone()]));
                } else {
                    inner.current_input = None;
                }
            }
        }
    }
}

impl GremlinStep for UnionStep {
    fn add_upper(&self, upstream: ConsumerIter) {
        self.inner.borrow_mut().upstream = Some(upstream);
    }
    fn reset(&self) {
        self.broadcast.borrow_mut().reset();
        let mut inner = self.inner.borrow_mut();
        if let Some(up) = &inner.upstream {
            up.reset();
        }
        for p in &inner.physical_plans {
            p.reset();
        }
        inner.current_input = None;
        inner.current_plan_idx = 0;
    }
}
