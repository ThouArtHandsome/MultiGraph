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

use crate::{
    engine::{
        context::GraphCtx,
        traverser::Traverser,
        volcano::steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
    },
    types::gvalue::{GValue, Primitive},
};

struct Inner {
    upstream: Option<ConsumerIter>,
    done: bool,
}

pub struct CountStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl CountStep {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { upstream: None, done: false }),
        })
    }
}

impl HasBroadcast for CountStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for CountStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Traverser; 4]>> {
        let mut inner = self.inner.borrow_mut();
        if inner.done {
            return None;
        }
        let mut count = 0;
        while inner.upstream.as_ref().unwrap().next(ctx).is_some() {
            count += 1;
        }
        inner.done = true;
        Some(smallvec![Traverser::new(GValue::Scalar(Primitive::Int32(count)))])
    }
}

impl GremlinStep for CountStep {
    fn add_upper(&self, upstream: ConsumerIter) {
        self.inner.borrow_mut().upstream = Some(upstream);
    }
    fn reset(&self) {
        self.broadcast.borrow_mut().reset();
        let mut inner = self.inner.borrow_mut();
        inner.done = false;
        if let Some(up) = &inner.upstream {
            up.reset();
        }
    }
}
