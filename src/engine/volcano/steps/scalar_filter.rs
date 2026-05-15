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

use crate::{
    engine::{
        context::GraphCtx,
        traverser::Traverser,
        volcano::steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
    },
    types::{GValue, Primitive},
};

struct Inner {
    upstream: Option<ConsumerIter>,
    expected: Primitive,
}

pub struct ScalarFilterStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl ScalarFilterStep {
    pub fn new(expected: Primitive) -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { upstream: None, expected }),
        })
    }
}

impl HasBroadcast for ScalarFilterStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for ScalarFilterStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Traverser; 4]>> {
        let inner = self.inner.borrow();
        loop {
            let t = inner.upstream.as_ref().unwrap().next(ctx)?;
            if matches!(&t.value, GValue::Scalar(p) if p == &inner.expected) {
                return Some(smallvec![t]);
            }
        }
    }
}

impl GremlinStep for ScalarFilterStep {
    fn add_upper(&self, upstream: ConsumerIter) {
        self.inner.borrow_mut().upstream = Some(upstream);
    }
    fn reset(&self) {
        self.broadcast.borrow_mut().reset();
        if let Some(up) = &self.inner.borrow().upstream {
            up.reset();
        }
    }
}
