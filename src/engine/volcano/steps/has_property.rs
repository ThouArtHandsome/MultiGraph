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
    types::{gvalue::Primitive, prop_key::PropKey},
};

struct Inner {
    upstream: Option<ConsumerIter>,
    prop_key: PropKey,
    expected_value: Primitive,
}

pub struct HasPropertyStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl HasPropertyStep {
    pub fn new(prop_key: PropKey, expected_value: Primitive) -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { upstream: None, prop_key, expected_value }),
        })
    }
}

impl HasBroadcast for HasPropertyStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for HasPropertyStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Traverser; 4]>> {
        let inner = self.inner.borrow();
        loop {
            let t = inner.upstream.as_ref().unwrap().next(ctx)?;
            match &t.value {
                crate::types::gvalue::GValue::Vertex(vk) => {
                    if let Some(vt) = ctx.get_vertex(*vk).unwrap() {
                        if vt.get_property(&inner.prop_key) == Some(inner.expected_value.clone()) {
                            return Some(smallvec![t]);
                        }
                    }
                }
                crate::types::gvalue::GValue::Edge(ek) => {
                    if let Some(et) = ctx.get_edge(*ek).unwrap() {
                        if et.get_property(&inner.prop_key) == Some(inner.expected_value.clone()) {
                            return Some(smallvec![t]);
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

impl GremlinStep for HasPropertyStep {
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
