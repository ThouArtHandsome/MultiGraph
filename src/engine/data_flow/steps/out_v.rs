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

use std::{cell::RefCell, rc::Rc, sync::Arc};

use smallvec::{smallvec, SmallVec};

use crate::engine::{
    context::GraphCtx,
    data_flow::{
        message::Message,
        steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
    },
};

struct Inner {
    upstream: Option<ConsumerIter>,
}

pub struct OutVStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl OutVStep {
    pub fn new() -> Rc<Self> {
        Rc::new(Self { broadcast: RefCell::new(BroadcastState::new()), inner: RefCell::new(Inner { upstream: None }) })
    }
}

impl HasBroadcast for OutVStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for OutVStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Message; 4]>> {
        let inner = self.inner.borrow();
        loop {
            let item = inner.upstream.as_ref().unwrap().next(ctx)?;
            if let Message::Traverser(t) = &item {
                match &t.value {
                    crate::types::gvalue::GValue::Edge(ek) => {
                        // outV retrieves the outgoing vertex (source of the edge)
                        let vk = ek.canonical_edge_key().src_id;

                        let mut vertex_traverser = t.clone();
                        vertex_traverser.value = crate::types::gvalue::GValue::Vertex(vk);
                        vertex_traverser.parent = Some(Arc::new(t.clone()));

                        return Some(smallvec![Message::Traverser(vertex_traverser)]);
                    }
                    _ => continue,
                }
            } else {
                return Some(smallvec![item]);
            }
        }
    }
}

impl GremlinStep for OutVStep {
    fn add_upper(&self, upstream: ConsumerIter, _label: &str) {
        self.inner.borrow_mut().upstream = Some(upstream);
    }
}
