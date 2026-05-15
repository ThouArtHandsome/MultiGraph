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

use crate::{
    engine::{
        context::GraphCtx,
        data_flow::{
            message::Message,
            steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
        },
    },
    types::LabelId,
};

struct Inner {
    upstream: Option<ConsumerIter>,
    label_filter: Option<LabelId>,
}

pub struct InEStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl InEStep {
    pub fn new(label_filter: Option<LabelId>) -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { upstream: None, label_filter }),
        })
    }
}

impl HasBroadcast for InEStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for InEStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Message; 4]>> {
        let inner = self.inner.borrow();
        loop {
            let item = inner.upstream.as_ref().unwrap().next(ctx)?;
            if let Message::Traverser(t) = &item {
                match &t.value {
                    crate::types::gvalue::GValue::Vertex(vk) => {
                        let edges = ctx.get_in_edges(*vk).unwrap();
                        let filtered_edges: SmallVec<[_; 8]> = if let Some(label_id) = inner.label_filter {
                            edges.into_iter().filter(|e| e.label_id == label_id).collect()
                        } else {
                            edges.into()
                        };

                        if filtered_edges.is_empty() {
                            continue;
                        }

                        return Some(
                            filtered_edges
                                .into_iter()
                                .map(|e| {
                                    let mut edge = t.clone();
                                    edge.value = crate::types::gvalue::GValue::Edge(e);
                                    edge.parent = Some(Arc::new(t.clone()));
                                    Message::Traverser(edge)
                                })
                                .collect(),
                        );
                    }
                    _ => continue,
                }
            } else {
                return Some(smallvec![item]);
            }
        }
    }
}

impl GremlinStep for InEStep {
    fn add_upper(&self, upstream: ConsumerIter, _label: &str) {
        self.inner.borrow_mut().upstream = Some(upstream);
    }
}
