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

use std::{cell::RefCell, collections::HashMap, rc::Rc};

use smallvec::{smallvec, SmallVec};

use crate::engine::{
    context::GraphCtx,
    data_flow::{
        group_id::GroupId,
        message::Message,
        steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
    },
    traverser::Traverser,
};

struct Inner {
    origin: Option<ConsumerIter>,
    sub: Option<ConsumerIter>,
    origin_group: HashMap<GroupId, Traverser>,
    sub_group: HashMap<GroupId, bool>,
}

/// Correlates origin and sub streams by `group_id`.
/// Only passes through origin traversers whose `group_id` also appeared in sub.
///
/// Wire upstreams with labels `"origin"` and `"sub"`.
pub struct WhereExitStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl WhereExitStep {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner {
                origin: None,
                sub: None,
                origin_group: HashMap::new(),
                sub_group: HashMap::new(),
            }),
        })
    }
}

impl HasBroadcast for WhereExitStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for WhereExitStep {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Message; 4]>> {
        let mut inner = self.inner.borrow_mut();
        loop {
            // Pull one from origin; None means origin is exhausted.
            let msg_org = inner.origin.as_ref().unwrap().next(ctx)?;
            if let Message::Traverser(t_org) = &msg_org {
                let gid = t_org.group_id.clone();
                inner.origin_group.insert(gid.clone(), t_org.clone());

                // Advance sub by one; it may consume ahead of origin via its own filter.
                if let Some(msg_sub) = inner.sub.as_ref().unwrap().next(ctx) {
                    if let Message::Traverser(t_sub) = msg_sub {
                        inner.sub_group.insert(t_sub.group_id.clone(), true);
                    }
                }

                // sub_group accumulates across iterations, so out-of-phase matches work.
                if inner.sub_group.get(&gid).copied().unwrap_or(false) {
                    inner.origin_group.remove(&gid);
                    return Some(smallvec![msg_org]);
                }
            } else {
                return Some(smallvec![msg_org]);
            }
        }
    }
}

impl GremlinStep for WhereExitStep {
    fn add_upper(&self, upstream: ConsumerIter, label: &str) {
        let mut inner = self.inner.borrow_mut();
        match label {
            "origin" => inner.origin = Some(upstream),
            "sub" => inner.sub = Some(upstream),
            other => panic!("WhereExitStep: unknown label '{other}'"),
        }
    }
}
