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

use smallvec::{smallvec, SmallVec};

use crate::{
    engine::{
        context::GraphCtx,
        data_flow::group_id::GroupId,
        traverser::Traverser,
        volcano::steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce, Pullable},
    },
    types::GValue,
};

struct Inner {
    items: VecDeque<Traverser>,
    items_backup: VecDeque<Traverser>,
}

pub struct VecSourceStep {
    broadcast: RefCell<BroadcastState>,
    inner: RefCell<Inner>,
}

impl VecSourceStep {
    pub fn empty() -> Rc<Self> {
        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { items: VecDeque::new(), items_backup: VecDeque::new() }),
        })
    }

    pub fn new(items: VecDeque<GValue>) -> Rc<Self> {
        let traversers: VecDeque<Traverser> = items
            .into_iter()
            .enumerate()
            .map(|(i, val)| {
                let mut t = Traverser::new(val);
                t.group_id = GroupId::new(i as u32);
                t
            })
            .collect();

        Rc::new(Self {
            broadcast: RefCell::new(BroadcastState::new()),
            inner: RefCell::new(Inner { items: traversers.clone(), items_backup: traversers }),
        })
    }

    pub fn inject(&self, items: VecDeque<Traverser>) {
        let mut inner = self.inner.borrow_mut();
        inner.items = items.clone();
        inner.items_backup = items;
    }
}

impl HasBroadcast for VecSourceStep {
    fn broadcast(&self) -> &RefCell<BroadcastState> {
        &self.broadcast
    }
}

impl Produce for VecSourceStep {
    fn produce(&self, _ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Traverser; 4]>> {
        let item = self.inner.borrow_mut().items.pop_front()?;
        Some(smallvec![item])
    }
}

impl GremlinStep for VecSourceStep {
    fn add_upper(&self, _upstream: ConsumerIter) {
        panic!("VecSourceStep is a source step and cannot have an upstream");
    }
    fn reset(&self) {
        self.broadcast.borrow_mut().reset();
        let mut inner = self.inner.borrow_mut();
        inner.items = inner.items_backup.clone();
    }
}
