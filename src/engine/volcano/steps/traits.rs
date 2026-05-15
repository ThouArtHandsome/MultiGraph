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

use crate::engine::{context::GraphCtx, traverser::Traverser};
use smallvec::SmallVec;
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

// ── BroadcastState ────────────────────────────────────────────────────────────

pub(crate) struct BroadcastState {
    buffer: VecDeque<Traverser>,
}

impl BroadcastState {
    pub(crate) fn new() -> Self {
        Self { buffer: VecDeque::new() }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub(crate) fn push(&mut self, items: SmallVec<[Traverser; 4]>) {
        self.buffer.extend(items);
    }

    pub(crate) fn advance(&mut self) -> Option<Traverser> {
        self.buffer.pop_front()
    }

    pub(crate) fn reset(&mut self) {
        self.buffer.clear();
    }
}

// ── HasBroadcast + Produce ────────────────────────────────────────────────────

pub(crate) trait HasBroadcast {
    fn broadcast(&self) -> &RefCell<BroadcastState>;
}

pub(crate) trait Produce {
    fn produce(&self, ctx: &mut dyn GraphCtx) -> Option<SmallVec<[Traverser; 4]>>;
}

// ── Pullable ──────────────────────────────────────────────────────────────────

pub(crate) trait Pullable {
    fn pull(&self, ctx: &mut dyn GraphCtx) -> Option<Traverser>;
    fn reset_step(&self);
}

impl<T: HasBroadcast + Produce + GremlinStep> Pullable for T {
    fn pull(&self, ctx: &mut dyn GraphCtx) -> Option<Traverser> {
        let mut buffer = self.broadcast().borrow_mut();
        if buffer.is_empty() {
            drop(buffer); // prevent refcell conflict with produce
            let items = self.produce(ctx)?;
            self.broadcast().borrow_mut().push(items);
            self.broadcast().borrow_mut().advance()
        } else {
            buffer.advance()
        }
    }
    fn reset_step(&self) {
        self.reset();
    }
}

// ── ConsumerIter ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ConsumerIter {
    pub(crate) source: Rc<dyn Pullable>,
}

impl ConsumerIter {
    pub(crate) fn new(source: Rc<dyn Pullable>) -> Self {
        Self { source }
    }

    pub fn next(&self, ctx: &mut dyn GraphCtx) -> Option<Traverser> {
        self.source.pull(ctx)
    }

    pub fn reset(&self) {
        self.source.reset_step();
    }
}

// ── GremlinStep ───────────────────────────────────────────────────────────────

pub trait GremlinStep {
    fn add_upper(&self, upstream: ConsumerIter);
    fn reset(&self);
}

// ── Step ──────────────────────────────────────────────────────────────────────

pub trait Step: GremlinStep + Pullable + Sized + 'static {
    fn subscribe(rc: &Rc<Self>) -> ConsumerIter {
        ConsumerIter::new(rc.clone())
    }
}

impl<T: GremlinStep + Pullable + Sized + 'static> Step for T {}
