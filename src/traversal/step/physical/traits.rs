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

use crate::traversal::{context::GraphCtx, Traverser};

// ── ConsumerId ────────────────────────────────────────────────────────────────

/// Opaque handle identifying one registered consumer of a step's broadcast buffer.
///
/// Obtained from [`BroadcastState::subscribe`] and stored inside a [`ConsumerIter`].
/// Steps never create `ConsumerId` values directly.
#[derive(Clone, Copy, Debug)]
pub struct ConsumerId(pub(crate) usize);

// ── BroadcastState ────────────────────────────────────────────────────────────

/// Shared buffer + per-consumer cursor state embedded in every step.
///
/// Enables fan-out: each registered consumer has its own read cursor so
/// multiple downstream steps can advance independently through the same
/// produced items.  Items are evicted from the front of the buffer once every
/// registered consumer has passed them, keeping memory bounded.
pub(crate) struct BroadcastState {
    buffer: VecDeque<Traverser>,
    cursors: Vec<usize>,
}

impl BroadcastState {
    pub(crate) fn new() -> Self {
        Self { buffer: VecDeque::new(), cursors: Vec::new() }
    }

    /// Register a new consumer; returns its opaque id.
    pub(crate) fn subscribe(&mut self) -> ConsumerId {
        let id = self.cursors.len();
        self.cursors.push(0);
        ConsumerId(id)
    }

    /// True when consumer `id` has caught up to the end of the buffer and
    /// the step needs to produce more items before `advance` can return one.
    pub(crate) fn needs_more(&self, id: ConsumerId) -> bool {
        self.cursors[id.0] >= self.buffer.len()
    }

    /// Append one or more items to the back of the buffer.
    pub(crate) fn push(&mut self, items: Vec<Traverser>) {
        self.buffer.extend(items);
    }

    /// Return the next buffered item for consumer `id`, advance its cursor,
    /// and evict any items all consumers have already passed.
    pub(crate) fn advance(&mut self, id: ConsumerId) -> Option<Traverser> {
        let pos = self.cursors[id.0];
        if pos >= self.buffer.len() {
            return None;
        }
        let item = self.buffer[pos].clone();
        self.cursors[id.0] += 1;
        let min = *self.cursors.iter().min().unwrap();
        for _ in 0..min {
            self.buffer.pop_front();
        }
        for c in &mut self.cursors {
            *c -= min;
        }
        Some(item)
    }
}

// ── HasBroadcast + Produce ────────────────────────────────────────────────────

/// Provides the step's broadcast buffer to the blanket [`Pullable`] impl.
/// Implemented by returning a reference to the step's `RefCell<BroadcastState>` field.
pub(crate) trait HasBroadcast {
    fn broadcast(&self) -> &RefCell<BroadcastState>;
}

/// The only method a step needs to write.  Called by the blanket [`Pullable`]
/// impl when the broadcast buffer is empty and a consumer needs more items.
///
/// Return `None` to signal that the source is exhausted.  The blanket impl
/// propagates that `None` directly to the caller without touching the buffer.
pub(crate) trait Produce {
    fn produce(&self, ctx: &dyn GraphCtx) -> Option<Vec<Traverser>>;
}

// ── Pullable ──────────────────────────────────────────────────────────────────

/// Back-reference trait held inside [`ConsumerIter`]; implemented automatically
/// for any step that provides [`HasBroadcast`] and [`Produce`].
///
/// Steps do **not** implement this directly — the blanket impl below handles
/// the `needs_more` / `push` / `advance` pattern uniformly.  `Rc<dyn Pullable>`
/// requires `&self` (not `&mut self`), so interior mutability via `RefCell` is used.
pub(crate) trait Pullable {
    fn pull(&self, id: ConsumerId, ctx: &dyn GraphCtx) -> Option<Traverser>;
    fn register(&self) -> ConsumerId;
}

impl<T: HasBroadcast + Produce> Pullable for T {
    fn pull(&self, id: ConsumerId, ctx: &dyn GraphCtx) -> Option<Traverser> {
        // Bind to a local so the immutable borrow is dropped before borrow_mut below.
        let needs_more = self.broadcast().borrow().needs_more(id);
        if needs_more {
            let items = self.produce(ctx)?;
            self.broadcast().borrow_mut().push(items);
        }
        self.broadcast().borrow_mut().advance(id)
    }

    fn register(&self) -> ConsumerId {
        self.broadcast().borrow_mut().subscribe()
    }
}

// ── ConsumerIter ──────────────────────────────────────────────────────────────

/// The sole connection type between pipeline steps.
///
/// Obtained by calling a step's `subscribe` associated function.  Each call to
/// `subscribe` creates a new `ConsumerIter` with an independent cursor, so a
/// step with two downstream readers issues two separate `ConsumerIter`s — both
/// will see every item the step produces (fan-out via [`BroadcastState`]).
///
/// Pass a `ConsumerIter` to [`GremlinStep::add_upper`] to wire it into the
/// next step.
pub struct ConsumerIter {
    pub(crate) source: Rc<dyn Pullable>,
    pub(crate) id: ConsumerId,
}

impl ConsumerIter {
    pub(crate) fn new(source: Rc<dyn Pullable>, id: ConsumerId) -> Self {
        Self { source, id }
    }

    pub fn next(&self, ctx: &dyn GraphCtx) -> Option<Traverser> {
        self.source.pull(self.id, ctx)
    }
}

// ── GremlinStep ───────────────────────────────────────────────────────────────

/// Wiring trait: attaches upstream [`ConsumerIter`]s to a step at construction time.
///
/// `next` is intentionally absent — consumers always pull via a [`ConsumerIter`]
/// obtained from the step's `subscribe` associated function.
///
/// The `label` argument distinguishes inputs for multi-input steps.  Single-input
/// steps may ignore it (use `_label`).  Multi-input steps should `match` on it
/// and `panic!` on unknown labels to catch wiring mistakes early.  The labels
/// `"origin"` and `"sub"` are the convention used by
/// [`WhereExitStep`][super::where_exit::WhereExitStep].
pub trait GremlinStep {
    fn add_upper(&self, upstream: ConsumerIter, label: &str);
}

// ── Step ──────────────────────────────────────────────────────────────────────

/// Combined contract for all transform steps (steps that have at least one upstream).
///
/// **Enforcement**: any type that implements both [`GremlinStep`] and [`Pullable`]
/// satisfies `Step` automatically via the blanket impl below, and inherits the
/// default `subscribe` implementation.  If either trait is missing, calling
/// `subscribe` on the new type produces a compile error — so incomplete steps
/// are caught at their first usage.
///
/// Source steps (no upstream, e.g. `VecSourceStep`) are exempt: they implement
/// [`Pullable`] only and provide their own inherent `subscribe`.
pub trait Step: GremlinStep + Pullable + Sized + 'static {
    /// Returns a [`ConsumerIter`] wired to this step.
    /// Call once per downstream consumer; each call produces an independent cursor.
    ///
    /// The default implementation is correct for all steps — do not override it.
    fn subscribe(rc: &Rc<Self>) -> ConsumerIter {
        ConsumerIter::new(rc.clone(), rc.register())
    }
}

impl<T: GremlinStep + Pullable + Sized + 'static> Step for T {}
