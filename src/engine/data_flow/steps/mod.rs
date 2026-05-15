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

//! Pull-based Gremlin step pipeline.
//!
//! ## Architecture
//!
//! Steps form a lazy, demand-driven pipeline.  A traverser only moves when a
//! downstream consumer calls [`ConsumerIter::next`].  Each step:
//!
//! 1. Pulls from its upstream [`ConsumerIter`] on demand.
//! 2. Buffers the result in its internal [`BroadcastState`][traits::BroadcastState] so multiple downstream consumers
//!    can read the same item independently (fan-out).
//! 3. Exposes [`ConsumerIter`] handles — obtained via the step's `subscribe` associated function — for wiring into the
//!    next step.
//!
//! ## Implementing a new step
//!
//! Every step follows the same four-part shape:
//!
//! | Part | Purpose |
//! |------|---------|
//! | `struct Inner` | Step-specific mutable state: upstream handle, filter predicate, etc. |
//! | `pub struct MyStep` | Outer type with two `RefCell` fields: `broadcast` and `inner`. |
//! | `impl HasBroadcast` | One-liner: return `&self.broadcast`. |
//! | `impl Produce` | The only unique logic: pull from upstream and return items. |
//! | `impl GremlinStep` | Wiring — called once at construction time to attach an upstream. |
//!
//! `Pullable` is **not** written by hand — a blanket impl in `traits.rs` derives it
//! from `HasBroadcast + Produce`, handling `needs_more` / `push` / `advance` uniformly.
//! Implementing all three (`HasBroadcast`, `Produce`, `GremlinStep`) also satisfies
//! [`Step`] (another blanket impl), which provides `subscribe` for free.
//! A step that is missing `GremlinStep` will fail to compile at the first `subscribe` call.
//!
//! Source steps (no upstream, e.g. [`VecSourceStep`][vec_source::VecSourceStep])
//! omit `GremlinStep` and provide their own inherent `subscribe`.
//!
//! ### Template
//!
//! ```rust,ignore
//! use std::{cell::RefCell, rc::Rc};
//! use crate::engine::data_flow::{
//!     context::GraphCtx,
//!     steps::traits::{BroadcastState, ConsumerIter, GremlinStep, HasBroadcast, Produce},
//!     traverser::Traverser,
//! };
//!
//! struct Inner {
//!     upstream: Option<ConsumerIter>, // omit for source steps
//!     // ... step-specific fields (e.g. a filter predicate, a label id)
//! }
//!
//! pub struct MyStep {
//!     broadcast: RefCell<BroadcastState>, // kept separate so the blanket Pullable impl can access it
//!     inner: RefCell<Inner>,
//! }
//!
//! impl MyStep {
//!     pub fn new(/* step-specific params */) -> Rc<Self> {
//!         Rc::new(Self {
//!             broadcast: RefCell::new(BroadcastState::new()),
//!             inner: RefCell::new(Inner { upstream: None /* ... */ }),
//!         })
//!     }
//!     // subscribe() is provided free by the Step blanket impl — no need to write it.
//! }
//!
//! impl HasBroadcast for MyStep {
//!     fn broadcast(&self) -> &RefCell<BroadcastState> { &self.broadcast }
//! }
//!
//! impl Produce for MyStep {
//!     fn produce(&self, ctx: &dyn GraphCtx) -> Option<Vec<Traverser>> {
//!         let inner = self.inner.borrow();
//!
//!         // ── Transform step: pull one traverser from upstream. ─────────────
//!         let item = inner.upstream.as_ref().unwrap().next(ctx)?;
//!         // `?` propagates upstream exhaustion as None to the caller.
//!
//!         // ── Source step: produce from internal state instead. ─────────────
//!         // let item = inner.items.pop_front()?;
//!
//!         // Apply a transform here, or return multiple items (e.g. one vertex → many edges):
//!         //   return Some(ctx.out_edges(&item, filter));
//!         //
//!         // For a filter, loop until a match or upstream is exhausted:
//!         //   loop {
//!         //       let item = inner.upstream.as_ref().unwrap().next(ctx)?;
//!         //       if predicate(&item) { return Some(vec![item]); }
//!         //   }
//!
//!         Some(vec![item])
//!     }
//! }
//!
//! // Omit for source steps.
//! impl GremlinStep for MyStep {
//!     fn add_upper(&self, upstream: ConsumerIter, _label: &str) {
//!         // Multi-input steps match on `label`:
//!         //   match label { "origin" => ..., "sub" => ..., other => panic!(...) }
//!         self.inner.borrow_mut().upstream = Some(upstream);
//!     }
//! }
//! ```
//!
//! ### Wiring steps at runtime
//!
//! ```rust,ignore
//! let a = StepA::new(/* ... */);
//! let a_out = StepA::subscribe(&a);    // one ConsumerIter per downstream
//!
//! let b = StepB::new(/* ... */);
//! b.add_upper(a_out, "upstream");      // label matters only for multi-input steps
//! let b_out = StepB::subscribe(&b);    // obtain the handle to pass further downstream
//!
//! let ctx = NoopCtx;
//! while let Some(t) = b_out.next(&ctx) {
//!     // process traverser t
//! }
//! ```

// ── Pull-based runtime steps ──────────────────────────────────────────────────
pub mod has_property;
pub mod in_e;
pub mod in_v;
pub mod out_e;
pub mod out_v;
pub mod scalar_filter;
pub mod traits;
pub mod union_enter;
pub mod union_exit;
pub mod vec_source;
pub mod where_enter;
pub mod where_exit;

// ── Physical plan operators (storage-layer stubs) ─────────────────────────────

pub use traits::{ConsumerId, ConsumerIter, GremlinStep, Step};

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::{
        engine::{
            context::NoopCtx,
            data_flow::{
                group_id::GroupId,
                message::Message,
                steps::{
                    scalar_filter::ScalarFilterStep,
                    traits::{GremlinStep, Step},
                    vec_source::VecSourceStep,
                    where_enter::WhereEnterStep,
                    where_exit::WhereExitStep,
                },
            },
        },
        types::{gvalue::Primitive, GValue},
    };

    fn gvalue(value: i32) -> GValue {
        GValue::Scalar(Primitive::Int32(value))
    }

    // ── Pipeline test ─────────────────────────────────────────────────────────
    //
    // VecSource ──► WhereEnterStep (tee)
    //                 ├─► origin ──────────────────────────► WhereExitStep ──► result
    //                 └─► sub ──► ScalarFilter ────────────► WhereExitStep
    //
    // Traversers: [1→Int32(1), 2→Int32(2), 3→Int32(3)]
    // ScalarFilter passes only Int32(2), so WhereExitStep emits only group 2.

    #[test]
    fn where_filter_pipeline() {
        let source = VecSourceStep::new(VecDeque::from([gvalue(1), gvalue(2), gvalue(3)]));
        let source_consumer = VecSourceStep::subscribe(&source);

        let enter = WhereEnterStep::new();
        enter.add_upper(source_consumer, "upstream");
        let origin = WhereEnterStep::subscribe(&enter);
        let sub_raw = WhereEnterStep::subscribe(&enter);

        let filter = ScalarFilterStep::new(Primitive::Int32(2));
        filter.add_upper(sub_raw, "upstream");
        let filter_consumer = ScalarFilterStep::subscribe(&filter);

        let exit = WhereExitStep::new();
        exit.add_upper(origin, "origin");
        exit.add_upper(filter_consumer, "sub");
        let result = WhereExitStep::subscribe(&exit);

        let mut ctx = NoopCtx;
        let mut results = vec![];
        while let Some(msg) = result.next(&mut ctx) {
            if let Message::Traverser(t) = msg {
                results.push(t);
            }
        }

        assert_eq!(results.len(), 1, "only one traverser should pass the where filter");
        assert_eq!(
            results[0].value,
            GValue::Scalar(Primitive::Int32(2)),
            "the passing traverser should carry value Int32(2)"
        );
        assert_eq!(results[0].group_id, GroupId::new(1), "group_id must match");
    }
}
