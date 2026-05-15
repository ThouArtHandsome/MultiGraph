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

use std::{collections::VecDeque, rc::Rc};

use crate::engine::{
    context::GraphCtx,
    logical_step::LogicalPlan,
    traverser::Traverser,
    volcano::steps::{
        traits::{ConsumerIter, GremlinStep, Step},
        vec_source::VecSourceStep,
    },
};

#[derive(Clone)]
pub struct PhysicalPlan {
    pub source: Rc<VecSourceStep>,
    pub tail: ConsumerIter,
}

impl PhysicalPlan {
    pub fn inject(&self, items: VecDeque<Traverser>) {
        self.source.inject(items);
    }

    pub fn next(&self, ctx: &mut dyn GraphCtx) -> Option<Traverser> {
        self.tail.next(ctx)
    }

    pub fn reset(&self) {
        self.tail.reset();
    }
}

pub struct PhysicalPlanBuilder;

impl PhysicalPlanBuilder {
    pub fn new() -> Self {
        Self {}
    }

    pub fn build(&mut self, plan: &LogicalPlan) -> PhysicalPlan {
        let source = VecSourceStep::empty();
        let mut upstream = Some(Step::subscribe(&source));

        for step in &plan.steps {
            upstream = step.build(self, upstream);
        }

        PhysicalPlan { source, tail: upstream.expect("Plan must have at least the source step") }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::PhysicalPlanBuilder;
    use crate::{
        engine::{
            context::NoopCtx,
            logical_step::{CountStep, LogicalPlan, LogicalStep, ScalarFilterStep, WhereStep},
            traverser::Traverser,
        },
        types::gvalue::{GValue, Primitive},
    };

    fn gvalue(value: i32) -> GValue {
        GValue::Scalar(Primitive::Int32(value))
    }

    fn traverser(value: i32) -> Traverser {
        Traverser::new(gvalue(value))
    }

    #[test]
    fn test_simple_filter_plan() {
        let plan =
            LogicalPlan { steps: vec![LogicalStep::ScalarFilter(ScalarFilterStep { value: Primitive::Int32(2) })] };

        let mut builder = PhysicalPlanBuilder::new();
        let physical_plan = builder.build(&plan);

        physical_plan.inject(VecDeque::from(vec![traverser(1), traverser(2), traverser(3)]));

        let mut ctx = NoopCtx;
        let result = physical_plan.next(&mut ctx).expect("Expected one result");
        assert_eq!(result.value, gvalue(2));
        assert!(physical_plan.next(&mut ctx).is_none());
    }

    #[test]
    fn test_plan_reuse_with_reset() {
        let plan = LogicalPlan { steps: vec![LogicalStep::Count(CountStep {})] };

        let mut builder = PhysicalPlanBuilder::new();
        let physical_plan = builder.build(&plan);

        // First run: expect 3 items to be counted
        physical_plan.inject(VecDeque::from(vec![traverser(1), traverser(2), traverser(3)]));
        let mut ctx = NoopCtx;
        let result1 = physical_plan.next(&mut ctx).unwrap();
        assert_eq!(result1.value, gvalue(3));
        assert!(physical_plan.next(&mut ctx).is_none());

        // Reset and reuse: expect 2 items to be counted
        physical_plan.reset();
        physical_plan.inject(VecDeque::from(vec![traverser(1), traverser(2)]));
        let result2 = physical_plan.next(&mut ctx).unwrap();
        assert_eq!(result2.value, gvalue(2));
        assert!(physical_plan.next(&mut ctx).is_none());
    }

    #[test]
    fn test_where_step_plan() {
        let sub_plan =
            LogicalPlan { steps: vec![LogicalStep::ScalarFilter(ScalarFilterStep { value: Primitive::Int32(2) })] };
        let plan = LogicalPlan { steps: vec![LogicalStep::Where(WhereStep { plan: sub_plan })] };

        let mut builder = PhysicalPlanBuilder::new();
        let physical_plan = builder.build(&plan);

        physical_plan.inject(VecDeque::from(vec![traverser(1), traverser(2), traverser(3)]));

        let mut ctx = NoopCtx;
        let result = physical_plan.next(&mut ctx).expect("Expected one result");
        assert_eq!(result.value, gvalue(2));
        assert!(physical_plan.next(&mut ctx).is_none());
    }
}
