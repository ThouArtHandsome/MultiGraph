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

use crate::{
    engine::volcano::{
        builder::PhysicalPlanBuilder,
        steps::traits::{ConsumerIter, GremlinStep, Step},
    },
    types::{gvalue::Primitive, prop_key::PropKey, LabelId},
};

#[derive(Clone)]
pub struct LogicalPlan {
    pub steps: Vec<LogicalStep>,
}

#[derive(Clone)]
pub enum LogicalStep {
    Count(CountStep),
    HasProperty(HasPropertyStep),
    InE(InEStep),
    OutE(OutEStep),
    InV(InVStep),
    OutV(OutVStep),
    ScalarFilter(ScalarFilterStep),
    Where(WhereStep),
    Union(UnionStep),
}

#[derive(Clone)]
pub struct CountStep {}

#[derive(Clone)]
pub struct HasPropertyStep {
    pub key: PropKey,
    pub value: Primitive,
}

#[derive(Clone)]
pub struct InEStep {
    pub label_filter: Option<LabelId>,
}

#[derive(Clone)]
pub struct OutEStep {
    pub label_filter: Option<LabelId>,
}

#[derive(Clone)]
pub struct InVStep {}

#[derive(Clone)]
pub struct OutVStep {}

#[derive(Clone)]
pub struct ScalarFilterStep {
    pub value: Primitive,
}

#[derive(Clone)]
pub struct WhereStep {
    pub plan: LogicalPlan,
}

#[derive(Clone)]
pub struct UnionStep {
    pub plans: Vec<LogicalPlan>,
}

impl LogicalStep {
    pub fn build(&self, builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        match self {
            LogicalStep::Count(s) => s.build(builder, upstream),
            LogicalStep::HasProperty(s) => s.build(builder, upstream),
            LogicalStep::InE(s) => s.build(builder, upstream),
            LogicalStep::OutE(s) => s.build(builder, upstream),
            LogicalStep::InV(s) => s.build(builder, upstream),
            LogicalStep::OutV(s) => s.build(builder, upstream),
            LogicalStep::ScalarFilter(s) => s.build(builder, upstream),
            LogicalStep::Where(s) => s.build(builder, upstream),
            LogicalStep::Union(s) => s.build(builder, upstream),
        }
    }
}

impl CountStep {
    pub fn build(&self, _builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let s = crate::engine::volcano::steps::count::CountStep::new();
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl HasPropertyStep {
    pub fn build(&self, _builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let s = crate::engine::volcano::steps::has_property::HasPropertyStep::new(self.key.clone(), self.value.clone());
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl InEStep {
    pub fn build(&self, _builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let s = crate::engine::volcano::steps::in_e::InEStep::new(self.label_filter.clone());
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl OutEStep {
    pub fn build(&self, _builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let s = crate::engine::volcano::steps::out_e::OutEStep::new(self.label_filter.clone());
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl InVStep {
    pub fn build(&self, _builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let s = crate::engine::volcano::steps::in_v::InVStep::new();
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl OutVStep {
    pub fn build(&self, _builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let s = crate::engine::volcano::steps::out_v::OutVStep::new();
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl ScalarFilterStep {
    pub fn build(&self, _builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let s = crate::engine::volcano::steps::scalar_filter::ScalarFilterStep::new(self.value.clone());
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl WhereStep {
    pub fn build(&self, builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let physical_plan = builder.build(&self.plan);
        let s = crate::engine::volcano::steps::where_step::WhereStep::new(physical_plan);
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}

impl UnionStep {
    pub fn build(&self, builder: &mut PhysicalPlanBuilder, upstream: Option<ConsumerIter>) -> Option<ConsumerIter> {
        let physical_plans = self.plans.iter().map(|p| builder.build(p)).collect();
        let s = crate::engine::volcano::steps::union::UnionStep::new(physical_plans);
        if let Some(up) = upstream {
            s.add_upper(up);
        }
        Some(Step::subscribe(&s))
    }
}
