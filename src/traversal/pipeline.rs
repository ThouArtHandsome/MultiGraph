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

use crate::traversal::iterators::traits::Step;

pub struct PipelineBuilder {
    pub head: Option<Box<dyn Step>>,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self { head: None }
    }

    pub fn build(self) -> Option<Box<dyn Step>> {
        self.head
    }

    /// Add a step sequentially to the current pipeline tail
    pub fn add_step(mut self, mut step: Box<dyn Step>) -> Self {
        if let Some(head) = self.head.take() {
            step.add_upper(head);
        }
        self.head = Some(step);
        self
    }

    pub fn set_source(mut self, source: Box<dyn Step>) -> Self {
        self.head = Some(source);
        self
    }
}
