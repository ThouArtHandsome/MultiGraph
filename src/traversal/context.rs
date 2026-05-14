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

/// Graph access interface passed through every `BasicStep::next` call.
///
/// Steps receive a `&dyn GraphCtx` rather than holding a stored reference,
/// which avoids `Rc<RefCell<…>>` and gives compile-time borrow guarantees.
/// Methods will be added here as step implementations are fleshed out
/// (e.g. `get_out_edges`, `get_property`).
pub trait GraphCtx {}

/// Zero-cost context used in unit tests where no real graph is needed.
pub struct NoopCtx;
impl GraphCtx for NoopCtx {}
