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

use smol_str::SmolStr;

/// Human-readable label for a vertex or edge (e.g. `"person"`, `"knows"`).
/// Stack-allocated for strings up to 22 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Label(pub SmolStr);

impl Label {
    pub fn new(s: impl Into<SmolStr>) -> Self {
        Self(s.into())
    }
}

impl From<&str> for Label {
    fn from(s: &str) -> Self {
        Self(SmolStr::new(s))
    }
}
