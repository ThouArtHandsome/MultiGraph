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

use std::fmt;

#[derive(Debug)]
pub enum StoreError {
    /// A required key was not found.
    ///
    /// Not emitted by the storage layer itself (absent keys return `Ok(None)`);
    /// reserved for higher-level callers that treat absence as a hard error
    /// (e.g. a mutation step that requires a vertex to exist).
    NotFound,
    /// OCC commit failed because a key in the read-set was modified by a
    /// concurrent transaction.  Callers should retry from scratch.
    Conflict,
    Io(std::io::Error),
    Other(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::NotFound => write!(f, "key not found"),
            StoreError::Conflict => write!(f, "transaction conflict; retry"),
            StoreError::Io(e) => write!(f, "I/O error: {e}"),
            StoreError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}
