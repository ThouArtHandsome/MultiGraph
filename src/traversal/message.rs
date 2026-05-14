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

use crate::traversal::{GroupId, Traverser};
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum Message {
    Traverser(Traverser),
    GroupBegin(GroupId), // start of a new group
    GroupEnd(GroupId),   // end of a group
    Flush,               // no more data
    Abort,               // cancel
}

pub type OutboundHub = broadcast::Sender<Message>;
pub type InboundHub = broadcast::Receiver<Message>;

/// Describes the semantic role of a connection
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Descriptor {
    Primary,
    Secondary,
    Other(String), // for future extension
}
