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

pub mod encoding;
pub mod graph;
pub mod store;

pub use encoding::{
    decode_edge_key, decode_vertex_key, encode_edge_key, encode_vertex_key, EdgeValue, VertexValue,
    CF_EDGES_IN, CF_EDGES_OUT, CF_VERTICES,
};
pub use store::RocksStorage;
