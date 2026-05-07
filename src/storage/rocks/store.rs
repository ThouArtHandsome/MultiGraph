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

use std::path::Path;

use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::storage::rocks::encoding::{CF_EDGES_IN, CF_EDGES_OUT, CF_VERTICES};
use crate::types::StorageError;

/// RocksDB-backed graph store.
///
/// Owns the `DB` handle and the three column families:
/// `vertices`, `edges_out`, and `edges_in`.
pub struct RocksStorage {
    pub(super) db: DB,
}

impl RocksStorage {
    /// Open (or create) the database at `path`.
    ///
    /// All three column families are created if they do not exist yet.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = [CF_VERTICES, CF_EDGES_OUT, CF_EDGES_IN]
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect::<Vec<_>>();

        let db = DB::open_cf_descriptors(&opts, path, cfs)
            .map_err(|e| StorageError::Other(e.to_string()))?;

        Ok(Self { db })
    }
}
