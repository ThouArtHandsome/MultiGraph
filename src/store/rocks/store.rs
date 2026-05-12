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

use std::{path::Path, sync::Arc};

use rocksdb::{ColumnFamilyDescriptor, OptimisticTransactionDB, Options};

use crate::{
    store::{
        rocks::{
            encoding::{CF_EDGES_IN, CF_EDGES_OUT, CF_VERTEX_COUNTS, CF_VERTICES},
            transaction::Transaction,
        },
        traits::GraphStore,
    },
    types::StoreError,
};

/// RocksDB-backed graph store using `OptimisticTransactionDB`.
///
/// Owns the database handle.  Call
/// `begin` to start a transaction.
pub struct RocksStorage {
    pub(super) db: Arc<OptimisticTransactionDB>,
}

impl RocksStorage {
    /// Open (or create) the database at `path`.
    ///
    /// Creates all three column families if they do not exist yet, and
    /// initialises a 16-shard × 1 024-entry LRU vertex cache.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = [CF_VERTICES, CF_VERTEX_COUNTS, CF_EDGES_OUT, CF_EDGES_IN]
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect::<Vec<_>>();

        let db = OptimisticTransactionDB::open_cf_descriptors(&opts, path, cfs)
            .map_err(|e| StoreError::Other(e.to_string()))?;

        Ok(Self { db: Arc::new(db) })
    }
}

impl GraphStore for RocksStorage {
    type Txn = Transaction;

    fn begin(&self) -> Transaction {
        Transaction::new(Arc::clone(&self.db))
    }
}
