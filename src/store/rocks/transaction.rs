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

//! Thin RocksDB transaction adapter.
//!
//! # Responsibility
//!
//! `Transaction` is a pure I/O layer: it encodes and decodes graph elements
//! to/from RocksDB bytes and stages reads/writes on an `OptimisticTransactionDB`
//! handle.  All overlay logic (dirty tracking, query-scoped caching, key
//! allocation) lives in `LogicalGraph`, one layer above.
//!
//! # Read path
//!
//! `get_vertex` uses `GetForUpdate` to enrol the key in the OCC read-set.
//! Point edge reads (`get_edge`) also use `GetForUpdate`. Edge scans (`get_edges`)
//! use snapshot scans; their write-write conflicts are detected automatically
//! by the OCC at commit time because any modified edge key is in the write-set.
//!
//! # Write path
//!
//! This layer is physically pure: `put_edge` and `delete_edge` write or
//! delete exactly one record (either `CF_EDGES_OUT` or `CF_EDGES_IN`). Graph
//! consistency logic (like ensuring forward and backward edges exist) is entirely
//! deferred to `LogicalGraph`. All staged operations are flushed atomically by `commit`.
//!
//! # Lifetime erasure
//!
//! `rocksdb::Transaction<'_, OptimisticTransactionDB>` borrows the DB.
//! We transmute the lifetime to `'static` so the transaction can live
//! alongside `Arc<OptimisticTransactionDB>` in the same struct.
//!
//! **Safety invariant**: `db_txn` is declared *before* `db` in the struct.
//! Rust drops fields in declaration order, so `db_txn` is always destroyed
//! before `db`'s `Arc` decrements its refcount.  The `Arc` ensures the
//! underlying `OptimisticTransactionDB` is alive for the entire duration of
//! both fields.

use std::{collections::HashSet, sync::Arc};

use rocksdb::{
    Direction as ScanDir, IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, ReadOptions,
    WriteOptions,
};

use crate::{
    store::{
        rocks::{
            encoding::{
                decode_edge_key_in, decode_edge_key_out, edge_scan_prefix, encode_edge_key_in, encode_edge_key_out,
                encode_vertex_key, prefix_upper_bound, EdgeValue, VertexValue, CF_EDGES_IN, CF_EDGES_OUT, CF_VERTICES,
            },
            graph::{build_full_edge, build_full_vertex, encode_props},
        },
        traits::GraphTransaction,
    },
    types::{gvalue::Property, CanonicalEdgeKey, Direction, Edge, LabelId, StoreError, Vertex, VertexKey},
};

type EdgeKeyDecoder = fn(&[u8]) -> Option<CanonicalEdgeKey>;

// ── Lifetime-erased RocksDB transaction ──────────────────────────────────────

type OwnedRocksTxn = rocksdb::Transaction<'static, OptimisticTransactionDB>;

/// Create a new optimistic transaction, erasing the `'db` lifetime.
///
/// # Safety
///
/// The caller must ensure the returned `OwnedRocksTxn` is dropped before the
/// `Arc<OptimisticTransactionDB>` it was created from.  In `Transaction` this
/// is guaranteed by field declaration order (`db_txn` before `db`).
fn begin_txn(db: &Arc<OptimisticTransactionDB>) -> OwnedRocksTxn {
    let mut opts = OptimisticTransactionOptions::default();
    opts.set_snapshot(true); // This is the equivalent of txn->SetSnapshot()
    let txn: rocksdb::Transaction<'_, OptimisticTransactionDB> = db.transaction_opt(&WriteOptions::default(), &opts);
    // SAFETY: see module doc and function safety note.
    unsafe { std::mem::transmute(txn) }
}

// ── Transaction ───────────────────────────────────────────────────────────────

pub struct Transaction {
    // IMPORTANT: `db_txn` must come before `db` — drop order is declaration order.
    db_txn: Option<OwnedRocksTxn>,
    db: Arc<OptimisticTransactionDB>,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(txn) = self.db_txn.take() {
            let _ = txn.rollback();
        }
        // `db_txn` is now None; the `Arc<OTD>` in `db` drops after this.
    }
}

impl Transaction {
    pub fn new(db: Arc<OptimisticTransactionDB>) -> Self {
        let db_txn = begin_txn(&db);
        Self { db_txn: Some(db_txn), db }
    }
}

// ── GraphTransaction ──────────────────────────────────────────────────────────

impl GraphTransaction for Transaction {
    fn get_vertex(&mut self, key: VertexKey) -> Result<Option<Arc<Vertex>>, StoreError> {
        let cf = self.db.cf_handle(CF_VERTICES).ok_or_else(|| StoreError::Other("missing CF: vertices".into()))?;
        let raw_opt = self
            .db_txn
            .as_ref()
            .expect("no active transaction")
            .get_for_update_cf(&cf, encode_vertex_key(key), true)
            .map_err(|e| StoreError::Other(e.to_string()))?;

        match raw_opt {
            None => Ok(None),
            Some(raw) => {
                let vv = VertexValue::decode(&raw).ok_or_else(|| StoreError::Other("corrupt vertex value".into()))?;
                Ok(Some(Arc::new(build_full_vertex(key, &vv)?)))
            }
        }
    }

    // TODO: do we need `GetForUpdate` for edges?
    fn get_edge(&mut self, key: CanonicalEdgeKey, direction: Direction) -> Result<Option<Arc<Edge>>, StoreError> {
        let (cf_name, key_bytes) = match direction {
            Direction::OUT => (CF_EDGES_OUT, encode_edge_key_out(key)),
            Direction::IN => (CF_EDGES_IN, encode_edge_key_in(key)),
        };
        let cf = self.db.cf_handle(cf_name).ok_or_else(|| StoreError::Other(format!("missing CF: {cf_name}")))?;
        let raw_opt = self
            .db_txn
            .as_ref()
            .expect("no active transaction")
            .get_for_update_cf(&cf, key_bytes, false)
            .map_err(|e| StoreError::Other(e.to_string()))?;

        match raw_opt {
            None => Ok(None),
            Some(raw) => Ok(Some(Arc::new(build_full_edge(key, &EdgeValue::decode(&raw))?))),
        }
    }

    fn get_edges(
        &mut self,
        vertex: VertexKey,
        direction: Direction,
        label: Option<LabelId>,
        dst: Option<&[VertexKey]>,
    ) -> Result<Vec<Arc<Edge>>, StoreError> {
        let (cf_name, decode_fn): (&str, EdgeKeyDecoder) = match direction {
            Direction::OUT => (CF_EDGES_OUT, decode_edge_key_out),
            Direction::IN => (CF_EDGES_IN, decode_edge_key_in),
        };

        let prefix = edge_scan_prefix(vertex, label);
        let mut read_opts = ReadOptions::default();
        if let Some(upper) = prefix_upper_bound(&prefix) {
            read_opts.set_iterate_upper_bound(upper);
        }

        let dst_set: Option<HashSet<VertexKey>> = dst.map(|k| k.iter().copied().collect());

        let cf = self.db.cf_handle(cf_name).ok_or_else(|| StoreError::Other(format!("missing CF: {cf_name}")))?;
        let txn = self.db_txn.as_ref().expect("no active transaction");
        let iter = txn.iterator_cf_opt(&cf, read_opts, IteratorMode::From(&prefix, ScanDir::Forward));

        let mut result = Vec::new();
        for item in iter {
            let (key_bytes, val_bytes) = item.map_err(|e| StoreError::Other(e.to_string()))?;
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            let cek = decode_fn(&key_bytes).ok_or_else(|| StoreError::Other("corrupt edge key".into()))?;
            if let Some(ref set) = dst_set {
                let remote = match direction {
                    Direction::OUT => cek.dst_id,
                    Direction::IN => cek.src_id,
                };
                if !set.contains(&remote) {
                    continue;
                }
            }
            result.push(Arc::new(build_full_edge(cek, &EdgeValue::decode(&val_bytes))?));
        }
        Ok(result)
    }

    fn put_vertex(
        &mut self,
        key: VertexKey,
        label_id: LabelId,
        out_e_cnt: u32,
        in_e_cnt: u32,
        props: &[Property],
    ) -> Result<(), StoreError> {
        let cf = self.db.cf_handle(CF_VERTICES).ok_or_else(|| StoreError::Other("missing CF: vertices".into()))?;
        let vv = VertexValue { label_id, out_e_cnt, in_e_cnt, property_blob: encode_props(props) };
        self.db_txn
            .as_ref()
            .expect("no active transaction")
            .put_cf(&cf, encode_vertex_key(key), vv.encode())
            .map_err(|e| StoreError::Other(e.to_string()))
    }

    fn put_edge(&mut self, key: CanonicalEdgeKey, direction: Direction, props: &[Property]) -> Result<(), StoreError> {
        let (cf_name, key_bytes) = match direction {
            Direction::OUT => (CF_EDGES_OUT, encode_edge_key_out(key)),
            Direction::IN => (CF_EDGES_IN, encode_edge_key_in(key)),
        };
        let cf = self.db.cf_handle(cf_name).ok_or_else(|| StoreError::Other(format!("missing CF: {cf_name}")))?;
        let ev_bytes = EdgeValue { property_blob: encode_props(props) }.encode().to_vec();
        let txn = self.db_txn.as_ref().expect("no active transaction");
        txn.put_cf(&cf, key_bytes, &ev_bytes).map_err(|e| StoreError::Other(e.to_string()))
    }

    fn delete_vertex(&mut self, key: VertexKey) -> Result<(), StoreError> {
        let cf = self.db.cf_handle(CF_VERTICES).ok_or_else(|| StoreError::Other("missing CF: vertices".into()))?;
        self.db_txn
            .as_ref()
            .expect("no active transaction")
            .delete_cf(&cf, encode_vertex_key(key))
            .map_err(|e| StoreError::Other(e.to_string()))
    }

    fn delete_edge(&mut self, key: CanonicalEdgeKey, direction: Direction) -> Result<(), StoreError> {
        let (cf_name, key_bytes) = match direction {
            Direction::OUT => (CF_EDGES_OUT, encode_edge_key_out(key)),
            Direction::IN => (CF_EDGES_IN, encode_edge_key_in(key)),
        };
        let cf = self.db.cf_handle(cf_name).ok_or_else(|| StoreError::Other(format!("missing CF: {cf_name}")))?;
        let txn = self.db_txn.as_ref().expect("no active transaction");
        txn.delete_cf(&cf, key_bytes).map_err(|e| StoreError::Other(e.to_string()))
    }

    fn commit(&mut self) -> Result<(), StoreError> {
        let txn = self.db_txn.take().expect("no active transaction");
        let result = txn.commit().map_err(|e| {
            if e.to_string().contains("Resource busy") {
                StoreError::Conflict
            } else {
                StoreError::Other(e.to_string())
            }
        });
        self.db_txn = Some(begin_txn(&self.db));
        result
    }

    fn abort(&mut self) {
        if let Some(txn) = self.db_txn.take() {
            let _ = txn.rollback();
        }
        self.db_txn = Some(begin_txn(&self.db));
    }
}
