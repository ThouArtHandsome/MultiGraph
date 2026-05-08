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

//! Helpers for merging a committed baseline element with a `DirtyEntry`.
//!
//! All returned `FullVertex` / `FullEdge` values carry dead (`Weak::new()`)
//! owner back-pointers on their properties — the same convention used by the
//! RocksDB read path.  The traversal engine wraps returned values in
//! `Arc<FullElement>` via `Arc::new_cyclic` when live back-pointers are
//! needed.

use std::collections::HashSet;
use std::sync::Weak;

use smol_str::SmolStr;

use crate::transaction::dirty_cache::{DirtyEntry, PropMutation};
use crate::types::gvalue::Property;
use crate::types::prop_key::PropKey;
use crate::types::{EdgeKey, FullEdge, FullElement, FullVertex, VertexKey};

// ── Core prop merge ───────────────────────────────────────────────────────────

/// Apply `mutations` on top of `base_props` and return the resulting property
/// list with dead owner pointers.
///
/// Order guarantee: base properties that survive appear first (in their
/// original order), followed by any net-new properties introduced by `Set`
/// mutations.
pub fn apply_prop_mutations(
    base_props: &[Property],
    dirty: &DirtyEntry,
) -> Vec<Property> {
    let mutations = &dirty.props;

    // Surviving base props (with mutations applied where present).
    let mut result: Vec<Property> = base_props
        .iter()
        .filter_map(|p| match mutations.get(&p.key) {
            None => Some(p.clone()),
            Some(PropMutation::Set(v)) => Some(Property {
                owner: Weak::<FullElement>::new(),
                key: p.key.clone(),
                value: v.clone(),
            }),
            Some(PropMutation::Removed) => None,
        })
        .collect();

    // Net-new props (Set mutations for keys absent in the base).
    let base_keys: HashSet<&PropKey> = base_props.iter().map(|p| &p.key).collect();
    for (k, m) in mutations {
        if !base_keys.contains(k) {
            if let PropMutation::Set(v) = m {
                result.push(Property {
                    owner: Weak::<FullElement>::new(),
                    key: k.clone(),
                    value: v.clone(),
                });
            }
        }
    }

    result
}

/// Build a property list from pure mutations (no baseline — used for newly
/// created elements).  Only `Set` mutations contribute; `Removed` entries are
/// ignored since there is no prior value to remove.
pub fn props_from_mutations(dirty: &DirtyEntry) -> Vec<Property> {
    apply_prop_mutations(&[], dirty)
}

// ── Element builders ──────────────────────────────────────────────────────────

/// Merge a committed vertex with its dirty overlay, producing a fresh
/// `FullVertex` reflecting the transaction's view.
pub fn merge_vertex(base: &FullVertex, dirty: &DirtyEntry) -> FullVertex {
    FullVertex {
        id: base.id,
        label: base.label.clone(),
        props: apply_prop_mutations(&base.props, dirty),
    }
}

/// Merge a committed edge with its dirty overlay.
///
/// `view_key` is the direction-specific key to embed in the result — callers
/// set this to the OUT or IN form depending on which CF the edge came from.
pub fn merge_edge(base: &FullEdge, dirty: &DirtyEntry, view_key: EdgeKey) -> FullEdge {
    FullEdge { key: view_key, props: apply_prop_mutations(&base.props, dirty) }
}

/// Build a brand-new vertex from a `NewVertex` dirty entry (no baseline).
pub fn build_new_vertex(id: VertexKey, label: SmolStr, dirty: &DirtyEntry) -> FullVertex {
    FullVertex { id, label, props: props_from_mutations(dirty) }
}

/// Build a brand-new edge from a `NewEdge` dirty entry (no baseline).
pub fn build_new_edge(view_key: EdgeKey, dirty: &DirtyEntry) -> FullEdge {
    FullEdge { key: view_key, props: props_from_mutations(dirty) }
}
