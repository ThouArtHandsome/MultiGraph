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

use serde::Serialize;

#[derive(Serialize, Debug)]
struct Point {
    x: i32,
    y: i32,
}

fn main() {
    println!("Hello, world!");

    let point = Point { x: 1, y: 2 };

    // Convert the Point to a JSON string.
    let serialized = serde_json::to_string(&point).unwrap();

    // Prints serialized = {"x":1,"y":2}
    println!("Serialized point = {serialized}",);
}

#[cfg(test)]
mod tests {
    use multigraph::context::GraphContext;
    use multigraph::store::{GraphStore, RocksStorage};
    use multigraph::types::gvalue::Primitive;
    use multigraph::types::keys::{CanonicalEdgeKey, CanonicalKey, Direction};
    use smol_str::SmolStr;

    fn open() -> (RocksStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksStorage::open(dir.path()).unwrap();
        (store, dir)
    }

    fn new_ctx(store: &RocksStorage) -> GraphContext<RocksStorage> {
        GraphContext::new(store.begin(), store.id_gen())
    }

    fn cek(src: u64, label: u16, dst: u64) -> CanonicalEdgeKey {
        CanonicalEdgeKey { src_id: src, label_id: label, rank: 0, dst_id: dst }
    }

    fn prop(v: &multigraph::types::full_element::FullVertex, key: &str) -> Option<Primitive> {
        v.props.iter().find(|p| p.key == key).map(|p| p.value.clone())
    }

    fn eprop(e: &multigraph::types::full_element::FullEdge, key: &str) -> Option<Primitive> {
        e.props.iter().find(|p| p.key == key).map(|p| p.value.clone())
    }

    // ── Multi-context sequential insert + retrieve ────────────────────────────

    // Build a 3-vertex, 2-edge graph across three independent contexts, then
    // verify the full graph is visible to a fourth context.
    #[test]
    fn three_contexts_build_graph_fourth_reads_all() {
        let (store, _dir) = open();

        // ctx1 — person: Alice
        let alice = {
            let mut c = new_ctx(&store);
            let (key, _) = c.add_vertex(1);
            c.set_property(CanonicalKey::Vertex(key), SmolStr::new("name"), Primitive::String(SmolStr::new("Alice")))
                .unwrap();
            c.set_property(CanonicalKey::Vertex(key), SmolStr::new("age"), Primitive::Int32(30)).unwrap();
            c.commit().unwrap();
            key
        };

        // ctx2 — person: Bob
        let bob = {
            let mut c = new_ctx(&store);
            let (key, _) = c.add_vertex(1);
            c.set_property(CanonicalKey::Vertex(key), SmolStr::new("name"), Primitive::String(SmolStr::new("Bob")))
                .unwrap();
            c.set_property(CanonicalKey::Vertex(key), SmolStr::new("age"), Primitive::Int32(25)).unwrap();
            c.commit().unwrap();
            key
        };

        // ctx3 — city: London + two "lives_in" edges (label=2) from each person
        let london = {
            let mut c = new_ctx(&store);
            let (city_key, _) = c.add_vertex(2);
            c.set_property(
                CanonicalKey::Vertex(city_key),
                SmolStr::new("name"),
                Primitive::String(SmolStr::new("London")),
            )
            .unwrap();
            // Alice -> London
            let e1 = cek(alice, 2, city_key);
            c.add_edge(e1);
            c.set_property(CanonicalKey::Edge(e1), SmolStr::new("since"), Primitive::Int32(2015)).unwrap();
            // Bob -> London
            let e2 = cek(bob, 2, city_key);
            c.add_edge(e2);
            c.set_property(CanonicalKey::Edge(e2), SmolStr::new("since"), Primitive::Int32(2019)).unwrap();
            c.commit().unwrap();
            city_key
        };

        // ctx4 — read-only verification
        let mut c = new_ctx(&store);

        // Vertices survive across contexts.
        let alice_idx = c.get_vertex(alice).unwrap().unwrap();
        assert_eq!(alice_idx.label_id, 1);
        assert_eq!(prop(&alice_idx, "name"), Some(Primitive::String(SmolStr::new("Alice"))));
        assert_eq!(prop(&alice_idx, "age"), Some(Primitive::Int32(30)));

        let bob_idx = c.get_vertex(bob).unwrap().unwrap();
        assert_eq!(bob_idx.label_id, 1);
        assert_eq!(prop(&bob_idx, "name"), Some(Primitive::String(SmolStr::new("Bob"))));

        let london_idx = c.get_vertex(london).unwrap().unwrap();
        assert_eq!(london_idx.label_id, 2);
        assert_eq!(prop(&london_idx, "name"), Some(Primitive::String(SmolStr::new("London"))));

        // Both outgoing "lives_in" edges from Alice land at London.
        let alice_out = c.get_edges(alice, Direction::OUT, Some(2), None).unwrap();
        assert_eq!(alice_out.len(), 1);
        let (e_idx, fe) = &alice_out[0];
        assert_eq!(e_idx.secondary_id, london);
        assert_eq!(eprop(&fe, "since"), Some(Primitive::Int32(2015)));

        // London has two incoming edges: one from Alice, one from Bob.
        let london_in = c.get_edges(london, Direction::IN, Some(2), None).unwrap();
        assert_eq!(london_in.len(), 2);
        let mut src_ids: Vec<u64> = london_in.iter().map(|(ek, _)| ek.primary_id).collect();
        src_ids.sort_unstable();
        assert_eq!(src_ids, vec![alice.min(bob), alice.max(bob)]);
    }

    // ── Same-context: vertices + edges visible before commit ──────────────────

    #[test]
    fn single_context_read_your_writes_vertices_and_edges() {
        let (store, _dir) = open();
        let mut c = new_ctx(&store);

        let (v1, idx1) = c.add_vertex(1);
        let (v2, idx2) = c.add_vertex(1);
        
        c.set_property(CanonicalKey::Vertex(v1), SmolStr::new("role"), Primitive::String(SmolStr::new("src"))).unwrap();
        c.set_property(CanonicalKey::Vertex(v2), SmolStr::new("role"), Primitive::String(SmolStr::new("dst"))).unwrap();
        
        let e = cek(v1, 3, v2);
        c.add_edge(e);
        c.set_property(CanonicalKey::Edge(e), SmolStr::new("w"), Primitive::Float64(0.5)).unwrap();

        // All elements are visible in the same context before any commit.
        assert_eq!(prop(&idx1, "role"), Some(Primitive::String(SmolStr::new("src"))));
        assert_eq!(prop(&idx2, "role"), Some(Primitive::String(SmolStr::new("dst"))));
        let edges = c.get_edges(v1, Direction::OUT, Some(3), None).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(eprop(&edges[0].1, "w"), Some(Primitive::Float64(0.5)));

        // After commit, a fresh context can read everything.
        c.commit().unwrap();
        let mut c2 = new_ctx(&store);
        let idx = c2.get_vertex(v1).unwrap().unwrap();
        assert_eq!(prop(&idx, "role"), Some(Primitive::String(SmolStr::new("src"))));
        let committed_edges = c2.get_edges(v1, Direction::OUT, Some(3), None).unwrap();
        assert_eq!(committed_edges.len(), 1);
    }

    // ── Sequential contexts accumulate data ───────────────────────────────────

    #[test]
    fn sequential_contexts_accumulate_edges() {
        let (store, _dir) = open();

        // Build edges in separate contexts; each must see all previously committed edges.
        let hub = {
            let mut c = new_ctx(&store);
            let (key, _) = c.add_vertex(1);
            c.commit().unwrap();
            key
        };

        let spokes: Vec<u64> = (0..4)
            .map(|_| {
                let mut c = new_ctx(&store);
                let (key, _) = c.add_vertex(1);
                c.add_edge(cek(hub, 1, key));
                c.commit().unwrap();
                key
            })
            .collect();

        // A final context must see all 4 outgoing edges from hub.
        let mut c = new_ctx(&store);
        let out = c.get_edges(hub, Direction::OUT, Some(1), None).unwrap();
        assert_eq!(out.len(), 4);
        let mut dst_ids: Vec<u64> = out.iter().map(|(ek, _)| ek.secondary_id).collect();
        dst_ids.sort_unstable();
        let mut expected = spokes.clone();
        expected.sort_unstable();
        assert_eq!(dst_ids, expected);

        // Each spoke has exactly one incoming edge from hub.
        for &spoke in &spokes {
            let in_edges = c.get_edges(spoke, Direction::IN, Some(1), None).unwrap();
            assert_eq!(in_edges.len(), 1);
            assert_eq!(in_edges[0].1.src_id, hub);
        }
    }
}
