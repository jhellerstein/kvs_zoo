//! Coordination Criterion analysis for kvs_zoo architectures.

use hydro_lang::compile::builder::FlowBuilder;
use kvs_zoo::before_storage::routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::{plumb_kvs_dataflow, plumb_kvs_dataflow_lattice, plumb_kvs_dataflow_ordered};
use kvs_zoo::values::CausalWrapper;

#[derive(Clone)]
struct N;

/// Local single-node KVS with overwrite (plain String) values.
/// FAIL: overwrite fold is not commutative.
#[test]
fn coordination_local_kvs() {
    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: KVSCluster<N, SingleNodeRouter, (), ()> = Default::default();
    let _ = plumb_kvs_dataflow::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Local KVS (overwrite, single node) ===\n{report}");
}

/// Sharded KVS with overwrite values.
/// FAIL: overwrite fold is not commutative.
#[test]
fn coordination_sharded_kvs() {
    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: KVSCluster<N, ShardedRouter, (), ()> = Default::default();
    let _ = plumb_kvs_dataflow::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Sharded KVS (overwrite) ===\n{report}");
}

/// Replicated KVS with gossip and CausalWrapper (lattice) values.
/// PASS: lattice merge is commutative+idempotent.
#[test]
fn coordination_replicated_causal_kvs() {
    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: KVSCluster<N, RoundRobinRouter, SimpleGossip<String, CausalWrapper<String>>, ()> = Default::default();
    let _ = plumb_kvs_dataflow_lattice::<String, CausalWrapper<String>, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Replicated KVS (gossip + CausalWrapper lattice) ===\n{report}");
}

/// Test that scan on a TotalOrder stream proves prefix order.
/// This simulates the Paxos-ordered path: operations arrive in total order,
/// scan processes them sequentially, producing a prefix-ordered output.
#[test]
fn coordination_scan_total_order() {
    use hydro_lang::prelude::*;
    use hydro_lang::live_collections::stream::TotalOrder;

    let mut flow = FlowBuilder::new();
    let process = flow.process::<()>();

    // Simulate a TotalOrder input (e.g., from Paxos)
    let input: Stream<String, _, _, TotalOrder> = process
        .source_iter(q!(vec!["a".to_string(), "b".to_string()]))
        .assume_ordering::<TotalOrder>(nondet!(/** simulating Paxos output */));

    // Scan: sequential stateful processing on TotalOrder input
    let output = input.scan(
        q!(|| Vec::<String>::new()),
        q!(|state, item| {
            state.push(item.clone());
            Some(format!("{}: {:?}", item, state))
        }),
    );

    // Observable sink: for_each (side effect)
    output.for_each(q!(|item| println!("{item}")));

    let report = flow.finalize().check_coordination();
    println!("\n=== Scan on TotalOrder (simulated Paxos) ===\n{report}");
    assert!(report.all_monotone(), "Scan on TotalOrder should be monotone under prefix order");
}

// TODO: Full Paxos-ordered KVS test requires the Paxos dispatcher to produce
// TotalOrder output in the Hydro type system. Currently it produces NoOrder
// and relies on runtime ordering guarantees. This is a gap between the
// protocol's semantic guarantees and the type-level representation.

/// Paxos-ordered KVS with overwrite values (no replication).
/// Should PASS: Paxos provides TotalOrder, scan processes sequentially.
#[test]
fn coordination_paxos_kvs() {
    use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;

    #[derive(Clone)]
    struct Ordered;

    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: KVSCluster<Ordered, PaxosDispatcher<String, String>, (), ()> = Default::default();
    let _ = plumb_kvs_dataflow_ordered::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Paxos-ordered KVS (overwrite, no replication) ===\n{report}");
}

/// Linearizable replicated KVS: Paxos ordering + broadcast overwrite replication.
/// Should PASS: Paxos provides TotalOrder, scan processes sequentially,
/// replicas receive ordered writes via BroadcastOverwrite.
#[test]
fn coordination_linearizable_replicated_kvs() {
    use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
    use kvs_zoo::before_storage::ordering::SlotOrderEnforcer;
    use kvs_zoo::before_storage::Pipeline;
    use kvs_zoo::after_storage::replication::BroadcastOverwrite;
    use kvs_zoo::after_storage::responders::Responder;
    use kvs_zoo::kvs_layer::KVSNode;

    #[derive(Clone)]
    struct Ordered;
    #[derive(Clone)]
    struct SeqRep;
    #[derive(Clone)]
    struct Leaf;

    type LinearizableKVS = KVSCluster<
        Ordered,
        Pipeline<PaxosDispatcher<String, String>, RoundRobinRouter>,
        (),
        KVSCluster<
            SeqRep,
            RoundRobinRouter,
            BroadcastOverwrite<String, String>,
            KVSNode<Leaf, SlotOrderEnforcer, Responder>,
        >,
    >;

    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: LinearizableKVS = Default::default();
    let _ = plumb_kvs_dataflow_ordered::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Linearizable Replicated KVS (Paxos + BroadcastOverwrite) ===\n{report}");
}
