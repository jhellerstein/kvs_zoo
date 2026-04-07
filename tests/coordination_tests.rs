//! Coordination Criterion analysis for kvs_zoo architectures.

use hydro_lang::compile::builder::FlowBuilder;
use kvs_zoo::before_storage::routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::{plumb_kvs_dataflow, plumb_kvs_dataflow_lattice};
use kvs_zoo::values::CausalWrapper;

#[derive(Clone)]
struct N;

/// Local single-node KVS with overwrite (plain String) values.
/// Should be future-monotone: single node, deterministic order.
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
/// Should be future-monotone: each shard is single-node.
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
/// Should be future-monotone: lattice merge is commutative.
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

// TODO: Linearizable KVS needs BroadcastReplication to support overwrite values.
// Currently BroadcastReplication requires V: Merge, but with Paxos ordering
// the replicas should just apply writes in order (no merge needed).
// This is a future refactor — split BroadcastReplication into lattice vs ordered variants.
//
// type LinearizableKVS = KVSCluster<
//     Ordered,
//     Pipeline<PaxosDispatcher<String, String>, RoundRobinRouter>,
//     (),
//     KVSCluster<
//         SeqRep,
//         RoundRobinRouter,
//         BroadcastReplication<String, String>,
//         KVSNode<Leaf, SlotOrderEnforcer, Responder>,
//     >,
// >;
//
// #[test]
// fn coordination_linearizable_kvs() {
//     let mut flow = FlowBuilder::new();
//     let proxy = flow.process::<()>();
//     let ext = flow.external::<()>();
//     let kvs: LinearizableKVS = Default::default();
//     let _ = plumb_kvs_dataflow::<String, String, _>(&proxy, &ext, &mut flow, kvs);
//     let report = flow.finalize().check_coordination();
//     println!("\n=== Linearizable Replicated KVS (Paxos) ===\n{report}");
// }



