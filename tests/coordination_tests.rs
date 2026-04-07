//! Coordination Criterion analysis for kvs_zoo architectures.

use hydro_lang::compile::builder::FlowBuilder;
use kvs_zoo::before_storage::routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};
use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
use kvs_zoo::before_storage::ordering::SlotOrderEnforcer;
use kvs_zoo::before_storage::Pipeline;
use kvs_zoo::after_storage::replication::{BroadcastReplication, SimpleGossip};
use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::plumbing::plumb_kvs_dataflow;

#[derive(Clone)]
struct N;

#[test]
fn coordination_local_kvs() {
    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: KVSCluster<N, SingleNodeRouter, (), ()> = Default::default();
    let _ = plumb_kvs_dataflow::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Local KVS ===\n{report}");
}

#[test]
fn coordination_replicated_kvs() {
    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: KVSCluster<N, RoundRobinRouter, SimpleGossip<String, String>, ()> = Default::default();
    let _ = plumb_kvs_dataflow::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Replicated KVS (gossip) ===\n{report}");
}

#[test]
fn coordination_sharded_kvs() {
    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: KVSCluster<N, ShardedRouter, (), ()> = Default::default();
    let _ = plumb_kvs_dataflow::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Sharded KVS ===\n{report}");
}

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
        BroadcastReplication<String, String>,
        KVSNode<Leaf, SlotOrderEnforcer, Responder>,
    >,
>;

#[test]
fn coordination_linearizable_kvs() {
    let mut flow = FlowBuilder::new();
    let proxy = flow.process::<()>();
    let ext = flow.external::<()>();
    let kvs: LinearizableKVS = Default::default();
    let _ = plumb_kvs_dataflow::<String, String, _>(&proxy, &ext, &mut flow, kvs);
    let report = flow.finalize().check_coordination();
    println!("\n=== Linearizable Replicated KVS (Paxos) ===\n{report}");
}
