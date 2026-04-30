//! Sharded + Replicated KVS (gossip + CausalWrapper) — CONVERGENT
//!
//! Combines sharding with lattice-based gossip replication.
//! Each shard uses CausalWrapper merge — convergent.

use clap::Parser;
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::{BroadcastReplication, BroadcastReplicationConfig};
use kvs_zoo::before_storage::routing::{RoundRobinRouter, ShardedRouter};
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::CausalString;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

// Hydro location types = KVS layer types (no duplication!)
#[derive(Clone)]
struct Shard;

#[derive(Clone)]
struct Replica;

// Architecture: nested layers - sharding at top, replication within each shard
type ShardedReplicatedKVS = KVSCluster<
    Shard,
    ShardedRouter,
    (),
    KVSCluster<Replica, RoundRobinRouter, BroadcastReplication<String, CausalString>, ()>,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("🚀 Sharded + Replicated KVS Demo");


    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture via defaults (Sharded → RR → Broadcast)
    let mut kvs_spec: ShardedReplicatedKVS = Default::default();
    // An example of overriding defaults down the layers
    kvs_spec.child.after =
        BroadcastReplication::with_config(BroadcastReplicationConfig::low_latency());

    // Build a Hydro graph for the ShardedReplicatedKVS type, return layer handles and client I/O ports
    let (layers, port) =
        plumb_kvs_dataflow::<String, CausalString, _>(&proxy, &client_external, &mut flow, kvs_spec);

    let built = flow.finalize();
    
    
    built.generate_graph(&args.graph)?;
    Ok(())
}
