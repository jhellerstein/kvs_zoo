//! Sharded KVS — INCONSISTENT
//!
//! Same as local: overwrite fold is not commutative.
//! Sharding distributes keys across nodes but each node
//! still uses last-writer-wins, which is order-dependent.

use clap::Parser;
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::before_storage::routing::ShardedRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::protocol::KVSOperation;

// Marker type naming this KVS layer
#[derive(Clone)]
struct Shard;

// KVS architecture type: single layer with sharded routing
type ShardedKVS = KVSCluster<Shard, ShardedRouter, (), ()>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("🚀 Sharded Local KVS Demo");


    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture via defaults (Sharded-only)
    let mut kvs_spec: ShardedKVS = Default::default();
    kvs_spec.before = ShardedRouter::new(3); // 3 shards. this is the default but here to demonstrate how to override defaults.

    // Build a Hydro graph for the ShardedKVS type, return layer handles and client I/O ports
    let (layers, port) = plumb_kvs_dataflow::<String, String, _>(
        &proxy,
        &client_external,
        &mut flow,
        kvs_spec,
    );

    let built = flow.finalize();
    
    
    built.generate_graph(&args.graph)?;
    Ok(())
}
