//! Local KVS (single node) — INCONSISTENT
//!
//! This architecture uses a non-commutative overwrite fold: the final value
//! of a key depends on the order operations are processed. If two clients
//! concurrently write different values to the same key, the result depends
//! on message ordering — different runs may produce different final states.
//!
//! The coordination analysis correctly identifies this as INCONSISTENT:
//! the `foldkeyed` operator breaks the monotonicity proof because it lacks
//! a commutativity+idempotency proof (it's last-writer-wins, not a lattice).

use clap::Parser;
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::before_storage::routing::SingleNodeRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow;

// Marker type for Hydro location type / KVS layer type
#[derive(Clone)]
struct LocalStorage;

// Architecture: single layer, single node
type LocalKVS = KVSCluster<LocalStorage, SingleNodeRouter, (), ()>; // KVSCluster<Marker type, Before, After, Nested layer>

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("🚀 Local KVS Demo (single node)");


    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Build a Hydro graph for the LocalKVS type, return layer handles and client I/O ports
    let (layers, port) = plumb_kvs_dataflow::<String, String, _>(
        &proxy,
        &client_external,
        &mut flow,
        LocalKVS::default(),
    );

    let built = flow.finalize();
    
    

    built.generate_graph(&args.graph)?;
    Ok(())
}
