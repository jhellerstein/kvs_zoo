//! Local KVS (single node)

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
    let report = built.check_coordination();
    println!("{report}");
    built.generate_graph_with_config(&args.graph, None)?;
    Ok(())
}
