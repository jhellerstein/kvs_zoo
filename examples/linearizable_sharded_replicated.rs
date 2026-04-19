//! Linearizable Sharded+Replicated KVS (Paxos ordering → all replicas)
//!
//! NOTE: Simplified to use PaxosDispatcher directly (no sharding layer)
//! because ShardedRouter erases TotalOrder in the type system.
//! The coordination analysis correctly reports SEQUENTIALLY CONSISTENT.

use clap::Parser;
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
use kvs_zoo::before_storage::routing::SingleNodeRouter;
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::plumbing::plumb_kvs_dataflow_ordered;
use kvs_zoo::protocol::KVSOperation;

#[derive(Clone)]
struct OrderedCluster;
#[derive(Clone)]
struct Leaf;

type LinearizableKVS = KVSCluster<
    OrderedCluster,
    PaxosDispatcher<String, String>,
    (),
    KVSNode<Leaf, SingleNodeRouter, Responder>,
>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("🚀 Linearizable KVS Demo (Paxos ordering)");


    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let kvs_spec: LinearizableKVS = Default::default();

    let (layers, bidi_port) = plumb_kvs_dataflow_ordered::<String, String, _>(
        &proxy,
        &client_external,
        &mut flow,
        kvs_spec,
    );

    let built = flow.finalize();
    let report = built.check_coordination();
    println!("{report}");
    built.generate_graph_with_config(&args.graph, None)?;
    Ok(())
}
