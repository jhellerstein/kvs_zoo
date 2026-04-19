//! Replicated KVS (RoundRobin + Gossip)

use clap::{Parser, ValueEnum};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow_lattice;
use kvs_zoo::values::CausalString;

#[derive(Clone)]
struct Replica;

type ReplicatedKVS = KVSCluster<Replica, RoundRobinRouter, SimpleGossip<String, CausalString>, ()>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("🚀 Replicated KVS Demo (gossip + CausalWrapper lattice)");

    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let mut kvs_spec: ReplicatedKVS = Default::default();
    kvs_spec.after = SimpleGossip::new(100usize);

    let (_layers, _port) =
        plumb_kvs_dataflow_lattice::<String, CausalString, _>(&proxy, &client_external, &mut flow, kvs_spec);

    let built = flow.finalize();
    let report = built.check_coordination();
    println!("{report}");
    built.generate_graph_with_config(&args.graph, None)?;
    Ok(())
}
