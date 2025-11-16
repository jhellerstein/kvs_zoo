//! Replicated KVS with tombstone deletes (idealized ergonomic target)
//!
//! This is the clean target style we want to support. It mirrors `replicated.rs`
//! and adds a Delete demonstrating eventual NOT FOUND plus live Tomb metadata.

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::background::TombIndexBackground;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct Replica;

type ReplicatedKVS = KVSCluster<
    Replica,
    RoundRobinRouter,
    SimpleGossip<LwwWrapper<String>>,
    (),                  // no nested child layer
    TombIndexBackground, // background tomb indexer
>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("🚀 Replicated KVS Tombstone Demo (idealized)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let mut spec: ReplicatedKVS = Default::default();
    spec.after = SimpleGossip::new(100); // faster gossip for demo
    spec.background = TombIndexBackground::new()
        .with_logging(true)
        .with_summaries(true);

    let (layers, port) =
        plumb_kvs_dataflow::<LwwWrapper<String>, _>(&proxy, &client_external, &flow, spec);

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;
    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<Replica>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    use kvs_zoo::protocol::KVSOperation as Op;
    let ops = vec![
        Op::Put("alpha".into(), LwwWrapper::new("one".into())),
        Op::Get("alpha".into()),
        Op::Delete("alpha".into()), // emits MetaEvent::Tomb("alpha") to background stage
        Op::Get("alpha".into()),    // expect NOT FOUND
    ];

    for (i, op) in ops.into_iter().enumerate() {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
        if i == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    // TODO(phase-2): Replace stdout logging with structured metrics collection.

    deployment.stop().await?;
    println!("✅ Idealized tombstone demo complete (meta pending)");
    Ok(())
}
