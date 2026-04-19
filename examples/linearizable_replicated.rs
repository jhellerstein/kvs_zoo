//! Linearizable Replicated KVS (Paxos ordering → all replicas)

use clap::Parser;
use futures::{SinkExt, StreamExt};
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

// Paxos provides TotalOrder directly to all replicas.
// No separate replication layer needed — Paxos broadcast delivers to all members.
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

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

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
    built.generate_graph_with_config(&args.graph, None)?;
    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: Paxos role clusters + target cluster + leaf
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get_role::<OrderedCluster, kvs_zoo::before_storage::ordering::Proposer>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get_role::<OrderedCluster, kvs_zoo::before_storage::ordering::Acceptor>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<OrderedCluster>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<Leaf>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(bidi_port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Demo operations
    let ops = vec![
        KVSOperation::Put("acct".into(), "100".to_string(), 1, None),
        KVSOperation::Get("acct".into(), 2, None),
        KVSOperation::Put("acct".into(), "200".to_string(), 3, None),
        KVSOperation::Get("acct".into(), 4, None),
    ];
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ Linearizable Replicated demo complete");
    Ok(())
}
