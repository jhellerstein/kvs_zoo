//! Replicated KVS with Tombstone-Based Deletion (RoundRobin + Gossip)
//!
//! This example demonstrates tombstone-based deletion in a replicated KVS.
//! Unlike standard deletion (which removes keys), tombstone deletion marks
//! keys as permanently deleted. Once tombstoned, a key cannot be resurrected
//! by subsequent PUT operations.

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;

// Marker type naming this KVS layer
#[derive(Clone)]
struct Replica;

// KVS architecture type: single layer with RoundRobin + Gossip
type ReplicatedTombstoneKVS<V> = KVSCluster<Replica, RoundRobinRouter, SimpleGossip<String, V>, ()>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run_example(&args).await
}

async fn run_example(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS with Tombstones Demo");
    println!("   This demo shows permanent tombstone-based deletion\n");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture via defaults; override only the gossip interval
    let mut kvs_spec: ReplicatedTombstoneKVS<String> = Default::default();
    kvs_spec.after = SimpleGossip::new(100usize); // 100ms gossip interval

    // Build a Hydro graph for the ReplicatedTombstoneKVS type with tombstone storage
    let (layers, port) = kvs_zoo::plumbing::plumb_kvs_dataflow_with_tombstones(
        &proxy,
        &client_external,
        &flow,
        kvs_spec,
    );

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;

    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: 3 replicas for the cluster
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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Run demo operations showing tombstone behavior
    let ops = tombstone_demo_ops();

    for (i, (op, description)) in ops.into_iter().enumerate() {
        println!("Step {}: {}", i + 1, description);
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("   → {}", resp);
        }

        // Pause after operations to allow gossip propagation
        if i == 0 || i == 2 || i == 4 {
            tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        }
    }

    deployment.stop().await?;
    println!("\n✅ Replicated tombstone demo complete");
    println!("   Key observation: After deletion, PUT cannot resurrect the key!");
    Ok(())
}

/// Generate operations that demonstrate tombstone permanence
fn tombstone_demo_ops() -> Vec<(
    kvs_zoo::protocol::KVSOperation<String, String>,
    &'static str,
)> {
    use kvs_zoo::protocol::KVSOperation as Op;

    vec![
        (
            Op::Put("x".to_string(), "1".to_string(), 1, None),
            "PUT x = \"1\"",
        ),
        (Op::Get("x".to_string(), 2, None), "GET x (expect \"1\")"),
        (
            Op::Delete("x".to_string(), 3, None),
            "DELETE x (tombstone created)",
        ),
        (
            Op::Get("x".to_string(), 4, None),
            "GET x (expect None - tombstoned)",
        ),
        (
            Op::Put("x".to_string(), "2".to_string(), 5, None),
            "PUT x = \"2\" (attempt resurrection)",
        ),
        (
            Op::Get("x".to_string(), 6, None),
            "GET x (expect None - tombstone is permanent!)",
        ),
    ]
}
