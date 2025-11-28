//! Sharded + Replicated KVS (shards × replicas)

use clap::Parser;
use futures::{SinkExt, StreamExt};
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

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture via defaults (Sharded → RR → Broadcast)
    let mut kvs_spec: ShardedReplicatedKVS = Default::default();
    // An example of overriding defaults down the layers
    kvs_spec.child.after =
        BroadcastReplication::with_config(BroadcastReplicationConfig::low_latency());

    // Build a Hydro graph for the ShardedReplicatedKVS type, return layer handles and client I/O ports
    let (layers, port) =
        plumb_kvs_dataflow::<String, CausalString, _>(&proxy, &client_external, &flow, kvs_spec);

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;
    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: one cluster per layer
    // - Shard cluster: 3 members (default)
    // - Replica cluster: 3 members (default)
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<Shard>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<Replica>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Run operations
    fn causal(node: &str, v: &str) -> CausalString {
        let mut vc = kvs_zoo::values::VCWrapper::new();
        vc.bump(node.to_string());
        CausalString::new(vc, v.to_string())
    }
    let ops = vec![
        KVSOperation::Put("user:alice".into(), causal("a", "x"), 1, None),
        KVSOperation::Put("user:bob".into(), causal("b", "y"), 2, None),
        KVSOperation::Get("user:alice".into(), 3, None),
        KVSOperation::Get("user:bob".into(), 4, None),
    ];

    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ Sharded+Replicated demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<String, CausalString>, shard_count: usize) -> Option<String> {
    match op {
        KVSOperation::Put(key, _, _, _) | KVSOperation::Get(key, _, _) | KVSOperation::Delete(key, _, _) => {
            let shard_id = kvs_zoo::before_storage::routing::ShardedRouter::calculate_shard_id(
                key,
                shard_count,
            );
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
