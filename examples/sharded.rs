//! Sharded KVS (hash-partitioned)

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::before_storage::routing::ShardedRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

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

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture via defaults (Sharded-only)
    let mut kvs_spec: ShardedKVS = Default::default();
    kvs_spec.before = ShardedRouter::new(3); // 3 shards. this is the default but here to demonstrate how to override defaults.

    // Build a Hydro graph for the ShardedKVS type, return layer handles and client I/O ports
    let (layers, port) =
        plumb_kvs_dataflow::<String, LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs_spec);

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;
    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: 3 shards, 1 node each
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<Shard>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Workload inline
    let ops = vec![
        KVSOperation::Put("user:1".into(), LwwWrapper::new("alice".into()), 1, None),
        KVSOperation::Put("user:2".into(), LwwWrapper::new("bob".into()), 2, None),
        KVSOperation::Get("user:1".into(), 3, None),
        KVSOperation::Get("user:2".into(), 4, None),
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
    println!("✅ Sharded local demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<String, LwwWrapper<String>>, shards: u64) -> Option<String> {
    match op {
        KVSOperation::Put(key, _, _, _) | KVSOperation::Get(key, _, _) | KVSOperation::Delete(key, _, _) => {
            let shard_id = kvs_zoo::before_storage::routing::ShardedRouter::calculate_shard_id(
                key,
                shards as usize,
            );
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
