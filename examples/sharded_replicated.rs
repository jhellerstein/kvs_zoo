//! Sharded + Replicated KVS (shards × replicas)

use futures::{SinkExt, StreamExt};
use kvs_zoo::before_storage::routing::{RoundRobinRouter, ShardedRouter};
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::after_storage::replication::BroadcastReplication;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::wire_kvs_dataflow;
use kvs_zoo::values::CausalString;

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
    KVSCluster<Replica, RoundRobinRouter, BroadcastReplication<CausalString>, ()>,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded + Replicated KVS Demo");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture: nested layers
    let kvs_spec = ShardedReplicatedKVS::new(
        ShardedRouter::new(3), // route to shard by key hash
        (),                    // no maintenance at shard level
        KVSCluster::new(
            RoundRobinRouter::new(),         // load-balance within shard
            BroadcastReplication::default(), // replicate within each shard
            (),
        ),
    );

    // Build a Hydro graph for the ShardedReplicatedKVS type, return layer handles and client I/O ports
    let (layers, port) =
        wire_kvs_dataflow::<CausalString, _>(&proxy, &client_external, &flow, kvs_spec);

    // Deploy: one cluster per layer
    // - Shard cluster: 3 members
    // - Replica cluster: 3 members
    let nodes = flow
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
        KVSOperation::Put("user:alice".into(), causal("a", "x")),
        KVSOperation::Put("user:bob".into(), causal("b", "y")),
        KVSOperation::Get("user:alice".into()),
        KVSOperation::Get("user:bob".into()),
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

fn shard_info(op: &KVSOperation<CausalString>, shard_count: usize) -> Option<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
            let shard_id = kvs_zoo::before_storage::routing::ShardedRouter::calculate_shard_id(key, shard_count);
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
