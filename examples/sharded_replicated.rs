//! Advanced sharded + replicated KVS demonstrating full composition
//! Shows ShardedKVSServer<ReplicatedKVSServer> with symmetric pipeline composition

use futures::{SinkExt, StreamExt};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, ReplicatedKVSServer, ShardedKVSServer};
use kvs_zoo::values::{CausalString, VCWrapper};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Advanced Sharded + Replicated KVS Demo");
    println!("🎯 Full composition: ShardedKVSServer<ReplicatedKVSServer>");
    println!("⚡ Pipeline: ShardedRouter.then(RoundRobinRouter)");
    println!();

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Full composition: ShardedKVSServer<ReplicatedKVSServer> (educational structure preserved)
    type FullComposition = ShardedKVSServer<
        ReplicatedKVSServer<CausalString, kvs_zoo::replication::BroadcastReplication<CausalString>>,
    >;

    // Symmetric pipeline composition: ShardedRouter.then(RoundRobinRouter)
    let pipeline = kvs_zoo::interception::Pipeline::new(
        kvs_zoo::interception::ShardedRouter::new(3), // 3 shards
        kvs_zoo::interception::RoundRobinRouter::new(), // Round-robin within each shard
    );

    // Use broadcast replication for strong consistency within shards
    let replication = kvs_zoo::replication::BroadcastReplication::default();

    println!("📋 Configuration:");
    println!("   • 3 shards (hash-based partitioning)");
    println!("   • 3 replicas per shard (broadcast replication)");
    println!("   • Total: 9 nodes (3×3)");
    println!("   • Pipeline: ShardedRouter → RoundRobinRouter");
    println!();

    let deployment_cluster =
        FullComposition::create_deployment(&flow, pipeline.clone(), replication.clone());

    let client_port = FullComposition::run(
        &proxy,
        &deployment_cluster,
        &client_external,
        pipeline,
        replication,
        &flow,
    );

    // Deploy to localhost cluster (9 nodes total)
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&deployment_cluster, vec![localhost.clone(); 9])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await?;

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    println!("📤 Sending operations across shards...");

    // Operations designed to hit different shards
    let operations = vec![
        // Shard 0 operations
        KVSOperation::Put(
            "user:alice".to_string(),
            CausalString::new(VCWrapper::new(), "Alice Smith".to_string()),
        ),
        KVSOperation::Put(
            "user:bob".to_string(),
            CausalString::new(VCWrapper::new(), "Bob Jones".to_string()),
        ),
        // Shard 1 operations
        KVSOperation::Put(
            "config:timeout".to_string(),
            CausalString::new(VCWrapper::new(), "30s".to_string()),
        ),
        KVSOperation::Put(
            "config:retries".to_string(),
            CausalString::new(VCWrapper::new(), "3".to_string()),
        ),
        // Shard 2 operations
        KVSOperation::Put(
            "data:metrics".to_string(),
            CausalString::new(VCWrapper::new(), "enabled".to_string()),
        ),
        KVSOperation::Put(
            "data:logs".to_string(),
            CausalString::new(VCWrapper::new(), "debug".to_string()),
        ),
        // Read operations across all shards
        KVSOperation::Get("user:alice".to_string()),
        KVSOperation::Get("config:timeout".to_string()),
        KVSOperation::Get("data:metrics".to_string()),
        KVSOperation::Get("nonexistent".to_string()),
    ];

    for (i, op) in operations.into_iter().enumerate() {
        // Show which shard this operation targets
        let shard_info = match &op {
            KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
                let hash = std::collections::hash_map::DefaultHasher::new();
                use std::hash::{Hash, Hasher};
                let mut hasher = hash;
                key.hash(&mut hasher);
                let shard_id = hasher.finish() % 3;
                format!("→ shard {}", shard_id)
            }
        };

        println!("  {} {:?} {}", i + 1, op, shard_info);

        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error: {}", e);
            break;
        }

        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(800), client_out.next())
                .await
                .ok()
                .flatten()
        {
            println!("     ← {}", response);
        }

        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }

    println!("✅ Demo completed");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())
}
