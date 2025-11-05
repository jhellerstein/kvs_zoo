//! # Sharded + Replicated KVS Architecture Example
//!
//! This example demonstrates a sharded and replicated KVS architecture:
//! - **Shard-aware routing**: Operations routed to primary node of correct shard
//! - **Shard-aware replication**: Replication only within shard boundaries  
//! - **Scalability**: Can handle more data by adding more shards
//! - **High availability**: Can tolerate replica failures within shards
//!
//! **Architecture**: Hash-based key routing with shard-aware broadcast replication
//! **Trade-offs**: True scalability with controlled replication overhead
//! **Use case**: Large-scale applications requiring both scalability and fault tolerance

use futures::{SinkExt, StreamExt};

use kvs_zoo::driver::KVSDemo;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::replicated::KVSReplicated;
use kvs_zoo::routers::{ShardAwareBroadcastReplication, ShardedRoundRobin};
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::LwwWrapper;

struct ShardedReplicatedDemo;

impl Default for ShardedReplicatedDemo {
    fn default() -> Self {
        ShardedReplicatedDemo
    }
}

impl KVSDemo for ShardedReplicatedDemo {
    type Value = LwwWrapper<String>;
    type Storage = KVSReplicated<ShardAwareBroadcastReplication>;
    type Router = ShardedRoundRobin;

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        ShardedRoundRobin::new(3, 3) // 3 shards, 3 replicas each
    }

    fn cluster_size(&self) -> usize {
        9 // 3 shards × 3 replicas per shard
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Hash-based sharding with shard-aware replication\n\
         🔀 Consistency: Per-shard causal, cross-shard eventual\n\
         🛡️ Fault tolerance: Can survive replica failures within shards\n\
         🎯 Use case: Large-scale applications with controlled replication"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        vec![
            KVSOperation::Put(
                "shard_key_0".to_string(),
                LwwWrapper::new("value_0".to_string()),
            ),
            KVSOperation::Put(
                "shard_key_1".to_string(),
                LwwWrapper::new("value_1".to_string()),
            ),
            KVSOperation::Put(
                "shard_key_2".to_string(),
                LwwWrapper::new("value_2".to_string()),
            ),
            KVSOperation::Get("shard_key_0".to_string()),
            KVSOperation::Get("shard_key_1".to_string()),
            KVSOperation::Get("nonexistent".to_string()),
        ]
    }

    fn name(&self) -> &'static str {
        "Sharded + Replicated KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let demo = ShardedReplicatedDemo;

    println!("🚀 Running {} Demo", demo.name());
    println!("{}", demo.description());
    println!();
    println!("🎯 COMPOSABLE SERVER ARCHITECTURE:");
    println!("   ShardedKVSServer<ReplicatedKVSServer<CausalString>>");
    println!("   = 3 shards × 3 replicas = 9 total nodes");
    println!("   = True nested server composability!");
    println!();

    // Set up localhost deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Create sharded + replicated KVS deployment using server architecture
    // ShardedKVSServer<ReplicatedKVSServer> = 3 shards, each being a 3-node replicated cluster
    let shard_deployments = kvs_zoo::server::ShardedKVSServer::<
        kvs_zoo::server::ReplicatedKVSServer<kvs_zoo::values::CausalString>,
    >::create_deployment(&flow);

    // Execute the server
    let client_port = kvs_zoo::server::ShardedKVSServer::<
        kvs_zoo::server::ReplicatedKVSServer<kvs_zoo::values::CausalString>,
    >::run(&proxy, &shard_deployments, &client_external);

    // Deploy with multiple shards (each shard is a 3-node replicated cluster)
    let mut flow_builder = flow
        .with_process(&proxy, localhost.clone())
        .with_external(&client_external, localhost.clone());

    // Add each shard deployment (each shard gets 3 replicas)
    for (i, shard_deployment) in shard_deployments.iter().enumerate() {
        println!("🔧 Deploying shard {} with 3 replicas", i);
        flow_builder = flow_builder.with_cluster(shard_deployment, vec![localhost.clone(); 3]);
    }

    let nodes = flow_builder.deploy(&mut deployment);

    // Start the deployment
    deployment.deploy().await?;

    // Connect to the client interface
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;

    deployment.start().await?;

    // Small delay to let server start up
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    println!("📤 Sending demo operations...");

    // Generate operations that will demonstrate sharding
    use std::collections::HashSet;

    // Create vector clocks for different operations
    let mut vc1 = kvs_zoo::values::VCWrapper::new();
    vc1.bump("demo_node_1".to_string());

    let mut vc2 = kvs_zoo::values::VCWrapper::new();
    vc2.bump("demo_node_2".to_string());

    let mut vc3 = kvs_zoo::values::VCWrapper::new();
    vc3.bump("demo_node_3".to_string());

    let operations = vec![
        KVSOperation::Put(
            "shard_key_0".to_string(),
            kvs_zoo::values::CausalString::new_with_set(
                vc1,
                HashSet::from(["value_0".to_string()]),
            ),
        ),
        KVSOperation::Put(
            "shard_key_1".to_string(),
            kvs_zoo::values::CausalString::new_with_set(
                vc2,
                HashSet::from(["value_1".to_string()]),
            ),
        ),
        KVSOperation::Put(
            "shard_key_2".to_string(),
            kvs_zoo::values::CausalString::new_with_set(
                vc3,
                HashSet::from(["value_2".to_string()]),
            ),
        ),
        KVSOperation::Get("shard_key_0".to_string()),
        KVSOperation::Get("shard_key_1".to_string()),
        KVSOperation::Get("shard_key_2".to_string()),
        KVSOperation::Get("nonexistent".to_string()),
    ];

    let total_ops = operations.len();

    for (i, op) in operations.into_iter().enumerate() {
        // Log the operation with shard information
        print!("  {} ", i + 1);
        match &op {
            KVSOperation::Put(k, v) => {
                let shard_id = kvs_zoo::sharded::calculate_shard_id(k, 3);
                println!(
                    "PUT {} => {:?} (→ shard {})",
                    k,
                    v.values().iter().collect::<Vec<_>>(),
                    shard_id
                );
            }
            KVSOperation::Get(k) => {
                let shard_id = kvs_zoo::sharded::calculate_shard_id(k, 3);
                println!("GET {} (→ shard {})", k, shard_id);
            }
        }

        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error sending operation: {}", e);
            break;
        }

        // Try to receive response
        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(1000), client_out.next())
                .await
                .ok()
                .flatten()
        {
            println!("     → {}", response);
        }

        // Small delay to see the operations processed in order
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    println!("✅ {} demo completed successfully!", demo.name());
    println!("   📊 Processed {} operations", total_ops);
    println!("   🎯 Demonstrated: ShardedKVSServer<ReplicatedKVSServer>");
    println!("   ✨ True composable server architecture achieved!");

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    Ok(())
}
