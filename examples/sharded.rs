//! # Sharded KVS Architecture Example
//!
//! This example demonstrates a sharded (partitioned) KVS architecture:
//! - **Horizontal partitioning**: Data is split across multiple shards by key hash
//! - **Scalability**: Can handle more data by adding more shards
//! - **Load distribution**: Requests are distributed across shards
//! - **No replication**: Each key exists on exactly one shard (single point of failure)
//!
//! **Architecture**: Hash-based key routing to independent shard nodes
//! **Trade-offs**: High scalability, but availability depends on all shards being up
//! **Use case**: Large datasets that need to be partitioned (analytics, big data)

use futures::{SinkExt, StreamExt};

use kvs_zoo::driver::KVSDemo;
use kvs_zoo::lww::KVSLww;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::ShardedRouter;
use kvs_zoo::server::KVSServer;

struct ShardedDemo;

impl Default for ShardedDemo {
    fn default() -> Self {
        ShardedDemo
    }
}

impl KVSDemo for ShardedDemo {
    type Value = String;
    type Storage = KVSLww;
    type Router = ShardedRouter;

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        ShardedRouter::new(3) // 3 shards
    }

    fn cluster_size(&self) -> usize {
        3 // 3 shards
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Hash-based key routing to independent shard nodes\n\
         🔀 Consistency: Per-shard strong, global eventual\n\
         🎯 Use case: Scalability for large datasets"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        vec![
            KVSOperation::Put("key1".to_string(), "value1".to_string()),
            KVSOperation::Put("key2".to_string(), "value2".to_string()),
            KVSOperation::Get("key1".to_string()),
            KVSOperation::Get("nonexistent".to_string()),
            KVSOperation::Put("key1".to_string(), "updated_value1".to_string()),
            KVSOperation::Get("key1".to_string()),
        ]
    }

    fn name(&self) -> &'static str {
        "Sharded KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let demo = ShardedDemo;

    println!("🚀 Running {} Demo", demo.name());
    println!("{}", demo.description());
    println!();

    // Set up localhost deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Create sharded KVS deployment using server architecture
    // ShardedKVSServer<LocalKVSServer> = 3 shards, each being a single local node
    let shard_deployments = kvs_zoo::server::ShardedKVSServer::<
        kvs_zoo::server::LocalKVSServer<String>,
    >::create_deployment(&flow);

    // Execute the server
    let client_port =
        kvs_zoo::server::ShardedKVSServer::<kvs_zoo::server::LocalKVSServer<String>>::run(
            &proxy,
            &shard_deployments,
            &client_external,
        );

    // Deploy with multiple shards (each shard is a single node for LocalKVSService)
    let mut flow_builder = flow
        .with_process(&proxy, localhost.clone())
        .with_external(&client_external, localhost.clone());

    // Add each shard deployment
    for shard_deployment in &shard_deployments {
        flow_builder = flow_builder.with_cluster(shard_deployment, vec![localhost.clone(); 1]);
    }

    let nodes = flow_builder.deploy(&mut deployment);

    // Start the deployment
    deployment.deploy().await?;

    // Connect to the client interface
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;

    deployment.start().await?;

    // Small delay to let server start up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Sending demo operations...");

    // Generate and send operations
    let operations = demo.operations();
    let total_ops = operations.len();

    for (i, op) in operations.into_iter().enumerate() {
        // Log the operation using the demo's custom logging
        demo.log_operation(&op);

        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error sending operation: {}", e);
            break;
        }

        // Try to receive response
        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(500), client_out.next())
                .await
                .ok()
                .flatten()
        {
            println!("📥 Response: {}", response);
        }

        // Small delay to see the operations processed in order
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Progress indicator for longer demos
        if total_ops > 5 && (i + 1) % 3 == 0 {
            println!("   ... {} of {} operations sent", i + 1, total_ops);
        }
    }

    println!("✅ {} demo completed successfully!", demo.name());
    println!("   📊 Processed {} operations", total_ops);

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}
