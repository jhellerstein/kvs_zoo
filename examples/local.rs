//! # Local KVS Architecture Example
//!
//! This example demonstrates the simplest KVS architecture:
//! - **Single process**: No distribution, no networking between KVS nodes
//! - **Last-Writer-Wins**: Deterministic conflict resolution
//! - **No replication**: Data exists on one node only
//! - **No sharding**: All data on the same node
//!
//! **Architecture**: Process-to-cluster communication (even for single node)
//! **Use case**: Development, testing, or scenarios where simplicity > availability

use futures::{SinkExt, StreamExt};
use kvs_zoo::driver::KVSDemo;
use kvs_zoo::lww::KVSLww;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::LocalRouter;
use kvs_zoo::server::KVSServer;

#[derive(Default)]
struct LocalDemo;

impl KVSDemo for LocalDemo {
    type Value = String;
    type Storage = KVSLww;
    type Router = LocalRouter;

    fn cluster_size(&self) -> usize {
        1 // Single node
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Single process, no networking\n\
         🔒 Consistency: Strong (deterministic)\n\
         🎯 Use case: Development, testing, simple applications"
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
        "Local KVS"
    }

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        LocalRouter
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let demo = LocalDemo;

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

    // Create local KVS deployment using server architecture
    let kvs_cluster = kvs_zoo::server::LocalKVSServer::<String>::create_deployment(&flow);

    // Execute the server
    let client_port =
        kvs_zoo::server::LocalKVSServer::<String>::run(&proxy, &kvs_cluster, &client_external);

    // Deploy
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); demo.cluster_size()])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

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
        print!("  {} ", i + 1);
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
            println!("     → {}", response);
        }

        // Small delay to see the operations processed in order
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    println!("✅ {} demo completed successfully!", demo.name());
    println!("   📊 Processed {} operations", total_ops);

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}
