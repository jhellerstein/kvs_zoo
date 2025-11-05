//! # Replicated KVS Architecture Example
//!
//! This example demonstrates a replicated KVS architecture:
//! - **Full replication**: All data is replicated across all nodes
//! - **Broadcast operations**: All operations are sent to all replicas
//! - **Strong consistency**: All replicas process operations in the same order
//! - **Fault tolerance**: Can tolerate node failures (as long as one replica survives)
//!
//! **Architecture**: Broadcast router with full replication
//! **Trade-offs**: Strong consistency and fault tolerance, but limited scalability
//! **Use case**: Critical data that needs high availability and consistency

use futures::{SinkExt, StreamExt};

use kvs_zoo::driver::KVSDemo;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::replicated::KVSReplicated;
use kvs_zoo::routers::{BroadcastReplication, RoundRobinRouter};
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::{CausalString, generate_causal_operations};

/// Type alias for replicated KVS with broadcast replication
type ReplicatedKVS = KVSReplicated<BroadcastReplication<CausalString>>;

struct ReplicatedDemo;

impl Default for ReplicatedDemo {
    fn default() -> Self {
        ReplicatedDemo
    }
}

impl KVSDemo for ReplicatedDemo {
    type Value = CausalString;
    type Storage = ReplicatedKVS;
    type Router = RoundRobinRouter;

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        RoundRobinRouter // Distributes operations across replicas
    }

    fn cluster_size(&self) -> usize {
        3 // 3 replicas
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Broadcast router with full replication\n\
         🔄 Consistency: Causal consistency with vector clocks\n\
         🛡️ Fault tolerance: Can survive node failures\n\
         🎯 Use case: Critical data requiring high availability"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        generate_causal_operations()
    }

    fn name(&self) -> &'static str {
        "Replicated KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let demo = ReplicatedDemo;

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

    // Create replicated KVS deployment using server architecture
    let kvs_cluster =
        kvs_zoo::server::ReplicatedKVSServer::<CausalString>::create_deployment(&flow);

    // Execute the server
    let client_port = kvs_zoo::server::ReplicatedKVSServer::<CausalString>::run(
        &proxy,
        &kvs_cluster,
        &client_external,
    );

    // Deploy with replicas
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
