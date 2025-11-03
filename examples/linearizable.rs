//! # Linearizable KVS Architecture Example
//! 
//! This example demonstrates a strongly consistent KVS architecture:
//! - **Paxos consensus**: Uses Multi-Paxos for strong consistency
//! - **Linearizability**: Strongest consistency guarantee - operations appear atomic
//! - **Fault tolerance**: Can tolerate f failures with 2f+1 nodes
//! - **Performance trade-off**: Consistency comes at the cost of latency
//! 
//! **Architecture**: Proposers + Acceptors + Replicas with Paxos protocol
//! **Use case**: When you need strong consistency guarantees (banking, critical systems)

use futures::SinkExt;
use hydro_deploy::Deployment;
use kvs_zoo::client::KVSClient;
use kvs_zoo::linearizable::StringLinearizableKVSServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running Linearizable KVS Demo");
    println!("📋 Architecture: Proposers + Acceptors + Replicas with Paxos protocol");
    println!("🔒 Consistency: Linearizable (strongest guarantee)");
    println!("🎯 Use case: Critical systems requiring strong consistency");
    println!();

    // Set up localhost deployment
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow with proxy, proposers, acceptors, replicas, and external client
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process();
    let proposers = flow.cluster();
    let acceptors = flow.cluster();
    let replicas = flow.cluster();
    let client_external = flow.external();

    // Build the linearizable KVS with Paxos consensus
    // f=1 means we can tolerate 1 failure (need 2f+1=3 nodes)
    let (client_input_port, _get_results_port) = StringLinearizableKVSServer::run_linearizable_kvs(
        &proxy,
        &proposers,
        &acceptors,
        &replicas,
        &client_external,
        1, // f = 1, so we need 3 acceptors/proposers
        1, // i (unique cluster ID)
    );

    // Deploy to localhost with 3-node clusters
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&proposers, vec![localhost.clone(); 3])
        .with_cluster(&acceptors, vec![localhost.clone(); 3])
        .with_cluster(&replicas, vec![localhost.clone(); 3])
        .with_external(&client_external, localhost.clone())
        .deploy(&mut deployment);

    deployment.deploy().await?;

    // Connect to the external client interface before starting
    let mut client_sink = nodes.connect(client_input_port).await;

    deployment.start().await?;

    // Small delay to let server start up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Sending demo operations through Paxos consensus...");

    // Generate and send operations
    let operations = KVSClient::generate_demo_operations();

    for op in operations {
        KVSClient::log_operation(&op);
        if let Err(e) = client_sink.send(op).await {
            eprintln!("❌ Error sending operation: {}", e);
            break;
        }

        // Longer delay for Paxos consensus
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    println!("✅ Linearizable KVS demo operations completed");
    println!("🔒 All operations were totally ordered through Paxos consensus");

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}