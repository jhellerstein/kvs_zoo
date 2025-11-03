//! # Local KVS Architecture Example
//! 
//! This example demonstrates the simplest KVS architecture:
//! - **Single process**: No distribution, no networking between KVS nodes
//! - **Last-Writer-Wins**: Deterministic conflict resolution
//! - **No replication**: Data exists on one node only
//! - **No sharding**: All data on the same node
//! 
//! **Architecture**: Direct use of KVS core operations on a single process
//! **Use case**: Development, testing, or scenarios where simplicity > availability

use kvs_zoo::client::KVSClient;
use kvs_zoo::protocol::KVSOperation;
use futures::SinkExt;
use hydro_deploy::Deployment;
use hydro_lang::prelude::*;

/// Local KVS server implementation using core KVS operations directly
/// This shows how to build a simple KVS without the unified driver
struct LocalKVSServer;

impl LocalKVSServer {
    /// A single-node KVS server using LWW semantics
    /// Because it's single-node it's deterministic, so we can use last-writer-wins safely
    fn run_local_kvs<'a>(
        server_process: &Process<'a, ()>,
        client_external: &External<'a, ()>,
    ) -> (
        hydro_lang::location::external_process::ExternalBincodeSink<KVSOperation<String>>,
        hydro_lang::location::external_process::ExternalBincodeStream<(String, Option<String>), hydro_lang::live_collections::stream::NoOrder>,
    ) {
        // Network sources from clients
        let (input_port, operations) = server_process.source_external_bincode(client_external);

        // Demux operations into puts and gets
        let (put_tuples, get_keys) = kvs_zoo::core::KVSCore::demux_ops(operations);

        // puts are streaming and eventually consistent
        let ht = kvs_zoo::lww::KVSLww::put(put_tuples);

        // we will need ticks to freeze time for the non-deterministic gets
        let gets_ticker = &server_process.tick();
        let get_results = kvs_zoo::lww::KVSLww::get(
            get_keys.batch(gets_ticker, nondet!(/** gets are non-deterministic */)),
            ht.snapshot(gets_ticker, nondet!(/** gets are non-deterministic! */)),
        );

        // Send results to client
        let get_results_port = get_results
            .all_ticks()
            .send_bincode_external(client_external);

        (input_port, get_results_port)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running Local KVS Demo");
    println!("📋 Architecture: Single process, no networking");
    println!("🔒 Consistency: Strong (deterministic)");
    println!("🎯 Use case: Development, testing, simple applications");
    println!();

    // Set up localhost deployment
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow with server process and external client interface
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let server_process = flow.process();
    let client_external = flow.external();

    // Set up the local KVS server using core operations directly
    let (client_input_port, _) =
        LocalKVSServer::run_local_kvs(&server_process, &client_external);

    // Deploy to localhost
    let nodes = flow
        .with_process(&server_process, localhost.clone())
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    // Start the deployment
    deployment.deploy().await?;

    // Connect to the external client interface before starting
    let mut client_sink = nodes.connect(client_input_port).await;

    deployment.start().await?;

    // Small delay to let server start up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Sending demo operations...");

    // Generate and send operations
    let operations = KVSClient::generate_demo_operations();

    for op in operations {
        KVSClient::log_operation(&op);
        if let Err(e) = client_sink.send(op).await {
            eprintln!("❌ Error sending operation: {}", e);
            break;
        }

        // Small delay to see the operations processed in order
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    println!("✅ Local KVS demo operations completed");
    println!("   📝 This example shows how to use KVS core operations directly");
    println!("   📝 For more complex architectures, use the unified driver API");

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}