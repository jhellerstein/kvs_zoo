use futures::SinkExt;
use hydro_deploy::Deployment;
use kvs_zoo::sharded_replicated::ShardedReplicatedKVSServer;
use kvs_zoo::examples_support::{CausalString, generate_causal_operations, log_operation};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up localhost deployment
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow: proxy process, three shard clusters (each replicated), and external client
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process();
    let shard0 = flow.cluster();
    let shard1 = flow.cluster();
    let shard2 = flow.cluster();
    let client_external = flow.external();

    // Start a sharded+replicated KVS. We'll provide three shard clusters.
    let (client_input_port, _results_port, _fails_port) =
        ShardedReplicatedKVSServer::<CausalString>::run_sharded_replicated_cluster(
            &proxy,
            &[shard0.clone(), shard1.clone(), shard2.clone()],
            &client_external,
        );

    // Deploy to localhost
    // - Proxy and External on localhost
    // - Each shard cluster has 3 replicas (3 members)
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&shard0, vec![localhost.clone(); 3])
        .with_cluster(&shard1, vec![localhost.clone(); 3])
        .with_cluster(&shard2, vec![localhost.clone(); 3])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    // Start the deployment
    deployment.deploy().await?;

    // Connect to the external client interface before starting
    let mut client_sink = nodes.connect(client_input_port).await;

    deployment.start().await?;

    // Small delay to let servers start up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("Client: Starting sharded-replicated demo operations with causal values...");
    println!("        Using DomPair<VectorClock, SetUnion<String>>");
    println!();

    // Generate and send operations
    let operations = generate_causal_operations();

    for op in operations {
        log_operation(&op);
        if let Err(e) = client_sink.send(op).await {
            eprintln!("Client: Error sending operation: {}", e);
            break;
        }

        // Small delay to observe ordering and allow processing/gossip
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    println!("Client: Sharded-replicated demo operations completed");

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}

// Helpers are imported from kvs_zoo::examples_support
