use futures::SinkExt;
use hydro_deploy::Deployment;
use kvs_zoo::replicated::ReplicatedKVSServer;
use kvs_zoo::examples_support::{CausalString, generate_causal_operations, log_operation};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up localhost deployment
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow with proxy process, cluster, and external client interface
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process();
    let kvs_cluster = flow.cluster();
    let client_external = flow.external();

    // Set up the replicated KVS with gossip protocol
    // Using DomPair<VectorClock, SetUnion<String>> for causally-consistent string values
    let (client_input_port, _, _) =
        ReplicatedKVSServer::<CausalString>::run_replicated_kvs(
            &proxy,
            &kvs_cluster,
            &client_external,
        );

    // Deploy to localhost
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); 3]) // 3-node cluster
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    // Start the deployment
    deployment.deploy().await?;

    // Connect to the external client interface before starting
    let mut client_sink = nodes.connect(client_input_port).await;

    deployment.start().await?;

    // Small delay to let server start up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        println!("Client: Starting demo operations with causal consistency...");
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

            // Small delay to see the operations processed in order and allow gossip
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    println!("Client: Demo operations completed");

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}
