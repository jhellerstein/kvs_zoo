use futures::SinkExt;
use hydro_deploy::Deployment;
use kvs_zoo::client::KVSClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up localhost deployment
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow with server process and external client interface
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let server_process = flow.process();
    let client_external = flow.external();

    // Set up the KVS server to receive operations from external clients
    let (client_input_port, _, _) =
        kvs_zoo::local::StringKVSServer::run_local_kvs(
            &server_process,
            &client_external,
        );

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

    println!("Client: Starting demo operations...");

    // Generate and send operations
    let operations = KVSClient::generate_demo_operations();

    for op in operations {
        KVSClient::log_operation(&op);
        if let Err(e) = client_sink.send(op).await {
            eprintln!("Client: Error sending operation: {}", e);
            break;
        }

        // Small delay to see the operations processed in order
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    println!("Client: Demo operations completed");

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}
