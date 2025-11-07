//! Local KVS example
//! Single-node KVS with LocalRouter and no replication

use futures::{SinkExt, StreamExt};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, LocalKVSServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Local KVS Demo");
    println!("📋 Single process, no networking, strong consistency");
    println!();

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    // Currently, External clients cannot talk to a Cluster, so we set up a proxy Process.
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // A more sophisticated KVS might have a pipeline of traffic "interceptors"
    // to do things like order or route the inbound operations.
    // It might also have a replication strategy like Broadcast or Epidemic Gossip.
    // But here we'll just create no-ops for each!
    let op_pipeline = kvs_zoo::dispatch::SingleNodeRouter::new();
    let replication = (); // No replication for single node

    // Create a deployment using the server API
    let kvs_cluster =
        LocalKVSServer::<String>::create_deployment(&flow, op_pipeline.clone(), replication);

    // Set up the server
    let client_port = LocalKVSServer::<String>::run(
        &proxy,
        &kvs_cluster,
        &client_external,
        op_pipeline,
        replication,
    );

    // Deploy to localhost
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); 1])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await?;

    // give it a chance to launch
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Sending operations...");

    // Demo operations
    let operations = vec![
        KVSOperation::Put("key1".to_string(), "value1".to_string()),
        KVSOperation::Put("key2".to_string(), "value2".to_string()),
        KVSOperation::Get("key1".to_string()),
        KVSOperation::Get("nonexistent".to_string()),
        KVSOperation::Put("key1".to_string(), "updated_value1".to_string()),
        KVSOperation::Get("key1".to_string()),
    ];

    for (i, op) in operations.into_iter().enumerate() {
        println!("  {} {:?}", i + 1, op);

        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error: {}", e);
            break;
        }

        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(500), client_out.next())
                .await
                .ok()
                .flatten()
        {
            println!("     → {}", response);
        }

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    println!("✅ Demo completed");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())
}
