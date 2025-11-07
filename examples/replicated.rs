//! Replicated KVS example showing different replication strategies
//! Demonstrates gossip vs broadcast replication with incremental complexity

use futures::{SinkExt, StreamExt};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, ReplicatedKVSServer};
use kvs_zoo::values::{CausalString, VCWrapper};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS Demo");
    println!("🔄 RoundRobin routing + configurable replication");
    println!();

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    // Currently, External clients cannot talk to a Cluster, so we set up a proxy Process.
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    println!("📋 Example: Gossip Replication");

    // The intercept's job for KVSReplicated is just to load-balance requests across the cluster.
    // We use RoundRobinRouter to achieve that.
    let op_pipeline = kvs_zoo::interception::RoundRobinRouter::new();
    // The ReplicatedKVSServer needs to be set up with a replication protocol.
    // As an example, we'll use async background Gossip replication (suitable for large clusters)
    // We could have chosen Broadcast replication instead, which can run synchronously
    // or in the background.
    // Use small_cluster config for faster gossip (500ms interval instead of 1s)
    let gossip_replication = kvs_zoo::replication::EpidemicGossip::with_config(
        kvs_zoo::replication::EpidemicGossipConfig::small_cluster(),
    );

    let kvs_cluster = ReplicatedKVSServer::<CausalString, _>::create_deployment(
        &flow,
        op_pipeline.clone(),
        gossip_replication.clone(),
    );

    let client_port = ReplicatedKVSServer::<CausalString, _>::run(
        &proxy,
        &kvs_cluster,
        &client_external,
        op_pipeline,
        gossip_replication,
        &flow,
    );

    // Deploy to localhost cluster
    let cluster_size = 3;
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); cluster_size])
        .with_external(&client_external, localhost.clone())
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Sending operations with gossip replication...");

    // Demo operations
    let put_operations = vec![
        KVSOperation::Put(
            "user:1".to_string(),
            CausalString::new(VCWrapper::new(), "Alice".to_string()),
        ),
        KVSOperation::Put(
            "user:2".to_string(),
            CausalString::new(VCWrapper::new(), "Bob".to_string()),
        ),
        KVSOperation::Put(
            "user:1".to_string(),
            CausalString::new(VCWrapper::new(), "Alice Updated".to_string()),
        ),
    ];
    let get_operations = vec![
        KVSOperation::Get("user:1".to_string()),
        KVSOperation::Get("user:1".to_string()),
        KVSOperation::Get("user:2".to_string()),
    ];

    for (i, op) in put_operations.into_iter().enumerate() {
        println!("  {} {:?}", i + 1, op);

        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error: {}", e);
            break;
        }

        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(1200), client_out.next())
                .await
                .ok()
                .flatten()
        {
            println!("     → {}", response);
        }
    }

    // Wait for gossip replication (small_cluster config: 500ms interval, wait 6x)
    tokio::time::sleep(std::time::Duration::from_millis(3000)).await;

    for (i, op) in get_operations.into_iter().enumerate() {
        println!("  {} {:?}", i + 1, op);

        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error: {}", e);
            break;
        }

        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(1200), client_out.next())
                .await
                .ok()
                .flatten()
        {
            println!("     → {}", response);
        }
    }

    println!("✅ Gossip replication demo completed");
    println!();
    println!("🎓 Replication Strategy Notes:");
    println!("   • This example uses EpidemicGossip for eventual consistency");
    println!("   • Alternative: synchronous BroadcastReplication for lower-latency replication");
    println!("   • Both strategies work with the same RoundRobinRouter pipeline");
    println!("   • The composable architecture separates routing from replication concerns");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())
}
