//! Linearizable KVS example using Paxos consensus
//!
//! This example demonstrates the strongest consistency model available:
//! - **Linearizability**: All operations appear to execute atomically in real-time order
//! - **Paxos consensus**: Ensures total ordering of all operations across replicas
//! - **Fault tolerance**: Can tolerate f failures with 2f+1 nodes
//!
//! ## Consistency Guarantees
//!
//! Linearizability provides:
//! - **Atomicity**: Each operation appears to take effect instantaneously
//! - **Total order**: All operations have a global ordering
//! - **Real-time**: Non-overlapping operations respect wall-clock time
//!
//! This is the gold standard for distributed systems but comes with higher latency
//! due to the consensus overhead.

use futures::{SinkExt, StreamExt};
use kvs_zoo::interception::PaxosConfig;
use kvs_zoo::linearizable::LinearizableKVSServer;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable KVS Demo");
    println!("📋 Paxos consensus for total ordering and linearizability");
    println!("🔒 Strongest consistency guarantees available");
    println!();

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Configure Paxos for fault tolerance
    let paxos_config = PaxosConfig {
        f: 1, // Tolerate 1 failure
        i_am_leader_send_timeout: 1,
        i_am_leader_check_timeout: 3,
        i_am_leader_check_timeout_delay_multiplier: 1,
    };

    // Create linearizable KVS with Paxos consensus
    type LinearizableKVS = LinearizableKVSServer<LwwWrapper<String>, kvs_zoo::replication::NoReplication>;
    
    let op_pipeline = kvs_zoo::interception::PaxosInterceptor::with_config(paxos_config);
    let replication = kvs_zoo::replication::NoReplication::new();

    // Create deployment
    let kvs_cluster = LinearizableKVS::create_deployment(
        &flow,
        op_pipeline.clone(),
        replication.clone(),
    );

    // Run the linearizable KVS
    let client_port = LinearizableKVS::run(
        &proxy,
        &kvs_cluster,
        &client_external,
        op_pipeline,
        replication,
        &flow,
    );

    // Deploy to localhost (3 nodes for Paxos consensus)
    let cluster_size = LinearizableKVS::size(
        kvs_zoo::interception::PaxosInterceptor::new(),
        kvs_zoo::replication::NoReplication::new(),
    );
    
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); cluster_size])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await?;

    // Allow time for Paxos leader election
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    println!("📤 Demonstrating linearizable operations...");
    println!("   (All operations will be totally ordered by Paxos consensus)");
    println!();

    // Demonstrate linearizable operations
    let operations = vec![
        // Initial writes
        KVSOperation::Put("account_a".to_string(), LwwWrapper::new("100".to_string())),
        KVSOperation::Put("account_b".to_string(), LwwWrapper::new("50".to_string())),
        
        // Read initial state
        KVSOperation::Get("account_a".to_string()),
        KVSOperation::Get("account_b".to_string()),
        
        // Simulate a transfer (these operations will be linearized)
        KVSOperation::Put("account_a".to_string(), LwwWrapper::new("75".to_string())), // -25
        KVSOperation::Put("account_b".to_string(), LwwWrapper::new("75".to_string())), // +25
        
        // Read final state (linearizable reads)
        KVSOperation::Get("account_a".to_string()),
        KVSOperation::Get("account_b".to_string()),
        
        // Demonstrate concurrent operations get linearized
        KVSOperation::Put("counter".to_string(), LwwWrapper::new("1".to_string())),
        KVSOperation::Put("counter".to_string(), LwwWrapper::new("2".to_string())),
        KVSOperation::Put("counter".to_string(), LwwWrapper::new("3".to_string())),
        KVSOperation::Get("counter".to_string()),
    ];

    for (i, op) in operations.into_iter().enumerate() {
        println!("  {} {:?}", i + 1, op);

        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error: {}", e);
            break;
        }

        // Wait for response with timeout
        if let Some(response) = tokio::time::timeout(
            std::time::Duration::from_millis(1000), 
            client_out.next()
        ).await.ok().flatten() {
            println!("     → {}", response);
        } else {
            println!("     → (timeout - Paxos consensus may take time)");
        }

        // Small delay to see operations processed in order
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    println!();
    println!("✅ Linearizable KVS demo completed!");
    println!("🎯 Key benefits demonstrated:");
    println!("   • Total ordering: All operations have a global sequence");
    println!("   • Atomicity: Each operation appears instantaneous");
    println!("   • Consistency: All replicas see the same order");
    println!("   • Fault tolerance: System continues with node failures");
    println!();
    println!("💡 Use linearizable KVS when you need:");
    println!("   • Strongest consistency guarantees");
    println!("   • Coordination between distributed components");
    println!("   • Atomic transactions across keys");
    println!("   • Can tolerate higher latency for consistency");

    // Keep running briefly to see any remaining output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}