use futures::{SinkExt, StreamExt};
use hydro_deploy::Deployment;
use hydro_lang::prelude::*;
use kvs_zoo::core::KVSNode;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::KVSRouter;
use kvs_zoo::sharded::KVSShardable;

use serde::{Deserialize, Serialize};

/// Configuration for a KVS demo
/// 
/// This trait provides a unified interface for different KVS architectures:
/// - Local: Single-process KVS
/// - Replicated: Multi-replica with gossip synchronization  
/// - Sharded: Hash-based partitioning across nodes
pub trait KVSDemo {
    /// The value type used in this demo
    type Value: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + std::fmt::Debug + Send + Sync + 'static;
    
    /// The storage implementation
    type Storage: KVSShardable<Self::Value>;
    
    /// The routing strategy
    type Router: KVSRouter<Self::Value>;

    /// Create the router instance
    /// 
    /// The flow parameter allows creating additional clusters if needed,
    /// though most routers only need the main KVS cluster.
    fn create_router<'a>(&self, flow: &hydro_lang::compile::builder::FlowBuilder<'a>) -> Self::Router;
    
    /// Get the number of cluster nodes needed
    fn cluster_size(&self) -> usize;
    
    /// Get demo description and architecture info
    fn description(&self) -> &'static str;
    
    /// Get operations to run in the demo
    fn operations(&self) -> Vec<KVSOperation<Self::Value>>;
    
    /// Get the demo name for logging
    fn name(&self) -> &'static str;
    
    /// Log an operation (can be overridden for custom logging)
    fn log_operation(&self, op: &KVSOperation<Self::Value>) {
        match op {
            KVSOperation::Put(key, _) => println!("Client: PUT {}", key),
            KVSOperation::Get(key) => println!("Client: GET {}", key),
        }
    }
}

/// Unified driver function that can run any KVS demo
/// 
/// This driver provides a clean, consistent interface for running different
/// KVS architectures. It handles:
/// - Deployment setup and cluster creation
/// - Client-server communication
/// - Operation execution and logging
/// - Graceful shutdown
pub async fn run_kvs_demo<D: KVSDemo>(demo: D) -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running {} Demo", demo.name());
    println!("{}", demo.description());
    println!();

    // Set up localhost deployment
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow with proxy process, cluster, and external client interface
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let kvs_cluster = flow.cluster::<KVSNode>();
    let client_external = flow.external::<()>();

    // Create the router (allows creating additional clusters like Paxos proposers/acceptors)
    let router = demo.create_router(&flow);

    // Use bidirectional external connection (the pattern that works!)
    let (bidi_port, operations_stream, _membership, complete_sink) = proxy
        .bidi_external_many_bincode::<_, KVSOperation<D::Value>, String>(&client_external);

    // Route operations using the provided router
    let routed_operations = router.route_operations(
        operations_stream.entries().map(q!(|(_client_id, op)| op)).assume_ordering(nondet!(/** Todo: WHY? */)),
        &kvs_cluster,
    );

    // Use demux_ops to split operations (testing that it works!)
    let (put_tuples, get_keys) = kvs_zoo::core::KVSCore::demux_ops(routed_operations);

    // Execute operations using the storage implementation
    let kvs_state = D::Storage::put(put_tuples.inspect(q!(|(key, value)| {
        println!("💾 Storing: {} = {:?}", key, value);
    })), &kvs_cluster);

    // Handle GET operations
    let ticker = kvs_cluster.tick();
    let get_results = D::Storage::get(
        get_keys.inspect(q!(|key| {
            println!("🔍 Looking up: {}", key);
        })).batch(&ticker, nondet!(/** batch gets */)),
        kvs_state.snapshot(&ticker, nondet!(/** snapshot for gets */)),
    );

    // Send results back through proxy to external
    let proxy_responses = get_results.all_ticks().map(q!(|(key, result)| {
        match result {
            Some(value) => {
                println!("✅ Found: {} = {:?}", key, value);
                format!("GET {} = {:?}", key, value)
            }
            None => {
                println!("❌ Not found: {}", key);
                format!("GET {} = NOT FOUND", key)
            }
        }
    })).send_bincode(&proxy);

    // Complete the bidirectional connection
    complete_sink.complete(
        proxy_responses.entries().map(q!(|(_member_id, response)| (0u64, response))).into_keyed()
    );

    let client_input_port = bidi_port;

    // Deploy to localhost
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); demo.cluster_size()])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    // Start the deployment
    deployment.deploy().await?;

    // Connect bidirectionally to the external client interface
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_input_port).await;

    deployment.start().await?;

    // Small delay to let server start up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Sending demo operations...");

    // Generate and send operations
    let operations = demo.operations();
    let total_ops = operations.len();

    for (i, op) in operations.into_iter().enumerate() {
        // Log the operation using the demo's custom logging
        demo.log_operation(&op);
        
        if let Err(e) = client_in.send(op).await {
            eprintln!("❌ Error sending operation: {}", e);
            break;
        }

        // Try to receive response
        if let Some(response) = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            client_out.next()
        ).await.ok().flatten() {
            println!("📥 Response: {}", response);
        }

        // Small delay to see the operations processed in order
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        
        // Progress indicator for longer demos
        if total_ops > 5 && (i + 1) % 3 == 0 {
            println!("   ... {} of {} operations sent", i + 1, total_ops);
        }
    }

    println!("✅ {} demo completed successfully!", demo.name());
    println!("   📊 Processed {} operations", total_ops);

    // Keep running briefly to see server output
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}