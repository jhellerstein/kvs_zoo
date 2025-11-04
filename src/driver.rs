//! Demo driver utilities for KVS examples
//!
//! This module provides a unified interface for running different KVS architectures
//! in examples. It handles deployment setup, client-server communication, and
//! operation execution with consistent logging and error handling.

use crate::protocol::KVSOperation;
use crate::routers::KVSRouter;
use crate::sharded::KVSShardable;
use serde::{Deserialize, Serialize};

/// Configuration for a KVS demo
///
/// This trait provides a unified interface for different KVS architectures:
/// - Local: Single-process KVS
/// - Replicated: Multi-replica with gossip synchronization  
/// - Sharded: Hash-based partitioning across nodes
pub trait KVSDemo {
    /// The value type used in this demo
    type Value: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + Send
        + Sync
        + 'static;

    /// The storage implementation
    type Storage: KVSShardable<Self::Value>;

    /// The routing strategy
    type Router: KVSRouter<Self::Value>;

    /// Create the router instance
    ///
    /// The flow parameter allows creating additional clusters if needed,
    /// though most routers only need the main KVS cluster.
    fn create_router<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router;

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

/// Macro to generate the run_kvs_demo function for examples
///
/// This macro generates the boilerplate code needed to run a KVS demo.
/// It must be used in example files that have imported the necessary dependencies:
/// - futures::{SinkExt, StreamExt}
/// - hydro_deploy::Deployment
/// - hydro_lang::prelude::*
/// - tokio
///
/// Usage:
/// ```rust
/// use kvs_zoo::driver::{KVSDemo, run_kvs_demo_impl};
///
/// struct MyDemo;
/// impl KVSDemo for MyDemo { /* ... */ }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     run_kvs_demo_impl!(MyDemo)
/// }
/// ```
#[macro_export]
macro_rules! run_kvs_demo_impl {
    ($demo_type:ty) => {
        async {
            let demo = <$demo_type>::default();

            println!("🚀 Running {} Demo", demo.name());
            println!("{}", demo.description());
            println!();

            // Set up localhost deployment
            let mut deployment = hydro_deploy::Deployment::new();
            let localhost = deployment.Localhost();

            // Create Hydro flow with proxy process, cluster, and external client interface
            let flow = hydro_lang::compile::builder::FlowBuilder::new();
            let proxy = flow.process::<()>();
            let kvs_cluster = flow.cluster::<kvs_zoo::core::KVSNode>();
            let client_external = flow.external::<()>();

            // Create the router
            let router = demo.create_router(&flow);

            // Use bidirectional external connection
            let (bidi_port, operations_stream, _membership, complete_sink) = proxy
                .bidi_external_many_bincode::<_, kvs_zoo::protocol::KVSOperation<_>, String>(&client_external);

            // Route operations using the provided router
            let routed_operations = router.route_operations(
                operations_stream.entries().map(q!(|(_client_id, op)| op)).assume_ordering(nondet!(/** Todo: WHY? */)),
                &kvs_cluster,
            );

            // Use demux_ops to split operations
            let (put_tuples, get_keys) = kvs_zoo::core::KVSCore::demux_ops(routed_operations);

            // Execute operations using the storage implementation
            let kvs_state = <
                <$demo_type as kvs_zoo::driver::KVSDemo>::Storage as kvs_zoo::sharded::KVSShardable<
                    <$demo_type as kvs_zoo::driver::KVSDemo>::Value
                >
            >::put(
                put_tuples.inspect(q!(|(key, value)| {
                    println!("💾 Storing: {} = {:?}", key, value);
                })),
                &kvs_cluster,
            );

            // Handle GET operations
            let ticker = kvs_cluster.tick();
            let get_results = <
                <$demo_type as kvs_zoo::driver::KVSDemo>::Storage as kvs_zoo::sharded::KVSShardable<
                    <$demo_type as kvs_zoo::driver::KVSDemo>::Value
                >
            >::get(
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

            Ok::<(), Box<dyn std::error::Error>>(())
        }.await
    };
}
