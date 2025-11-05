//! Demo driver utilities for KVS examples
//!
//! This module provides a unified interface for running different KVS architectures
//! in examples. It handles deployment setup, client-server communication, and
//! operation execution with consistent logging and error handling.

use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use crate::routers::KVSRouter;
use crate::sharded::KVSShardable;
use hydro_lang::prelude::*;
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

/// Configuration for a multi-cluster KVS demo
///
/// This trait supports architectures that use multiple separate clusters,
/// such as sharded systems where each shard is its own cluster with
/// internal replication.
pub trait MultiClusterKVSDemo {
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

    /// The storage implementation (used within each cluster)
    type Storage: KVSShardable<Self::Value>;

    /// The multi-cluster router
    type Router: MultiClusterRouter<Self::Value>;

    /// Create the router instance
    fn create_router<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router;

    /// Get the number of shard clusters needed
    fn shard_count(&self) -> usize;

    /// Get the number of replicas per shard cluster
    fn replica_count(&self) -> usize;

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

/// Trait for routers that work with multiple clusters
pub trait MultiClusterRouter<V> {
    /// Route operations to multiple shard clusters
    fn route_to_shards<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        shard_clusters: &[Cluster<'a, KVSNode>],
    ) -> Vec<Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static + std::fmt::Debug;
}

/// Macro to generate the run_kvs_demo function using composable architecture
///
/// This macro generates the boilerplate code needed to run a KVS demo using
/// the composable service architecture instead of server.rs.
///
/// Usage:
/// ```rust,ignore
/// use kvs_zoo::{run_composable_demo_impl, driver::KVSDemo};
/// use kvs_zoo::protocol::KVSOperation;
/// use kvs_zoo::routers::LocalRouter;
/// use hydro_lang::prelude::*;
/// use futures::{SinkExt, StreamExt};
///
/// #[derive(Default)]
/// struct MyDemo;
///
/// impl KVSDemo for MyDemo {
///     type Value = String;
///     type Storage = kvs_zoo::lww::KVSLww;
///     type Router = LocalRouter;
///     // ... other trait methods
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     run_composable_demo_impl!(MyDemo)
/// }
/// ```
#[macro_export]
macro_rules! run_composable_demo_impl {
    ($demo_type:ty) => {
        async {
            let demo = <$demo_type>::default();

            println!("🚀 Running {} Demo", demo.name());
            println!("{}", demo.description());
            println!();

            // Set up localhost deployment
            let mut deployment = hydro_deploy::Deployment::new();
            let localhost = deployment.Localhost();

            // Create Hydro flow
            let flow = hydro_lang::compile::builder::FlowBuilder::new();
            let client_external = flow.external::<()>();

            // Determine which composable service to use based on the demo configuration
            let service = kvs_zoo::composable::create_service_for_demo::<$demo_type>(&demo, &flow);

            // Create client interface
            let (client_port, operations_stream, responses_sink) = service.create_client_interface(&client_external);

            // Execute operations using the composable service
            service.execute(operations_stream, responses_sink);

            // Deploy the service
            let nodes = service.deploy(&flow, &mut deployment, localhost).await?;

            // Start the deployment
            deployment.deploy().await?;

            // Connect to the client interface
            let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;

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

/// Legacy macro for backward compatibility - now uses composable architecture
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

            // Create Hydro flow
            let flow = hydro_lang::compile::builder::FlowBuilder::new();
            let client_external = flow.external::<()>();

            // Create the appropriate composable service based on demo type
            let service = kvs_zoo::composable::create_service_from_demo(&demo, &flow);

            // Execute the service
            let client_port = service.run(&client_external);

            // Deploy
            let nodes = service.deploy(&flow, &mut deployment, localhost).await?;

            // Start the deployment
            deployment.deploy().await?;

            // Connect to the client interface
            let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;

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

/// Macro to generate the run_multi_cluster_kvs_demo function for multi-cluster examples
#[macro_export]
macro_rules! run_multi_cluster_kvs_demo_impl {
    ($demo_type:ty) => {
        async {
            let demo = <$demo_type>::default();

            println!("🚀 Running {} Demo", demo.name());
            println!("{}", demo.description());
            println!();

            // Set up localhost deployment
            let mut deployment = hydro_deploy::Deployment::new();
            let localhost = deployment.Localhost();

            // Create Hydro flow with proxy process, multiple shard clusters, and external client interface
            let flow = hydro_lang::compile::builder::FlowBuilder::new();
            let proxy = flow.process::<()>();
            let client_external = flow.external::<()>();

            // Create multiple shard clusters
            let shard_clusters: Vec<_> = (0..demo.shard_count())
                .map(|_| flow.cluster::<kvs_zoo::core::KVSNode>())
                .collect();

            // Create the router
            let router = demo.create_router(&flow);

            // Use bidirectional external connection
            let (bidi_port, operations_stream, _membership, complete_sink) = proxy
                .bidi_external_many_bincode::<_, kvs_zoo::protocol::KVSOperation<_>, String>(&client_external);

            // Route operations to shard clusters
            let shard_operations = router.route_to_shards(
                operations_stream.entries().map(q!(|(_client_id, op)| op)).assume_ordering(nondet!(/** Todo: WHY? */)),
                &shard_clusters,
            );

            // Process operations within each shard cluster
            let shard_responses: Vec<_> = shard_clusters
                .iter()
                .zip(shard_operations.iter())
                .map(|(cluster, ops)| {
                    // Use demux_ops to split operations within this shard
                    let (put_tuples, get_keys) = kvs_zoo::core::KVSCore::demux_ops(ops.clone());

                    // Execute operations using the storage implementation within this shard
                    let kvs_state = <
                        <$demo_type as kvs_zoo::driver::MultiClusterKVSDemo>::Storage as kvs_zoo::sharded::KVSShardable<
                            <$demo_type as kvs_zoo::driver::MultiClusterKVSDemo>::Value
                        >
                    >::put(
                        put_tuples.inspect(q!(|(key, value)| {
                            println!("💾 Storing: {} = {:?}", key, value);
                        })),
                        cluster,
                    );

                    // Handle GET operations within this shard
                    let ticker = cluster.tick();
                    <
                        <$demo_type as kvs_zoo::driver::MultiClusterKVSDemo>::Storage as kvs_zoo::sharded::KVSShardable<
                            <$demo_type as kvs_zoo::driver::MultiClusterKVSDemo>::Value
                        >
                    >::get(
                        get_keys.inspect(q!(|key| {
                            println!("🔍 Looking up: {}", key);
                        })).batch(&ticker, nondet!(/** batch gets */)),
                        kvs_state.snapshot(&ticker, nondet!(/** snapshot for gets */)),
                    )
                })
                .collect();

            // Combine responses from all shards using interleave
            let all_responses = match shard_responses.len() {
                0 => panic!("No shard responses"),
                1 => shard_responses.into_iter().next().unwrap().all_ticks(),
                _ => {
                    let mut iter = shard_responses.into_iter();
                    let first = iter.next().unwrap().all_ticks();
                    iter.fold(first, |acc, shard_resp| acc.interleave(shard_resp.all_ticks()))
                }
            };

            // Send results back through proxy to external
            let proxy_responses = all_responses.map(q!(|(key, result)| {
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

            // Deploy to localhost with multiple clusters
            let nodes = shard_clusters
                .iter()
                .fold(
                    flow.with_process(&proxy, localhost.clone()),
                    |builder, cluster| {
                        builder.with_cluster(cluster, vec![localhost.clone(); demo.replica_count()])
                    }
                )
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
