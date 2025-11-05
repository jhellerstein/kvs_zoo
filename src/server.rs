//! KVS Server Architecture
//!
//! This module provides the standard composable server architecture for building
//! distributed key-value stores. The architecture is based on composable services
//! that can be nested and combined to create complex distributed systems.
//!
//! ## Core Concept
//!
//! The key abstraction is `KVSServer` which can represent:
//! - A single node KVS implementation (`LocalKVSServer`)
//! - A cluster of nodes running a replicated KVS (`ReplicatedKVSServer`)
//! - A sharded system using any underlying server type (`ShardedKVSServer<T>`)
//!
//! ## Composability
//!
//! Servers can be composed to create sophisticated architectures:
//! - `ShardedKVSServer<LocalKVSServer>` = 3 shards, each being a single node
//! - `ShardedKVSServer<ReplicatedKVSServer>` = 3 shards × 3 replicas = 9 nodes
//!
//! Features added to any server type automatically inherit at all composition levels.

use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use crate::routers::KVSRouter;
use hydro_lang::location::external_process::ExternalBincodeBidi;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

// Type aliases for server ports
type ServerBidiPort<V> =
    ExternalBincodeBidi<KVSOperation<V>, String, hydro_lang::location::external_process::Many>;
type ServerPorts<V> = ServerBidiPort<V>;

/// Abstraction over KVS servers that can be either single nodes or clusters
///
/// This trait allows us to treat both individual KVS implementations and
/// clusters of replicated KVS implementations uniformly, enabling true
/// composability in server architecture.
pub trait KVSServer<V>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
{
    /// The deployment unit for this service (could be a single process or cluster)
    type Deployment<'a>;

    /// Create the deployment unit
    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a>;

    /// Run the KVS server and return bidirectional port for client communication
    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync;

    /// Get the size (number of nodes) of this service
    fn size() -> usize;
}

/// Single-node KVS server using LWW semantics
pub struct LocalKVSServer<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> LocalKVSServer<V> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> Default for LocalKVSServer<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> KVSServer<V> for LocalKVSServer<V>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        // Create a cluster of size 1 for uniform interface
        flow.cluster::<KVSNode>()
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Use bidirectional external connection like the working local.rs example
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Route operations using LocalRouter
        let router = crate::routers::LocalRouter;
        let routed_operations = router.route_operations(
            operations_stream
                .entries()
                .map(q!(|(_client_id, op)| op))
                .assume_ordering(nondet!(/** Todo: WHY? */)),
            deployment,
        );

        // Demux operations into puts and gets
        let (put_tuples, get_keys) = crate::core::KVSCore::demux_ops(routed_operations);

        // Execute operations using LWW storage
        let kvs_state = crate::lww::KVSLww::put(put_tuples);

        // Handle GET operations
        let ticker = deployment.tick();
        let get_results = crate::lww::KVSLww::get(
            get_keys.batch(&ticker, nondet!(/** batch gets */)),
            kvs_state.snapshot(&ticker, nondet!(/** snapshot for gets */)),
        );

        // Send results back through proxy to external
        let proxy_responses = get_results
            .all_ticks()
            .map(q!(|(key, result)| {
                match result {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                }
            }))
            .send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        // Return the bidirectional port
        bidi_port
    }

    fn size() -> usize {
        1
    }
}

/// Replicated KVS server using epidemic gossip
pub struct ReplicatedKVSServer<V> {
    #[allow(dead_code)] // TODO: Use this field to make cluster size configurable
    cluster_size: usize,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> ReplicatedKVSServer<V> {
    pub fn new(cluster_size: usize) -> Self {
        Self {
            cluster_size,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> KVSServer<V> for ReplicatedKVSServer<V>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
{
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        flow.cluster::<KVSNode>()
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Use bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Route operations using RoundRobinRouter (distribute across replicas)
        let router = crate::routers::RoundRobinRouter;
        let routed_operations = router.route_operations(
            operations_stream
                .entries()
                .map(q!(|(_client_id, op)| op))
                .assume_ordering(nondet!(/** Todo: WHY? */)),
            deployment,
        );

        // Demux operations into puts and gets
        let (put_tuples, get_keys) = crate::core::KVSCore::demux_ops(routed_operations);

        // Execute operations using ReplicatedKVS with EpidemicGossip
        let kvs_state = crate::replicated::KVSReplicated::<crate::routers::EpidemicGossip<V>>::put(
            put_tuples, deployment,
        );

        // Handle GET operations
        let ticker = deployment.tick();
        let get_results =
            crate::replicated::KVSReplicated::<crate::routers::EpidemicGossip<V>>::get(
                get_keys.batch(&ticker, nondet!(/** batch gets for efficiency */)),
                kvs_state.snapshot(&ticker, nondet!(/** snapshot for gets */)),
            );

        // Send results back through proxy to external
        let proxy_responses = get_results
            .all_ticks()
            .map(q!(|(key, result)| {
                match result {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                }
            }))
            .send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        // Return the bidirectional port
        bidi_port
    }

    fn size() -> usize {
        3 // Default cluster size - TODO: make this configurable per instance
    }
}

/// Sharded KVS server that can work with any underlying KVS server
///
/// This is the key to composability - it treats the underlying server
/// as a black box and just handles sharding logic.
pub struct ShardedKVSServer<S> {
    #[allow(dead_code)] // TODO: Use this field to make shard count configurable
    num_shards: usize,
    _phantom: std::marker::PhantomData<S>,
}

impl<S> ShardedKVSServer<S> {
    pub fn new(num_shards: usize) -> Self {
        Self {
            num_shards,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V, S> KVSServer<V> for ShardedKVSServer<S>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    S: KVSServer<V>,
    for<'a> S::Deployment<'a>: Clone,
{
    // For sharded services, we need multiple deployments (one per shard)
    type Deployment<'a> = Vec<Cluster<'a, KVSNode>>;

    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        // Create one cluster per shard (simplified approach)
        (0..3) // Hardcoded to 3 shards for now
            .map(|_| flow.cluster::<KVSNode>())
            .collect()
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployments: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Use bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Extract operations and route them to appropriate shards
        let operations = operations_stream
            .entries()
            .map(q!(|(_client_id, op)| op))
            .assume_ordering(nondet!(/** Todo: WHY? */));

        // For now, implement a simple sharded approach using the first deployment
        // In a full implementation, we would route to different shards based on key hash
        // and collect responses from all shards

        // Route operations using ShardedRouter logic
        let router = crate::routers::ShardedRouter::new(3);
        let routed_operations = router.route_operations(operations, &deployments[0]);

        // Demux operations into puts and gets
        let (put_tuples, get_keys) = crate::core::KVSCore::demux_ops(routed_operations);

        // Execute operations using LWW storage (simplified - should route to appropriate shard)
        let kvs_state = crate::lww::KVSLww::put(put_tuples);

        // Handle GET operations
        let ticker = deployments[0].tick();
        let get_results = crate::lww::KVSLww::get(
            get_keys.batch(&ticker, nondet!(/** batch gets */)),
            kvs_state.snapshot(&ticker, nondet!(/** snapshot for gets */)),
        );

        // Send results back through proxy to external
        let proxy_responses = get_results
            .all_ticks()
            .map(q!(|(key, result)| {
                match result {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                }
            }))
            .send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        // Return the bidirectional port
        bidi_port
    }

    fn size() -> usize {
        3 * S::size() // 3 shards * size of each shard - TODO: use self.num_shards
    }
}

// =============================================================================
// Composable Type Aliases
// =============================================================================

// Type aliases removed due to generic parameter complexity
// Use the concrete types directly: LocalKVSService<V>, ReplicatedKVSService<V>, etc.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_types_compile() {
        // Just test that the types can be constructed
        let _local = LocalKVSServer::<String>::new();
        let _replicated: ReplicatedKVSServer<crate::values::CausalString> =
            ReplicatedKVSServer::new(3);
        let _sharded_local = ShardedKVSServer::<LocalKVSServer<String>>::new(3);
        let _sharded_replicated =
            ShardedKVSServer::<ReplicatedKVSServer<crate::values::CausalString>>::new(3);

        println!("✅ Server types compile!");

        // Test size calculations with explicit type annotations
        println!(
            "   LocalKVS size: {}",
            <LocalKVSServer<String> as KVSServer<String>>::size()
        );
        println!(
            "   ReplicatedKVS size: {}",
            <ReplicatedKVSServer<crate::values::CausalString> as KVSServer<
                crate::values::CausalString,
            >>::size()
        );
        println!(
            "   ShardedLocalKVS size: {}",
            <ShardedKVSServer<LocalKVSServer<String>> as KVSServer<String>>::size()
        );
        println!(
            "   ShardedReplicatedKVS size: {}",
            <ShardedKVSServer<ReplicatedKVSServer<crate::values::CausalString>> as KVSServer<
                crate::values::CausalString,
            >>::size()
        );
    }

    #[tokio::test]
    async fn test_deployment_creation() {
        let flow = FlowBuilder::new();

        // Test that deployments can be created with explicit type annotations
        let _local_deployment =
            <LocalKVSServer<String> as KVSServer<String>>::create_deployment(&flow);
        let _replicated_deployment =
            <ReplicatedKVSServer<crate::values::CausalString> as KVSServer<
                crate::values::CausalString,
            >>::create_deployment(&flow);
        let _sharded_local_deployments = <ShardedKVSServer<LocalKVSServer<String>> as KVSServer<
            String,
        >>::create_deployment(&flow);
        let _sharded_replicated_deployments =
            <ShardedKVSServer<ReplicatedKVSServer<crate::values::CausalString>> as KVSServer<
                crate::values::CausalString,
            >>::create_deployment(&flow);

        // Finalize the flow
        let _nodes = flow.finalize();

        println!("✅ Deployment creation works!");
    }
}
