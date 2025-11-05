//! Composable KVS architecture experiment
//!
//! This module explores a composable approach where:
//! - A ReplicatedKVS + Cluster can be treated as a single KVS service
//! - A ShardedKVS can work with any KVS service (local or replicated)
//! - Features added to clusters inherit at both scales
//!
//! The key abstraction is `KVSService` which can represent either:
//! - A single node KVS implementation
//! - A cluster of nodes running a replicated KVS

use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

// Type aliases to reduce complexity warnings
type ServiceInputSink<V> = ExternalBincodeSink<KVSOperation<V>>;
type ServiceOutputStream<V> = ExternalBincodeStream<(String, Option<V>), NoOrder>;
type ServicePorts<V> = (ServiceInputSink<V>, ServiceOutputStream<V>);

/// Abstraction over KVS services that can be either single nodes or clusters
///
/// This trait allows us to treat both individual KVS implementations and
/// clusters of replicated KVS implementations uniformly, enabling true
/// compositionality.
pub trait KVSService<V>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
{
    /// The deployment unit for this service (could be a single process or cluster)
    type Deployment<'a>;

    /// Create the deployment unit
    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a>;

    /// Run the KVS service and return external ports for client communication
    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
    ) -> ServicePorts<V>
    where
        V: std::fmt::Debug + Send + Sync;

    /// Get the size (number of nodes) of this service
    fn size() -> usize;
}

/// Single-node KVS service using LWW semantics
pub struct LocalKVSService;

impl<V> KVSService<V> for LocalKVSService
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
    ) -> ServicePorts<V> {
        // Use existing KVSServer with LocalRouter and LWW storage
        crate::server::KVSServer::<V, crate::lww::KVSLww, crate::routers::LocalRouter>::run(
            proxy,
            deployment,
            client_external,
            &crate::routers::LocalRouter,
        )
    }

    fn size() -> usize {
        1
    }
}

/// Replicated KVS service using epidemic gossip
pub struct ReplicatedKVSService {
    cluster_size: usize,
}

impl ReplicatedKVSService {
    pub fn new(cluster_size: usize) -> Self {
        Self { cluster_size }
    }
}

impl<V> KVSService<V> for ReplicatedKVSService
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
    ) -> ServicePorts<V> {
        // Use existing KVSServer with RoundRobinRouter and ReplicatedKVS
        crate::server::KVSServer::<
            V,
            crate::replicated::KVSReplicated<crate::routers::EpidemicGossip<V>>,
            crate::routers::RoundRobinRouter,
        >::run(
            proxy,
            deployment,
            client_external,
            &crate::routers::RoundRobinRouter,
        )
    }

    fn size() -> usize {
        3 // Default cluster size for now
    }
}

/// Sharded KVS service that can work with any underlying KVS service
///
/// This is the key to compositionality - it treats the underlying service
/// as a black box and just handles sharding logic.
pub struct ShardedKVSService<S> {
    num_shards: usize,
    _phantom: std::marker::PhantomData<S>,
}

impl<S> ShardedKVSService<S> {
    pub fn new(num_shards: usize) -> Self {
        Self {
            num_shards,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V, S> KVSService<V> for ShardedKVSService<S>
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
    S: KVSService<V>,
{
    // For sharded services, we need multiple deployments (one per shard)
    type Deployment<'a> = Vec<S::Deployment<'a>>;

    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        // Create one deployment per shard
        (0..3) // Hardcoded to 3 shards for now
            .map(|_| S::create_deployment(flow))
            .collect()
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployments: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
    ) -> ServicePorts<V> {
        // Get operations from external clients
        let (input_port, operations) = proxy.source_external_bincode::<(), KVSOperation<V>, hydro_lang::live_collections::stream::NoOrder, hydro_lang::live_collections::stream::AtLeastOnce>(client_external);

        // Route operations to the correct shard based on key hash
        let shard_0_ops = operations.clone().filter(q!(|op| {
            let key = match op {
                crate::protocol::KVSOperation::Put(k, _) => k,
                crate::protocol::KVSOperation::Get(k) => k,
            };
            let shard_id = crate::sharded::calculate_shard_id(key, 3);
            if shard_id == 0 {
                println!("🎯 Routing {} to Shard 0", key);
                true
            } else {
                false
            }
        }));

        let shard_1_ops = operations.clone().filter(q!(|op| {
            let key = match op {
                crate::protocol::KVSOperation::Put(k, _) => k,
                crate::protocol::KVSOperation::Get(k) => k,
            };
            let shard_id = crate::sharded::calculate_shard_id(key, 3);
            if shard_id == 1 {
                println!("🎯 Routing {} to Shard 1", key);
                true
            } else {
                false
            }
        }));

        let shard_2_ops = operations.filter(q!(|op| {
            let key = match op {
                crate::protocol::KVSOperation::Put(k, _) => k,
                crate::protocol::KVSOperation::Get(k) => k,
            };
            let shard_id = crate::sharded::calculate_shard_id(key, 3);
            if shard_id == 2 {
                println!("🎯 Routing {} to Shard 2", key);
                true
            } else {
                false
            }
        }));

        // For now, demonstrate the sharding concept by routing to the first shard
        // In a full implementation, we'd need proper multi-shard deployment
        
        // Combine all operations and show which shard they would go to
        let all_ops = shard_0_ops.interleave(shard_1_ops).interleave(shard_2_ops);
        
        // Send to external interface
        all_ops.send_bincode_external(client_external);
        
        // Run the first shard service (representing the sharded system)
        // TODO: Implement true multi-shard deployment with separate clusters
        S::run(proxy, &deployments[0], client_external)
    }

    fn size() -> usize {
        3 * S::size() // 3 shards * size of each shard
    }
}

// =============================================================================
// Composable Type Aliases
// =============================================================================

/// Local KVS (single node)
pub type LocalKVS = LocalKVSService;

/// Replicated KVS (cluster of nodes with replication)
pub type ReplicatedKVS = ReplicatedKVSService;

/// Sharded Local KVS (shards of single nodes)
pub type ShardedLocalKVS = ShardedKVSService<LocalKVSService>;

/// Sharded Replicated KVS (shards of replicated clusters)
/// This is the holy grail - true compositionality!
pub type ShardedReplicatedKVS = ShardedKVSService<ReplicatedKVSService>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_composable_types_compile() {
        // Just test that the types can be constructed
        let _local = LocalKVSService;
        let _replicated = ReplicatedKVSService::new(3);
        let _sharded_local = ShardedKVSService::<LocalKVSService>::new(3);
        let _sharded_replicated = ShardedKVSService::<ReplicatedKVSService>::new(3);
        
        println!("✅ Composable types compile!");
        
        // Test size calculations with explicit type annotations
        println!("   LocalKVS size: {}", <LocalKVSService as KVSService<String>>::size());
        println!("   ReplicatedKVS size: {}", <ReplicatedKVSService as KVSService<crate::values::CausalString>>::size());
        println!("   ShardedLocalKVS size: {}", <ShardedKVSService<LocalKVSService> as KVSService<String>>::size());
        println!("   ShardedReplicatedKVS size: {}", <ShardedKVSService<ReplicatedKVSService> as KVSService<crate::values::CausalString>>::size());
    }

    #[tokio::test]
    async fn test_deployment_creation() {
        let flow = FlowBuilder::new();
        
        // Test that deployments can be created with explicit type annotations
        let _local_deployment = <LocalKVSService as KVSService<String>>::create_deployment(&flow);
        let _replicated_deployment = <ReplicatedKVSService as KVSService<crate::values::CausalString>>::create_deployment(&flow);
        let _sharded_local_deployments = <ShardedKVSService<LocalKVSService> as KVSService<String>>::create_deployment(&flow);
        let _sharded_replicated_deployments = <ShardedKVSService<ReplicatedKVSService> as KVSService<crate::values::CausalString>>::create_deployment(&flow);
        
        // Finalize the flow
        let _nodes = flow.finalize();
        
        println!("✅ Deployment creation works!");
    }
}