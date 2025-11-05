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

/// Placeholder macro - examples now use composable services directly
#[macro_export]
macro_rules! run_kvs_demo_impl {
    ($demo_type:ty) => {
        compile_error!("Use composable services directly instead of this macro. See examples/local.rs for the new pattern.")
    };
}
