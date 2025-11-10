//! Declarative cluster specification with inline dispatch and maintenance
//!
//! This module provides a clean, declarative API for specifying KVS cluster
//! topologies with dispatch and maintenance strategies attached to the tree nodes.
//!
//! ## Design Philosophy
//!
//! The cluster specification is a tree where:
//! - **Dispatch** strategies attach to each level (how to route at that level)
//! - **Maintenance** strategies attach to each level (how to sync at that level)
//! - **Count** defines the fanout at that level
//! - **Each** defines the child configuration
//!
//! Notes:
//! - At leaf nodes (actual storage replicas), routing is often unnecessary.
//!   To make that explicit and educative, you can write `dispatch: ()` for a
//!   `KVSNode` to indicate “no routing here”. The unit type implements
//!   `OpDispatch` as a trivial forwarder to the single member.
//! - At the cluster level, we require a real dispatcher (e.g., sharding or
//!   replica selection). Using `dispatch: ()` at the top is intentionally
//!   disallowed by the `from_spec` API to avoid ambiguity.
//!
//! Example: Sharded + Replicated (3 shards × 3 replicas = 9 nodes)
//!
//! ```ignore
//! KVSCluster {
//!     dispatch: ShardedRouter::new(3),        // Route to shard by key
//!     maintenance: ZeroMaintenance,           // No cross-shard sync
//!     count: 3,                               // 3 shards
//!     each: KVSNode {
//!         dispatch: RoundRobinRouter::new(),  // Round-robin within shard
//!         maintenance: BroadcastReplication::default(),  // Sync replicas
//!         count: 3                            // 3 replicas per shard
//!     }
//! }
//! ```

use std::fmt;
use crate::dispatch::{OpDispatch, ClusterLevelDispatch, Pipeline};
use crate::maintenance::ReplicationStrategy;
use crate::server::{KVSBuilder, BuiltKVS};

/// Top-level cluster specification with dispatch and maintenance
///
/// - `CD`: Cluster-level dispatch type (e.g., ShardedRouter, SingleNodeRouter)
/// - `ND`: Node-level dispatch type (e.g., RoundRobinRouter, SingleNodeRouter)
/// - `CM`: Cluster-level maintenance type
/// - `NM`: Node-level maintenance type
#[derive(Clone)]
pub struct KVSCluster<CD, ND, CM, NM> {
    /// Dispatch strategy for routing between shards/clusters
    pub dispatch: CD,
    /// Maintenance strategy at cluster level (typically () for no cross-shard sync)
    pub maintenance: CM,
    /// Number of shards/clusters
    pub count: usize,
    /// Configuration for each shard/cluster
    pub each: KVSNode<ND, NM>,
}

/// Node-level specification with dispatch and maintenance
///
/// - `D`: Dispatch type (e.g., RoundRobinRouter for replicas, SingleNodeRouter for single node)
/// - `M`: Maintenance type (e.g., SimpleGossip, BroadcastReplication, ZeroMaintenance)
#[derive(Clone)]
pub struct KVSNode<D, M> {
    /// Dispatch strategy within this node (e.g., round-robin across replicas)
    pub dispatch: D,
    /// Maintenance strategy for this node (e.g., gossip between replicas)
    pub maintenance: M,
    /// Number of replicas
    pub count: usize,
}

// Debug implementations
impl<CD: fmt::Debug, ND: fmt::Debug, CM: fmt::Debug, NM: fmt::Debug> fmt::Debug for KVSCluster<CD, ND, CM, NM> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KVSCluster")
            .field("dispatch", &self.dispatch)
            .field("maintenance", &self.maintenance)
            .field("count", &self.count)
            .field("each", &self.each)
            .finish()
    }
}

impl<D: fmt::Debug, M: fmt::Debug> fmt::Debug for KVSNode<D, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KVSNode")
            .field("dispatch", &self.dispatch)
            .field("maintenance", &self.maintenance)
            .field("count", &self.count)
            .finish()
    }
}

// Helper methods
impl<CD, ND, CM, NM> KVSCluster<CD, ND, CM, NM> {
    /// Total number of nodes in the cluster (shards × replicas)
    pub fn total_nodes(&self) -> usize {
        self.count * self.each.count
    }

    /// Number of shards
    pub fn shard_count(&self) -> usize {
        self.count
    }

    /// Number of replicas per shard
    pub fn replicas_per_shard(&self) -> usize {
        self.each.count
    }

    /// Is this a sharded configuration?
    pub fn is_sharded(&self) -> bool {
        self.count > 1
    }

    /// Is this a replicated configuration?
    pub fn is_replicated(&self) -> bool {
        self.each.count > 1
    }

    /// Create a KVSBuilder from this spec, inferring dispatcher and maintenance types from the spec.
    ///
    /// Supply only the value type `V`; the dispatch and maintenance types are taken from the spec.
    /// Maintenance attached at this level and at its children is combined automatically.
    pub fn builder_for<V>(self) -> KVSBuilder<V, Pipeline<CD, ND>, crate::maintenance::CombinedMaintenance<CM, NM>>
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
        CD: OpDispatch<V> + ClusterLevelDispatch + Clone,
        ND: OpDispatch<V> + Clone,
        CM: ReplicationStrategy<V> + Clone,
        NM: ReplicationStrategy<V> + Clone,
        Pipeline<CD, ND>: OpDispatch<V>,
    {
    KVSBuilder::<V, Pipeline<CD, ND>, crate::maintenance::CombinedMaintenance<CM, NM>>::from_spec(self)
    }

    /// Build a server directly from this spec by specifying only the value type `V`.
    /// Maintenance attached at this level and at its children is combined automatically.
    pub async fn build_server<V>(self) -> Result<BuiltKVS<V, Pipeline<CD, ND>, crate::maintenance::CombinedMaintenance<CM, NM>>, Box<dyn std::error::Error>>
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
        CD: OpDispatch<V> + ClusterLevelDispatch + Clone,
        ND: OpDispatch<V> + Clone,
        CM: ReplicationStrategy<V> + Clone,
        NM: ReplicationStrategy<V> + Clone,
        Pipeline<CD, ND>: OpDispatch<V>,
    {
        self.builder_for::<V>().build().await
    }
}
