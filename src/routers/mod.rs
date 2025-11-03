//! Routing strategies for distributed key-value stores
//!
//! This module provides all routing implementations, from basic patterns
//! to advanced replication strategies that handle both routing and data
//! consistency.
//!
//! ## Basic Routers
//! - [`LocalRouter`] - Broadcast to all nodes (single-node development)
//! - [`RoundRobinRouter`] - Round-robin distribution across nodes
//! - [`ShardedRouter`] - Hash-based key partitioning
//!
//! ## Advanced Replication Protocols
//! - [`EpidemicGossip`] - Gossip-based eventual consistency
//! - [`BroadcastReplication`] - Reliable broadcast replication

use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Trait for routing KVS operations to appropriate nodes
///
/// This is the fundamental interface that all routers must implement.
/// It defines how operations from external clients are distributed
/// to the cluster nodes.
pub trait KVSRouter<V> {
    /// Route operations from external clients to cluster nodes
    ///
    /// Takes a stream of operations from a single external process and
    /// returns a stream of operations distributed across the cluster
    /// according to the router's strategy.
    fn route_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
}

// Basic routers
pub mod local;
pub mod round_robin;
pub mod sharded;

// Advanced replication protocols
pub mod broadcast;
pub mod common;
pub mod gossip;

// Re-export all router types for convenience
pub use local::LocalRouter;
pub use round_robin::RoundRobinRouter;
pub use sharded::ShardedRouter;

// Re-export advanced replication types
pub use broadcast::{BroadcastReplication, BroadcastReplicationConfig};
pub use common::{BaseReplicationConfig, ReplicationCommon, ReplicationMessage, ReplicationMode};
pub use gossip::{EpidemicGossip, EpidemicGossipConfig};

/// Trait for advanced routing protocols that handle replication
///
/// This trait defines routing strategies that combine routing with replication,
/// providing both data distribution and consistency guarantees.
pub trait ReplicationProtocol<V>
where
    V: Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default
        + lattices::Merge<V>,
{
    /// Handle replication for a stream of local PUT operations
    ///
    /// Takes local PUT operations as (key, value) tuples and returns a stream of
    /// replicated (key, value) tuples received from other nodes.
    ///
    /// Each protocol implementation uses its own default configuration.
    fn handle_replication<'a>(
        cluster: &hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
        local_put_tuples: hydro_lang::prelude::Stream<
            (String, V),
            hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
            hydro_lang::prelude::Unbounded,
        >,
    ) -> hydro_lang::prelude::Stream<
        (String, V),
        hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
        hydro_lang::prelude::Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >;
}

// Implement the trait for our replication protocols
impl<V> ReplicationProtocol<V> for EpidemicGossip<V>
where
    V: Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default
        + lattices::Merge<V>,
{
    fn handle_replication<'a>(
        cluster: &hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
        local_put_tuples: hydro_lang::prelude::Stream<
            (String, V),
            hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
            hydro_lang::prelude::Unbounded,
        >,
    ) -> hydro_lang::prelude::Stream<
        (String, V),
        hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
        hydro_lang::prelude::Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    > {
        Self::handle_gossip_simple(cluster, local_put_tuples)
    }
}

impl<V> ReplicationProtocol<V> for BroadcastReplication<V>
where
    V: Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default
        + lattices::Merge<V>,
{
    fn handle_replication<'a>(
        cluster: &hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
        local_put_tuples: hydro_lang::prelude::Stream<
            (String, V),
            hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
            hydro_lang::prelude::Unbounded,
        >,
    ) -> hydro_lang::prelude::Stream<
        (String, V),
        hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
        hydro_lang::prelude::Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    > {
        Self::handle_broadcast_simple(cluster, local_put_tuples)
    }
}
