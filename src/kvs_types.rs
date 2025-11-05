//! Convenient type aliases for common KVS configurations
//!
//! This module provides pre-configured type aliases that combine different
//! storage implementations, routing strategies, and value types for common
//! distributed KVS architectures.
//!
//! ## Architecture Categories
//!
//! - **Local**: Single-node configurations for development/testing
//! - **Replicated**: Multi-replica configurations with replication protocols
//! - **Sharded**: Hash-partitioned configurations for scalability
//! - **Sharded + Replicated**: Combined approaches for both scalability and availability
//!
//! ## Usage (New KVS-prefix naming)
//!
//! ```rust
//! use kvs_zoo::kvs_types::{StringKVSLocalLww, StringKVSReplicatedEpidemicGossip};
//!
//! // Use pre-configured types with consistent KVS-prefix naming
//! type LocalKVS = StringKVSLocalLww;
//! type ReplicatedKVS = StringKVSReplicatedEpidemicGossip;
//! ```

use crate::server::KVSServer;

// =============================================================================
// Local Configurations (Single-node) - NEW KVS-PREFIX NAMING
// =============================================================================

/// Local KVS with Last-Writer-Wins semantics
///
/// **Architecture**: Single node with LWW conflict resolution
/// **Use case**: Development, testing, simple applications
/// **Consistency**: Strong (single node)
/// **Availability**: Low (single point of failure)
pub type KVSLocalLww<V> = KVSServer<V, crate::lww::KVSLww, crate::routers::LocalRouter>;

// =============================================================================
// Replicated Configurations (Multi-replica) - NEW KVS-PREFIX NAMING
// =============================================================================

/// Replicated KVS with epidemic gossip protocol
///
/// **Architecture**: Multiple replicas with gossip-based synchronization
/// **Use case**: High availability, partition tolerance
/// **Consistency**: Causal (with vector clocks)
/// **Availability**: High (fault tolerant)
pub type KVSReplicatedEpidemicGossip<V> =
    KVSServer<V, crate::replicated::KVSReplicatedEpidemic<V>, crate::routers::RoundRobinRouter>;

/// Replicated KVS with reliable broadcast protocol
///
/// **Architecture**: Multiple replicas with broadcast synchronization
/// **Use case**: Strong consistency requirements with replication
/// **Consistency**: Strong (reliable broadcast)
/// **Availability**: High (fault tolerant)
pub type KVSReplicatedBroadcast<V> =
    KVSServer<V, crate::replicated::KVSReplicatedBroadcast<V>, crate::routers::RoundRobinRouter>;

// =============================================================================
// Sharded Configurations (Partitioned) - NEW KVS-PREFIX NAMING
// =============================================================================

/// Sharded KVS with Last-Writer-Wins semantics
///
/// **Architecture**: Hash-based key partitioning across nodes
/// **Use case**: Horizontal scalability for large datasets
/// **Consistency**: Per-shard strong, global eventual
/// **Availability**: Medium (shard failures affect subset of keys)
pub type KVSShardedLww<V> = KVSServer<V, crate::lww::KVSLww, crate::routers::ShardedRouter>;

// =============================================================================
// Sharded + Replicated Configurations (Scalability + Availability) - NEW KVS-PREFIX NAMING
// =============================================================================

/// Sharded + Replicated KVS with epidemic gossip
///
/// **Architecture**: Hash partitioning + per-shard replication with gossip
/// **Use case**: Web-scale applications requiring both scalability and availability
/// **Consistency**: Per-shard causal, global eventual
/// **Availability**: High (both sharding and replication)
pub type KVSShardedReplicatedEpidemicGossip<V> =
    KVSServer<V, crate::replicated::KVSReplicatedEpidemic<V>, crate::routers::ShardedRouter>;

/// Sharded + Replicated KVS with reliable broadcast
///
/// **Architecture**: Hash partitioning + per-shard replication with broadcast
/// **Use case**: Large-scale systems with strong per-shard consistency
/// **Consistency**: Per-shard strong, global eventual
/// **Availability**: High (both sharding and replication)
pub type KVSShardedReplicatedBroadcast<V> =
    KVSServer<V, crate::replicated::KVSReplicatedBroadcast<V>, crate::routers::ShardedRouter>;

// =============================================================================
// String-based Convenience Aliases - NEW KVS-PREFIX NAMING
// =============================================================================

/// String-based local KVS (most common case)
pub type StringKVSLocalLww = KVSLocalLww<String>;

/// String-based replicated KVS with epidemic gossip
pub type StringKVSReplicatedEpidemicGossip = KVSReplicatedEpidemicGossip<String>;

/// String-based replicated KVS with broadcast
pub type StringKVSReplicatedBroadcast = KVSReplicatedBroadcast<String>;

/// String-based sharded KVS
pub type StringKVSShardedLww = KVSShardedLww<String>;

/// String-based sharded + replicated KVS with epidemic gossip
pub type StringKVSShardedReplicatedEpidemicGossip = KVSShardedReplicatedEpidemicGossip<String>;

/// String-based sharded + replicated KVS with broadcast
pub type StringKVSShardedReplicatedBroadcast = KVSShardedReplicatedBroadcast<String>;

// =============================================================================
// Value-Specific Aliases (Using values module) - NEW KVS-PREFIX NAMING
// =============================================================================

/// Local KVS with LWW wrapper values
pub type KVSLocalLwwWrapper<T> = KVSLocalLww<crate::values::LwwWrapper<T>>;

/// Replicated KVS with causal values (most common for replicated systems)
pub type KVSReplicatedCausal = KVSReplicatedEpidemicGossip<crate::values::CausalString>;

/// Sharded KVS with causal values
pub type KVSShardedCausal = KVSShardedReplicatedEpidemicGossip<crate::values::CausalString>;
