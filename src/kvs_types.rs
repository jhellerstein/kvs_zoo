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
//! - **Sharded + Replicated**: True sharded architecture with shard-aware replication
//!
//! ## Sharded + Replicated Architecture
//!
//! The sharded+replicated types use a compositional approach:
//! - **Router**: `ShardedRoundRobin` - routes operations to shard primaries based on key hash
//! - **Storage**: `KVSReplicated<ShardAwareBroadcastReplication>` - replicates only within shard boundaries
//! - **Result**: 3 shards × 3 replicas = 9 total nodes with true scalability and fault tolerance
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

/// Sharded + Replicated KVS with shard-aware broadcast replication
///
/// **Architecture**: Hash partitioning with shard-boundary-aware replication
/// **Use case**: Web-scale applications requiring both scalability and availability
/// **Consistency**: Per-shard strong, cross-shard eventual
/// **Availability**: High (fault tolerance within each shard)
/// **Routing**: ShardedRoundRobin - routes to shard primaries
/// **Replication**: ShardAwareBroadcastReplication - replicates only within shard boundaries
pub type KVSShardedReplicated<V> = KVSServer<
    V,
    crate::replicated::KVSReplicated<crate::routers::ShardAwareBroadcastReplication>,
    crate::routers::ShardedRoundRobin,
>;

// =============================================================================
// String-based Convenience Aliases
// =============================================================================

/// String-based local KVS (most common case)
pub type StringKVSLocalLww = KVSLocalLww<String>;

/// String-based replicated KVS with epidemic gossip
pub type StringKVSReplicatedEpidemicGossip = KVSReplicatedEpidemicGossip<String>;

/// String-based replicated KVS with broadcast
pub type StringKVSReplicatedBroadcast = KVSReplicatedBroadcast<String>;

/// String-based sharded KVS
pub type StringKVSShardedLww = KVSShardedLww<String>;

/// String-based sharded + replicated KVS with shard-aware replication
pub type StringKVSShardedReplicated = KVSShardedReplicated<String>;

// =============================================================================
// Value-Specific Aliases (Using values module) - NEW KVS-PREFIX NAMING
// =============================================================================

/// Local KVS with LWW wrapper values
pub type KVSLocalLwwWrapper<T> = KVSLocalLww<crate::values::LwwWrapper<T>>;

/// Replicated KVS with causal values (most common for replicated systems)
pub type KVSReplicatedCausal = KVSReplicatedEpidemicGossip<crate::values::CausalString>;

/// Sharded + Replicated KVS with causal values (recommended for production)
pub type KVSShardedReplicatedCausal = KVSShardedReplicated<crate::values::CausalString>;

/// Sharded + Replicated KVS with LWW values (simpler conflict resolution)
pub type KVSShardedReplicatedLww = KVSShardedReplicated<crate::values::LwwWrapper<String>>;
