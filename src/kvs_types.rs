//! Type aliases for common KVS configurations
//!
//! Pre-configured combinations of storage, routing, and value types:
//! - **Local**: Single-node (development/testing)
//! - **Replicated**: Multi-replica with replication protocols  
//! - **Sharded**: Hash-partitioned for scalability
//! - **Sharded + Replicated**: Partitioned with per-shard replication
//!
//! ```rust
//! use kvs_zoo::kvs_types::{StringKVSLocalLww, StringKVSReplicatedEpidemicGossip};
//!
//! type LocalKVS = StringKVSLocalLww;
//! type ReplicatedKVS = StringKVSReplicatedEpidemicGossip;
//! ```

use crate::server::{LocalKVSServer, ReplicatedKVSServer, ShardedKVSServer};

// =============================================================================
// Local Configurations (Single-node)
// =============================================================================

/// Local KVS with Last-Writer-Wins semantics
pub type KVSLocalLww<V> = LocalKVSServer<V>;

// =============================================================================
// Replicated Configurations (Multi-replica)
// =============================================================================

/// Replicated KVS with epidemic gossip protocol
pub type KVSReplicatedEpidemicGossip<V> = ReplicatedKVSServer<V>;

/// Replicated KVS with reliable broadcast protocol  
pub type KVSReplicatedBroadcast<V> = ReplicatedKVSServer<V>;

// =============================================================================
// Sharded Configurations (Partitioned)
// =============================================================================

/// Sharded KVS with Last-Writer-Wins semantics
pub type KVSShardedLww<V> = ShardedKVSServer<LocalKVSServer<V>>;

// =============================================================================
// Sharded + Replicated Configurations
// =============================================================================

/// Sharded + Replicated KVS with shard-aware replication
pub type KVSShardedReplicated<V> = ShardedKVSServer<ReplicatedKVSServer<V>>;

// =============================================================================
// String-based Convenience Aliases
// =============================================================================

/// String-based local KVS
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
// Value-Specific Aliases
// =============================================================================

/// Local KVS with LWW wrapper values
pub type KVSLocalLwwWrapper<T> = KVSLocalLww<crate::values::LwwWrapper<T>>;

/// Replicated KVS with causal values (most common for replicated systems)
pub type KVSReplicatedCausal = KVSReplicatedEpidemicGossip<crate::values::CausalString>;

/// Sharded + Replicated KVS with causal values (recommended for production)
pub type KVSShardedReplicatedCausal = KVSShardedReplicated<crate::values::CausalString>;

/// Sharded + Replicated KVS with LWW values (simpler conflict resolution)
pub type KVSShardedReplicatedLww = KVSShardedReplicated<crate::values::LwwWrapper<String>>;
