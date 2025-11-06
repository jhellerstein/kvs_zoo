//! Simple configuration for KVS servers
//!
//! This module provides basic configuration structs for the different KVS server types.
//! Only includes settings that are actually used by the implementations.

/// Configuration for replicated KVS servers
#[derive(Debug, Clone)]
pub struct ReplicatedConfig {
    /// Number of replica nodes in the cluster
    pub cluster_size: usize,
}

impl Default for ReplicatedConfig {
    fn default() -> Self {
        Self { cluster_size: 3 }
    }
}

/// Configuration for sharded KVS servers
#[derive(Debug, Clone)]
pub struct ShardedConfig {
    /// Number of shards
    pub shard_count: usize,
}

impl Default for ShardedConfig {
    fn default() -> Self {
        Self { shard_count: 3 }
    }
}

/// Configuration for sharded + replicated KVS servers
#[derive(Debug, Clone)]
pub struct ShardedReplicatedConfig {
    /// Number of shards
    pub shard_count: usize,
    /// Number of replicas per shard
    pub replicas_per_shard: usize,
}

impl Default for ShardedReplicatedConfig {
    fn default() -> Self {
        Self {
            shard_count: 3,
            replicas_per_shard: 3,
        }
    }
}

impl ShardedReplicatedConfig {
    /// Total number of nodes in the cluster
    pub fn total_nodes(&self) -> usize {
        self.shard_count * self.replicas_per_shard
    }
}
