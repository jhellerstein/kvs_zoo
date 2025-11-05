use crate::core::KVSNode;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use lattices::Merge;

use super::common::BaseReplicationConfig;

/// Configuration for broadcast replication
#[derive(Clone, Debug)]
pub struct BroadcastReplicationConfig {
    /// Base replication configuration (timing, intervals)
    pub base: BaseReplicationConfig,
    /// Batch multiple updates before broadcasting
    pub enable_batching: bool,
    /// Maximum time to wait before sending a batch
    pub batch_timeout: std::time::Duration,
    /// Maximum number of keys per batch
    pub max_batch_size: usize,
}

impl Default for BroadcastReplicationConfig {
    fn default() -> Self {
        Self {
            base: BaseReplicationConfig::default(),
            enable_batching: false,
            batch_timeout: std::time::Duration::from_millis(100),
            max_batch_size: 50,
        }
    }
}

impl BroadcastReplicationConfig {
    /// Create config optimized for low latency (immediate broadcasting)
    pub fn low_latency() -> Self {
        Self {
            base: BaseReplicationConfig::background(std::time::Duration::from_millis(100)),
            enable_batching: false,
            ..Default::default()
        }
    }

    /// Create config optimized for high throughput (batched broadcasting)
    pub fn high_throughput() -> Self {
        Self {
            base: BaseReplicationConfig::background(std::time::Duration::from_secs(1)),
            enable_batching: true,
            batch_timeout: std::time::Duration::from_millis(200),
            max_batch_size: 100,
        }
    }

    /// Create config for synchronous broadcasting (immediate, no background)
    pub fn synchronous() -> Self {
        Self {
            base: BaseReplicationConfig::synchronous(),
            enable_batching: false,
            ..Default::default()
        }
    }
}

/// Broadcast replication: sends updates to all cluster nodes
///
/// Uses deterministic all-to-all broadcasting. Higher message overhead than gossip
/// but faster convergence and simpler reasoning about consistency.
pub struct BroadcastReplication<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for BroadcastReplication<V> {
    fn default() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> BroadcastReplication<V>
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
        + Merge<V>,
{
    /// Simple broadcast replication - broadcasts all local operations to all nodes
    pub fn handle_replication<'a>(
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        _changed_keys: Stream<String, Cluster<'a, KVSNode>, Unbounded>, // Unused - kept for API compatibility
        _main_kvs: KeyedSingleton<String, V, Cluster<'a, KVSNode>, Unbounded>, // Unused - kept for API compatibility
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        local_put_tuples
            .broadcast_bincode(cluster, nondet!(/** broadcast to all nodes */))
            .values()
    }
}
