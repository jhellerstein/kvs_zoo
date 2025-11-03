use crate::core::KVSNode;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Replication message containing buffered PUT operations
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationMessage<V> {
    /// Buffered PUT operations to be replicated
    pub puts: Vec<(String, V)>,
    /// Timestamp for ordering (simple counter for now)
    pub timestamp: u64,
    /// Node ID that originated this replication message (for debugging)
    pub sender_id: u32,
}

/// Replication timing mode configuration
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Synchronous replication: immediately propagate operations as they arrive
    /// - Lower latency, operations are replicated immediately
    /// - Higher network overhead due to individual message sending
    Synchronous,

    /// Background replication: batch and periodically propagate operations
    /// - Higher latency, operations are batched and sent periodically
    /// - Lower network overhead due to batching and sampling
    Background,
}

/// Base configuration shared by all replication protocols
#[derive(Clone, Debug)]
pub struct BaseReplicationConfig {
    /// Replication timing mode (synchronous vs background)
    pub mode: ReplicationMode,

    /// How often to run replication rounds (periodic sampling interval)
    /// Only used in Background mode
    pub replication_interval: std::time::Duration,
}

impl Default for BaseReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Background, // Default to background for efficiency
            replication_interval: std::time::Duration::from_secs(1),
        }
    }
}

impl BaseReplicationConfig {
    /// Create a synchronous replication configuration
    pub fn synchronous() -> Self {
        Self {
            mode: ReplicationMode::Synchronous,
            ..Default::default()
        }
    }

    /// Create a background replication configuration with custom interval
    pub fn background(interval: std::time::Duration) -> Self {
        Self {
            mode: ReplicationMode::Background,
            replication_interval: interval,
        }
    }
}

/// Common replication utilities shared across different replication implementations
pub struct ReplicationCommon;

impl ReplicationCommon {
    /// Get cluster member IDs for replication targets
    ///
    /// This is a common pattern used by all replication protocols to discover
    /// the set of peers they can send messages to.
    pub fn get_cluster_members<'a>(
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<MemberId<KVSNode>, Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        cluster
            .source_cluster_members(cluster)
            .map_with_key(q!(|(member_id, _event)| member_id))
            .values()
    }
}
