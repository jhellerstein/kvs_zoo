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

/// Replication timing configuration
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BaseReplicationConfig {
    /// Synchronous replication: immediately propagate operations as they arrive
    /// - Lower latency, operations are replicated immediately
    /// - Higher network overhead due to individual message sending
    Synchronous,

    /// Background replication: batch and periodically propagate operations
    /// - Higher latency, operations are batched and sent periodically
    /// - Lower network overhead due to batching and sampling
    Background {
        /// How often to run replication rounds (periodic sampling interval)
        replication_interval: std::time::Duration,
    },
}

impl Default for BaseReplicationConfig {
    fn default() -> Self {
        // Default to background for efficiency
        Self::Background {
            replication_interval: std::time::Duration::from_secs(1),
        }
    }
}

impl BaseReplicationConfig {
    /// Create a synchronous replication configuration
    pub fn synchronous() -> Self {
        Self::Synchronous
    }

    /// Create a background replication configuration with custom interval
    pub fn background(interval: std::time::Duration) -> Self {
        Self::Background {
            replication_interval: interval,
        }
    }

    /// Get the replication interval if this is a background configuration
    pub fn replication_interval(&self) -> Option<std::time::Duration> {
        match self {
            Self::Synchronous => None,
            Self::Background {
                replication_interval,
            } => Some(*replication_interval),
        }
    }

    /// Check if this is synchronous replication
    pub fn is_synchronous(&self) -> bool {
        matches!(self, Self::Synchronous)
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
