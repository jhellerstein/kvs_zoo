//! Tombstone Cleanup Strategy
//!
//! Provides background garbage collection for tombstoned (deleted) entries in
//! replicated systems. This maintenance strategy runs independently to reclaim
//! storage space from deleted keys once they are no longer needed for convergence.
//!
//! ## Tombstone Lifecycle
//!
//! 1. **Deletion**: Key is marked with a tombstone (logical delete)
//! 2. **Propagation**: Tombstone spreads via replication (gossip, broadcast, etc.)
//! 3. **Quiescence**: All nodes have seen the deletion
//! 4. **Cleanup**: Safe to remove tombstone and reclaim storage
//!
//! ## Safety Considerations
//!
//! - Must ensure all nodes have received tombstone before cleanup
//! - Cleanup too early → deleted keys may reappear (resurrection problem)
//! - Cleanup too late → wasted storage space
//! - Typically uses time-based or version-based heuristics
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use kvs_zoo::maintain::{TombstoneCleanup, TombstoneCleanupConfig};
//!
//! let config = TombstoneCleanupConfig {
//!     cleanup_interval_ms: 60_000,  // Check every minute
//!     tombstone_ttl_ms: 3600_000,   // Clean after 1 hour
//! };
//! let cleanup = TombstoneCleanup::new(config);
//! ```

use crate::kvs_core::KVSNode;
use crate::maintain::ReplicationStrategy;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Configuration for tombstone cleanup strategy
#[derive(Clone, Debug)]
pub struct TombstoneCleanupConfig {
    /// How often to run cleanup (in milliseconds)
    pub cleanup_interval_ms: u64,
    /// How long to wait before cleaning a tombstone (in milliseconds)
    pub tombstone_ttl_ms: u64,
}

impl Default for TombstoneCleanupConfig {
    fn default() -> Self {
        Self {
            cleanup_interval_ms: 60_000,   // 1 minute
            tombstone_ttl_ms: 3_600_000,   // 1 hour
        }
    }
}

/// Tombstone cleanup maintenance strategy
///
/// Periodically scans for tombstoned entries that are safe to garbage collect
/// and removes them to reclaim storage space. This is a pure maintenance
/// operation that doesn't affect the core replication logic.
#[derive(Clone, Debug)]
pub struct TombstoneCleanup {
    config: TombstoneCleanupConfig,
}

impl TombstoneCleanup {
    /// Create a new tombstone cleanup strategy with the given configuration
    pub fn new(config: TombstoneCleanupConfig) -> Self {
        Self { config }
    }
}

impl<V> ReplicationStrategy<V> for TombstoneCleanup {
    fn replicate_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        _local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        todo!(
            "Tombstone cleanup would periodically scan for expired tombstones \
             (older than {}ms) and remove them. Cleanup runs every {}ms.",
            self.config.tombstone_ttl_ms,
            self.config.cleanup_interval_ms
        )
    }

    fn replicate_slotted_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        _local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        todo!(
            "Slotted tombstone cleanup would track slot numbers to determine \
             when all nodes have seen a tombstone before cleaning it up."
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tombstone_cleanup_creation() {
        let cleanup = TombstoneCleanup::new(TombstoneCleanupConfig::default());
        assert_eq!(cleanup.config.cleanup_interval_ms, 60_000);
        assert_eq!(cleanup.config.tombstone_ttl_ms, 3_600_000);
    }

    #[test]
    fn test_custom_config() {
        let config = TombstoneCleanupConfig {
            cleanup_interval_ms: 5_000,
            tombstone_ttl_ms: 10_000,
        };
        let cleanup = TombstoneCleanup::new(config);
        assert_eq!(cleanup.config.cleanup_interval_ms, 5_000);
        assert_eq!(cleanup.config.tombstone_ttl_ms, 10_000);
    }
}
