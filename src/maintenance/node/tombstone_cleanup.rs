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
//! use kvs_zoo::maintenance::{TombstoneCleanup, TombstoneCleanupConfig};
//!
//! let config = TombstoneCleanupConfig {
//!     cleanup_interval_ms: 60_000,  // Check every minute
//!     tombstone_ttl_ms: 3600_000,   // Clean after 1 hour
//! };
//! let cleanup = TombstoneCleanup::new(config);
//! ```

use crate::kvs_core::KVSNode;
use crate::maintenance::ReplicationStrategy;
use crate::maintenance::MaintenanceAfterResponses;
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
            cleanup_interval_ms: 60_000, // 1 minute
            tombstone_ttl_ms: 3_600_000, // 1 hour
        }
    }
}

impl From<usize> for TombstoneCleanupConfig {
    /// Interpret usize as milliseconds for the cleanup interval; tombstone TTL stays default
    fn from(ms: usize) -> Self {
        TombstoneCleanupConfig {
            cleanup_interval_ms: ms as u64,
            ..Default::default()
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
    #[allow(dead_code)]
    config: TombstoneCleanupConfig,
}

impl TombstoneCleanup {
    /// Create a new tombstone cleanup strategy with the given configuration. Accepts any Into<TombstoneCleanupConfig>
    pub fn new<C>(config: C) -> Self
    where
        C: Into<TombstoneCleanupConfig>,
    {
        Self {
            config: config.into(),
        }
    }
}

impl<V> ReplicationStrategy<V> for TombstoneCleanup {
    fn replicate_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Interval gating: only consider cleanup when an input PUT arrives AND
        // the configured cleanup interval has elapsed since the last gated item.
        // We don't yet have a Duration-aware sampling combinator in this scope.
        // For now, return an empty stream (same semantics as before) until a
        // time-based gating primitive is introduced for node-level maintenance.
        local_data.filter(q!(|_kv| false))
    }

    fn replicate_slotted_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        _local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Placeholder: no slotted cleanup emitted yet
        _local_slotted_data.filter(q!(|_s| false))
    }
}

// Upward pass: tombstone cleanup does not alter responses
impl MaintenanceAfterResponses for TombstoneCleanup {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
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
