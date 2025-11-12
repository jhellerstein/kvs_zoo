//! Tombstone Cleanup Strategy (after-storage, native)
//!
//! Provides background garbage collection for tombstoned (deleted) entries.

use crate::after_storage::{MaintenanceAfterResponses, ReplicationStrategy};
use crate::kvs_core::KVSNode;
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
        Self { cleanup_interval_ms: 60_000, tombstone_ttl_ms: 3_600_000 }
    }
}

impl From<usize> for TombstoneCleanupConfig {
    /// Interpret usize as milliseconds for the cleanup interval; tombstone TTL stays default
    fn from(ms: usize) -> Self {
        TombstoneCleanupConfig { cleanup_interval_ms: ms as u64, ..Default::default() }
    }
}

/// Tombstone cleanup maintenance strategy
#[derive(Clone, Debug)]
pub struct TombstoneCleanup {
    #[allow(dead_code)]
    pub(crate) config: TombstoneCleanupConfig,
}

impl TombstoneCleanup {
    /// Create a new tombstone cleanup strategy with the given configuration. Accepts any Into<TombstoneCleanupConfig>
    pub fn new<C>(config: C) -> Self
    where
        C: Into<TombstoneCleanupConfig>,
    {
        Self { config: config.into() }
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
        // Placeholder: no-op emission for now; future: time-gated cleanup emissions
        local_data.filter(q!(|_kv| false))
    }

    fn replicate_slotted_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Placeholder: no slotted cleanup emitted yet
        local_slotted_data.filter(q!(|_s| false))
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
        let config = TombstoneCleanupConfig { cleanup_interval_ms: 5_000, tombstone_ttl_ms: 10_000 };
        let cleanup = TombstoneCleanup::new(config);
        assert_eq!(cleanup.config.cleanup_interval_ms, 5_000);
        assert_eq!(cleanup.config.tombstone_ttl_ms, 10_000);
    }
}
