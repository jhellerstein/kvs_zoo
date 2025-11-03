use crate::core::KVSNode;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use lattices::Merge;

use super::common::{BaseReplicationConfig, ReplicationCommon};

/// Configuration specific to broadcast replication protocol
#[derive(Clone, Debug)]
pub struct BroadcastReplicationConfig {
    /// Base replication configuration (timing, intervals)
    pub base: BaseReplicationConfig,

    /// Whether to batch multiple updates before broadcasting
    /// - true: Collect multiple key updates and send them together (lower message count)
    /// - false: Broadcast each key update immediately (lower latency)
    pub enable_batching: bool,

    /// Maximum time to wait before sending a batch (only used if batching is enabled)
    pub batch_timeout: std::time::Duration,

    /// Maximum number of keys to include in a single batch
    pub max_batch_size: usize,
}

impl Default for BroadcastReplicationConfig {
    fn default() -> Self {
        Self {
            base: BaseReplicationConfig::default(),
            enable_batching: false, // Default to immediate broadcasting for simplicity
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

/// Broadcast replication protocol implementation
///
/// This implements a deterministic replication strategy where updates are periodically
/// broadcast to all nodes in the cluster. While less efficient than epidemic
/// gossip in terms of message complexity, it provides stronger consistency
/// guarantees and simpler reasoning about convergence time.
///
/// Note: This is a replication protocol, not a gossip protocol. It uses
/// deterministic all-to-all broadcasting rather than probabilistic gossip.
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
    /// Simplified broadcast implementation that directly forwards PUT operations
    ///
    /// This broadcasts all local PUT operations to all cluster members.
    /// Simpler than the full broadcast protocol since PUT operations already contain values.
    pub fn handle_broadcast_simple<'a>(
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        // Get cluster member IDs for broadcast targets
        let cluster_members = ReplicationCommon::get_cluster_members(cluster);

        // Broadcast local PUT tuples to ALL cluster members (no filtering)
        let broadcast_sent = local_put_tuples
            .clone()
            .cross_product(cluster_members.assume_retries(nondet!(/** member list OK */)))
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Return received broadcast operations
        broadcast_sent
            .values()
            .assume_ordering(nondet!(/** broadcast messages unordered */))
            .assume_retries(nondet!(/** broadcast retries OK */))
    }
    /// Broadcast replication implementation
    ///
    /// ## Design
    /// - Uses LatticeKVSCore for value merging semantics
    /// - Tracks recently changed keys in a simple HashMap-based store
    /// - Periodically broadcasts all recently changed keys to ALL cluster members
    /// - Uses a time-based expiration for the "recent changes" tracking
    /// - Simpler than epidemic gossip but higher message complexity O(n²)
    ///
    /// ## Trade-offs
    /// - **Pros**: Faster convergence, simpler logic, stronger consistency
    /// - **Cons**: Higher network overhead (O(n²) vs O(n log n) for epidemic)
    ///
    /// This is suitable for smaller clusters where network bandwidth is not
    /// a primary concern but fast, predictable convergence is important.
    pub fn handle_replication<'a>(
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        changed_keys: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
        main_kvs: KeyedSingleton<String, V, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        let ticker = cluster.tick();

        // Step 1: Get cluster member IDs for replication targets
        let cluster_members = ReplicationCommon::get_cluster_members(cluster);

        // Step 2: Set up cyclic dataflow - broadcast to ALL peers (no filtering)
        let replication_sent_initial = local_put_tuples
            .clone()
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            // No filtering - send to ALL peers (broadcast)
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Incoming replication from other nodes
        let replication_received = replication_sent_initial.values();

        // Step 3: Track recently changed keys in a simple store
        // Unlike epidemic gossip, we use a simple HashMap to track recent changes
        // without tombstones - we rely on time-based expiration instead
        let recent_changes_insertions = changed_keys.map(q!(|k| (k, ())));

        let recent_changes_store = crate::lww::KVSLww::put(recent_changes_insertions);

        // Step 4: Periodic broadcast rounds - get all recently changed keys
        let recent_changes_snapshot = recent_changes_store
            .snapshot(&ticker, nondet!(/** snapshot recent changes per tick */));

        // Extract all keys that have changed recently
        let recent_keys_per_tick = recent_changes_snapshot.entries().map(q!(|(key, _)| key));

        // Sample recent keys every second for periodic broadcast
        let recent_keys_sampled = recent_keys_per_tick.all_ticks().sample_every(
            q!(std::time::Duration::from_secs(1)),
            nondet!(/** 1-second broadcast interval */),
        );

        // Step 5: Look up merged lattice values for recent keys and broadcast them
        let keys_batched =
            recent_keys_sampled.batch(&ticker, nondet!(/** batch keys for lookup */));

        let broadcast_payloads = keys_batched.map(q!(|k| (k, ()))).into_keyed();

        let broadcast_tuples = main_kvs
            .snapshot(&ticker, nondet!(/** snapshot lattice values */))
            .get_many_if_present(broadcast_payloads)
            .entries()
            .map(q!(|(k, (v, ()))| (k, v)))
            .all_ticks();

        // Broadcast to ALL cluster members (no probabilistic filtering)
        broadcast_tuples
            .cross_product(cluster_members.assume_retries(nondet!(/** member list OK */)))
            // No filtering - send to ALL peers
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Return all received replication for KVS merging
        replication_received
            .assume_ordering(nondet!(/** replication messages unordered */))
            .assume_retries(nondet!(/** replication retries OK */))
    }
}
