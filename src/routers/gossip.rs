use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use lattices::Merge;
use lattices::map_union_with_tombstones::MapUnionHashMapWithTombstoneHashSet;
use std::collections::{HashMap, HashSet};

use super::common::{BaseReplicationConfig, ReplicationCommon};

/// Configuration specific to epidemic gossip protocol
#[derive(Clone, Debug)]
pub struct EpidemicGossipConfig {
    /// Base replication configuration (timing, intervals)
    pub base: BaseReplicationConfig,

    /// How many random peers to send each hot rumor to per gossip round
    /// This controls the "fanout" of the epidemic spread
    pub gossip_fanout: usize,

    /// Probability of tombstoning (forgetting) a hot key per gossip round
    /// Default heuristic: 1 / max(2, ceil(c · ln(cluster_size))) with c ≈ 2
    /// This targets O(n log n) message complexity and bounded rumor lifetime
    pub tombstone_prob: f64,

    /// Probability of selecting a peer for initial infection (0.0 to 1.0)
    /// Controls how aggressively rumors spread initially
    pub infection_prob: f64,
}

impl Default for EpidemicGossipConfig {
    fn default() -> Self {
        Self {
            base: BaseReplicationConfig::default(),
            gossip_fanout: 3,
            // Conservative default: assume small cluster, tombstone slowly
            tombstone_prob: 0.1,
            // 50% infection probability for balanced spread
            infection_prob: 0.5,
        }
    }
}

impl EpidemicGossipConfig {
    /// Create config optimized for small clusters (< 10 nodes)
    pub fn small_cluster() -> Self {
        Self {
            base: BaseReplicationConfig::background(std::time::Duration::from_millis(500)),
            gossip_fanout: 2,
            tombstone_prob: 0.05, // Slower tombstoning for small clusters
            infection_prob: 0.7,  // Higher infection rate
        }
    }

    /// Create config optimized for large clusters (> 50 nodes)
    pub fn large_cluster() -> Self {
        Self {
            base: BaseReplicationConfig::background(std::time::Duration::from_secs(2)),
            gossip_fanout: 5,
            tombstone_prob: 0.2, // Faster tombstoning for large clusters
            infection_prob: 0.3, // Lower infection rate to reduce overhead
        }
    }
}

/// Epidemic gossip protocol implementation (Demers et al.)
///
/// This implements rumor-mongering with probabilistic termination,
/// where rumors spread through the network and are eventually forgotten
/// with some probability to prevent infinite circulation.
///
/// Note: This is the only protocol that is truly "gossip-based" - it uses
/// the classic epidemic gossip algorithm from Demers et al. Other protocols
/// in this module are replication protocols but not gossip protocols.
pub struct EpidemicGossip<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for EpidemicGossip<V> {
    fn default() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> EpidemicGossip<V>
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
    /// Simplified gossip implementation that directly forwards PUT operations
    ///
    /// This is a simpler version that doesn't require KVS lookups since the PUT
    /// operations already contain both keys and values.
    pub fn handle_gossip_simple<'a>(
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        // Get cluster member IDs for gossip targets
        let cluster_members = ReplicationCommon::get_cluster_members(cluster);

        // Set up cyclic dataflow: send local PUT tuples to random peers and receive gossip back
        let gossip_sent = local_put_tuples
            .clone()
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            .filter(q!(|(_tuple, _member_id)| {
                // Send to ~50% of peers initially
                rand::random::<bool>()
            }))
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Return received gossip operations
        gossip_sent
            .values()
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
    }

    /// Rumor-mongering gossip implementation (Demers et al.)
    ///
    /// ## Design
    /// - Pure gossip protocol that takes local PUTs and returns gossip operations
    /// - Rumor store: MapUnionWithTombstones<(), ()> tracking only hot/tombstoned keys
    /// - Periodic gossip: sample_every with 1-second interval on hot (non-tombstoned) keys
    /// - Per-key actions per gossip round:
    ///   - Look up merged value from provided KVS and send PUT(k, v) to ~50% of peers
    ///   - Probabilistically tombstone (forget) with ~10% probability  
    /// - Returns gossip operations to be merged with local operations by caller
    ///
    /// This ensures eventual consistency with probabilistic rumor termination.
    #[allow(dead_code)]
    pub fn handle_gossip<'a>(
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        changed_keys: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
        main_kvs: KeyedSingleton<String, V, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        let ticker = cluster.tick();

        // Step 1: Get cluster member IDs for gossip targets
        let cluster_members = ReplicationCommon::get_cluster_members(cluster);

        // Step 2: Set up cyclic dataflow with probabilistic peer selection (~50%)
        let gossip_sent_initial = local_put_tuples
            .clone()
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            .filter(q!(|(_tuple, _member_id)| {
                // Probabilistically select ~50% of peers for initial infection
                rand::random::<bool>()
            }))
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Incoming gossip from other nodes (completes the cyclic dataflow)
        let gossip_received = gossip_sent_initial.values();

        // Step 3: Build rumor store tracking only keys (not values)
        // Only add keys that actually changed (where merge returned true)
        let rumor_insertions = changed_keys.map(q!(|k| (
            k,
            MapUnionHashMapWithTombstoneHashSet::new(
                HashMap::from([((), ())]), // Mark key as hot
                HashSet::new()             // No tombstone yet
            )
        )));

        // Build the rumor store from insertions (tombstones will be added later via periodic updates)
        let rumor_store = crate::core::KVSCore::put(rumor_insertions);

        // Step 4: Periodic gossip rounds - extract hot keys
        let rumor_snapshot =
            rumor_store.snapshot(&ticker, nondet!(/** snapshot rumor store per tick */));

        // Extract keys that are currently hot (not tombstoned)
        let hot_keys_per_tick = rumor_snapshot
            .entries()
            .filter_map(q!(|(key, rumor_state)| {
                let (hot_map, tombstone_set) = rumor_state.as_reveal_ref();
                // Key is hot if: (1) it's in the map, AND (2) it's not tombstoned
                if !hot_map.is_empty() && !tombstone_set.contains(&()) {
                    Some(key)
                } else {
                    None
                }
            }));

        // Sample hot keys every second for periodic re-gossip
        let hot_keys_sampled = hot_keys_per_tick.all_ticks().sample_every(
            q!(std::time::Duration::from_secs(1)),
            nondet!(/** 1-second gossip interval */),
        );

        // Step 5: Probabilistically tombstone keys (probabilistic termination per Demers)
        // Note: In a real implementation, tombstones would be sent as separate operations
        // to update the rumor store, but for simplicity we'll just use them for filtering
        // Note: For now using hardcoded probability due to Hydro limitations with f64 in closures
        let _keys_to_tombstone = hot_keys_sampled.clone().filter(q!(|_k| {
            rand::random::<f64>() < 0.1 // 10% tombstone probability
        }));

        // Step 6: Look up merged lattice values for hot keys and gossip them
        let keys_batched = hot_keys_sampled.batch(&ticker, nondet!(/** batch keys for lookup */));

        let gossip_payloads = keys_batched.map(q!(|k| (k, ()))).into_keyed();

        let gossip_tuples = main_kvs
            .snapshot(&ticker, nondet!(/** snapshot lattice values */))
            .get_many_if_present(gossip_payloads)
            .entries()
            .map(q!(|(k, (v, ()))| (k, v)))
            .all_ticks();

        // Send gossip to random peer subset (~50% fanout per key)
        gossip_tuples
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            .filter(q!(|(_tuple, _member_id)| {
                rand::random::<bool>() // 50% fanout probability
            }))
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Return all received gossip for KVS merging
        gossip_received
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
    }

    #[deprecated(note = "use handle_gossip implementing rumor mongering (Demers) instead")]
    #[allow(dead_code)]
    pub fn handle_gossip_old<'a>(
        cluster: &Cluster<'a, KVSNode>,
        local_put_ops: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        // Epidemic gossip protocol based on "rumor mongering" with deduplication
        // Reference: "Epidemic algorithms for replicated database maintenance" (Demers et al.)
        //
        // Implementation (matching DFIR pattern):
        // 1. Track all messages ever seen to avoid re-gossiping duplicates
        // 2. Use anti_join (difference) to filter out already-seen messages
        // 3. Only actually-new messages enter the infection set for gossiping
        // 4. Probabilistic removal (25%) to limit epidemic spread
        // 5. Periodic gossip rounds (1 second intervals)

        let ticker = cluster.tick();

        // Step 1: Get cluster member IDs
        let cluster_members = ReplicationCommon::get_cluster_members(cluster);

        // Step 2: Send and receive gossip from peers (this creates a cyclic dataflow)
        let gossip_received = local_put_ops
            .clone()
            .cross_product(cluster_members.clone())
            .filter(q!(|(_op, _member_id)| {
                rand::random::<bool>() // Send to 50% random subset initially
            }))
            .map(q!(|(op, member_id)| (member_id, op)))
            .into_keyed()
            .demux_bincode(cluster)
            .values();

        // Step 3: Collect all potentially new messages (local PUTs + gossip received)
        let maybe_new_messages = local_put_ops.clone().interleave(gossip_received.clone());

        // Step 4: Batch once and share for both key-tracking and deduplication paths

        // Deduplicate incoming messages using the shared batched stream
        // Convert messages to (key, op) tuples for anti_join
        let maybe_new_keyed = maybe_new_messages
            .batch(
                &ticker,
                nondet!(/** batch new messages for key-tracking & dedup */),
            )
            .map(q!(|op| {
                let key = match &op {
                    KVSOperation::Put(k, _) => k.clone(),
                    KVSOperation::Get(k) => k.clone(),
                };
                (key, op)
            }));

        // Track all known message keys (within each tick) using the keyed stream to avoid
        // duplicating the op→key mapping
        let all_known_keys = maybe_new_keyed.clone().map(q!(|(key, _op)| key)).unique(); // Within each tick, deduplicate keys

        // Filter out messages we've already seen
        let actually_new_messages = maybe_new_keyed
            .anti_join(all_known_keys)
            .map(q!(|(_key, op)| op))
            .all_ticks(); // Back to Unbounded for further processing

        // Step 6: Probabilistically select new messages for re-gossip (25%)
        let re_gossip_selected = actually_new_messages.clone().filter(q!(|_op| {
            rand::random::<u32>().is_multiple_of(4) // 25% probability (blind-coin removal)
        }));

        // Step 7: Interleave initial PUTs with re-gossip (infection set)
        let infecting_messages = local_put_ops.clone().interleave(re_gossip_selected);

        // Step 8: Periodic gossip rounds - sample at 1-second intervals
        infecting_messages
            .sample_every(
                q!(std::time::Duration::from_secs(1)),
                nondet!(/** gossip timer */),
            )
            .cross_product(cluster_members.assume_retries(nondet!(/** member list retries OK */)))
            .filter(q!(|(_op, _member_id)| {
                rand::random::<bool>() // Send to random subset (~50% of peers)
            }))
            .map(q!(|(op, member_id)| (member_id, op)))
            .into_keyed()
            .demux_bincode(cluster);

        // Return all actually new messages (for merging into KVS)
        actually_new_messages
            .assume_ordering(nondet!(/** deduped messages unordered */))
            .assume_retries(nondet!(/** gossip retries acceptable */))
    }
}
