use crate::local::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use lattices::map_union_with_tombstones::MapUnionHashMapWithTombstoneHashSet;
use lattices::Merge;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Gossip message containing buffered PUT operations
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GossipMessage<V> {
    /// Buffered PUT operations to be gossiped
    pub puts: Vec<(String, V)>,
    /// Timestamp for ordering (simple counter for now)
    pub timestamp: u64,
    /// Node ID that originated this gossip message (for debugging)
    pub sender_id: u32,
}

/// Configuration for the rumor-mongering gossip protocol (Demers et al.)
#[derive(Clone, Debug)]
pub struct GossipConfig {
    /// How often to run gossip rounds (periodic sampling interval)
    pub gossip_interval: std::time::Duration,
    
    /// How many random peers to send each hot rumor to per gossip round
    pub gossip_fanout: usize,
    
    /// Probability of tombstoning (forgetting) a hot key per gossip round
    /// Default heuristic: 1 / max(2, ceil(c · ln(cluster_size))) with c ≈ 2
    /// This targets O(n log n) message complexity and bounded rumor lifetime
    pub tombstone_prob: f64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: std::time::Duration::from_secs(1),
            gossip_fanout: 3,
            // Conservative default: assume small cluster, tombstone slowly
            tombstone_prob: 0.1,
        }
    }
}

/// Replicated KVS server using all-to-all gossip protocol
pub struct ReplicatedKVSServer<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for ReplicatedKVSServer<V> {
    fn default() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> ReplicatedKVSServer<V>
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
    /// Run a replicated KVS cluster using all-to-all gossip protocol with external proxy
    ///
    /// ## Architecture
    ///
    /// 1. **Thin Proxy**: Minimal intermediary for external communication
    ///                    (Temporary workaround for the fact that Cluster can't yet directly communicate with External)
    /// 2. **Epidemic Gossip**: Each node uses probabilistic gossip to disseminate PUTs to random peer subsets with deduplication
    /// 3. **Local Processing**: Each node processes its local PUTs/GETs immediately as in the local KVS
    /// 4. **Gossip Reception**: Nodes receive gossip messages from other nodes, treat as additional PUTS
    
    /// The wrapper function that sets up the replicated KVS
    pub fn run_replicated_kvs<'a>(
        proxy: &Process<'a, ()>,
        cluster: &Cluster<'a, KVSNode>,
        client_external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, V), NoOrder>,
        ExternalBincodeStream<String, NoOrder>,
    ) {
        // Proxy receives operations from external clients (thin intermediary)
        let (input_port, operations) = proxy.source_external_bincode(client_external);

        // Proxy forwards each operation to one cluster member for processing (round-robin)
        let cluster_operations =
            operations.round_robin_bincode(cluster, nondet!(/** distribute operations */));

        // At each cluster member, handle any forwarded operations
        let (get_results, get_fails) = Self::gossip_kvs(cluster_operations, cluster);

        // Collect results back to proxy (thin intermediary)
        let proxy_results = get_results.send_bincode(proxy).values();
        let proxy_fails = get_fails.send_bincode(proxy).values();

        // Send results back to external clients
        let get_results_port = proxy_results
            .send_bincode_external(client_external);
        let get_fails_port = proxy_fails
            // .assume_ordering(nondet!(/** failures from cluster are non-deterministic */))
            .send_bincode_external(client_external);

        (input_port, get_results_port, get_fails_port)
    }

    /// Core gossip-based KVS implementation
    ///
    /// This implements an epidemic gossip protocol where:
    /// - Each PUT operation is gossiped probabilistically to random peer subsets
    /// - Deduplication prevents re-gossiping messages already seen (anti-join pattern)
    /// - Probabilistic removal (25%) limits epidemic spread ("blind-coin" removal)
    /// - Local and gossip PUTs are merged using `chain` for eventual consistency
    /// - GET operations read from the merged state (local + gossip)
    ///
    /// **Gossip Mechanism**: Implements "rumor mongering" epidemic protocol with
    /// periodic gossip rounds (1-second intervals), message deduplication, and
    /// probabilistic peer selection for efficient eventual consistency.
    /// Uses LatticeKVSCore so values have lattice merge semantics.
    pub fn gossip_kvs<'a>(
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> (
        Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) {
        let ticker = cluster.tick();

        // Step 1: Split operations to get PUT operations for gossip
        let (local_put_ops, _gets) = crate::lattice_core::LatticeKVSCore::demux_ops(operations.clone());

        // Step 2: Handle gossip - this also builds the lattice KVS internally
        let gossip_operations = Self::handle_gossip(cluster, local_put_ops);

        // Step 3: Interleave local and gossip operations
        let merged_operations = operations
            .clone()
            .interleave(gossip_operations);

        // Step 4: Use LatticeKVSCore::put to build the final KVS from merged operations
        let (final_kvs, _changed_keys) = crate::lattice_core::LatticeKVSCore::put(merged_operations);

        // Step 5: Handle GETs using the merged lattice KVS
        crate::lattice_core::LatticeKVSCore::get(
            operations.batch(&ticker, nondet!(/** batch gets for efficiency */)),
            final_kvs.snapshot(&ticker, nondet!(/** snapshot for gets */)),
        )
    }

    #[deprecated(note = "use handle_gossip implementing rumor mongering (Demers) instead")]
    #[allow(dead_code)]
    fn handle_gossip_old<'a>(
        cluster: &Cluster<'a, KVSNode>, 
        local_put_ops: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
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
        let cluster_members = cluster
            .source_cluster_members(cluster)
            .map_with_key(q!(|(member_id, _event)| member_id))
            .values();
        
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
            .batch(&ticker, nondet!(/** batch new messages for key-tracking & dedup */))
            .map(q!(|op| {
                let key = match &op {
                    KVSOperation::Put(k, _) => k.clone(),
                    KVSOperation::Get(k) => k.clone(),
                };
                (key, op)
            }));

        // Track all known message keys (within each tick) using the keyed stream to avoid
        // duplicating the op→key mapping
        let all_known_keys = maybe_new_keyed
            .clone()
            .map(q!(|(key, _op)| key))
            .unique(); // Within each tick, deduplicate keys
        
        // Filter out messages we've already seen
        let actually_new_messages = maybe_new_keyed
            .anti_join(all_known_keys)
            .map(q!(|(_key, op)| op))
            .all_ticks(); // Back to Unbounded for further processing
        
        // Step 6: Probabilistically select new messages for re-gossip (25%)
        let re_gossip_selected = actually_new_messages
            .clone()
            .filter(q!(|_op| {
                rand::random::<u32>() % 4 == 0 // 25% probability (blind-coin removal)
            }));
        
        // Step 7: Interleave initial PUTs with re-gossip (infection set)
        let infecting_messages = local_put_ops
            .clone()
            .interleave(re_gossip_selected);
        
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

    /// Rumor-mongering gossip implementation (Demers et al.)
    ///
    /// ## Design
    /// - Uses LatticeKVSCore (not vanilla KVSCore) so values have lattice merge semantics
    /// - Rumor store: MapUnionWithTombstones<(), ()> tracking only hot/tombstoned keys
    ///   - Keys are added to rumor store only when LatticeKVSCore::put returns true (value changed)
    ///   - This optimization avoids re-gossiping when redundant PUTs arrive
    /// - When gossiping: pull merged lattice values from the LatticeKVSCore store
    ///   - Gossips the max of all messages seen for recently-updated keys (not raw messages)
    /// - Periodic gossip: sample_every with 1-second interval on hot (non-tombstoned) keys
    /// - Per-key actions per gossip round:
    ///   - Look up merged value from LatticeKVS and send PUT(k, v) to ~50% of peers
    ///   - Probabilistically tombstone (forget) with ~10% probability  
    /// - Incoming gossip: returned directly for merging into the main LatticeKVS
    ///
    /// This ensures eventual consistency with probabilistic rumor termination.
    fn handle_gossip<'a>(
        cluster: &Cluster<'a, KVSNode>, 
        local_put_ops: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        let ticker = cluster.tick();
        
        // Step 1: Get cluster member IDs for gossip targets
        let cluster_members = cluster
            .source_cluster_members(cluster)
            .map_with_key(q!(|(member_id, _event)| member_id))
            .values();
        
        // Step 2: Set up cyclic dataflow: send local PUTs to random peers and receive gossip
        // This creates the initial infection and feedback loop for rumor propagation
        let gossip_sent_initial = local_put_ops
            .clone()
            .cross_product(cluster_members.clone().assume_retries(nondet!(/** member list OK */)))
            .filter(q!(|(_op, _member_id)| {
                // Probabilistically select ~50% of peers for initial infection
                rand::random::<bool>()
            }))
            .map(q!(|(op, member_id)| (member_id, op)))
            .into_keyed()
            .demux_bincode(cluster);
        
        // Incoming gossip from other nodes (completes the cyclic dataflow)
        let gossip_received = gossip_sent_initial.values();
        
        // Step 3: Build lattice KVS that merges all PUTs (local + gossip)
        // Returns both the store and a stream of keys that changed (optimization!)
        let all_put_ops = local_put_ops
            .clone()
            .interleave(gossip_received.clone());
        
        let (lattice_kvs, changed_keys) = crate::lattice_core::LatticeKVSCore::put(all_put_ops);
        
        // Step 4: Build rumor store tracking only keys (not values)
        // Only add keys that actually changed (where merge returned true)
        // Value type: MapUnionWithTombstones<HashMap<(), ()>, HashSet<()>>
        let rumor_insertions = changed_keys
            .map(q!(|k| KVSOperation::Put(k, MapUnionHashMapWithTombstoneHashSet::new(
                HashMap::from([((), ())]), // Mark key as hot
                HashSet::new() // No tombstone yet
            ))));
        
        let (rumor_store, _) = crate::lattice_core::LatticeKVSCore::put(rumor_insertions);
        
        // Step 5: Periodic gossip rounds - extract hot keys
        let rumor_snapshot = rumor_store
            .snapshot(&ticker, nondet!(/** snapshot rumor store per tick */));
        
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
        let hot_keys_sampled = hot_keys_per_tick
            .all_ticks()
            .sample_every(
                q!(std::time::Duration::from_secs(1)),
                nondet!(/** 1-second gossip interval */),
            );
        
        // Step 6: Probabilistically tombstone keys (probabilistic termination per Demers)
        let keys_to_tombstone = hot_keys_sampled
            .clone()
            .filter(q!(|_k| {
                rand::random::<f64>() < 0.1 // 10% tombstone probability
            }));
        
        // Insert tombstones into rumor store
        let tombstone_insertions = keys_to_tombstone
            .map(q!(|k| KVSOperation::Put(k, MapUnionHashMapWithTombstoneHashSet::new(
                HashMap::<(), ()>::new(), // Empty map side
                HashSet::from_iter([()])  // Insert into tombstone side = mark as forgotten
            ))))
            .assume_retries(nondet!(/** tombstone insertions can retry */));
        
        let (_, _) = crate::lattice_core::LatticeKVSCore::put(tombstone_insertions);
        
        // Step 7: Look up merged lattice values for hot keys and gossip them
        // Batch hot keys and look up their current merged values from the lattice KVS
        let keys_batched = hot_keys_sampled
            .batch(&ticker, nondet!(/** batch keys for lookup */))
            .map(q!(|k| (k, ())))
            .into_keyed();
        
        let gossip_payloads = lattice_kvs
            .snapshot(&ticker, nondet!(/** snapshot lattice values */))
            .get_many_if_present(keys_batched)
            .entries()
            .map(q!(|(k, (v, ()))| KVSOperation::Put(k, v)))
            .all_ticks();
        
        // Send gossip to random peer subset (~50% fanout per key)
        gossip_payloads
            .cross_product(cluster_members.clone().assume_retries(nondet!(/** member list OK */)))
            .filter(q!(|(_op, _member_id)| {
                rand::random::<bool>() // 50% fanout probability
            }))
            .map(q!(|(op, member_id)| (member_id, op)))
            .into_keyed()
            .demux_bincode(cluster);
        
        // Return all received gossip for KVS merging
        gossip_received
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
    }
}

/// Type alias for String-based replicated KVS server
pub type StringReplicatedKVSServer = ReplicatedKVSServer<String>;

