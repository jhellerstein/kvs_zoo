use crate::core::KVSCore;
use crate::local::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::location::MemberId;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A sharded Key-Value Store implementation using Hydro
///
/// This implementation partitions data across multiple cluster nodes based on key hashing.
/// Each key is consistently routed to the same shard, providing horizontal scalability.
///
/// ## Sharding Strategy
///
/// 1. **Consistent Hashing**: Keys are hashed and modulo'd by shard count
/// 2. **Specialized Proxy**: Proxy calculates shard for each key and unicasts to specific cluster member
/// 3. **Independent Shards**: Each shard maintains its own local KVS state
/// 4. **No Cross-Shard Communication**: Shards operate independently for maximum performance
///
/// ## Architecture
///
/// ```text
/// External Client
///       ↓
/// Sharding Proxy (hash key → shard ID)
///       ↓
/// Shard 0  Shard 1  Shard 2  ...
/// [KVS]    [KVS]    [KVS]
/// ```
pub struct ShardedKVSServer<V>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
{
    _phantom: std::marker::PhantomData<V>,
}

/// Standalone function for shard calculation (can be used in q!() macros)
pub fn calculate_shard_id(key: &str, shard_count: usize) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % shard_count as u64) as u32
}

impl<V> ShardedKVSServer<V>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + 'static,
{
    /// Run a sharded KVS cluster with hash-based key routing
    ///
    /// This creates a specialized sharding proxy that routes operations to specific cluster members
    /// based on key hashing, and a cluster of independent KVS shards.
    ///
    /// ## Sharding Architecture
    ///
    /// 1. **Sharding Proxy**: Calculates shard ID for each key and unicasts to specific cluster member
    /// 2. **Hash-based Routing**: Each key consistently maps to the same shard
    /// 3. **Unicast Distribution**: Operations go directly to the responsible shard (no round-robin)
    /// 4. **Independent Shards**: Each shard maintains its own KVS state independently
    /// 5. **Result Aggregation**: Results are collected back through the proxy
    ///
    /// The sharding provides horizontal scalability - adding more shards increases capacity.
    pub fn run_sharded_cluster<'a>(
        proxy: &Process<'a, ()>,
        cluster: &Cluster<'a, KVSNode>,
        client_external: &External<'a, ()>,
        shard_count: usize,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, V)>,
        ExternalBincodeStream<String>,
    ) {
        // Proxy receives operations from external clients
        let (input_port, operations) = proxy.source_external_bincode(client_external);

        // Step 1: Route operations to specific shards based on key hashing
        let shard_operations = operations
            .inspect(q!(|op| {
                match op {
                    KVSOperation::Put(key, value) => {
                        println!("🔀 Sharding: PUT {} = {:?}", key, value)
                    }
                    KVSOperation::Get(key) => println!("🔀 Sharding: GET {}", key),
                }
            }))
            .map(q!(move |op| {
                let key = match &op {
                    KVSOperation::Put(key, _) => key,
                    KVSOperation::Get(key) => key,
                };
                // Inline hash calculation to avoid function call in q!()
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::hash::Hash::hash(key, &mut hasher);
                let shard_id = (std::hash::Hasher::finish(&hasher) % shard_count as u64) as u32;
                (MemberId::from_raw(shard_id), op)
            }));

        // Step 2: Route operations to specific cluster members based on shard ID
        // Convert to keyed stream and send to specific cluster members
        let cluster_operations = shard_operations
            .into_keyed() // Convert (shard_id, op) to KeyedStream<u32, KVSOperation<V>>
            .demux_bincode(cluster); // Route to specific cluster members based on key

        // Step 3: Process operations in each shard independently
        let (shard_results, shard_fails) =
            Self::process_shard_operations(cluster_operations, cluster, shard_count);

        // Step 4: Collect results back to proxy
        let proxy_results = shard_results.send_bincode(proxy).values();
        let proxy_fails = shard_fails.send_bincode(proxy).values();

        // Step 5: Send results back to external clients
        let get_results_port = proxy_results
            .assume_ordering(nondet!(/** results from shards are non-deterministic */))
            .send_bincode_external(client_external);
        let get_fails_port = proxy_fails
            .assume_ordering(nondet!(/** failures from shards are non-deterministic */))
            .send_bincode_external(client_external);

        (input_port, get_results_port, get_fails_port)
    }

    /// Process operations within each shard independently
    ///
    /// Each cluster member acts as a shard and only processes operations for keys that hash to its shard ID.
    /// This provides true horizontal partitioning with no cross-shard communication.
    fn process_shard_operations<'a>(
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
        _shard_count: usize,
    ) -> (
        Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) {
        let ticker = cluster.tick();

        // Each cluster member processes all operations it receives (proxy handles sharding)
        let my_operations = operations;

        // Use KVSCore helper to split operations and build this shard's KVS
        let shard_kvs = KVSCore::put(my_operations.clone());

        // Query this shard's local KVS for GET operations (symmetric to PUT handling)
        KVSCore::get(
            my_operations.batch(&ticker, nondet!(/** batch gets for efficiency */)),
            shard_kvs.snapshot(&ticker, nondet!(/** snapshot for gets */)),
        )
    }

    /// Create a simple sharded KVS for testing with 3 shards
    pub fn run_simple_sharded<'a>(
        proxy: &Process<'a, ()>,
        cluster: &Cluster<'a, KVSNode>,
        client_external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, V)>,
        ExternalBincodeStream<String>,
    ) {
        Self::run_sharded_cluster(proxy, cluster, client_external, 3)
    }
}

/// Type alias for String-based sharded KVS server
pub type StringShardedKVSServer = ShardedKVSServer<String>;

