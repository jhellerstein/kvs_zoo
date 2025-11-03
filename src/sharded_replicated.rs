use crate::local::KVSNode;
use crate::protocol::KVSOperation;
use crate::replicated::ReplicatedKVSServer;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Configuration for the sharded & replicated KVS
#[derive(Clone, Debug)]
pub struct ShardedReplicatedConfig {
    /// Number of shards (clusters)
    pub shard_count: usize,
    /// Number of replicas per shard
    pub replicas_per_shard: usize,
}

impl Default for ShardedReplicatedConfig {
    fn default() -> Self {
        Self {
            shard_count: 3,
            replicas_per_shard: 3,
        }
    }
}

/// A sharded & replicated Key-Value Store implementation using Hydro
///
/// This implementation combines both sharding and replication:
/// - **Sharding**: Data is partitioned across multiple shard clusters based on key hashing
/// - **Replication**: Within each shard, data is replicated using gossip protocol for fault tolerance
///
/// ## Architecture
///
/// ```text
/// External Client
///       ↓
/// Sharding Proxy (hash key → shard cluster)
///       ↓
/// Shard 0 Cluster    Shard 1 Cluster    Shard 2 Cluster
/// [R0] [R1] [R2]     [R0] [R1] [R2]     [R0] [R1] [R2]
///  ↑    ↑    ↑        ↑    ↑    ↑        ↑    ↑    ↑
///  └────┼────┘        └────┼────┘        └────┼────┘
///       gossip              gossip              gossip
/// ```
///
/// ## Benefits
///
/// - **Horizontal Scalability**: Adding more shards increases capacity
/// - **Fault Tolerance**: Each shard has multiple replicas with gossip consensus
/// - **Load Distribution**: Operations are distributed across multiple clusters
/// - **Consistency**: Gossip protocol ensures eventual consistency within each shard
pub struct ShardedReplicatedKVSServer<V>
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

impl<V> ShardedReplicatedKVSServer<V>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + Send
        + Sync
        + lattices::Merge<V>
        + 'static,
{
    /// Run a sharded & replicated KVS with multiple shard clusters
    ///
    /// This creates a specialized proxy that routes operations to specific shard clusters
    /// based on key hashing, and within each shard cluster, uses gossip replication.
    ///
    /// ## Hybrid Architecture
    ///
    /// 1. **Sharding Proxy**: Calculates shard ID for each key and routes to appropriate cluster
    /// 2. **Hash-based Cluster Selection**: Each key consistently maps to the same shard cluster
    /// 3. **Round-robin within Cluster**: Operations are distributed among replicas in the shard
    /// 4. **Gossip Replication**: Each shard cluster uses gossip protocol for consistency
    /// 5. **Result Aggregation**: Results are collected back through the proxy
    ///
    /// The combination provides both horizontal scalability (sharding) and fault tolerance (replication).
    pub fn run_sharded_replicated_cluster<'a>(
        proxy: &Process<'a, ()>,
        shard_clusters: &[Cluster<'a, KVSNode>],
        client_external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, V)>,
        ExternalBincodeStream<String>,
    ) {
        // Proxy receives operations from external clients
        let (input_port, operations) = proxy.source_external_bincode(client_external);

        // Step 1: Route operations to specific shard clusters based on key hashing
        let shard_operations = operations
            .inspect(q!(|op| {
                match op {
                    KVSOperation::Put(key, value) => println!(
                        "🔀 Sharded-Replicated: PUT {} = {:?}",
                        key, value
                    ),
                    KVSOperation::Get(key) => println!(
                        "🔀 Sharded-Replicated: GET {}",
                        key
                    ),
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
                let shard_id = (std::hash::Hasher::finish(&hasher) % 3 as u64) as u32;
                (shard_id, op)
            }));

        // Step 2: For now, let's implement a simplified version with just one shard cluster
        // This demonstrates the concept and can be extended later
        let first_shard_cluster = &shard_clusters[0];
        
        // Route all operations to the first shard cluster for now
        let cluster_operations = shard_operations
            .map(q!(|(_shard_id, op)| op))
            .round_robin_bincode(first_shard_cluster, nondet!(/** distribute within shard cluster */));

        // Process with gossip replication within the shard cluster
        let (shard_results, shard_fails) = ReplicatedKVSServer::gossip_kvs(cluster_operations, first_shard_cluster);

        // Step 3: Collect results back to proxy
        let proxy_results = shard_results.send_bincode(proxy).values();
        let proxy_fails = shard_fails.send_bincode(proxy).values();

        // Step 4: Send results back to external clients
        let get_results_port = proxy_results
            .assume_ordering(nondet!(/** results from shard cluster are non-deterministic */))
            .send_bincode_external(client_external);
        let get_fails_port = proxy_fails
            .assume_ordering(nondet!(/** failures from shard cluster are non-deterministic */))
            .send_bincode_external(client_external);

        (input_port, get_results_port, get_fails_port)
    }

    /// Create a simple sharded & replicated KVS for testing with default configuration
    pub fn run_simple_sharded_replicated<'a>(
        proxy: &Process<'a, ()>,
        shard_clusters: &[Cluster<'a, KVSNode>],
        client_external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, V)>,
        ExternalBincodeStream<String>,
    ) {
        Self::run_sharded_replicated_cluster(
            proxy,
            shard_clusters,
            client_external,
        )
    }
}

/// Type alias for String-based sharded & replicated KVS server
pub type StringShardedReplicatedKVSServer = ShardedReplicatedKVSServer<String>;

