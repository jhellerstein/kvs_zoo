//! Broadcast replication for ordered (overwrite) values.
//!
//! Unlike `BroadcastReplication` which requires `V: Merge` for lattice
//! accumulation, this variant simply forwards writes to all replicas.
//! Suitable for architectures where ordering is established upstream
//! (e.g., Paxos) and replicas apply writes sequentially.

use crate::after_storage::{AfterResponses, ClusterCommunication, ReplicationStrategy};
use crate::kvs_core::KVSNode;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Broadcast replication with overwrite semantics (no `Merge` required).
///
/// Broadcasts each write to all cluster nodes immediately.
/// Replicas receive writes in network order — deterministic only if
/// the upstream architecture provides ordering (e.g., Paxos).
#[derive(Clone, Debug, Default)]
pub struct BroadcastOverwrite<K, V>(std::marker::PhantomData<(K, V)>);

impl<K, V> BroadcastOverwrite<K, V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<K, V> ClusterCommunication for BroadcastOverwrite<K, V> {
    fn requires_cluster_scope() -> bool {
        true
    }
}

impl<K, V> ReplicationStrategy<K, V> for BroadcastOverwrite<K, V>
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    fn replicate_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        local_data
            .broadcast_bincode(cluster, nondet!(/** broadcast writes to all replicas */))
            .values()
            .weakest_ordering()
    }
}

impl<K, V> AfterResponses for BroadcastOverwrite<K, V> {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}
