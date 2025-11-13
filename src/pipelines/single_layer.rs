//! Single-layer pipeline: Process -> Cluster routing, replication, processing
//!
//! Steps:
//! 1) before_storage: route operations from Process to target Cluster (routing/ordering)
//! 2) Split local operations (respond) and local PUTs (replication deltas)
//! 3) after_storage: replicate PUTs and re-tag as non-responding
//! 4) Merge and process with KVSCore::process_with_responses at the target Cluster

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::before_storage::OpDispatch;
use crate::after_storage::ReplicationStrategy;
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

pub fn pipeline_single_layer_from_process<'a, V, D, M>(
    target_cluster: &Cluster<'a, KVSNode>,
    routing: &D,
    replication: &M,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
) -> Stream<String, Cluster<'a, KVSNode>, Unbounded>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
    // 1) before_storage routing to target cluster
    let routed_operations = routing.dispatch_from_process(operations, target_cluster);

    // 2) Split local vs rep deltas
    let local_tagged = routed_operations.clone().map(q!(|op| (true, op)));
    let local_puts = routed_operations.filter_map(q!(|op| match op {
        KVSOperation::Put(k, v) => Some((k, v)),
        KVSOperation::Get(_) => None,
    }));

    // 3) after_storage replication and tag as non-responding
    let replicated_puts = replication.replicate_data(target_cluster, local_puts);
    let replicated_tagged = replicated_puts.map(q!(|(k, v)| (false, KVSOperation::Put(k, v))));

    // 4) Merge and process
    let all_tagged = local_tagged
        .interleave(replicated_tagged)
        .assume_ordering(nondet!(/** sequential processing */));

    crate::kvs_core::KVSCore::process_with_responses(all_tagged)
}
