//! Single-layer pipeline: Process -> Cluster routing, processing, after_storage
//!
//! Steps:
//! 1) before_storage: route operations from Process to target Cluster (routing/ordering)
//! 2) processing: apply operations with KVSCore, emitting (responses, applied PUT deltas)
//! 3) after_storage: replicate applied PUT deltas across the cluster
//! 4) apply replicated PUTs without client responses to update remote replicas

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
    // 1) before_storage: route to target cluster
    let routed_operations = routing.dispatch_from_process(operations, target_cluster);

    // Ensure sequential processing at the leaf
    let leaf_ops_ordered = routed_operations.assume_ordering(nondet!(/** sequential processing */));

    // 2) processing: produce client responses and applied PUT deltas
    let (local_responses, local_puts) = crate::kvs_core::KVSCore::process_with_deltas(leaf_ops_ordered);

    // 3) after_storage: replicate applied PUT deltas
    let replicated_puts = replication.replicate_data(target_cluster, local_puts);

    // 4) apply replicated PUTs (no client responses expected)
    let replicate_tagged = replicated_puts.map(q!(|(k, v)| (false, KVSOperation::Put(k, v))));
    let replicate_responses = crate::kvs_core::KVSCore::process_with_responses(
        replicate_tagged.assume_ordering(nondet!(/** sequential apply of replicated PUTs */)),
    );

    // Merge to keep the replicate path live; replicate_responses is typically empty
    local_responses
        .interleave(replicate_responses)
        .assume_ordering(nondet!(/** client responses in leaf order */))
}
