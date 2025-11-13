//! Unified layer flow: parent before_storage, optional leaf before_storage, processing, after_storage
//!
//! Steps:
//! 1) before_storage (parent): route Process ops to the parent Cluster
//! 2) before_storage (leaf): route ops within the parent Cluster to the target leaf (use NoLeaf for no-op)
//! 3) processing: apply ops with KVSCore, emitting (responses, applied PUT deltas)
//! 4) after_storage (parent): replicate applied PUT deltas across the cluster
//! 5) before_storage (leaf): route replicated PUTs to the target leaf and apply without responses

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::before_storage::OpDispatch;
use crate::after_storage::ReplicationStrategy;
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

/// Unified two-layer pipeline over arbitrary input items convertible into KVSOperation
pub fn layer_flow<'a, V, DParent, After, DLeaf, In>(
    parent_cluster: &Cluster<'a, KVSNode>,
    parent_before: &DParent,
    parent_after: &After,
    leaf_before: &DLeaf,
    inputs: Stream<In, Process<'a, ()>, Unbounded>,
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
    DParent: OpDispatch<V> + Clone,
    After: ReplicationStrategy<V> + Clone,
    DLeaf: OpDispatch<V> + Clone,
    In: Into<KVSOperation<V>> + 'static,
{
    // Convert inputs to bare operations
    let operations = inputs.map(q!(|x| x.into()));

    // 1) before_storage (parent)
    let parent_routed_ops = parent_before.dispatch_from_process(operations, parent_cluster);

    // 2) before_storage (leaf)
    let leaf_ops =
        leaf_before.dispatch_from_cluster(parent_routed_ops, parent_cluster, parent_cluster);

    // Ensure sequential processing at the leaf
    let leaf_ops_ordered = leaf_ops.assume_ordering(nondet!(/** sequential processing at leaf */));

    // 3) processing: produce client responses and applied PUT deltas
    let (local_responses, applied_puts) =
        crate::kvs_core::KVSCore::process_with_deltas(leaf_ops_ordered);

    // 4) after_storage (parent): replicate applied PUT deltas
    let replicated_puts = parent_after.replicate_data(parent_cluster, applied_puts);

    // 5) before_storage (leaf): route replicated PUTs, apply without responses
    let replicated_ops = replicated_puts.map(q!(|(k, v)| KVSOperation::Put(k, v)));
    let leaf_replicated_ops = leaf_before
        .dispatch_from_cluster(replicated_ops, parent_cluster, parent_cluster)
        .assume_ordering(nondet!(/** sequential apply of replicated PUTs */));
    let replicated_tagged = leaf_replicated_ops.map(q!(|op| (false, op)));
    let replicate_responses = crate::kvs_core::KVSCore::process_with_responses(replicated_tagged);

    // Merge to keep the replicate path live; replicate_responses is typically empty
    local_responses
        .interleave(replicate_responses)
        .assume_ordering(nondet!(/** client responses in leaf order */))
}
