//! Two-layer pipeline: parent routing/replication, then leaf routing, then processing
//!
//! Steps:
//! 1) before_storage (parent): route Process ops to the parent Cluster
//! 2) after_storage (parent): replicate PUT deltas within the parent Cluster
//! 3) before_storage (leaf): route both local and replicated ops again within the same Cluster
//! 4) Merge tagged ops and process with KVSCore::process_with_responses

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::before_storage::OpDispatch;
use crate::after_storage::ReplicationStrategy;
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

pub fn pipeline_two_layer_from_process<'a, V, D, M, DLeaf>(
    parent_cluster: &Cluster<'a, KVSNode>,
    parent_routing: &D,
    parent_replication: &M,
    leaf_routing: &DLeaf,
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
    DLeaf: OpDispatch<V> + Clone,
{
    // 1) before_storage (parent)
    let routed_operations = parent_routing.dispatch_from_process(operations, parent_cluster);

    // Split local vs replicate deltas at parent
    let local_ops = routed_operations.clone();
    let local_puts = routed_operations.filter_map(q!(|op| match op {
        KVSOperation::Put(k, v) => Some((k, v)),
        KVSOperation::Get(_) => None,
    }));

    // 2) after_storage (parent)
    let replicated_puts = parent_replication.replicate_data(parent_cluster, local_puts);
    let replicated_ops = replicated_puts.map(q!(|(k, v)| KVSOperation::Put(k, v)));

    // 3) before_storage (leaf): route within the same parent Cluster
    let leaf_local_ops = leaf_routing.dispatch_from_cluster(local_ops, parent_cluster, parent_cluster);
    let leaf_replicated_ops =
        leaf_routing.dispatch_from_cluster(replicated_ops, parent_cluster, parent_cluster);

    // Tag and process
    let local_tagged = leaf_local_ops.map(q!(|op| (true, op)));
    let replicated_tagged = leaf_replicated_ops.map(q!(|op| (false, op)));
    let all_tagged = local_tagged
        .interleave(replicated_tagged)
        .assume_ordering(nondet!(/** sequential processing at leaf */));

    crate::kvs_core::KVSCore::process_with_responses(all_tagged)
}

pub fn pipeline_two_layer_from_enveloped<'a, V, Meta, D, M, DLeaf>(
    parent_cluster: &Cluster<'a, KVSNode>,
    parent_routing: &D,
    parent_replication: &M,
    leaf_routing: &DLeaf,
    enveloped_operations: Stream<crate::protocol::Envelope<Meta, KVSOperation<V>>, Process<'a, ()>, Unbounded>,
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
    Meta: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
    DLeaf: OpDispatch<V> + Clone,
{
    // Unwrap envelope for parent routing
    let bare_operations = enveloped_operations.clone().map(q!(|env| env.operation));

    // 1) before_storage (parent)
    let routed_operations = parent_routing.dispatch_from_process(bare_operations, parent_cluster);

    // Split local vs replicate deltas at parent
    let local_ops = routed_operations.clone();
    let local_puts = routed_operations.filter_map(q!(|op| match op {
        KVSOperation::Put(k, v) => Some((k, v)),
        KVSOperation::Get(_) => None,
    }));

    // 2) after_storage (parent)
    let replicated_puts = parent_replication.replicate_data(parent_cluster, local_puts);
    let replicated_ops = replicated_puts.map(q!(|(k, v)| KVSOperation::Put(k, v)));

    // 3) before_storage (leaf): route within the same parent Cluster
    let leaf_local_ops = leaf_routing.dispatch_from_cluster(local_ops, parent_cluster, parent_cluster);
    let leaf_replicated_ops =
        leaf_routing.dispatch_from_cluster(replicated_ops, parent_cluster, parent_cluster);

    // Tag and process
    let local_tagged = leaf_local_ops.map(q!(|op| (true, op)));
    let replicated_tagged = leaf_replicated_ops.map(q!(|op| (false, op)));
    let all_tagged = local_tagged
        .interleave(replicated_tagged)
        .assume_ordering(nondet!(/** sequential processing at leaf */));

    crate::kvs_core::KVSCore::process_with_responses(all_tagged)
}
