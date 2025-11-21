//! Unified layer flow: parent before_storage, optional leaf before_storage, processing, after_storage
//!
//! Steps:
//! 1) before_storage (parent): route Process ops to the parent Cluster
//! 2) before_storage (leaf): route ops within the parent Cluster to the target leaf (use NoLeaf for no-op)
//! 3) processing: apply ops with KVSCore, emitting (responses, applied PUT deltas)
//! 4) after_storage (parent): replicate applied PUT deltas across the cluster
//! 5) before_storage (leaf): route replicated PUTs to the target leaf and apply without responses

use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::after_storage::ReplicationStrategy;
use crate::before_storage::Before;
use crate::kvs_core::KVSNode;
use crate::kvs_core::events::{DataEvent, MetaEvent};
use crate::protocol::KVSOperation;

/// Composite output from the cross-layer helper so background stages can
/// subscribe to the same data/meta feed without relying on legacy control
/// helpers.
pub struct CrossLayerFlowResult<'a, V> {
    pub responses: Stream<String, Cluster<'a, KVSNode>, Unbounded, TotalOrder>,
    pub data: Stream<DataEvent<V>, Cluster<'a, KVSNode>, Unbounded, TotalOrder>,
    pub meta: Stream<MetaEvent, Cluster<'a, KVSNode>, Unbounded, TotalOrder>,
}

/// Pipeline over arbitrary input items convertible into KVSOperation
/// Works across ClusterKVS<ClusterKVS<...>> layers as well as
/// ClusterKVS<KVSNode> (which is two different kinds of layers)
pub fn cross_layer_flow<'a, V, DParent, After, DLeaf, In>(
    parent_cluster: &Cluster<'a, KVSNode>,
    parent_before: &DParent,
    parent_after: &After,
    leaf_before: &DLeaf,
    inputs: Stream<In, Process<'a, ()>, Unbounded>,
) -> CrossLayerFlowResult<'a, V>
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
    DParent: Before<V> + Clone,
    After: ReplicationStrategy<V> + Clone,
    DLeaf: Before<V> + Clone,
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

    // 3) processing: client responses via minimal core + applied PUT deltas via helper
    let crate::kvs_core::CoreOutput {
        responses: local_responses,
        data: local_data,
        meta: local_meta,
    } = crate::kvs_core::KVSCore::process(leaf_ops_ordered.clone());
    let (_ops_clone, applied_puts) = crate::plumbing::extract_put_deltas(leaf_ops_ordered);

    // 4) after_storage (parent): replicate applied PUT deltas
    let replicated_puts = parent_after.replicate_data(parent_cluster, applied_puts);

    // 5) before_storage (leaf): route replicated PUTs, apply without responses
    let replicated_ops = replicated_puts.map(q!(|(k, v)| KVSOperation::Put(k, v, None)));
    let leaf_replicated_ops = leaf_before
        .dispatch_from_cluster(replicated_ops, parent_cluster, parent_cluster)
        .assume_ordering(nondet!(/** sequential apply of replicated PUTs */));
    let crate::kvs_core::CoreOutput {
        responses: replicate_responses,
        data: replicate_data,
        meta: replicate_meta,
    } = crate::kvs_core::KVSCore::process(leaf_replicated_ops);

    // Merge to keep the replicate path live; replicate_responses is typically empty
    let combined_responses = local_responses
        .interleave(replicate_responses)
        .assume_ordering::<TotalOrder>(nondet!(/** client responses in leaf order */));

    // Convert KVSResponse to String for compatibility with existing code
    let responses = combined_responses.map(q!(|response| response.to_string()));

    let data = local_data
        .interleave(replicate_data)
        .assume_ordering(nondet!(/** combined data events for background */));

    let meta = local_meta
        .interleave(replicate_meta)
        .assume_ordering(nondet!(/** combined meta events for background */));

    CrossLayerFlowResult {
        responses,
        data,
        meta,
    }
}
