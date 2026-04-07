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

use crate::after_storage::ReplicationStrategy;
use crate::before_storage::Before;
use crate::kvs_core::KVSNode;
use crate::kvs_core::events::DataEvent;
use crate::protocol::KVSOperation;

/// Composite output from the cross-layer helper so background stages can
/// subscribe to the same data/meta feed without relying on legacy control
/// helpers.
use hydro_lang::live_collections::stream::NoOrder;

pub struct CrossLayerFlowResult<'a, K, V> {
    pub responses: Stream<String, Cluster<'a, KVSNode>, Unbounded, NoOrder>,
    pub data: Stream<DataEvent<K, V>, Cluster<'a, KVSNode>, Unbounded, NoOrder>,
    pub meta: Stream<crate::background::MetaLattice<K>, Cluster<'a, KVSNode>, Unbounded, NoOrder>,
}

/// Pipeline over arbitrary input items convertible into KVSOperation
/// Works across ClusterKVS<ClusterKVS<...>> layers as well as
/// ClusterKVS<KVSNode> (which is two different kinds of layers)
pub fn cross_layer_flow<'a, K, V, DParent, After, DLeaf, In>(
    parent_cluster: &Cluster<'a, KVSNode>,
    parent_before: &DParent,
    parent_after: &After,
    leaf_before: &DLeaf,
    inputs: Stream<In, Process<'a, ()>, Unbounded>,
) -> CrossLayerFlowResult<'a, K, V>
where
    K: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + std::hash::Hash
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + lattices::LatticeFrom<V>
        + lattices::IsBot
        + Send
        + Sync
        + 'static,
    DParent: Before<K, V> + Clone,
    After: ReplicationStrategy<K, V> + Clone,
    DLeaf: Before<K, V> + Clone,
    In: Into<KVSOperation<K, V>> + 'static,
{
    // Convert inputs to bare operations
    let operations = inputs.map(q!(|x| x.into()));

    // 1) before_storage (parent)
    let parent_routed_ops = parent_before.dispatch_from_process(operations, parent_cluster);

    // 2) before_storage (leaf)
    let leaf_ops =
        leaf_before.dispatch_from_cluster(parent_routed_ops, parent_cluster, parent_cluster);

    // 3) processing: client responses via minimal core + applied PUT deltas via helper
    // Use coordination-free processing
    let crate::kvs_core::CoreOutput {
        responses: local_responses,
        data: local_data,
        meta: local_meta,
    } = crate::kvs_core::KVSCore::process_lattice::<K, V, _>(
        leaf_ops.clone().weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
    );
    let (_ops_clone, applied_puts) = crate::plumbing::extract_put_deltas(leaf_ops);

    // 4) after_storage (parent): replicate applied PUT deltas
    let replicated_puts = parent_after.replicate_data(parent_cluster, applied_puts);

    // 5) before_storage (leaf): route replicated PUTs, apply without responses
    let replicated_ops = replicated_puts.map(q!(|(k, v)| KVSOperation::Put(k, v, u64::MAX, None)));
    let leaf_replicated_ops =
        leaf_before.dispatch_from_cluster(replicated_ops, parent_cluster, parent_cluster);
    // Use coordination-free processing for replicated operations
    let crate::kvs_core::CoreOutput {
        responses: replicate_responses,
        data: replicate_data,
        meta: replicate_meta,
    } = crate::kvs_core::KVSCore::process_lattice::<K, V, _>(
        leaf_replicated_ops.weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
    );

    // Merge to keep the replicate path live; replicate_responses is typically empty
    // Both streams are NoOrder (coordination-free processing), so interleave is fine
    let combined_responses = local_responses.interleave(replicate_responses);

    // Convert KVSResponse to String for compatibility with existing code
    let responses = combined_responses.map(q!(|response| response.to_string()));

    // Both data streams are NoOrder (coordination-free processing), so interleave is fine
    let data = local_data.interleave(replicate_data);

    // Wrap meta events in lattice singletons for monotonic composition
    let meta =
        local_meta
            .map(q!(|ev| lattices::set_union::SetUnionHashSet::new_from([
                ev
            ])))
            .interleave(replicate_meta.map(q!(|ev| {
                lattices::set_union::SetUnionHashSet::new_from([ev])
            })));

    CrossLayerFlowResult {
        responses,
        data,
        meta,
    }
}
