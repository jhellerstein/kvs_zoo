//! Public plumbing helpers for KVS specs and Hydro dataflow.
//!
//! These are used by examples and tests to plumb before/after layers to the core
//! and connect external I/O, without relying on test-only server conveniences.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::kvs_layer::ReplicationPlumb;

type OperationStream<K, V, L, O = hydro_lang::live_collections::stream::TotalOrder> =
    Stream<crate::protocol::KVSOperation<K, V>, L, Unbounded, O>;
type PutDeltaStream<K, V, L, O = hydro_lang::live_collections::stream::TotalOrder> =
    Stream<(K, V), L, Unbounded, O>;

// Traits are required in bounds; no direct uses here.

/// Extract (key, value) deltas for each applied PUT while also returning
/// the original operation sequence unchanged. Lightweight replacement for
/// the former KVSCore::process_with_deltas helper.
#[allow(clippy::type_complexity)]
pub fn extract_put_deltas<'a, K, V, L, O>(
    operations: OperationStream<K, V, L, O>,
) -> (OperationStream<K, V, L, O>, PutDeltaStream<K, V, L, O>)
where
    O: hydro_lang::live_collections::stream::Ordering,
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
        + Send
        + Sync
        + 'static,
    L: hydro_lang::location::Location<'a> + Clone + 'a,
{
    let cloned = operations.clone();
    let deltas = operations.filter_map(q!(|op| match op {
        crate::protocol::KVSOperation::Put(k, v, _, _) => Some((k, v)),
        crate::protocol::KVSOperation::Get(_, _, _) => None,
        crate::protocol::KVSOperation::Delete(_, _, _) => None,
    }));
    (cloned, deltas)
}

/// Macro to avoid duplicating the plumbing logic between storage backends.
///
/// This macro expands to the common dataflow plumbing logic, with the storage-specific
/// processing function injected via the `$process_expr` parameter.
macro_rules! plumb_kvs_dataflow_impl {
    ($KeyType:ty, $V:ty, $proxy:expr, $client_external:expr, $flow:expr, $kvs:expr, $process_expr:expr) => {{
        let mut kvs = $kvs;

        // Create all clusters for all layers
        let mut layers = crate::kvs_layer::KVSClusters::new();
        let _entry_cluster = kvs.create_clusters($flow, &mut layers);

        // Create bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            $proxy.bidi_external_many_bincode::<_, KVSOperation<$KeyType, $V>, String>($client_external);

        // Build initial operation stream from external input
        let initial_ops = operations_stream
            .entries()
            .map(q!(|(client_id, op)| op.with_client_id(Some(client_id))));

        // Downward pass via before_storage chain (KVSPlumb)
        let routed_ops = kvs.plumb_from_process(&layers, initial_ops);

        // Split client operations so we can extract PUT deltas for replication.
        let (client_ops, local_put_deltas) = extract_put_deltas(routed_ops);

        // Fan out PUT deltas through any configured replication layers. Replicas generate
        // operations that enter the core without triggering client responses.
        let (_pass_up, replication_ops) = kvs.replicate_puts(&layers, local_put_deltas);

        // Core processing for client-originating operations (storage-specific).
        // Preserve ordering if the KVS architecture requires linearizability,
        // otherwise downgrade to NoOrder for coordination-free processing.
        let client_ops_to_process = if kvs.requires_linearizable() {
            client_ops  // Preserve TotalOrder from ordering layers (e.g., Paxos)
        } else {
            client_ops.weakest_ordering()  // Downgrade to NoOrder for process()
        };
        let crate::kvs_core::CoreOutput {
            responses: client_responses,
            data: client_data_events,
            meta: client_meta_stream,
        } = $process_expr(client_ops_to_process);

        // Replicated operations flow through the same core path.
        // Replicated ops have client_id=None, so they won't generate responses.
        // Replication uses NoOrder (lattice merge is coordination-free)
        let crate::kvs_core::CoreOutput {
            responses: replica_responses,
            data: replica_data_events,
            meta: replica_meta_stream,
        } = $process_expr(replication_ops);

        // Responses and data are already NoOrder, so interleave is fine.
        let combined_responses = client_responses
            .interleave(replica_responses);

        let data_events = client_data_events
            .interleave(replica_data_events);

        // Wrap meta events in lattice singletons for monotonic composition
        let meta_stream = client_meta_stream
            .map(q!(|ev| lattices::set_union::SetUnionHashSet::new_from([ev])))
            .interleave(
                replica_meta_stream
                    .map(q!(|ev| lattices::set_union::SetUnionHashSet::new_from([ev])))
            );

        // Background plumbing (returns streams for potential chaining)
        let (_bg_data, _bg_meta) = kvs.plumb_background(&layers, data_events, meta_stream);

        // Send KVSResponse structs to proxy (they contain client_id)
        let proxy_responses = combined_responses.send_bincode($proxy);

        // Extract client IDs and format responses for completion
        let to_complete = proxy_responses
            .entries()
            .filter_map(q!(|(_member_id, response)| {
                response.client_id().map(|cid| (cid, response.to_string()))
            }))
            .into_keyed();

        // Complete the bidirectional connection
        complete_sink.complete(to_complete);

        (layers, bidi_port)
    }};
}

/// Standalone plumbing function: binds a KVS layer specification into Hydro dataflow.
///
/// This function takes a KVS architecture (expressed as nested `KVSCluster` types),
/// creates a cluster for each layer, plumbs inter-cluster communication, and returns
/// cluster handles plus the client I/O port.
///
/// Users then assign hosts to the returned cluster handles using standard Hydro
/// deployment APIs (`.with_cluster(layers.get::<Name>(), ...)`).
///
/// This version uses standard HashMap storage (keys can be deleted and re-added).
/// Automatically selects ordered or unordered processing based on KVS configuration.
pub fn plumb_kvs_dataflow<'a, KeyType, V, K>(
    proxy: &Process<'a, ()>,
    client_external: &External<'a, ()>,
    flow: &mut hydro_lang::compile::builder::FlowBuilder<'a>,
    kvs: K,
) -> (
    crate::kvs_layer::KVSClusters<'a>,
    ExternalBincodeBidi<
        KVSOperation<KeyType, V>,
        String,
        hydro_lang::location::external_process::Many,
    >,
)
where
    KeyType: Clone
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
    K: crate::kvs_layer::KVSSpec<V>
        + crate::kvs_layer::KVSPlumb<KeyType, V>
        + crate::kvs_layer::AfterPlumb<V>
        + ReplicationPlumb<KeyType, V>
        + crate::background::BackgroundPlumb<KeyType, V>,
{
    // Default to unordered processing
    plumb_kvs_dataflow_impl!(
        KeyType,
        V,
        proxy,
        client_external,
        flow,
        kvs,
        |ops| crate::kvs_core::KVSCore::process::<KeyType, V, _, _, _, _>(ops, q!(|| std::collections::HashMap::<KeyType, V>::new()))
    )
}



/// Standalone plumbing function with tombstone-based storage.
///
/// This variant uses `LocalHashMapFst<V>` for permanent tombstone deletion
/// instead of standard HashMap. Once a key is deleted, it cannot be resurrected
/// by subsequent PUT operations. Defaults to unordered processing.
///
/// Note: Currently restricted to String keys due to FST requirements.
pub fn plumb_kvs_dataflow_with_tombstones<'a, V, K>(
    proxy: &Process<'a, ()>,
    client_external: &External<'a, ()>,
    flow: &mut hydro_lang::compile::builder::FlowBuilder<'a>,
    kvs: K,
) -> (
    crate::kvs_layer::KVSClusters<'a>,
    ExternalBincodeBidi<
        KVSOperation<String, V>,
        String,
        hydro_lang::location::external_process::Many,
    >,
)
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
        + lattices::LatticeFrom<V>
        + lattices::IsBot
        + Send
        + Sync
        + 'static,
    K: crate::kvs_layer::KVSSpec<V>
        + crate::kvs_layer::KVSPlumb<String, V>
        + crate::kvs_layer::AfterPlumb<V>
        + ReplicationPlumb<String, V>
        + crate::background::BackgroundPlumb<String, V>,
{
    plumb_kvs_dataflow_impl!(
        String,
        V,
        proxy,
        client_external,
        flow,
        kvs,
        |ops| crate::kvs_core::KVSCore::process::<String, V, _, _, _, _>(ops, q!(|| crate::kvs_core::local_map::LocalHashMapFst::<V>::default()))
    )
}

// Re-export types used in the signature to minimize import churn in examples
use crate::protocol::KVSOperation;
use hydro_lang::location::external_process::ExternalBincodeBidi;
