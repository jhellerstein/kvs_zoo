//! Public plumbing helpers for KVS specs and Hydro dataflow.
//!
//! These are used by examples and tests to plumb before/after layers to the core
//! and connect external I/O, without relying on test-only server conveniences.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

// Traits are required in bounds; no direct uses here.

/// Extract (key, value) deltas for each applied PUT while also returning
/// the original operation sequence unchanged. Lightweight replacement for
/// the former KVSCore::process_with_deltas helper.
pub fn extract_put_deltas<'a, V, L>(
    operations: Stream<crate::protocol::KVSOperation<V>, L, Unbounded>,
) -> (
    Stream<crate::protocol::KVSOperation<V>, L, Unbounded>,
    Stream<(String, V), L, Unbounded>,
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
        + Send
        + Sync
        + 'static,
    L: hydro_lang::location::Location<'a> + Clone + 'a,
{
    let cloned = operations.clone();
    let deltas = operations.filter_map(q!(|op| match op {
        crate::protocol::KVSOperation::Put(k, v) => Some((k, v)),
        crate::protocol::KVSOperation::Get(_) => None,
        crate::protocol::KVSOperation::Delete(_) => None,
    }));
    (cloned, deltas)
}

/// Standalone plumbing function: binds a KVS layer specification into Hydro dataflow.
///
/// This function takes a KVS architecture (expressed as nested `KVSCluster` types),
/// creates a cluster for each layer, plumbs inter-cluster communication, and returns
/// cluster handles plus the client I/O port.
///
/// Users then assign hosts to the returned cluster handles using standard Hydro
/// deployment APIs (`.with_cluster(layers.get::<Name>(), ...)`).
pub fn plumb_kvs_dataflow<'a, V, K>(
    proxy: &Process<'a, ()>,
    client_external: &External<'a, ()>,
    flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    kvs: K,
) -> (
    crate::kvs_layer::KVSClusters<'a>,
    ExternalBincodeBidi<KVSOperation<V>, String, hydro_lang::location::external_process::Many>,
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
        + Send
        + Sync
        + 'static,
    K: crate::kvs_layer::KVSSpec<V>
        + crate::kvs_layer::KVSPlumb<V>
        + crate::kvs_layer::AfterPlumb<V>,
{
    // Create all clusters for all layers
    let mut layers = crate::kvs_layer::KVSClusters::new();
    let _entry_cluster = kvs.create_clusters(flow, &mut layers);

    // Create bidirectional external connection
    let (bidi_port, operations_stream, _membership, complete_sink) =
        proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

    // Build initial operation stream from external input
    let initial_ops = operations_stream
        .entries()
        .map(q!(|(_client_id, op)| op))
        .assume_ordering(nondet!(/** client op stream */));
    // Downward pass via before_storage chain (KVSPlumb)
    let routed_ops = kvs.plumb_from_process(&layers, initial_ops);

    // Core processing at leaf (assume total order already imposed by before_storage components)
    let tagged_routed = routed_ops.map(q!(|op| crate::protocol::Envelope::new(true, op)));
    let core_responses = crate::kvs_core::KVSCore::process(tagged_routed);

    // Upward after_storage pass: traverse replication/responders chain from leaf to root.
    let final_responses = kvs.after_responses(&layers, core_responses);

    // Send responses back to proxy (optionally stamp member id)
    let proxy_responses = final_responses.send_bincode(proxy);
    let stamp_member = std::env::var("KVS_STAMP_MEMBER")
        .map(|v| v != "0")
        .unwrap_or(false);
    let to_complete = if stamp_member {
        proxy_responses
            .entries()
            .map(q!(|(member_id, response)| (
                0u64,
                format!("[{}] {}", member_id, response)
            )))
            .into_keyed()
    } else {
        proxy_responses
            .entries()
            .map(q!(|(_member_id, response)| (0u64, response)))
            .into_keyed()
    };

    // Complete the bidirectional connection
    complete_sink.complete(to_complete);

    (layers, bidi_port)
}

// Re-export types used in the signature to minimize import churn in examples
use crate::protocol::KVSOperation;
use hydro_lang::location::external_process::ExternalBincodeBidi;
