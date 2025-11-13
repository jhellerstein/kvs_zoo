//! Internal KVS server runtime wiring.
//!
//! The public user-facing API is the recursive cluster spec in `cluster_spec.rs`.
//! This module now provides only the minimal primitives required by that builder:
//! - `KVSServer` struct (type-level composition of before_storage + after_storage)
//! - Minimal runtime primitives to bind before_storage + after_storage to Hydro dataflow
//! - `run` function to bind before_storage + after_storage to Hydro dataflow
//!
//! All higher-level construction flows through `KVSCluster::build_server` and
//! typical users never touch this module directly.

use crate::before_storage::OpDispatch;
use crate::kvs_layer::{AfterWire, KVSWire};
use crate::after_storage::ReplicationStrategy;
use crate::protocol::KVSOperation;
use hydro_lang::location::external_process::ExternalBincodeBidi;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use crate::pipelines::pipeline_single_layer_from_process;

// Type aliases for server ports
type ServerBidiPort<V> =
    ExternalBincodeBidi<KVSOperation<V>, String, hydro_lang::location::external_process::Many>;
pub type ServerPorts<V> = ServerBidiPort<V>;

/// Unified KVS Server parameterized by Value, Dispatch, and Maintenance
///
/// This single struct handles all KVS architectures through composition.
pub struct KVSServer<V, D, M>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
    D: OpDispatch<V>,
    M: ReplicationStrategy<V>,
{
    _phantom: std::marker::PhantomData<(V, D, M)>,
}

/// Readability alias for the most basic server: single node, no after_storage.
///
/// Equivalent to `KVSServer<V, SingleNodeRouter, ZeroMaintenance>`.
pub type LocalKVSServer<V> =
    KVSServer<V, crate::before_storage::routing::SingleNodeRouter, crate::after_storage::ZeroMaintenance>;

// Legacy deploy helpers removed: construction flows through KVSBuilder produced by cluster spec.
impl<V, D, M> KVSServer<V, D, M>
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
    D: OpDispatch<V> + Clone + Default,
    M: ReplicationStrategy<V> + Clone + Default,
{
    /// Minimal deploy helper kept for internal tests; prefer cluster spec elsewhere.
    pub fn deploy<'a>(flow: &FlowBuilder<'a>) -> (Cluster<'a, crate::kvs_core::KVSNode>, D, M) {
        let routing = D::default();
        let replication = M::default();
        // In the simplified model, deployments are plain clusters created explicitly.
        let cluster = flow.cluster::<crate::kvs_core::KVSNode>();
        (cluster, routing, replication)
    }

    /// Convenience: deploy and run with defaults. Used by integration tests.
    pub fn deploy_and_run<'a>(
        flow: &FlowBuilder<'a>,
        proxy: &Process<'a, ()>,
        client_external: &External<'a, ()>,
    ) -> (Cluster<'a, crate::kvs_core::KVSNode>, ServerPorts<V>)
    where
        V: std::fmt::Debug + Send + Sync,
    {
        let (cluster, routing, replication) = Self::deploy(flow);
        let port = Self::run(proxy, &cluster, client_external, routing, replication);
        (cluster, port)
    }
}

impl<V, D, M> KVSServer<V, D, M>
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
    /// Run the KVS server
    ///
    /// This is the universal run method that works for all before_storage/after_storage combinations.
    pub fn run<'a>(
        proxy: &Process<'a, ()>,
        target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        client_external: &External<'a, ()>,
        routing: D,
        replication: M,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Create bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Build core single-layer pipeline (before_storage routing + after_storage replication)
        let initial_ops = operations_stream
            .entries()
            .map(q!(|(_client_id, op)| op))
            .assume_ordering(nondet!(/** client op stream */));
        let responses = pipeline_single_layer_from_process(target_cluster, &routing, &replication, initial_ops);

        // Send responses back to clients (optionally stamp member id)
        let proxy_responses = responses.send_bincode(proxy);

        // Optional stamping of member id in responses for diagnostics
        let stamp_member = std::env::var("KVS_STAMP_MEMBER").map(|v| v != "0").unwrap_or(false);
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

        bidi_port
    }
}

/// Wire a single-layer KVS from an already-ordered Process stream.
///
/// This helper mirrors the core of `run`, but instead of reading from an
/// external port it takes a Process-located stream of operations and wires:
/// - before_storage: routing/ordering via the spec's component
/// - after_storage: replication via the spec's component
/// - processing: KVSCore::process_with_responses
///
/// Returns a cluster-located response stream, suitable for `.send_bincode(proxy)`
/// and forwarding to a complete sink in the caller (examples/tests).
pub fn wire_single_layer_from_operations<'a, V, Name, D, M>(
    _proxy: &Process<'a, ()>,
    layers: &crate::kvs_layer::KVSClusters<'a>,
    kvs: &crate::kvs_layer::KVSCluster<Name, D, M, ()>,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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
    M: crate::after_storage::ReplicationStrategy<V> + Clone,
    Name: 'static,
{
    let target_cluster = layers.get::<Name>();
    pipeline_single_layer_from_process(target_cluster, &kvs.dispatch, &kvs.maintenance, operations)
}

/// Wire a two-layer KVS: a cluster layer with dispatch+maintenance, then a leaf layer with its own dispatch.
///
/// This preserves the standard replication semantics at the cluster layer and then applies
/// the leaf-level dispatch (e.g., SlotOrderEnforcer) before processing.
pub fn wire_two_layer_from_operations<'a, V, ClusterName, D, M, LeafName, DLeaf, MLeaf>(
    _proxy: &Process<'a, ()>,
    layers: &crate::kvs_layer::KVSClusters<'a>,
    kvs: &crate::kvs_layer::KVSCluster<
        ClusterName,
        D,
        M,
        crate::kvs_layer::KVSNode<LeafName, DLeaf, MLeaf>,
    >,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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
    M: crate::after_storage::ReplicationStrategy<V> + Clone,
    DLeaf: OpDispatch<V> + Clone,
    MLeaf: crate::after_storage::ReplicationStrategy<V> + Clone,
    ClusterName: 'static,
    LeafName: 'static,
{
    let target_cluster = layers.get::<ClusterName>();

    // before_storage: route operations via the cluster's routing/ordering
    let routed_operations = kvs
        .dispatch
        .dispatch_from_process(operations, target_cluster);

    // Split local and replicated paths at the cluster layer
    let local_ops = routed_operations.clone();
    let local_puts = routed_operations.filter_map(q!(|op| match op {
        KVSOperation::Put(k, v) => Some((k, v)),
        KVSOperation::Get(_) => None,
    }));

    // after_storage: replicate data in the target cluster
    let replicated_puts = kvs.maintenance.replicate_data(target_cluster, local_puts);
    let replicated_ops = replicated_puts.map(q!(|(k, v)| KVSOperation::Put(k, v)));

    // before_storage at leaf: apply leaf-level routing within the same (parent) cluster
    let leaf_local_ops =
        kvs.child
            .dispatch
            .dispatch_from_cluster(local_ops, target_cluster, target_cluster);
    let leaf_replicated_ops =
        kvs.child
            .dispatch
            .dispatch_from_cluster(replicated_ops, target_cluster, target_cluster);

    // Tag after leaf dispatch: local should respond, replicated should not
    let local_tagged = leaf_local_ops.map(q!(|op| (true, op)));
    let replicated_tagged = leaf_replicated_ops.map(q!(|op| (false, op)));

    // Merge and process at the leaf cluster
    let all_tagged = local_tagged
        .interleave(replicated_tagged)
        .assume_ordering(nondet!(/** sequential processing at leaf */));

    // Process operations with selective responses
    let responses = crate::kvs_core::KVSCore::process_with_responses(all_tagged);

    // Keep response stream cluster-located
    responses.assume_ordering(nondet!(/** responses at leaf cluster */))
}

/// Wire a two-layer KVS with enveloped operations (e.g., slotted from Paxos).
///
/// This variant takes operations wrapped in Envelope<Meta, KVSOperation<V>> from a Process stream,
/// unwraps them for routing through cluster dispatch, rewraps for leaf dispatch, and extracts
/// the bare operation for processing.
///
/// Use case: Paxos produces Stream<(usize, KVSOperation<V>), ...>. Map to Envelope<usize, ...>,
/// pass here, and slots will be preserved down to the leaf level where SlotOrderEnforcer can use them.
pub fn wire_two_layer_from_enveloped<'a, V, Meta, ClusterName, D, M, LeafName, DLeaf, MLeaf>(
    _proxy: &Process<'a, ()>,
    layers: &crate::kvs_layer::KVSClusters<'a>,
    kvs: &crate::kvs_layer::KVSCluster<
        ClusterName,
        D,
        M,
        crate::kvs_layer::KVSNode<LeafName, DLeaf, MLeaf>,
    >,
    enveloped_operations: Stream<
        crate::protocol::Envelope<Meta, KVSOperation<V>>,
        Process<'a, ()>,
        Unbounded,
    >,
) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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
    M: crate::after_storage::ReplicationStrategy<V> + Clone,
    DLeaf: OpDispatch<V> + Clone,
    MLeaf: crate::after_storage::ReplicationStrategy<V> + Clone,
    ClusterName: 'static,
    LeafName: 'static,
{
    let target_cluster = layers.get::<ClusterName>();

    // Unwrap envelope for cluster routing (routers only see bare operations)
    let bare_operations = enveloped_operations.clone().map(q!(|env| env.operation));

    // before_storage: route via cluster component
    let routed_operations = kvs
        .dispatch
        .dispatch_from_process(bare_operations, target_cluster);

    // Split for replication (cluster layer still sees bare operations)
    let local_ops = routed_operations.clone();
    let local_puts = routed_operations.filter_map(q!(|op| match op {
        KVSOperation::Put(k, v) => Some((k, v)),
        KVSOperation::Get(_) => None,
    }));

    // after_storage: replicate at cluster layer
    let replicated_puts = kvs.maintenance.replicate_data(target_cluster, local_puts);
    let replicated_ops = replicated_puts.map(q!(|(k, v)| KVSOperation::Put(k, v)));

    // TODO: Rewrap with preserved metadata for leaf dispatch
    // For now, just apply leaf routing on bare operations
    let leaf_local_ops =
        kvs.child
            .dispatch
            .dispatch_from_cluster(local_ops, target_cluster, target_cluster);
    let leaf_replicated_ops =
        kvs.child
            .dispatch
            .dispatch_from_cluster(replicated_ops, target_cluster, target_cluster);

    // Tag and process
    let local_tagged = leaf_local_ops.map(q!(|op| (true, op)));
    let replicated_tagged = leaf_replicated_ops.map(q!(|op| (false, op)));

    let all_tagged = local_tagged
        .interleave(replicated_tagged)
        .assume_ordering(nondet!(/** sequential processing at leaf */));

    let responses = crate::kvs_core::KVSCore::process_with_responses(all_tagged);

    responses.assume_ordering(nondet!(/** responses at leaf cluster */))
}

// Note: Slotted-specific wiring was removed in favor of the generic pipeline
// (KVSWire down + AfterWire up). For slotted metadata, wrap ops in Envelope<slot, op>
// and use wire_two_layer_from_enveloped, letting leaf components (e.g., SlotOrderEnforcer)
// consume the slot metadata directly.

/// Standalone wiring function: binds a KVS layer specification into Hydro dataflow.
///
/// This function takes a KVS architecture (expressed as nested `KVSCluster` types),
/// creates a cluster for each layer, wires inter-cluster communication, and returns
/// cluster handles plus the client I/O port.
///
/// Users then assign hosts to the returned cluster handles using standard Hydro
/// deployment APIs (`.with_cluster(layers.get::<Name>(), ...)`).
pub fn wire_kvs_dataflow<'a, V, K>(
    proxy: &Process<'a, ()>,
    client_external: &External<'a, ()>,
    flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    kvs: K,
) -> (crate::kvs_layer::KVSClusters<'a>, ServerPorts<V>)
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
    K: crate::kvs_layer::KVSSpec<V> + KVSWire<V> + AfterWire<V>,
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
    // Downward pass via before_storage chain (KVSWire)
    let routed_ops = kvs.wire_from_process(&layers, initial_ops);

    // Core processing at leaf (assume total order already imposed by before_storage components)
    let core_responses = crate::kvs_core::KVSCore::process(routed_ops);

    // Upward after_storage pass: traverse replication/responders chain from leaf to root.
    let final_responses = kvs.after_responses(&layers, core_responses);

    // Send responses back to proxy (optionally stamp member id)
    let proxy_responses = final_responses.send_bincode(proxy);
    let stamp_member = std::env::var("KVS_STAMP_MEMBER").map(|v| v != "0").unwrap_or(false);
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

// Paxos-specific deploy helper removed; cluster specs configure Paxos via dispatcher value.

// Nothing else lives here: construction happens via cluster specs and runtime module.

// =============================================================================
// Convenient Type Aliases for Common Configurations
// =============================================================================

/// Type aliases for common server configurations
pub mod common {
    use super::*;
    use crate::before_storage::ordering::PaxosDispatcher;
    use crate::before_storage::Pipeline;
    use crate::before_storage::routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};
    use crate::after_storage::replication::{BroadcastReplication, LogBasedDelivery, SimpleGossip};
    use crate::after_storage::NoReplication;
    use crate::values::{CausalString, LwwWrapper};

    /// Local single-node server with LWW semantics
    pub type Local<V = LwwWrapper<String>> = KVSServer<V, SingleNodeRouter, ()>;

    /// Replicated server with no replication (for testing)
    pub type Replicated<V = CausalString> = KVSServer<V, RoundRobinRouter, NoReplication>;

    /// Replicated server with gossip replication
    pub type ReplicatedGossip<V = CausalString> = KVSServer<V, RoundRobinRouter, SimpleGossip<V>>;

    /// Replicated server with broadcast replication
    pub type ReplicatedBroadcast<V = CausalString> =
        KVSServer<V, RoundRobinRouter, BroadcastReplication<V>>;

    /// Sharded local server (3 shards default)
    pub type ShardedLocal<V = LwwWrapper<String>> =
        KVSServer<V, Pipeline<ShardedRouter, SingleNodeRouter>, ()>;

    /// Sharded + Replicated with broadcast
    pub type ShardedReplicated<V = CausalString> =
        KVSServer<V, Pipeline<ShardedRouter, RoundRobinRouter>, BroadcastReplication<V>>;

    /// Linearizable server with Paxos and log-based replication
    pub type Linearizable<V = LwwWrapper<String>> =
        KVSServer<V, PaxosDispatcher<V>, LogBasedDelivery<BroadcastReplication<V>>>;
}
