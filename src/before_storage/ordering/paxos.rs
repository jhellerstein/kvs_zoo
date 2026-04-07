//! Paxos-based ordering (Before stage)

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::before_storage::{Before, RequiresLinearizable};
pub use crate::before_storage::ordering::paxos_core::PaxosConfig;
use crate::before_storage::ordering::paxos_core::{PaxosPayload, paxos_core};
use crate::before_storage::ordering::sequence_payloads::sequence_payloads;
use crate::protocol::KVSOperation;

#[derive(Clone)]
pub struct PaxosDispatcher<K, V> {
    pub config: PaxosConfig,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> PaxosDispatcher<K, V> {
    pub fn new() -> Self {
        Self {
            config: PaxosConfig::default(),
            _phantom: PhantomData,
        }
    }
    pub fn with_config(config: PaxosConfig) -> Self {
        Self {
            config,
            _phantom: PhantomData,
        }
    }

    pub fn paxos_run<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
        proposers: &Cluster<'a, crate::kvs_core::KVSNode>,
        acceptors: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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
        V: PaxosPayload + Eq,
    {
        let (checkpoint_complete, checkpoint) = acceptors.forward_ref::<Optional<usize, _, _>>();
        let checkpoint_opt: Optional<usize, _, _> = acceptors.singleton(q!(0)).into();
        checkpoint_complete.complete(checkpoint_opt);

        let ops_at_proposers = operations
            .broadcast_bincode(proposers, nondet!(/** broadcast to proposers */));

        let (_ballot_stream, ordered_slots) = paxos_core(
            proposers,
            acceptors,
            checkpoint,
            |_ballot_stream| ops_at_proposers,
            self.config,
            nondet!(/** leader election nondeterminism */),
            nondet!(/** commit nondeterminism */),
        );

        let seq_payloads_at_proposers = ordered_slots.map(q!(|(seq, payload)| {
            crate::before_storage::ordering::sequence_payloads::SequencedPayload { seq, payload }
        }));

        let proposer_tick = proposers.tick();
        let (sequenced_ops, next_slot_cycle) =
            sequence_payloads(&proposer_tick, seq_payloads_at_proposers);

        // Use type inference inside q! closure to avoid generic parameter annotation issues.
        // The macro struggles with explicit generic types like SequencedPayload<_>.
        next_slot_cycle.complete_next_tick(sequenced_ops.clone().across_ticks(|s| s.fold(
            q!(|| 0),
            q!(|next_slot, sequenced| {
                *next_slot = sequenced.seq + 1;
            }, commutative = manual_proof!(/** max seq is commutative */)),
        )));

        sequenced_ops
            .filter_map(q!(|sequenced| sequenced.payload))
            .all_ticks()
    }
}

impl<K, V> Default for PaxosDispatcher<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

// Paxos establishes total order and requires linearizable processing
impl<K, V> RequiresLinearizable for PaxosDispatcher<K, V> {
    fn requires_linearizable() -> bool {
        true
    }
}

impl<K, V> Before<K, V> for PaxosDispatcher<K, V>
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    V: PaxosPayload + Eq,
{
    fn dispatch_from_process<'a, O>(
        &self,
        _operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
        _target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<
        KVSOperation<K, V>,
        Cluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        panic!(
            "PaxosDispatcher::dispatch_from_process should never be called. \
             Use dispatch_from_process_with_layers to access proposer/acceptor clusters."
        )
    }

    fn dispatch_from_process_with_layers<'a, Name: 'static, O>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
        target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<
        KVSOperation<K, V>,
        Cluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
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
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
    {
        // Look up role-specific clusters for this layer (stored as KVSNode-tagged clusters).
        let proposers = layers.get_role::<Name, crate::before_storage::ordering::Proposer>();
        let acceptors = layers.get_role::<Name, crate::before_storage::ordering::Acceptor>();

        // Run Paxos to impose a total order at the proposers cluster.
        let ordered_at_proposers = self.paxos_run(operations, proposers, acceptors);

        // Deliver ordered operations to the target KVS cluster for further processing.
        ordered_at_proposers
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw_id(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
            .values()
    }

    fn dispatch_from_cluster_with_layers<'a, Name: 'static, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded, O>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
    ) -> Stream<
        KVSOperation<K, V>,
        Cluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // For inter-cluster hops, network operations produce NoOrder
        self.dispatch_from_cluster(operations, source_cluster, target_cluster)
    }

    fn register_role_clusters<'a, Name: 'static>(
        &self,
        flow: &mut hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) {
        // Create role clusters as KVSNode-tagged so they can be stored in KVSClusters
        let proposers = flow.cluster::<crate::kvs_core::KVSNode>();
        let acceptors = flow.cluster::<crate::kvs_core::KVSNode>();
        layers.insert_role::<Name, crate::before_storage::ordering::Proposer>(proposers);
        layers.insert_role::<Name, crate::before_storage::ordering::Acceptor>(acceptors);
    }

    fn dispatch_from_cluster<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded, O>,
        _source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        _target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<
        KVSOperation<K, V>,
        Cluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations.weakest_ordering()
    }
}

pub fn paxos_order<
    'a,
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
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<K, V>,
    operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, crate::kvs_core::KVSNode>,
    acceptors: &Cluster<'a, crate::kvs_core::KVSNode>,
) -> Stream<KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded> {
    dispatcher.paxos_run(operations, proposers, acceptors)
}

pub fn paxos_order_to_proxy<
    'a,
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
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<K, V>,
    operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, crate::kvs_core::KVSNode>,
    acceptors: &Cluster<'a, crate::kvs_core::KVSNode>,
    proxy: &Process<'a, ()>,
) -> Stream<
    KVSOperation<K, V>,
    Process<'a, ()>,
    Unbounded,
    hydro_lang::live_collections::stream::NoOrder,
> {
    dispatcher
        .paxos_run(operations, proposers, acceptors)
        .send_bincode(proxy)
        .entries()
        .map(q!(|(_member_id, op)| op))
        .weakest_ordering()
}

pub fn paxos_order_slotted<
    'a,
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
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<K, V>,
    operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, crate::kvs_core::KVSNode>,
    acceptors: &Cluster<'a, crate::kvs_core::KVSNode>,
) -> Stream<(usize, KVSOperation<K, V>), Cluster<'a, crate::kvs_core::KVSNode>, Unbounded> {
    dispatcher
        .paxos_run(operations, proposers, acceptors)
        .enumerate()
        .map(q!(|(idx, op)| (idx, op)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::KVSOperation;

    #[test]
    #[should_panic(expected = "dispatch_from_process should never be called")]
    fn test_paxos_dispatch_from_process_panics() {
        let dispatcher = PaxosDispatcher::<String, String>::new();
        let flow = hydro_lang::compile::builder::FlowBuilder::new();
        let process = flow.process::<()>();
        let cluster = flow.cluster::<crate::kvs_core::KVSNode>();

        // Create a dummy operation stream
        let ops = process.source_iter(q!(vec![
            KVSOperation::Get("key".to_string(), 1, None)
        ]));

        // This should panic because PaxosDispatcher requires dispatch_from_process_with_layers
        let _result = dispatcher.dispatch_from_process(ops, &cluster);
    }
}
