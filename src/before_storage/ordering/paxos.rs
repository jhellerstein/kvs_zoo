//! Paxos-based operation dispatcher (before-storage)

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::before_storage::OpDispatch;
pub use crate::before_storage::ordering::paxos_core::PaxosConfig;
use crate::before_storage::ordering::paxos_core::{Acceptor, PaxosPayload, Proposer, paxos_core};
use crate::before_storage::ordering::sequence_payloads::{SequencedPayload, sequence_payloads};
use crate::protocol::KVSOperation;

#[derive(Clone)]
pub struct PaxosDispatcher<V> { pub config: PaxosConfig, _phantom: PhantomData<V> }

impl<V> PaxosDispatcher<V> {
    pub fn new() -> Self { Self { config: PaxosConfig::default(), _phantom: PhantomData } }
    pub fn with_config(config: PaxosConfig) -> Self { Self { config, _phantom: PhantomData } }

    pub fn paxos_run<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        proposers: &Cluster<'a, Proposer>,
        acceptors: &Cluster<'a, Acceptor>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, Proposer>, Unbounded>
    where V: PaxosPayload + Eq,
    {
        let (checkpoint_complete, checkpoint) = acceptors.forward_ref::<Optional<usize, _, _>>();
        let checkpoint_opt: Optional<usize, _, _> = acceptors.singleton(q!(0)).into();
        checkpoint_complete.complete(checkpoint_opt);

        let ops_at_proposers =
            operations.broadcast_bincode(proposers, nondet!(/** broadcast to proposers */));

        let (_ballot_stream, ordered_slots) = paxos_core(
            proposers,
            acceptors,
            checkpoint,
            |_ballot_stream| ops_at_proposers,
            self.config,
            nondet!(/** leader election nondeterminism */),
            nondet!(/** commit nondeterminism */),
        );

        let seq_payloads_at_proposers = ordered_slots
            .map(q!(|(seq, payload)| { crate::before_storage::ordering::sequence_payloads::SequencedPayload { seq, payload } }));

        let proposer_tick = proposers.tick();
        let (sequenced_ops, next_slot_cycle) = sequence_payloads(&proposer_tick, seq_payloads_at_proposers);

        next_slot_cycle.complete_next_tick(sequenced_ops.clone().persist().fold(
            q!(|| 0),
            q!(|next_slot, payload: SequencedPayload<_>| { *next_slot = payload.seq + 1; }),
        ));

        sequenced_ops
            .filter_map(q!(|sequenced| sequenced.payload))
            .all_ticks()
    }
}

impl<V> Default for PaxosDispatcher<V> { fn default() -> Self { Self::new() } }

impl<V> OpDispatch<V> for PaxosDispatcher<V>
where V: PaxosPayload + Eq,
{
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .enumerate()
            .map(q!(|(i, op)| (i as u32, op)))
            .broadcast_bincode(target_cluster, nondet!(/** paxos-fallback */))
            .assume_ordering(nondet!(/** fallback ordered */))
            .map(q!(|(_i, op)| op))
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        _target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations.assume_ordering(nondet!(/** passthrough cluster hop */))
    }
}

pub fn paxos_order<'a,
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<V>,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
) -> Stream<KVSOperation<V>, Cluster<'a, Proposer>, Unbounded> {
    dispatcher.paxos_run(operations, proposers, acceptors)
}

pub fn paxos_order_to_proxy<'a,
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<V>,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    proxy: &Process<'a, ()>,
) -> Stream<KVSOperation<V>, Process<'a, ()>, Unbounded> {
    dispatcher
        .paxos_run(operations, proposers, acceptors)
        .send_bincode(proxy)
        .entries()
        .map(q!(|(_member_id, op)| op))
        .assume_ordering(nondet!(/** paxos ordered at proxy */))
}

pub fn paxos_order_slotted<'a,
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<V>,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
) -> Stream<(usize, KVSOperation<V>), Cluster<'a, Proposer>, Unbounded> {
    dispatcher
        .paxos_run(operations, proposers, acceptors)
        .enumerate()
        .map(q!(|(idx, op)| (idx, op)))
}
