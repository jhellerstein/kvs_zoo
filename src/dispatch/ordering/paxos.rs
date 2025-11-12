//! Paxos-based operation dispatcher for total ordering
//!
//! This module provides a Paxos consensus dispatcher that ensures all operations
//! are applied in a globally consistent order across all replicas, providing
//! linearizability guarantees for the KVS.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::dispatch::OpDispatch;
pub use crate::dispatch::ordering::paxos_core::PaxosConfig;
use crate::dispatch::ordering::paxos_core::{Acceptor, PaxosPayload, Proposer, paxos_core};
use crate::dispatch::ordering::sequence_payloads::{SequencedPayload, sequence_payloads};
use crate::protocol::KVSOperation;

// Dedicated wrapper that pairs a dispatcher with externally created proposer/acceptor clusters.
// This keeps Paxos-specific cluster knowledge out of generic infrastructure.
// NOTE: We do NOT store proposer/acceptor clusters inside the dispatcher to avoid
// lifetime entanglement. Instead, the example constructs them and calls
// `paxos_order` below to impose total order before feeding into the KVS layer.

/// Paxos dispatcher that provides total ordering of operations
///
/// This dispatcher uses the Paxos consensus algorithm to ensure that all operations
/// are totally ordered across all nodes in the cluster, providing linearizability.
///
/// Note: To use real Paxos, call `dispatch_with_paxos` directly with proposer/acceptor clusters.
/// The OpDispatch trait implementation provides a fallback single-node ordering for basic testing.
#[derive(Clone)]
pub struct PaxosDispatcher<V> {
    pub config: PaxosConfig,
    _phantom: PhantomData<V>,
}

impl<V> PaxosDispatcher<V> {
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

    /// Internal: run full Paxos and return totally ordered operations located at the proposers cluster.
    ///
    /// This function is intentionally independent of any KVS cluster types. It takes client operations
    /// at a process, runs Paxos using the supplied proposer/acceptor clusters, and produces an ordered
    /// stream of operations at the proposers cluster. Callers are responsible for routing these ordered
    /// operations to their own target cluster (e.g., a KVS cluster) using their dispatch layer.
    pub fn paxos_run<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        proposers: &Cluster<'a, Proposer>,
        acceptors: &Cluster<'a, Acceptor>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, Proposer>, Unbounded>
    where
        V: PaxosPayload + Eq,
    {
        // Create empty checkpoint (no checkpointing). Complete immediately with empty optional to avoid drop panic.
        let (checkpoint_complete, checkpoint) = acceptors.forward_ref::<Optional<usize, _, _>>();
        // Provide a trivial checkpoint value (slot 0) to satisfy the forward reference.
        let checkpoint_opt: Optional<usize, _, _> = acceptors.singleton(q!(0)).into();
        checkpoint_complete.complete(checkpoint_opt);

        // Send operations to all proposers
        let ops_at_proposers =
            operations.broadcast_bincode(proposers, nondet!(/** broadcast to proposers */));

        // Run Paxos consensus
        let (_ballot_stream, ordered_slots) = paxos_core(
            proposers,
            acceptors,
            checkpoint,
            |_ballot_stream| ops_at_proposers,
            self.config,
            nondet!(/** leader election nondeterminism */),
            nondet!(/** commit nondeterminism */),
        );

        // Convert to SequencedPayload at proposers; do not broadcast to any external cluster here
        let seq_payloads_at_proposers = ordered_slots
            .map(q!(|(seq, payload)| {
                crate::dispatch::ordering::sequence_payloads::SequencedPayload { seq, payload }
            }));

        // Use sequence_payloads to handle holes and enforce total order at proposers
        let proposer_tick = proposers.tick();
        let (sequenced_ops, next_slot_cycle) = sequence_payloads(&proposer_tick, seq_payloads_at_proposers);

        // Close the cycle - track next expected slot
        next_slot_cycle.complete_next_tick(sequenced_ops.clone().persist().fold(
            q!(|| 0),
            q!(|next_slot, payload: SequencedPayload<_>| {
                *next_slot = payload.seq + 1;
            }),
        ));

        // Extract operations (filter out None payloads that represent empty slots); located at proposers
        sequenced_ops
            .filter_map(q!(|sequenced| sequenced.payload))
            .all_ticks()
    }
}

impl<V> Default for PaxosDispatcher<V> {
    fn default() -> Self {
        Self::new()
    }
}

// OpDispatch implementation only for PaxosWithClusters, requiring the caller to supply clusters.
// Fallback OpDispatch implementation (no real Paxos). Used when the example does
// not supply auxiliary clusters. Real Paxos order should be imposed explicitly via `paxos_order`.
impl<V> OpDispatch<V> for PaxosDispatcher<V>
where
    V: PaxosPayload + Eq,
{
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Simple enumeration fallback (NOT real Paxos)
        let operations = operations
            .enumerate()
            .map(q!(|(i, op)| (i as u32, op)))
            .broadcast_bincode(target_cluster, nondet!(/** paxos-fallback */))
            .assume_ordering(nondet!(/** fallback ordered */))
            .map(q!(|(_i, op)| op));

        operations
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        _target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations.assume_ordering(nondet!(/** passthrough cluster hop */))
    }
}

/// Public helper: impose real Paxos total order given externally created clusters.
pub fn paxos_order<
    'a,
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<V>,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
) -> Stream<KVSOperation<V>, Cluster<'a, Proposer>, Unbounded> {
    dispatcher.paxos_run(operations, proposers, acceptors)
}

/// Helper: impose Paxos ordering then loop ordered ops back to the proxy process.
///
/// This keeps consensus (proposers/acceptors) separate from subsequent dispatch stages
/// by returning a process-located ordered stream suitable for normal `dispatch_from_process`.
pub fn paxos_order_to_proxy<
    'a,
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
        .send_bincode(proxy) // (member_id, op) at proxy
        .entries()
        .map(q!(|(_member_id, op)| op))
        .assume_ordering(nondet!(/** paxos ordered at proxy */))
}

/// Variant that returns slotted operations: (slot, op)
/// Useful when downstream wants to preserve slot numbers for gap-filling sequencing.
pub fn paxos_order_slotted<
    'a,
    V: PaxosPayload + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    dispatcher: &PaxosDispatcher<V>,
    operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
) -> Stream<(usize, KVSOperation<V>), Cluster<'a, Proposer>, Unbounded> {
    // Re-run internal Paxos but expose slot numbers before filtering.
    // Reuse paxos_run then re-tag with synthetic slot sequence by enumerating.
    // NOTE: paxos_run already imposed total order; slots here are stable enumeration indices.
    dispatcher
        .paxos_run(operations, proposers, acceptors)
        .enumerate() // (idx, op)
        .map(q!(|(idx, op)| (idx, op)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let p = PaxosDispatcher::<String>::new();
        assert_eq!(p.config.f, PaxosConfig::default().f);
    }
}
