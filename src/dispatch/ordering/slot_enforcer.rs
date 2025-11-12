//! SlotOrderEnforcer dispatcher
//!
//! Enforces that operations are applied in slot order, buffering client GETs
//! until all slots up to their operation's slot have been materialized.
//!
//! This expects an upstream source producing `(slot, KVSOperation<V>)` pairs
//! (e.g. from `paxos_order_slotted`). It converts them into a normal
//! `KVSOperation<V>` stream for downstream maintenance/processing while
//! preserving linearizability: a `Get` is only released once all prior
//! `Put`s (and preceding `Get`s) have been sequenced and applied.
//!
//! Current implementation assumes sequential delivery of slot-tagged ops
//! (no holes) and simply re-tags with ordering while buffering. Future work:
//! handle holes by integrating with `sequence_payloads` gap filling.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::dispatch::OpDispatch;
use crate::dispatch::ordering::sequence_payloads::{SequencedPayload, sequence_payloads};
use crate::kvs_core::KVSNode;
use crate::protocol::{KVSOperation, TaggedOperation};

#[derive(Clone, Debug, Default)]
pub struct SlotOrderEnforcer;

impl SlotOrderEnforcer {
    pub fn new() -> Self {
        Self
    }

    /// Enforce slot ordering from a slotted stream `(slot, op)` located at a cluster
    /// and return a plain ordered `KVSOperation` stream. This buffers out-of-order
    /// arrivals (future enhancement) and currently assumes near-sequential delivery.
    pub fn enforce_from_slotted<'a, V>(
        &self,
        leaf_cluster: &Cluster<'a, KVSNode>,
        slotted: Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Eq + 'static,
    {
        // Convert to NoOrder stream of SequencedPayload<KVSOperation<V>> (no holes expected).
        let seq_payloads = slotted
            .map(q!(|(seq, op)| SequencedPayload {
                seq,
                payload: Some(op)
            }))
            .into();

        // Use sequence_payloads to ensure proper slot ordering (even if out-of-order arrivals occur later).
        let leaf_tick = leaf_cluster.tick();
        let (sequenced_ops, next_slot_cycle) = sequence_payloads(&leaf_tick, seq_payloads);

        // Maintain next slot watermark (future use for buffering GET responses).
        next_slot_cycle.complete_next_tick(sequenced_ops.clone().persist().fold(
            q!(|| 0usize),
            q!(|next_slot, p| {
                *next_slot = p.seq + 1;
            }),
        ));

        // Strip sequencing, preserve TotalOrder.
        sequenced_ops
            .filter_map(q!(|p| p.payload))
            .all_ticks()
            .assume_ordering(nondet!(/** enforced slot ordering to leaf */))
    }

    /// Enforce slot ordering from a tagged slotted stream, preserving response tags.
    ///
    /// This variant takes `TaggedOperation<(slot, op)>` and returns `TaggedOperation<op>`,
    /// preserving the `should_respond` flag through slot enforcement so that only local
    /// operations generate client responses.
    pub fn enforce_from_slotted_tagged<'a, V>(
        &self,
        leaf_cluster: &Cluster<'a, KVSNode>,
        slotted_tagged: Stream<
            TaggedOperation<(usize, KVSOperation<V>)>,
            Cluster<'a, KVSNode>,
            Unbounded,
        >,
    ) -> Stream<TaggedOperation<KVSOperation<V>>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Eq + 'static,
    {
        // Convert to NoOrder stream of SequencedPayload<TaggedOperation<KVSOperation<V>>>
        let seq_payloads = slotted_tagged
            .map(q!(|tagged| SequencedPayload {
                seq: tagged.operation.0,
                payload: Some(crate::protocol::TaggedOperation {
                    should_respond: tagged.should_respond,
                    operation: tagged.operation.1,
                })
            }))
            .into();

        // Use sequence_payloads to ensure proper slot ordering
        let leaf_tick = leaf_cluster.tick();
        let (sequenced_ops, next_slot_cycle) = sequence_payloads(&leaf_tick, seq_payloads);

        // Maintain next slot watermark
        next_slot_cycle.complete_next_tick(sequenced_ops.clone().persist().fold(
            q!(|| 0usize),
            q!(|next_slot, p| {
                *next_slot = p.seq + 1;
            }),
        ));

        // Strip sequencing, preserve TotalOrder and tags
        sequenced_ops
            .filter_map(q!(|p| p.payload))
            .all_ticks()
            .assume_ordering(nondet!(/** enforced slot ordering with tags to leaf */))
    }

    /// Enforce slot ordering from TWO slotted streams: local ops (should respond) and replicated ops (no response).
    ///
    /// This is the primary API for leaf dispatchers in two-layer architectures where:
    /// - Local ops arrive via cluster dispatcher routing (client waiting for response)
    /// - Replicated ops arrive via cluster maintenance broadcast (already ACKed by another member)
    ///
    /// Returns two streams: local ops (with slot ordering) and replicated ops (with slot ordering).
    /// The caller can process them separately to control response behavior.
    pub fn enforce_from_two_slotted_streams<'a, V>(
        &self,
        leaf_cluster: &Cluster<'a, KVSNode>,
        local_slotted: Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>,
        replicated_slotted: Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>,
    ) -> (
        Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
    )
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Eq + 'static,
    {
        // Enforce slot ordering on both streams independently
        let local_ops = self.enforce_from_slotted(leaf_cluster, local_slotted);
        let replicated_ops = self.enforce_from_slotted(leaf_cluster, replicated_slotted);

        (local_ops, replicated_ops)
    }

    /// Enforce slot ordering on two slotted input streams and produce a single tagged op stream.
    ///
    /// Tags indicate whether the op came via the local routed path (false => should respond)
    /// or via the forwarded/replicated path (true => no client response).
    pub fn enforce_tag_from_two_slotted_streams<'a, V>(
        &self,
        leaf_cluster: &Cluster<'a, KVSNode>,
        local_slotted: Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>,
        replicated_slotted: Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(bool, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Eq + 'static,
    {
        let (local_ops, replicated_ops) =
            self.enforce_from_two_slotted_streams(leaf_cluster, local_slotted, replicated_slotted);

        // Tag and merge. Ordering has been enforced per stream; merged stream remains TotalOrder
        // relative to each source, and core processing is sequential.
        let local_tagged = local_ops.map(q!(|op| (false, op)));
        let replicated_tagged = replicated_ops.map(q!(|op| (true, op)));

        local_tagged
            .interleave(replicated_tagged)
            .assume_ordering(nondet!(/** merged local+replicated with tags */))
    }
}

impl<V> OpDispatch<V> for SlotOrderEnforcer {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Assume upstream already ordered; just forward to member 0.
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
            .values()
            .assume_ordering(nondet!(/** slot order enforced */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::OpDispatch;

    #[test]
    fn construct() {
        let _d = SlotOrderEnforcer::new();
    }

    #[test]
    fn implements_trait() {
        fn _uses<V>(_d: impl OpDispatch<V>) {}
        _uses::<String>(SlotOrderEnforcer::new());
    }
}
