// Handles sequencing of Paxos slots with potential holes (native)

use hydro_lang::forward_handle::TickCycleHandle;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::{Location, NoTick};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequencedPayload<T> {
    pub seq: usize,
    pub payload: Option<T>,
}

// Manually implement Ord to only compare by seq, not by payload
impl<T: Eq> PartialOrd for SequencedPayload<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Eq> Ord for SequencedPayload<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq.cmp(&other.seq)
    }
}

#[expect(clippy::type_complexity, reason = "Paxos internals")]
pub fn sequence_payloads<'a, T, L: Location<'a> + NoTick>(
    tick: &Tick<L>,
    incoming: Stream<SequencedPayload<T>, L, Unbounded, NoOrder>,
) -> (
    Stream<SequencedPayload<T>, Tick<L>, Bounded>,
    TickCycleHandle<'a, Singleton<usize, Tick<L>, Bounded>>,
)
where
    T: Clone + Serialize + for<'de> Deserialize<'de> + Eq,
{
    let (r_buffered_payloads_complete_cycle, r_buffered_payloads) =
        tick.cycle::<Stream<SequencedPayload<T>, Tick<L>, Bounded>>();

    let r_sorted_payloads = incoming
        .batch(
            tick,
            nondet!(
                /// because we fill slots one-by-one, we can safely batch
                /// because non-determinism is resolved when we sort by slots
            ),
        )
        .chain(r_buffered_payloads) // Combine with all payloads that we've received and not processed yet
        .sort();

    // Create a cycle since we'll use this seq before we define it
    let (r_next_slot_complete_cycle, r_next_slot) = tick.cycle_with_initial(tick.singleton(q!(0)));

    // Find the highest sequence number of any payload that can be processed in this tick.
    // This is the payload right before a hole.
    let r_next_slot_after_processing_payloads = r_sorted_payloads
        .clone()
        .cross_singleton(r_next_slot.clone())
        .fold(
            q!(|| 0),
            q!(|new_next_slot, (sorted_payload, next_slot)| {
                if sorted_payload.seq == std::cmp::max(*new_next_slot, next_slot) {
                    *new_next_slot = sorted_payload.seq + 1;
                }
            }),
        );

    // Find all payloads that can and cannot be processed in this tick.
    let r_processable_payloads = r_sorted_payloads
        .clone()
        .cross_singleton(r_next_slot_after_processing_payloads.clone())
        .filter(q!(
            |(sorted_payload, highest_seq)| sorted_payload.seq < *highest_seq
        ))
        .map(q!(|(sorted_payload, _)| { sorted_payload }));

    let r_new_non_processable_payloads = r_sorted_payloads
        .clone()
        .cross_singleton(r_next_slot_after_processing_payloads.clone())
        .filter(q!(
            |(sorted_payload, highest_seq)| sorted_payload.seq > *highest_seq
        ))
        .map(q!(|(sorted_payload, _)| { sorted_payload }));

    // Save these, we can process them once the hole has been filled
    r_buffered_payloads_complete_cycle.complete_next_tick(r_new_non_processable_payloads);

    (r_processable_payloads, r_next_slot_complete_cycle)
}
