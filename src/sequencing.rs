//! Shared sequencing utilities for handling slot-indexed operations
//!
//! This module provides reusable sequencing logic that can handle out-of-order
//! delivery of slot-indexed items, ensuring they are processed in the correct order.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::core::KVSNode;

/// Sequence slotted operations to ensure they are applied in order
///
/// This implements gap-filling logic:
/// - Track the next expected slot number
/// - Buffer operations that arrive early (gaps in sequence)
/// - Apply operations only when all prior slots are filled
///
/// Based on the pattern from hydro/hydro_test/src/cluster/kv_replica/sequence_payloads.rs
///
/// Note: We use a Vec-based approach instead of sort() since we can't require V: Ord
pub fn sequence_slotted_operations<'a, V>(
    cluster: &Cluster<'a, KVSNode>,
    slotted_operations: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    let tick = cluster.tick();

    // Create cycles for buffering out-of-order operations
    let (buffered_ops_complete, buffered_ops) =
        tick.cycle::<Stream<(usize, String, V), Tick<Cluster<'a, KVSNode>>, Bounded>>();

    // Batch incoming operations and combine with buffered ones
    // Collect into Vec, sort by slot number, and flatten back to stream
    let batched_ops = slotted_operations
        .batch(&tick, nondet!(/** batch for sequencing */))
        .chain(buffered_ops);

    let sorted_ops_singleton = batched_ops
        .fold(
            q!(|| Vec::new()),
            q!(|acc, op| {
                acc.push(op);
            }),
        )
        .map(q!(|mut ops| {
            ops.sort_by_key(|(slot, _, _)| *slot);
            ops
        }));

    // Convert singleton Vec back to stream
    let sorted_ops = sorted_ops_singleton.flat_map_ordered(q!(|ops| ops));

    // Track the next expected slot number
    let (next_slot_complete, next_slot) = tick.cycle_with_initial(tick.singleton(q!(0usize)));

    // Find the highest contiguous slot we can process
    let next_slot_after_processing = sorted_ops.clone().cross_singleton(next_slot.clone()).fold(
        q!(|| 0usize),
        q!(|new_next_slot, ((slot, _key, _value), next_slot)| {
            if slot == std::cmp::max(*new_next_slot, next_slot) {
                *new_next_slot = slot + 1;
            }
        }),
    );

    // Split operations into processable and buffered
    let processable_ops = sorted_ops
        .clone()
        .cross_singleton(next_slot_after_processing.clone())
        .filter(q!(
            |((slot, _key, _value), highest_slot)| *slot < *highest_slot
        ))
        .map(q!(|((slot, key, value), _)| (slot, key, value)));

    let new_buffered_ops = sorted_ops
        .cross_singleton(next_slot_after_processing.clone())
        .filter(q!(
            |((slot, _key, _value), highest_slot)| *slot > *highest_slot
        ))
        .map(q!(|((slot, key, value), _)| (slot, key, value)));

    // Complete the cycles
    buffered_ops_complete.complete_next_tick(new_buffered_ops);
    next_slot_complete.complete_next_tick(next_slot_after_processing);

    // Return operations in slot order
    processable_ops.all_ticks()
}
