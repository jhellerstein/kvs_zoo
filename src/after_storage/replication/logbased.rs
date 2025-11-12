//! Log-Based Replication Wrapper (after-storage)
//!
//! Provides a wrapper that adds slot-based total ordering guarantees to any
//! underlying replication strategy by sequencing slotted operations and
//! buffering out-of-order items until gaps are filled.

use crate::kvs_core::KVSNode;
use crate::after_storage::{MaintenanceAfterResponses, ReplicationStrategy};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Log-based replication wrapper
///
/// Wraps any replication strategy to add slot-based ordering guarantees.
/// Operations are replicated with their slot numbers and applied in
/// sequential order, buffering any operations that arrive out of order.
#[derive(Clone, Debug)]
pub struct LogBasedDelivery<R> {
    inner: R,
}

impl<R> LogBasedDelivery<R> {
    /// Create a new log-based replication wrapper
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    /// Get a reference to the inner replication strategy
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Consume the wrapper and return the inner replication strategy
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Default> Default for LogBasedDelivery<R> {
    fn default() -> Self {
        Self { inner: R::default() }
    }
}

impl<V, R> ReplicationStrategy<V> for LogBasedDelivery<R>
where
    R: ReplicationStrategy<V>,
{
    /// Unordered replication delegates to inner strategy
    fn replicate_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        self.inner.replicate_data(cluster, local_data)
    }

    /// Slotted replication with gap-filling sequencing
    fn replicate_slotted_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Step 1: Use inner strategy to disseminate slotted operations
        let replicated_slotted = self.disseminate_slotted(cluster, local_slotted_data);

        // Step 2: Apply gap-filling sequencing to ensure operations are applied in order
        Self::sequence_slotted_operations(cluster, replicated_slotted)
    }
}

// Upward pass hook: by default, this wrapper doesn't alter responses
impl<R> MaintenanceAfterResponses for LogBasedDelivery<R> {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

impl<R> LogBasedDelivery<R> {
    /// Disseminate slotted operations using the inner replication strategy
    fn disseminate_slotted<'a, V>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        R: ReplicationStrategy<V>,
    {
        self.inner
            .replicate_slotted_data(cluster, local_slotted_data)
    }

    /// Gap-filling sequence logic for slot-indexed operations
    fn sequence_slotted_operations<'a, V>(
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
        let next_slot_after_processing =
            sorted_ops.clone().cross_singleton(next_slot.clone()).fold(
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
}
