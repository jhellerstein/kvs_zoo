//! Replication strategies (after-storage) and sequencing wrapper

pub mod broadcast;
pub mod gossip;
pub use broadcast::*;
pub use gossip::*;

use crate::after_storage::{
    AfterResponses, ClusterCommunication, LeafCompatible, ReplicationStrategy,
};
use crate::kvs_core::KVSNode;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Sequenced replication wrapper (slot-ordered delivery)
///
/// Wraps any replication strategy to add slot-based ordering guarantees.
/// Slotted operations are disseminated by the inner strategy and then applied
/// in sequential order, buffering any operations that arrive out of order.
#[derive(Clone, Debug, Default)]
pub struct SequencedReplication<R> {
    inner: R,
}

impl<R> SequencedReplication<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
    pub fn inner(&self) -> &R {
        &self.inner
    }
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<K, V, R> ReplicationStrategy<K, V> for SequencedReplication<R>
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    R: ReplicationStrategy<K, V> + Clone,
{
    fn is_active() -> bool {
        R::is_active()
    }

    /// Unordered replication delegates to inner strategy
    fn replicate_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        self.inner.replicate_data(cluster, local_data)
    }

    /// Slotted replication with gap-filling sequencing
    fn replicate_slotted_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        // Step 1: Use inner strategy to disseminate slotted operations
        let replicated_slotted = self
            .inner
            .replicate_slotted_data(cluster, local_slotted_data);

        // Step 2: Apply gap-filling sequencing to ensure operations are applied in order
        sequence_slotted_operations(cluster, replicated_slotted)
    }
}

impl<R> ClusterCommunication for SequencedReplication<R>
where
    R: ClusterCommunication,
{
    fn requires_cluster_scope() -> bool {
        R::requires_cluster_scope()
    }
}

impl<R> LeafCompatible for SequencedReplication<R> where R: LeafCompatible {}

// Upward pass hook: by default, this wrapper doesn't alter responses
impl<R> AfterResponses for SequencedReplication<R> {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

/// Gap-filling sequence logic for slot-indexed operations
fn sequence_slotted_operations<'a, K, V, O>(
    cluster: &Cluster<'a, KVSNode>,
    slotted_operations: Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, O>,
) -> Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    O: hydro_lang::live_collections::stream::Ordering,
{
    let tick = cluster.tick();
    
    // Convert to TotalOrder for sequencing operations
    let slotted_operations = slotted_operations.assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** sequencing */));

    // Create cycles for buffering out-of-order operations
    let (buffered_ops_complete, buffered_ops) =
        tick.cycle::<Stream<(usize, K, V), Tick<Cluster<'a, KVSNode>>, Bounded>>();

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

    // Return operations in slot order, converting back to NoOrder
    processable_ops.all_ticks().weakest_ordering()
}
