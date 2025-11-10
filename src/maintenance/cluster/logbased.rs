//! Log-Based Replication Wrapper
//!
//! This module provides a wrapper that adds slot-based total ordering guarantees
//! to any underlying replication strategy. It ensures operations are applied
//! in sequential slot order, buffering out-of-order operations until gaps are filled.
//!
//! ## Problem
//!
//! When using unordered replication (like broadcast or gossip) with consensus
//! protocols (like Paxos), operations can arrive out of order:
//!
//! ```text
//! 1. Op1 (slot 0) sent to Node1, delayed in network
//! 2. Op2 (slot 1) sent to Node2
//! 3. Node2 replicates Op2 to Node1
//! 4. Node1 sees Op2 (slot 1) before Op1 (slot 0)
//! ```
//!
//! Without proper ordering, this can lead to:
//! - Operations processed out of order
//! - Inconsistent state across nodes
//! - Violation of total ordering guarantees
//!
//! ## Solution
//!
//! LogBased wrapper ensures:
//! 1. Only process operations when all prior slots are complete
//! 2. Buffer out-of-order operations until gaps are filled
//! 3. Always maintain sequential slot processing

use crate::kvs_core::KVSNode;
use crate::maintenance::ReplicationStrategy;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Log-based replication wrapper
///
/// Wraps any replication strategy to add slot-based ordering guarantees.
/// Operations are replicated with their slot numbers and applied in
/// sequential order, buffering any operations that arrive out of order.
///
/// ## Type Parameters
/// - `R`: The underlying replication strategy (Broadcast, Gossip, etc.)
///
/// ## Example
/// ```rust
/// use kvs_zoo::maintenance::{LogBased, BroadcastReplication};
/// use kvs_zoo::values::CausalString;
///
/// // Wrap broadcast replication with log-based ordering
/// let replication = LogBased::new(BroadcastReplication::<CausalString>::new());
/// ```
#[derive(Clone, Debug)]
pub struct LogBased<R> {
    inner: R,
}

impl<R> LogBased<R> {
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

impl<R: Default> Default for LogBased<R> {
    fn default() -> Self {
        Self {
            inner: R::default(),
        }
    }
}

impl<V, R> ReplicationStrategy<V> for LogBased<R>
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
        // The inner strategy handles HOW to send (broadcast, gossip, etc.)
        let replicated_slotted = self.disseminate_slotted(cluster, local_slotted_data);

        // Step 2: Apply gap-filling sequencing to ensure operations are applied in order
        Self::sequence_slotted_operations(cluster, replicated_slotted)
    }
}

impl<R> LogBased<R> {
    /// Disseminate slotted operations using the inner replication strategy
    ///
    /// This delegates to the inner strategy's slotted replication method,
    /// which will use its dissemination approach (broadcast, gossip, etc.)
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
    ///
    /// This implements gap-filling logic:
    /// - Track the next expected slot number
    /// - Buffer operations that arrive early (gaps in sequence)
    /// - Apply operations only when all prior slots are filled
    ///
    /// Based on: https://github.com/hydro-project/hydro/blob/main/hydro_test/src/cluster/kv_replica/sequence_payloads.rs
    ///
    /// Note: We use a Vec-based approach instead of sort() since we can't require V: Ord
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maintenance::{BroadcastReplication, SimpleGossip, NoReplication};

    #[test]
    fn test_logbased_creation() {
        let _logbased_broadcast =
            LogBased::new(BroadcastReplication::<crate::values::CausalString>::new());
        let _logbased_gossip = LogBased::new(SimpleGossip::<crate::values::CausalString>::default());
        let _logbased_none = LogBased::new(NoReplication::new());
    }

    #[test]
    fn test_logbased_default() {
        let _logbased: LogBased<NoReplication> = LogBased::default();
    }

    #[test]
    fn test_logbased_inner_access() {
        let broadcast = BroadcastReplication::<crate::values::CausalString>::new();
        let logbased = LogBased::new(broadcast.clone());

        let _inner_ref = logbased.inner();
        let _inner_owned = logbased.into_inner();
    }

    #[test]
    fn test_logbased_clone_debug() {
        let logbased = LogBased::new(NoReplication::new());
        let _cloned = logbased.clone();
        let _debug_str = format!("{:?}", logbased);
    }

    #[test]
    fn test_logbased_implements_replication_strategy() {
        fn _test_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}

        _test_replication_strategy::<crate::values::CausalString>(LogBased::new(
            BroadcastReplication::<crate::values::CausalString>::new(),
        ));
        _test_replication_strategy::<crate::values::CausalString>(LogBased::new(SimpleGossip::<
            crate::values::CausalString,
        >::default()));
    }

    #[test]
    fn test_logbased_composition() {
        // Test that LogBased can wrap different strategies
        type LogBasedBroadcast<V> = LogBased<BroadcastReplication<V>>;
        type LogBasedGossip<V> = LogBased<SimpleGossip<V>>;
        type LogBasedNone = LogBased<NoReplication>;

        let _broadcast: LogBasedBroadcast<crate::values::CausalString> =
            LogBased::new(BroadcastReplication::new());
        let _gossip: LogBasedGossip<crate::values::CausalString> =
            LogBased::new(SimpleGossip::default());
        let _none: LogBasedNone = LogBased::new(NoReplication::new());
    }

    #[test]
    fn test_logbased_send_sync() {
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}
        _requires_send_sync(LogBased::new(NoReplication::new()));
        _requires_send_sync(LogBased::new(BroadcastReplication::<
            crate::values::CausalString,
        >::new()));
    }
}
