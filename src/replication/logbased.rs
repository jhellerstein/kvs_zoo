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
//! 4. Node1 receives Op2 (slot 1) before Op1 (slot 0)
//! ```
//!
//! This causes replicas to diverge on operation order.
//!
//! ## Solution
//!
//! LogBased replication gossips slot-indexed operations and applies them
//! in sequential order:
//! - Operations arrive with slot numbers
//! - Buffer operations that arrive early (gaps in sequence)
//! - Apply operations only when all prior slots are filled
//! - Maintains total order across all replicas

use crate::core::KVSNode;
use crate::replication::ReplicationStrategy;
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
/// use kvs_zoo::replication::{LogBased, BroadcastReplication};
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
        crate::sequencing::sequence_slotted_operations(cluster, replicated_slotted)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{BroadcastReplication, EpidemicGossip, NoReplication};

    #[test]
    fn test_logbased_creation() {
        let _logbased_broadcast =
            LogBased::new(BroadcastReplication::<crate::values::CausalString>::new());
        let _logbased_gossip = LogBased::new(EpidemicGossip::<crate::values::CausalString>::new());
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
        _test_replication_strategy::<crate::values::CausalString>(LogBased::new(EpidemicGossip::<
            crate::values::CausalString,
        >::new()));
    }

    #[test]
    fn test_logbased_composition() {
        // Test that LogBased can wrap different strategies
        type LogBasedBroadcast<V> = LogBased<BroadcastReplication<V>>;
        type LogBasedGossip<V> = LogBased<EpidemicGossip<V>>;
        type LogBasedNone = LogBased<NoReplication>;

        let _broadcast: LogBasedBroadcast<crate::values::CausalString> =
            LogBased::new(BroadcastReplication::new());
        let _gossip: LogBasedGossip<crate::values::CausalString> =
            LogBased::new(EpidemicGossip::new());
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
