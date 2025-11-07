//! Log-Based Maintenance Wrapper
//!
//! This module provides a wrapper that adds slot-based total ordering guarantees
//! to any underlying dissemination strategy. It ensures operations are applied
//! in sequential slot order, buffering out-of-order operations until gaps are filled.
//!
//! ## Problem
//!
//! When using unordered dissemination (like broadcast or gossip) with consensus
//! protocols (like Paxos), operations can arrive out of order:
//!
//! ```text
//! 1. Op1 (slot 0) sent to Node1, delayed in network
//! 2. Op2 (slot 1) sent to Node2
//! 3. Node2 maintains Op2 to Node1
//! 4. Node1 receives Op2 (slot 1) before Op1 (slot 0)
//! ```
//!
//! This causes replicas to diverge on operation order.
//!
//! ## Solution
//!
//! LogBased dissemination gossips slot-indexed operations and applies them
//! in sequential order:
//! - Operations arrive with slot numbers
//! - Buffer operations that arrive early (gaps in sequence)
//! - Apply operations only when all prior slots are filled
//! - Maintains total order across all replicas

use crate::core::KVSNode;
use crate::maintain::MaintenanceStrategy;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Log-based dissemination wrapper
///
/// Wraps any dissemination strategy to add slot-based ordering guarantees.
/// Operations are maintaind with their slot numbers and applied in
/// sequential order, buffering any operations that arrive out of order.
///
/// ## Type Parameters
/// - `R`: The underlying maintenance strategy (Broadcast, Gossip, etc.)
///
/// ## Example
/// ```rust
/// use kvs_zoo::maintain::{LogBased, BroadcastMaintenance};
/// use kvs_zoo::values::CausalString;
///
/// // Wrap broadcast maintenance with log-based ordering
/// let maintenance = LogBased::new(BroadcastMaintenance::<CausalString>::new());
/// ```
#[derive(Clone, Debug)]
pub struct LogBased<R> {
    inner: R,
}

impl<R> LogBased<R> {
    /// Create a new log-based dissemination wrapper
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    /// Get a reference to the inner dissemination strategy
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Consume the wrapper and return the inner dissemination strategy
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

impl<V, R> MaintenanceStrategy<V> for LogBased<R>
where
    R: MaintenanceStrategy<V>,
{
    /// Unordered dissemination delegates to inner strategy
    fn maintain_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        self.inner.maintain_data(cluster, local_data)
    }

    /// Slotted dissemination with gap-filling sequencing
    fn maintain_slotted_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Step 1: Use inner strategy to maintain slotted operations
        // The inner strategy handles HOW to send (broadcast, gossip, etc.)
        let maintaind_slotted = self.maintain_slotted(cluster, local_slotted_data);

        // Step 2: Apply gap-filling sequencing to ensure operations are applied in order
        crate::sequencing::sequence_slotted_operations(cluster, maintaind_slotted)
    }
}

impl<R> LogBased<R> {
    /// Disseminate slotted operations using the inner dissemination strategy
    ///
    /// This delegates to the inner strategy's slotted dissemination method,
    /// which will use its dissemination approach (broadcast, gossip, etc.)
    fn maintain_slotted<'a, V>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        R: MaintenanceStrategy<V>,
    {
        self.inner
            .maintain_slotted_data(cluster, local_slotted_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maintain::{BroadcastMaintenance, EpidemicGossip, NoMaintenance};

    #[test]
    fn test_logbased_creation() {
        let _logbased_broadcast =
            LogBased::new(BroadcastMaintenance::<crate::values::CausalString>::new());
        let _logbased_gossip = LogBased::new(EpidemicGossip::<crate::values::CausalString>::new());
        let _logbased_none = LogBased::new(NoMaintenance::new());
    }

    #[test]
    fn test_logbased_default() {
        let _logbased: LogBased<NoMaintenance> = LogBased::default();
    }

    #[test]
    fn test_logbased_inner_access() {
        let broadcast = BroadcastMaintenance::<crate::values::CausalString>::new();
        let logbased = LogBased::new(broadcast.clone());

        let _inner_ref = logbased.inner();
        let _inner_owned = logbased.into_inner();
    }

    #[test]
    fn test_logbased_clone_debug() {
        let logbased = LogBased::new(NoMaintenance::new());
        let _cloned = logbased.clone();
        let _debug_str = format!("{:?}", logbased);
    }

    #[test]
    fn test_logbased_implements_dissemination_strategy() {
        fn _test_dissemination_strategy<V>(_strategy: impl MaintenanceStrategy<V>) {}

        _test_dissemination_strategy::<crate::values::CausalString>(LogBased::new(
            BroadcastMaintenance::<crate::values::CausalString>::new(),
        ));
        _test_dissemination_strategy::<crate::values::CausalString>(LogBased::new(
            EpidemicGossip::<crate::values::CausalString>::new(),
        ));
    }

    #[test]
    fn test_logbased_composition() {
        // Test that LogBased can wrap different strategies
        type LogBasedBroadcast<V> = LogBased<BroadcastMaintenance<V>>;
        type LogBasedGossip<V> = LogBased<EpidemicGossip<V>>;
        type LogBasedNone = LogBased<NoMaintenance>;

        let _broadcast: LogBasedBroadcast<crate::values::CausalString> =
            LogBased::new(BroadcastMaintenance::new());
        let _gossip: LogBasedGossip<crate::values::CausalString> =
            LogBased::new(EpidemicGossip::new());
        let _none: LogBasedNone = LogBased::new(NoMaintenance::new());
    }

    #[test]
    fn test_logbased_send_sync() {
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}
        _requires_send_sync(LogBased::new(NoMaintenance::new()));
        _requires_send_sync(LogBased::new(BroadcastMaintenance::<
            crate::values::CausalString,
        >::new()));
    }
}
