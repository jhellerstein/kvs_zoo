//! After-storage stages (replication, responders)

use crate::kvs_core::KVSNode;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub mod meta;
pub mod replication;
pub mod responders;

/* ------------------------------------------------------------------------- */
/* Core after-storage traits and helpers (migrated from legacy `maintenance`) */
/* ------------------------------------------------------------------------- */

/// Marker for after-storage hooks that require intra-cluster communication.
///
/// Hooks that return `true` from [`ClusterCommunication::requires_cluster_scope`] cannot run on
/// leaf layers (`KVSNode`) because they depend on communicating with peer members in the cluster.
/// Leaf-only hooks should override the default and return `false` so the plumbing can validate
/// placement statically.
pub trait ClusterCommunication {
    /// Whether this hook needs to execute on a multi-member cluster.
    fn requires_cluster_scope() -> bool {
        true
    }
}

/// Marker for after-storage hooks that may attach directly to a leaf `KVSNode`.
///
/// Implement this trait for strategies that do not require peer communication so the type checker
/// rejects accidental use of cluster-scoped hooks on leaf layers.
pub trait LeafCompatible: ClusterCommunication {}

/// Compose two maintenance strategies so they can run concurrently.
///
/// This allows, for example, running a replication strategy (with its own
/// tick/period) alongside an independent maintenance strategy (with a different
/// tick/period). Each strategy can schedule itself independently; their
/// emitted update streams are merged.
#[derive(Clone, Debug)]
pub struct CombinedAfter<A, B> {
    pub a: A,
    pub b: B,
}

impl<A, B> CombinedAfter<A, B> {
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

/// Extension trait to ergonomically compose After-stage strategies
pub trait AfterComposeExt: Sized {
    fn and<O>(self, other: O) -> CombinedAfter<Self, O> {
        CombinedAfter::new(self, other)
    }
}

impl<T> AfterComposeExt for T {}

impl<A, B> ClusterCommunication for CombinedAfter<A, B>
where
    A: ClusterCommunication,
    B: ClusterCommunication,
{
    fn requires_cluster_scope() -> bool {
        A::requires_cluster_scope() || B::requires_cluster_scope()
    }
}

impl<A, B> LeafCompatible for CombinedAfter<A, B>
where
    A: LeafCompatible,
    B: LeafCompatible,
{
}

/// Readability alias for "no after-stage/replication".
///
/// This is equivalent to the unit type `()` which already implements
/// `ReplicationStrategy<String, V>`. Prefer `ZeroAfter` in examples and type
/// signatures when you want to emphasize that there is intentionally no
/// background maintenance.
pub type ZeroAfter = ();

/// Unified replication update type
///
/// Replication strategies can accept and emit both unslotted (unordered)
/// and slotted (slot-ordered) updates through a single API via this enum.
#[derive(Clone, Debug)]
pub enum ReplicationUpdate<K, V> {
    Unslotted((K, V)),
    Slotted((usize, K, V)),
}

/// Core trait for replication strategies
///
/// Replication strategies handle background data synchronization between nodes,
/// operating independently of operation processing. They ensure data consistency
/// and availability across the distributed system.
pub trait ReplicationStrategy<K, V>: ClusterCommunication {
    /// Whether this strategy is active. Strategies like `NoReplication` or unit `()` override this
    /// to short-circuit the replication pipeline without relying on type-id checks.
    fn is_active() -> bool {
        true
    }

    /// Unified replication entry: replicate updates (slotted or unslotted)
    ///
    /// Implementers may override this to handle both update kinds in one place.
    /// Default dispatch adapters call `replicate_data` and `replicate_slotted_data`
    /// as appropriate and merge the results.
    /// 
    /// Note: Replication typically produces NoOrder streams due to network operations.
    fn replicate_updates<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        updates: Stream<ReplicationUpdate<K, V>, Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<ReplicationUpdate<K, V>, Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let unslotted_in = updates.clone().filter_map(q!(|u| match u {
            ReplicationUpdate::Unslotted(t) => Some(t),
            _ => None,
        }));
        let slotted_in = updates.filter_map(q!(|u| match u {
            ReplicationUpdate::Slotted(t) => Some(t),
            _ => None,
        }));

        let unslotted_out = self
            .replicate_data(cluster, unslotted_in)
            .map(q!(|t| ReplicationUpdate::Unslotted(t)));
        let slotted_out = self
            .replicate_slotted_data(cluster, slotted_in)
            .map(q!(|t| ReplicationUpdate::Slotted(t)));

        unslotted_out
            .interleave(slotted_out)
            .assume_ordering(nondet!(/** merged replication updates */))
    }

    /// Replicate data across the cluster (unordered)
    ///
    /// Takes a stream of local data updates and returns a stream of data
    /// replicated by other nodes. The strategy determines how data is
    /// synchronized (gossip, broadcast, etc.).
    /// 
    /// Note: Replication produces NoOrder streams due to network operations.
    fn replicate_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let updates = local_data.map(q!(|t| ReplicationUpdate::Unslotted(t)));
        self.replicate_updates(cluster, updates)
            .filter_map(q!(|u| match u {
                ReplicationUpdate::Unslotted(t) => Some(t),
                _ => None,
            }))
    }

    /// Replicate slotted data across the cluster (ordered by slot)
    ///
    /// Takes a stream of slot-indexed data updates and returns a stream of
    /// replicated data received from other nodes, maintaining slot ordering.
    /// This is used by consensus protocols like Paxos to ensure operations
    /// are applied in the same order across all replicas.
    ///
    /// Default implementation adapts to the unified `replicate_updates` API.
    /// Note: Replication produces NoOrder streams due to network operations.
    fn replicate_slotted_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let updates = local_slotted_data.map(q!(|(s, k, v)| ReplicationUpdate::Slotted((s, k, v))));
        self.replicate_updates(cluster, updates)
            .filter_map(q!(|u| match u {
                ReplicationUpdate::Slotted(t) => Some(t),
                _ => None,
            }))
    }
}

/// Upward pass (After storage) maintenance hook for responses
///
/// Default implementation is pass-through. Strategies that want to adjust
/// response behavior (e.g., add headers, redact, sample) can override.
pub trait AfterResponses {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded>;
}

// Default pass-through impl for unit type () so examples can use () as After-stage and still participate.
impl AfterResponses for () {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

/// No-op replication strategy for single-node systems
///
/// This strategy performs no replication, making it suitable for:
/// - Single-node deployments
/// - Development and testing
/// - Systems that don't require replication
#[derive(Clone, Debug, Default)]
pub struct NoReplication;

impl NoReplication {
    /// Create a new no-replication strategy
    pub fn new() -> Self {
        Self
    }
}

impl ClusterCommunication for NoReplication {
    fn requires_cluster_scope() -> bool {
        false
    }
}

impl LeafCompatible for NoReplication {}

impl<K, V> ReplicationStrategy<K, V> for NoReplication
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    fn is_active() -> bool {
        false
    }

    fn replicate_data<'a, O>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // No replication - convert to NoOrder for consistency
        local_data.assume_ordering(nondet!(/** no replication passthrough */))
    }
}

// Upward pass default for NoReplication: pass-through
impl AfterResponses for NoReplication {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

/// Unit type implementation for no replication
///
/// The unit type `()` can be used as a convenient no-op replication strategy,
/// providing the same behavior as NoReplication with even less overhead.
impl<K, V> ReplicationStrategy<K, V> for ()
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    fn is_active() -> bool {
        false
    }

    fn replicate_data<'a, O>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // No replication - convert to NoOrder for consistency
        local_data.assume_ordering(nondet!(/** no replication passthrough */))
    }
}

impl ClusterCommunication for () {
    fn requires_cluster_scope() -> bool {
        false
    }
}

impl LeafCompatible for () {}

// Blanket impls for combining two maintenance strategies
impl<K, V, A, B> ReplicationStrategy<K, V> for CombinedAfter<A, B>
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    A: ReplicationStrategy<K, V>,
    B: ReplicationStrategy<K, V>,
{
    fn is_active() -> bool {
        A::is_active() || B::is_active()
    }

    fn replicate_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        let a_out = self.a.replicate_data(cluster, local_data.clone());
        let b_out = self.b.replicate_data(cluster, local_data);
        // Both return NoOrder, interleave preserves that
        a_out.interleave(b_out)
    }

    fn replicate_slotted_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<(usize, K, V), Cluster<'a, KVSNode>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        let a_out = self
            .a
            .replicate_slotted_data(cluster, local_slotted_data.clone());
        let b_out = self.b.replicate_slotted_data(cluster, local_slotted_data);
        // Both return NoOrder, interleave preserves that
        a_out.interleave(b_out)
    }
}
