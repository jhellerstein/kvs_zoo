//! Before-storage stages (routing, ordering, etc.)

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

pub mod ordering;
pub mod routing;

/* ------------------------------------------------------------------------- */
/* Before-storage core (formerly `dispatch`)                                  */
/* ------------------------------------------------------------------------- */

/// Before-stage component for routing operations from proxy to cluster.
pub trait Before<V> {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Hook for dispatchers that need to inspect sibling layers before routing.
    /// Most implementations ignore the additional cluster handles and fall back to `dispatch_from_process`.
    fn dispatch_from_process_with_layers<'a, Name: 'static>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        self.dispatch_from_process(operations, target_cluster)
    }

    fn dispatch_slotted_from_process<'a>(
        &self,
        slotted_operations: Stream<(usize, KVSOperation<V>), Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let bare_ops = slotted_operations.map(q!(|(_slot, op)| op));
        let routed = self.dispatch_from_process(bare_ops, target_cluster);
        routed.map(q!(|op| (0usize, op)))
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
            .assume_ordering(nondet!(/** cluster hop routed */))
    }

    /// Mirrors `dispatch_from_process_with_layers` for inter-cluster routing with layer handles.
    /// Default implementations fall back to `dispatch_from_cluster`.
    fn dispatch_from_cluster_with_layers<'a, Name: 'static>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        self.dispatch_from_cluster(operations, source_cluster, target_cluster)
    }

    /// Hook to allow a dispatcher to register role-specific sub-clusters during cluster creation.
    /// Most dispatchers have nothing to register; default does nothing.
    fn register_role_clusters<'a, Name: 'static>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        _layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) {
    }
}

// (Trait renamed to `Before`; legacy alias removed.)

/// Unit (No‑Op) Before-stage for leaf nodes
impl<V> Before<V> for () {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
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
            .assume_ordering(nondet!(/** cluster hop routed */))
    }
}

/// Identity (broadcast) dispatcher placeholder.
#[derive(Clone, Debug, Default)]
pub struct IdentityDispatch;
impl IdentityDispatch {
    pub fn new() -> Self {
        Self
    }
}

impl<V> Before<V> for IdentityDispatch {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations.broadcast_bincode(target_cluster, nondet!(/** identity */))
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
            .broadcast_bincode(target_cluster, nondet!(/** identity layer hop */))
            .values()
            .assume_ordering(nondet!(/** cluster hop broadcast */))
    }
}

/// Static composition of two dispatchers.
#[derive(Clone, Debug)]
pub struct Pipeline<A, B> {
    pub first: A,
    pub second: B,
}
impl<A, B> Pipeline<A, B> {
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}
impl<A: Default, B: Default> Default for Pipeline<A, B> {
    fn default() -> Self {
        Self {
            first: A::default(),
            second: B::default(),
        }
    }
}

impl<A, B, V> Before<V> for Pipeline<A, B>
where
    A: Before<V>,
    B: Before<V>,
{
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let first_out = self.first.dispatch_from_process(operations, target_cluster);
        self.second
            .dispatch_from_cluster(first_out, target_cluster, target_cluster)
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let mid = self
            .first
            .dispatch_from_cluster(operations, source_cluster, target_cluster);
        self.second
            .dispatch_from_cluster(mid, target_cluster, target_cluster)
    }

    fn dispatch_from_process_with_layers<'a, Name: 'static>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let first_out = self.first.dispatch_from_process_with_layers::<Name>(
            layers,
            operations,
            target_cluster,
        );
        self.second.dispatch_from_cluster_with_layers::<Name>(
            first_out,
            target_cluster,
            target_cluster,
            layers,
        )
    }

    fn dispatch_from_cluster_with_layers<'a, Name: 'static>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
        layers: &crate::kvs_layer::KVSClusters<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let mid = self.first.dispatch_from_cluster_with_layers::<Name>(
            operations,
            source_cluster,
            target_cluster,
            layers,
        );
        self.second.dispatch_from_cluster_with_layers::<Name>(
            mid,
            target_cluster,
            target_cluster,
            layers,
        )
    }

    fn register_role_clusters<'a, Name: 'static>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) {
        self.first.register_role_clusters::<Name>(flow, layers);
        self.second.register_role_clusters::<Name>(flow, layers);
    }
}

/// Fluent extension for chaining.
pub trait BeforeExt<V>: Before<V> + Sized {
    fn then<N: Before<V>>(self, next: N) -> Pipeline<Self, N> {
        Pipeline::new(self, next)
    }
}
impl<V, T: Before<V>> BeforeExt<V> for T {}

/* ------------------------------------------------------------------------- */
/* Marker trait to forbid `()` at cluster level                               */
/* ------------------------------------------------------------------------- */
pub trait ClusterLevelBefore {}

impl ClusterLevelBefore for routing::SingleNodeRouter {}
impl ClusterLevelBefore for routing::ShardedRouter {}
impl ClusterLevelBefore for routing::RoundRobinRouter {}
impl<V> ClusterLevelBefore for ordering::PaxosDispatcher<V> {}
impl ClusterLevelBefore for IdentityDispatch {}
impl<A, B> ClusterLevelBefore for Pipeline<A, B>
where
    A: ClusterLevelBefore,
    B: ClusterLevelBefore,
{
}

// Convenient root re-exports for common ordering types
pub use ordering::{PaxosConfig, PaxosDispatcher, SlotOrderEnforcer};

/// Marker alias for “no leaf” before-stage when using a unified flow.
///
/// Use `NoLeaf` anywhere a leaf before_storage component is optional. It is
/// implemented via the unit type `()` which already implements `Before<V>`.
pub type NoLeaf = ();
/// Singleton value for `NoLeaf` to use in places expecting a reference
pub const NO_LEAF: NoLeaf = ();
