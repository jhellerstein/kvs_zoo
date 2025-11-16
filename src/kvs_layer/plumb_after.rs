use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::prelude::*;

use crate::after_storage::{AfterResponses, ReplicationStrategy};
use crate::protocol::KVSOperation;

type ClusterStream<'a, T> = Stream<T, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>;

/// Forward a delta stream to the parent cluster so cluster-scoped hooks can observe peer traffic.
fn forward_to_cluster<'a, V>(
    stream: ClusterStream<'a, (String, V)>,
    target_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
) -> ClusterStream<'a, (String, V)>
where
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
{
    stream
        .broadcast_bincode(target_cluster, nondet!(/** forward puts to parent layer */))
        .values()
        .assume_ordering::<TotalOrder>(nondet!(/** forwarded upstream */))
}

/// After-stage plumbing: traverse replication/responders chain from leaf upward.
pub trait AfterPlumb<V> {
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

impl<V> AfterPlumb<V> for () {
    fn after_responses<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        responses
    }
}

impl<V, Name, B, A, Child, Bg> AfterPlumb<V> for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: crate::before_storage::Before<V> + Clone,
    A: ReplicationStrategy<V> + AfterResponses + Clone,
    Child: AfterPlumb<V>,
{
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let bubbled = self.child.after_responses(layers, responses);
        self.after.after_responses(my_cluster, bubbled)
    }
}

impl<V, Name, B, A> AfterPlumb<V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: crate::before_storage::Before<V> + Clone,
    A: ReplicationStrategy<V> + AfterResponses + Clone,
{
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.after.after_responses(my_cluster, responses)
    }
}

/* ------------------------------------------------------------------------- */
/* Replication plumbing: walk After layers to fan out PUT deltas             */
/* ------------------------------------------------------------------------- */

/// Helper trait used by `plumb_kvs_dataflow` to invoke any configured
/// replication strategies in the After stack.
pub trait ReplicationPlumb<V> {
    fn replicate_puts<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (String, V)>,
    ) -> (
        ClusterStream<'a, (String, V)>,
        ClusterStream<'a, KVSOperation<V>>,
    )
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static;
}

impl<V> ReplicationPlumb<V> for () {
    fn replicate_puts<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (String, V)>,
    ) -> (
        ClusterStream<'a, (String, V)>,
        ClusterStream<'a, KVSOperation<V>>,
    )
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        let pass_up = deltas;
        let empty = pass_up
            .clone()
            .filter_map(q!(|_kv| None))
            .assume_ordering::<TotalOrder>(nondet!(/** no replication (unit) */));
        (pass_up, empty)
    }
}

fn should_skip_replication<A, V>() -> bool
where
    A: ReplicationStrategy<V> + 'static,
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
{
    !A::is_active()
}

impl<V, Name, B, A, Child, Bg> ReplicationPlumb<V>
    for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    B: crate::before_storage::Before<V> + Clone,
    A: ReplicationStrategy<V> + Clone + 'static,
    Child: ReplicationPlumb<V> + crate::kvs_layer::KVSPlumb<V>,
    Bg: Clone,
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
{
    fn replicate_puts<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (String, V)>,
    ) -> (
        ClusterStream<'a, (String, V)>,
        ClusterStream<'a, KVSOperation<V>>,
    )
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        let my_cluster = layers.get::<Name>();

        let (pass_up_from_child, child_ops) = self.child.replicate_puts(layers, deltas);
        let mut combined_ops = child_ops;

        let needs_cluster_scope = A::requires_cluster_scope();
        let pass_up_for_parent = if needs_cluster_scope {
            forward_to_cluster(pass_up_from_child.clone(), my_cluster)
        } else {
            pass_up_from_child.clone()
        };

        if !should_skip_replication::<A, V>() {
            let replication_input = if needs_cluster_scope {
                pass_up_for_parent.clone()
            } else {
                pass_up_from_child
            };

            let replicated = self
                .after
                .replicate_data(my_cluster, replication_input)
                .map(q!(|(k, v)| KVSOperation::Put(k, v)))
                .assume_ordering::<TotalOrder>(nondet!(/** replicated updates at layer */));

            let routed = self
                .child
                .plumb_from_cluster(layers, my_cluster, replicated)
                .assume_ordering::<TotalOrder>(nondet!(/** routed replicated ops to leaf */));

            combined_ops = combined_ops
                .interleave(routed)
                .assume_ordering::<TotalOrder>(nondet!(/** combined replication ops */));
        }

        (pass_up_for_parent, combined_ops)
    }
}

impl<V, Name, B, A> ReplicationPlumb<V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
    B: crate::before_storage::Before<V> + Clone,
    A: ReplicationStrategy<V> + Clone + 'static,
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
{
    fn replicate_puts<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (String, V)>,
    ) -> (
        ClusterStream<'a, (String, V)>,
        ClusterStream<'a, KVSOperation<V>>,
    )
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        let my_cluster = layers.get::<Name>();
        if should_skip_replication::<A, V>() {
            let pass_up = deltas;
            let empty = pass_up
                .clone()
                .filter_map(q!(|_kv| None))
                .assume_ordering::<TotalOrder>(nondet!(/** no replication at node */));
            (pass_up, empty)
        } else {
            let pass_up = deltas.clone();
            let replicated = self
                .after
                .replicate_data(my_cluster, deltas)
                .map(q!(|(k, v)| KVSOperation::Put(k, v)))
                .assume_ordering::<TotalOrder>(nondet!(/** replicated updates at node */));
            (pass_up, replicated)
        }
    }
}
