use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;

use crate::after_storage::{AfterResponses, ReplicationStrategy};
use crate::protocol::KVSOperation;

type ClusterStream<'a, T, O = NoOrder> =
    Stream<T, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded, O>;
type ClusterKVSOpStream<'a, K, V, O = NoOrder> =
    Stream<KVSOperation<K, V>, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded, O>;

/// Forward a delta stream to the parent cluster so cluster-scoped hooks can observe peer traffic.
fn forward_to_cluster<'a, K, V, O>(
    stream: Stream<(K, V), StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded, O>,
    target_cluster: &StaticCluster<'a, crate::kvs_core::KVSNode>,
) -> ClusterStream<'a, (K, V), NoOrder>
where
    O: hydro_lang::live_collections::stream::Ordering,
    K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
{
    // broadcast_bincode already introduces non-determinism, no need to re-assert ordering
    stream
        .broadcast(target_cluster, TCP.fail_stop().bincode())
        .values()
}

/// After-stage plumbing: traverse replication/responders chain from leaf upward.
pub trait AfterPlumb<V> {
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

impl<V> AfterPlumb<V> for () {
    fn after_responses<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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
    B: crate::before_storage::Before<String, V> + Clone,
    A: ReplicationStrategy<String, V> + AfterResponses + Clone,
    Child: AfterPlumb<V>,
{
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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
    B: crate::before_storage::Before<String, V> + Clone,
    A: ReplicationStrategy<String, V> + AfterResponses + Clone,
{
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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
pub trait ReplicationPlumb<K, V> {
    fn replicate_puts<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (K, V)>,
    ) -> (ClusterStream<'a, (K, V)>, ClusterKVSOpStream<'a, K, V>)
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + Send
            + Sync
            + 'static;
}

impl<K, V> ReplicationPlumb<K, V> for () {
    fn replicate_puts<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (K, V)>,
    ) -> (ClusterStream<'a, (K, V)>, ClusterKVSOpStream<'a, K, V>)
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + Send
            + Sync
            + 'static,
    {
        let pass_up = deltas;
        let empty: ClusterKVSOpStream<'a, K, V> = pass_up.clone().filter_map(q!(|_kv| None));
        (pass_up, empty)
    }
}

fn should_skip_replication<A, K, V>() -> bool
where
    A: ReplicationStrategy<K, V> + 'static,
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
{
    !A::is_active()
}

impl<K, V, Name, B, A, Child, Bg> ReplicationPlumb<K, V>
    for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    K: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + std::hash::Hash
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    B: crate::before_storage::Before<K, V> + Clone,
    A: ReplicationStrategy<K, V> + Clone + 'static,
    Child: ReplicationPlumb<K, V> + crate::kvs_layer::KVSPlumb<K, V>,
    Bg: Clone,
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
{
    fn replicate_puts<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (K, V)>,
    ) -> (ClusterStream<'a, (K, V)>, ClusterKVSOpStream<'a, K, V>)
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
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
            // Already NoOrder from child
            pass_up_from_child.clone()
        };

        if !should_skip_replication::<A, K, V>() {
            let replication_input = if needs_cluster_scope {
                pass_up_for_parent.clone()
            } else {
                // Already NoOrder from child
                pass_up_from_child
            };

            // replicate_data returns NoOrder, route and combine
            let replicated = self
                .after
                .replicate_data(my_cluster, replication_input)
                .map(q!(|(k, v)| crate::protocol::KVSOperation::Put(k, v, u64::MAX, None)));

            let routed = self
                .child
                .plumb_from_cluster(layers, my_cluster, replicated);

            combined_ops = combined_ops.merge_unordered(routed);
        }

        // Both streams are NoOrder from network operations
        (pass_up_for_parent, combined_ops)
    }
}

impl<K, V, Name, B, A> ReplicationPlumb<K, V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
    K: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + std::hash::Hash
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    B: crate::before_storage::Before<K, V> + Clone,
    A: ReplicationStrategy<K, V> + Clone + 'static,
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
{
    fn replicate_puts<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        deltas: ClusterStream<'a, (K, V)>,
    ) -> (ClusterStream<'a, (K, V)>, ClusterKVSOpStream<'a, K, V>)
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + Send
            + Sync
            + 'static,
    {
        let my_cluster = layers.get::<Name>();
        if should_skip_replication::<A, K, V>() {
            let pass_up = deltas;
            let empty: ClusterKVSOpStream<'a, K, V> = pass_up.clone().filter_map(q!(|_kv| None));
            (pass_up, empty)
        } else {
            let pass_up = deltas.clone();
            let replicated = self
                .after
                .replicate_data(my_cluster, deltas)
                .map(q!(|(k, v)| crate::protocol::KVSOperation::Put(k, v, u64::MAX, None)));
            (pass_up, replicated)
        }
    }
}
