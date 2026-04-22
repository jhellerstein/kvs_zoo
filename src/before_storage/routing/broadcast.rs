//! Broadcast router (Before stage): sends every operation to every cluster member.
//!
//! Preserves TotalOrder when the input is TotalOrder, since broadcast over
//! TCP fail-stop delivers elements in order to each member.

use crate::before_storage::{Before, RequiresLinearizable};
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
pub struct BroadcastRouter;

impl RequiresLinearizable for BroadcastRouter {}

impl<K, V> Before<K, V> for BroadcastRouter {
    type OutputOrder = TotalOrder;

    fn dispatch_from_process<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
        target_cluster: &StaticCluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<K, V>, StaticCluster<'a, KVSNode>, Unbounded, Self::OutputOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .broadcast_static(target_cluster, TCP.fail_stop().bincode())
            .assume_ordering::<TotalOrder>(nondet!(
                /// Broadcast over TCP fail-stop preserves ordering per member.
            ))
    }

    fn dispatch_from_cluster<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, StaticCluster<'a, KVSNode>, Unbounded, O>,
        _source_cluster: &StaticCluster<'a, KVSNode>,
        target_cluster: &StaticCluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<K, V>, StaticCluster<'a, KVSNode>, Unbounded, Self::OutputOrder>
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .broadcast(target_cluster, TCP.fail_stop().bincode())
            .values()
            .assume_ordering::<TotalOrder>(nondet!(
                /// Broadcast over TCP fail-stop preserves ordering per member.
            ))
    }
}
