// Native SlotOrderEnforcer (before-storage ordering; migrated from legacy dispatch::ordering::slot_enforcer)
#[derive(Debug, Default, Clone, Copy)]
pub struct SlotOrderEnforcer;

impl SlotOrderEnforcer {
    pub fn new() -> Self {
        Self
    }
}

use crate::before_storage::{Before, RequiresLinearizable};

// SlotOrderEnforcer requires linearizable processing
impl RequiresLinearizable for SlotOrderEnforcer {
    fn requires_linearizable() -> bool {
        true
    }
}
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

impl<K, V> Before<K, V> for SlotOrderEnforcer {
    type OutputOrder = hydro_lang::live_collections::stream::NoOrder;
    fn dispatch_from_process<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
        target_cluster: &StaticCluster<'a, KVSNode>,
    ) -> Stream<
        KVSOperation<K, V>,
        StaticCluster<'a, KVSNode>,
        Unbounded,
        Self::OutputOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // demux_bincode from Process returns Stream directly
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw_id(0u32),
                op
            )))
            .into_keyed()
            .demux_static(target_cluster, TCP.fail_stop().bincode())
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }

    fn dispatch_from_cluster<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, StaticCluster<'a, KVSNode>, Unbounded, O>,
        _source_cluster: &StaticCluster<'a, KVSNode>,
        target_cluster: &StaticCluster<'a, KVSNode>,
    ) -> Stream<
        KVSOperation<K, V>,
        StaticCluster<'a, KVSNode>,
        Unbounded,
        Self::OutputOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // demux_bincode from Cluster returns KeyedStream, need .values()
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw_id(0u32),
                op
            )))
            .into_keyed()
            .demux(target_cluster, TCP.fail_stop().bincode())
            .values()
    }
}
