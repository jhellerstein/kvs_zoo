// Native SlotOrderEnforcer (before-storage ordering; migrated from legacy dispatch::ordering::slot_enforcer)
#[derive(Debug, Default, Clone, Copy)]
pub struct SlotOrderEnforcer;

impl SlotOrderEnforcer {
    pub fn new() -> Self {
        Self
    }
}

use crate::before_storage::Before;
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

impl<K, V> Before<K, V> for SlotOrderEnforcer {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<K, V>, Cluster<'a, KVSNode>, Unbounded>
    where
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw_id(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<K, V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<K, V>, Cluster<'a, KVSNode>, Unbounded>
    where
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw_id(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
            .values()
            .assume_ordering(nondet!(/** slot order enforced */))
    }
}
