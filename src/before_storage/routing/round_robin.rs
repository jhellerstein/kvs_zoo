//! Round-robin router (Before stage)

use crate::before_storage::{Before, RequiresLinearizable};
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
pub struct RoundRobinRouter;

impl RoundRobinRouter {
    pub fn new() -> Self {
        Self
    }
}

// Routing layers don't require linearizable processing
impl RequiresLinearizable for RoundRobinRouter {}

impl<K, V> Before<K, V> for RoundRobinRouter {
    type OutputOrder = hydro_lang::live_collections::stream::NoOrder;
    fn dispatch_from_process<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<
        KVSOperation<K, V>,
        Cluster<'a, KVSNode>,
        Unbounded,
        Self::OutputOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw_id(0u32),
                op
            )))
            .into_keyed()
            .demux(target_cluster, TCP.fail_stop().bincode())
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }

    fn dispatch_slotted_from_process<'a, O>(
        &self,
        slotted_operations: Stream<(usize, KVSOperation<K, V>), Process<'a, ()>, Unbounded, O>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<
        (usize, KVSOperation<K, V>),
        Cluster<'a, KVSNode>,
        Unbounded,
        Self::OutputOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        slotted_operations
            .map(q!(|slotted_op| (
                hydro_lang::location::MemberId::from_raw_id(0u32),
                slotted_op
            )))
            .into_keyed()
            .demux(target_cluster, TCP.fail_stop().bincode())
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }

    fn dispatch_from_cluster<'a, O>(
        &self,
        operations: Stream<KVSOperation<K, V>, Cluster<'a, KVSNode>, Unbounded, O>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<
        KVSOperation<K, V>,
        Cluster<'a, KVSNode>,
        Unbounded,
        Self::OutputOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::before_storage::BeforeExt;

    #[test]
    fn test_round_robin_router_creation() {
        let _router = RoundRobinRouter::new();
        let _router_default = RoundRobinRouter::new();
    }

    #[test]
    fn test_round_robin_router_implements_dispatch() {
        let router = RoundRobinRouter::new();
        fn _test_dispatch<K, V>(_dispatcher: impl Before<K, V>) {}
        _test_dispatch::<String, String>(router);
    }

    #[test]
    fn test_round_robin_router_implements_dispatch_ext() {
        let router = RoundRobinRouter::new();
        fn _test_dispatch_ext<K, V>(_dispatcher: impl BeforeExt<K, V>) {}
        _test_dispatch_ext::<String, String>(router);
    }
}
