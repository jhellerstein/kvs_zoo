//! Round Robin Router Operation dispatcher

use crate::before_storage::OpDispatch;
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
pub struct RoundRobinRouter;

impl RoundRobinRouter {
    pub fn new() -> Self { Self }
}

impl<V> OpDispatch<V> for RoundRobinRouter {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (hydro_lang::location::MemberId::from_raw(0u32), op)))
            .into_keyed()
            .demux_bincode(target_cluster)
    }

    fn dispatch_slotted_from_process<'a>(
        &self,
        slotted_operations: Stream<(usize, KVSOperation<V>), Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        slotted_operations
            .map(q!(|slotted_op| (hydro_lang::location::MemberId::from_raw(0u32), slotted_op)))
            .into_keyed()
            .demux_bincode(target_cluster)
            .assume_ordering(nondet!(/** routed slotted to single member */))
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
            .map(q!(|op| (hydro_lang::location::MemberId::from_raw(0u32), op)))
            .into_keyed()
            .demux_bincode(target_cluster)
            .values()
            .assume_ordering(nondet!(/** cluster hop routed */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::before_storage::OpDispatchExt;

    #[test]
    fn test_round_robin_router_creation() {
        let _router = RoundRobinRouter::new();
        let _router_default = RoundRobinRouter::new();
    }

    #[test]
    fn test_round_robin_router_implements_dispatch() {
        let router = RoundRobinRouter::new();
        fn _test_dispatch<V>(_dispatcher: impl OpDispatch<V>) {}
        _test_dispatch::<String>(router);
    }

    #[test]
    fn test_round_robin_router_implements_dispatch_ext() {
        let router = RoundRobinRouter::new();
        fn _test_dispatch_ext<V>(_dispatcher: impl OpDispatchExt<V>) {}
        _test_dispatch_ext::<String>(router);
    }
}
