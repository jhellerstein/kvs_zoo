use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use crate::routers::KVSRouter;
use crate::sharded::KVSShardable;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// KVS server that works with any routing strategy and KVS implementation
pub struct KVSServer<V, K, R>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
    K: KVSShardable<V>,
    R: KVSRouter<V>,
{
    _phantom: std::marker::PhantomData<(V, K, R)>,
}

impl<V, K, R> KVSServer<V, K, R>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    K: KVSShardable<V>,
    R: KVSRouter<V>,
{
    /// Run a KVS cluster with configurable routing and KVS implementation
    pub fn run<'a>(
        proxy: &Process<'a, ()>,
        cluster: &Cluster<'a, KVSNode>,
        client_external: &External<'a, ()>,
        router: &R,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, Option<V>), NoOrder>,
    ) {
        // Get operations from external clients
        let (input_port, operations) = proxy.source_external_bincode(client_external);

        // Route operations using the provided router
        let routed_operations = router.route_operations(operations, cluster);

        // Demux operations into puts and gets
        let (put_tuples, get_keys) = crate::core::KVSCore::demux_ops(routed_operations);

        // Execute routed operations using the KVS implementation
        let ticker = cluster.tick();
        let kvs_state = K::put(put_tuples, cluster);
        let get_results = K::get(
            get_keys.batch(&ticker, nondet!(/** batch gets for efficiency */)),
            kvs_state.snapshot(&ticker, nondet!(/** snapshot for gets */)),
        );

        // Send results back to clients
        let proxy_results = get_results.all_ticks().send_bincode(proxy).values();
        let get_results_port = proxy_results.send_bincode_external(client_external);

        (input_port, get_results_port)
    }
}

// Type aliases moved to src/kvs_types.rs for better organization
// Re-export the new KVS-prefix names (clean, consistent naming)
pub use crate::kvs_types::{
    KVSLocalLww, KVSReplicatedEpidemicGossip, KVSReplicatedBroadcast,
    KVSShardedLww, KVSShardedReplicatedEpidemicGossip, KVSShardedReplicatedBroadcast,
    StringKVSLocalLww, StringKVSReplicatedEpidemicGossip, StringKVSReplicatedBroadcast,
    StringKVSShardedLww, StringKVSShardedReplicatedEpidemicGossip, StringKVSShardedReplicatedBroadcast,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::{ReplicatedRouter, ShardedRouter};

    #[tokio::test]
    async fn test_sharded_kvs() {
        let flow = hydro_lang::compile::builder::FlowBuilder::new();
        let proxy = flow.process::<()>();
        let cluster = flow.cluster::<crate::core::KVSNode>();
        let client_external = flow.external::<()>();

        // Test that the API compiles and can be called
        let (_input_port, _output_port) = KVSServer::<
            String,
            crate::lww::LwwKVS,
            ShardedRouter,
        >::run(
            &proxy,
            &cluster,
            &client_external,
            &ShardedRouter::new(3),
        );

        // Finalize the flow to avoid the warning
        let _nodes = flow.finalize();

        println!("✅ KVSServer with ShardedRouter compiles successfully!");
    }

    #[tokio::test]
    async fn test_replicated_kvs() {
        let flow = hydro_lang::compile::builder::FlowBuilder::new();
        let proxy = flow.process::<()>();
        let cluster = flow.cluster::<crate::core::KVSNode>();
        let client_external = flow.external::<()>();

        // Test that the API compiles with replicated KVS using CausalString
        let (_input_port, _output_port) = KVSServer::<
            crate::examples_support::CausalString,
            crate::replicated::EpidemicReplicatedKVS<crate::examples_support::CausalString>,
            ReplicatedRouter,
        >::run(
            &proxy,
            &cluster,
            &client_external,
            &ReplicatedRouter,
        );

        // Finalize the flow to avoid the warning
        let _nodes = flow.finalize();

        println!("✅ KVSServer with ReplicatedRouter compiles successfully!");
    }

    #[tokio::test]
    async fn test_sharded_replicated_kvs() {
        let flow = hydro_lang::compile::builder::FlowBuilder::new();
        let proxy = flow.process::<()>();
        let cluster = flow.cluster::<crate::core::KVSNode>();
        let client_external = flow.external::<()>();

        // Test the combination: Sharded + Replicated using CausalString
        let (_input_port, _output_port) = KVSServer::<
            crate::examples_support::CausalString,
            crate::replicated::EpidemicReplicatedKVS<crate::examples_support::CausalString>,
            ShardedRouter,
        >::run(
            &proxy,
            &cluster,
            &client_external,
            &ShardedRouter::new(3),
        );

        // Finalize the flow to avoid the warning
        let _nodes = flow.finalize();

        println!("✅ KVSServer with ShardedRouter + ReplicatedKVS compiles successfully!");
    }
}