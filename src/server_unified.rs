//! Unified KVS Server Architecture
//!
//! This module provides a single, composable server type parameterized by
//! dispatch and maintenance strategies.
//!
//! ## Usage
//!
//! ```ignore
//! // Local single-node server
//! type Local = KVSServer<LwwWrapper<String>, SingleNodeRouter, ()>;
//!
//! // Replicated with round-robin + gossip  
//! type Replicated = KVSServer<
//!     CausalString,
//!     RoundRobinRouter,
//!     EpidemicGossip<CausalString>
//! >;
//!
//! // Sharded + Replicated
//! type ShardedReplicated = KVSServer<
//!     CausalString,
//!     Pipeline<ShardedRouter, RoundRobinRouter>,
//!     BroadcastReplication<CausalString>
//! >;
//!
//! // Linearizable with Paxos
//! type Linearizable = KVSServer<
//!     LwwWrapper<String>,
//!     PaxosInterceptor<LwwWrapper<String>>,
//!     LogBased<BroadcastReplication<LwwWrapper<String>>>
//! >;
//!
//! // Deploy and run
//! let (deployment, dispatch, maintenance) = Server::deploy(&flow);
//! let port = Server::run(&proxy, &deployment, &client_external, dispatch, maintenance);
//! ```

use crate::dispatch::{OpIntercept, KVSDeployment};
use crate::kvs_core::KVSNode;
use crate::maintenance::ReplicationStrategy;
use crate::protocol::KVSOperation;
use hydro_lang::location::external_process::ExternalBincodeBidi;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

// Type aliases for server ports
type ServerBidiPort<V> =
    ExternalBincodeBidi<KVSOperation<V>, String, hydro_lang::location::external_process::Many>;
pub type ServerPorts<V> = ServerBidiPort<V>;

/// Unified KVS Server parameterized by Value, Dispatch, and Maintenance
///
/// This single struct handles all KVS architectures through composition.
pub struct KVSServer<V, D, M>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
    D: OpIntercept<V>,
    M: ReplicationStrategy<V>,
{
    _phantom: std::marker::PhantomData<(V, D, M)>,
}

impl<V, D, M> KVSServer<V, D, M>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
    D: OpIntercept<V> + Clone + Default,
    M: ReplicationStrategy<V> + Clone + Default,
{
    /// Deploy a KVS server with default dispatch and maintenance configuration
    pub fn deploy<'a>(
        flow: &FlowBuilder<'a>,
    ) -> (
        D::Deployment<'a>,
        D,
        M,
    ) {
        let dispatch = D::default();
        let maintenance = M::default();
        let deployment = dispatch.create_deployment(flow);
        (deployment, dispatch, maintenance)
    }

    /// Run the KVS server
    ///
    /// This is the universal run method that works for all dispatch/maintenance combinations.
    pub fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &D::Deployment<'a>,
        client_external: &External<'a, ()>,
        dispatch: D,
        maintenance: M,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Create bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Apply dispatch strategy to route operations
        let routed_operations = dispatch.intercept_operations(
            operations_stream
                .entries()
                .map(q!(|(_client_id, op)| op))
                .assume_ordering(nondet!(/** operations routing */)),
            deployment,
        );

        // Tag local operations (respond = true)
        let local_tagged = routed_operations.clone().map(q!(|op| (true, op)));

        // Extract local PUTs for replication
        let local_puts = routed_operations.filter_map(q!(|op| match op {
            KVSOperation::Put(k, v) => Some((k, v)),
            KVSOperation::Get(_) => None,
        }));

        // Apply maintenance strategy to replicate data
        // Use the KVS cluster from the deployment (works for both simple and Paxos deployments)
        let kvs_cluster = deployment.kvs_cluster();
        let replicated_puts = maintenance.replicate_data(kvs_cluster, local_puts);
        let replicated_tagged = replicated_puts.map(q!(|(k, v)| (false, KVSOperation::Put(k, v))));

        // Merge local and replicated operations
        let all_tagged = local_tagged
            .interleave(replicated_tagged)
            .assume_ordering(nondet!(/** sequential processing */));

        // Process operations with selective responses
        let responses = crate::kvs_core::KVSCore::process_with_responses(all_tagged);

        // Send responses back to clients
        let proxy_responses = responses.send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        bidi_port
    }
}
