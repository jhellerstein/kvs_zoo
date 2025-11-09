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
use crate::cluster_spec::KVSCluster;
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

    /// Convenience method: Deploy and run the KVS server with default configuration
    ///
    /// This combines `deploy()` and `run()` into a single call for simpler examples.
    /// For custom configuration (e.g., shard count, Paxos config), use `deploy()` and `run()` separately.
    ///
    /// Returns a tuple of (deployment, server_port).
    /// Call `.kvs_cluster()` on the deployment to get the cluster for the flow builder.
    pub fn deploy_and_run<'a>(
        flow: &FlowBuilder<'a>,
        proxy: &Process<'a, ()>,
        client_external: &External<'a, ()>,
    ) -> (D::Deployment<'a>, ServerPorts<V>)
    where
        V: std::fmt::Debug + Send + Sync,
    {
        let (deployment, dispatch, maintenance) = Self::deploy(flow);
        let port = Self::run(proxy, &deployment, client_external, dispatch, maintenance);
        (deployment, port)
    }
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
    D: OpIntercept<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
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

// =============================================================================
// Configuration Helpers for Specific Dispatchers
// =============================================================================

impl<V, M> KVSServer<V, crate::dispatch::PaxosInterceptor<V>, M>
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
    M: ReplicationStrategy<V> + Clone + Default,
{
    /// Deploy a Paxos-based KVS server with custom configuration
    ///
    /// This allows configuring the Paxos parameters (like fault tolerance f).
    pub fn deploy_with_paxos_config<'a>(
        flow: &FlowBuilder<'a>,
        paxos_config: crate::dispatch::PaxosConfig,
    ) -> (
        <crate::dispatch::PaxosInterceptor<V> as OpIntercept<V>>::Deployment<'a>,
        crate::dispatch::PaxosInterceptor<V>,
        M,
    ) {
        let dispatch = crate::dispatch::PaxosInterceptor::with_config(paxos_config);
        let maintenance = M::default();
        let deployment = dispatch.create_deployment(flow);
        (deployment, dispatch, maintenance)
    }
}

// =============================================================================
// Builder API for Easy Setup
// =============================================================================

/// Builder for setting up a KVS server with minimal boilerplate
pub struct KVSBuilder<V, D, M>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
    D: OpIntercept<V>,
    M: ReplicationStrategy<V>,
{
    num_nodes: usize,
    num_aux1: Option<usize>,
    num_aux2: Option<usize>,
    dispatch: Option<D>,
    maintenance: Option<M>,
    _phantom: std::marker::PhantomData<V>,
}

impl<V, D, M> KVSBuilder<V, D, M>
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
    D: OpIntercept<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
    /// Create a new KVS builder
    pub fn new() -> Self {
        Self {
            num_nodes: 1,
            num_aux1: None,
            num_aux2: None,
            dispatch: None,
            maintenance: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a builder from a cluster specification
    /// 
    /// This is the preferred API for unambiguous topology definition:
    /// ```ignore
    /// // 3 shards × 3 replicas = 9 nodes
    /// let spec = KVSCluster::sharded_replicated(3, 3);
    /// let builder = Server::from_spec(spec);
    /// ```
    pub fn from_spec(spec: KVSCluster) -> Self {
        Self {
            num_nodes: spec.total_nodes(),
            num_aux1: None,
            num_aux2: None,
            dispatch: None,
            maintenance: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the number of KVS cluster nodes to deploy
    /// 
    /// **Semantics depend on your dispatch strategy:**
    /// - `SingleNodeRouter`: 1 node (ignores this setting)
    /// - `RoundRobinRouter`: N replicas (this is the replica count)
    /// - `ShardedRouter(S)`: Should be S (one node per shard)
    /// - `Pipeline<ShardedRouter(S), RoundRobinRouter>`: Should be S (one per shard)
    ///   - To get S shards × R replicas, you need to configure the dispatch Pipeline properly
    ///   - The total nodes deployed = S, but routing distributes across S partitions
    /// 
    /// **For nested cluster architectures** (like sharded+replicated):
    /// This sets the number at the **outermost level** of the routing pipeline.
    /// Inner replication/routing is handled by the dispatch strategy's logic.
    pub fn with_cluster_size(mut self, num_nodes: usize) -> Self {
        self.num_nodes = num_nodes;
        self
    }

    /// Deprecated: Use `with_cluster_size()` instead
    #[deprecated(note = "Use with_cluster_size() for clarity about what is being configured")]
    pub fn with_nodes(self, num_nodes: usize) -> Self {
        self.with_cluster_size(num_nodes)
    }

    /// Set the number of aux1 cluster nodes (for multi-cluster deployments like Paxos)
    pub fn with_aux1_nodes(mut self, num_aux1: usize) -> Self {
        self.num_aux1 = Some(num_aux1);
        self
    }

    /// Set the number of aux2 cluster nodes (for multi-cluster deployments like Paxos)
    pub fn with_aux2_nodes(mut self, num_aux2: usize) -> Self {
        self.num_aux2 = Some(num_aux2);
        self
    }

    /// Set a custom dispatch strategy (instead of using Default)
    pub fn with_dispatch(mut self, dispatch: D) -> Self {
        self.dispatch = Some(dispatch);
        self
    }

    /// Set a custom maintenance strategy (instead of using Default)
    pub fn with_maintenance(mut self, maintenance: M) -> Self {
        self.maintenance = Some(maintenance);
        self
    }

    /// Build and return a ready-to-use KVS deployment
    /// 
    /// Returns (deployment, out stream, input sink)
    pub async fn build(
        mut self,
    ) -> Result<(
        hydro_deploy::Deployment,
        std::pin::Pin<Box<dyn futures::Stream<Item = String>>>,
        std::pin::Pin<Box<dyn futures::Sink<KVSOperation<V>, Error = std::io::Error>>>,
    ), Box<dyn std::error::Error>>
    where
        D: Default,
        M: Default,
    {
        let dispatch = self.dispatch.take().unwrap_or_default();
        let maintenance = self.maintenance.take().unwrap_or_default();
        self.build_with(dispatch, maintenance).await
    }

    /// Build with explicit dispatch and maintenance (no Default required)
    pub async fn build_with(
        self,
        dispatch: D,
        maintenance: M,
    ) -> Result<(
        hydro_deploy::Deployment,
        std::pin::Pin<Box<dyn futures::Stream<Item = String>>>,
        std::pin::Pin<Box<dyn futures::Sink<KVSOperation<V>, Error = std::io::Error>>>,
    ), Box<dyn std::error::Error>>
    {
        let mut deployment_hdro = hydro_deploy::Deployment::new();
        let localhost = deployment_hdro.Localhost();
        let flow = hydro_lang::compile::builder::FlowBuilder::new();
        let proxy = flow.process::<()>();
        let client_external = flow.external::<()>();

        let clusters = dispatch.create_deployment(&flow);
        let port = KVSServer::<V, D, M>::run(&proxy, &clusters, &client_external, dispatch, maintenance);

        // Deploy flow with process, cluster, external wiring
        let nodes = flow
            .with_default_optimize()
            .with_process(&proxy, localhost.clone())
            .with_cluster(clusters.kvs_cluster(), vec![localhost.clone(); self.num_nodes])
            .with_external(&client_external, localhost)
            .deploy(&mut deployment_hdro);

        deployment_hdro.deploy().await?;
        let (out, input) = nodes.connect_bincode(port).await;

        Ok((deployment_hdro, out, input))
    }
}

impl<V, D, M> Default for KVSBuilder<V, D, M>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + std::fmt::Debug + std::fmt::Display + lattices::Merge<V> + Send + Sync + 'static,
    D: OpIntercept<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
    fn default() -> Self {
        Self::new()
    }
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
    D: OpIntercept<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
    /// Create a builder for easy setup
    pub fn builder() -> KVSBuilder<V, D, M> {
        KVSBuilder::new()
    }

    /// Create a builder from a cluster specification
    /// 
    /// Preferred API for unambiguous topology definition.
    pub fn from_spec(spec: KVSCluster) -> KVSBuilder<V, D, M> {
        KVSBuilder::from_spec(spec)
    }
}

// =============================================================================
// Convenient Type Aliases for Common Configurations
// =============================================================================

/// Type aliases for common server configurations
pub mod common {
    use super::*;
    use crate::dispatch::{SingleNodeRouter, RoundRobinRouter, ShardedRouter, Pipeline, PaxosInterceptor};
    use crate::maintenance::{NoReplication, EpidemicGossip, BroadcastReplication, LogBased};
    use crate::values::{LwwWrapper, CausalString};

    /// Local single-node server with LWW semantics
    pub type Local<V = LwwWrapper<String>> = KVSServer<V, SingleNodeRouter, ()>;

    /// Replicated server with no replication (for testing)
    pub type Replicated<V = CausalString> = KVSServer<V, RoundRobinRouter, NoReplication>;

    /// Replicated server with gossip replication
    pub type ReplicatedGossip<V = CausalString> = KVSServer<V, RoundRobinRouter, EpidemicGossip<V>>;

    /// Replicated server with broadcast replication
    pub type ReplicatedBroadcast<V = CausalString> = KVSServer<V, RoundRobinRouter, BroadcastReplication<V>>;

    /// Sharded local server (3 shards default)
    pub type ShardedLocal<V = LwwWrapper<String>> = 
        KVSServer<V, Pipeline<ShardedRouter, SingleNodeRouter>, ()>;

    /// Sharded + Replicated with broadcast
    pub type ShardedReplicated<V = CausalString> = 
        KVSServer<V, Pipeline<ShardedRouter, RoundRobinRouter>, BroadcastReplication<V>>;

    /// Linearizable server with Paxos and log-based replication
    pub type Linearizable<V = LwwWrapper<String>> = 
        KVSServer<V, PaxosInterceptor<V>, LogBased<BroadcastReplication<V>>>;
}
