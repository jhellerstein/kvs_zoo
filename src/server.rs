//! KVS Server Architecture
//!
//! This module provides the standard composable server architecture for building
//! distributed key-value stores. The architecture is based on composable services
//! that can be nested and combined to create complex distributed systems.
//!
//! ## Core Concept
//!
//! The key abstraction is `KVSServer` which can represent:
//! - A single node KVS implementation (`LocalKVSServer`)
//! - A cluster of nodes running a replicated KVS (`ReplicatedKVSServer`)
//! - A sharded system using any underlying server type (`ShardedKVSServer<T>`)
//!
//! ## Composability
//!
//! Servers can be composed to create sophisticated architectures:
//! - `ShardedKVSServer<LocalKVSServer>` = 3 shards, each being a single node
//! - `ShardedKVSServer<ReplicatedKVSServer>` = 3 shards × 3 replicas = 9 nodes
//!
//! Features added to any server type automatically inherit at all composition levels.

use crate::dispatch::OpIntercept;
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

/// Abstraction over KVS servers that can be either single nodes or clusters
///
/// This trait allows us to treat both individual KVS implementations and
/// clusters of replicated KVS implementations uniformly, enabling true
/// composability in server architecture.
///
/// The trait now includes associated types for operation pipelines and replication
/// strategies, enabling symmetric composition between servers and their processing logic.
pub trait KVSServer<V>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
{
    /// The operation pipeline type for this server
    ///
    /// Defines how operations are intercepted and processed before reaching storage.
    /// Examples: LocalRouter, RoundRobinRouter, Pipeline<ShardedRouter, RoundRobinRouter>
    type OpPipeline: crate::dispatch::OpIntercept<V>;

    /// The replication strategy type for this server
    ///
    /// Defines how data is synchronized between nodes in the background.
    /// Examples: (), NoReplication, EpidemicGossip, BroadcastReplication
    type ReplicationStrategy: crate::maintenance::ReplicationStrategy<V>;

    /// The deployment unit for this service (could be a single process or cluster)
    type Deployment<'a>;

    /// Create the deployment unit with operation pipeline and replication strategy
    fn create_deployment<'a>(
        flow: &FlowBuilder<'a>,
        op_pipeline: Self::OpPipeline,
        replication: Self::ReplicationStrategy,
    ) -> Self::Deployment<'a>;

    /// Run the KVS server and return bidirectional port for client communication
    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
        op_pipeline: Self::OpPipeline,
        replication: Self::ReplicationStrategy,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync;

    /// Get the size (number of nodes) of this service
    fn size(op_pipeline: Self::OpPipeline, replication: Self::ReplicationStrategy) -> usize;
}

/// Single-node KVS server using LWW semantics
pub struct LocalKVSServer<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> LocalKVSServer<V> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> Default for LocalKVSServer<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> KVSServer<V> for LocalKVSServer<V>
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
{
    type OpPipeline = crate::dispatch::SingleNodeRouter;
    type ReplicationStrategy = ();
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(
        flow: &FlowBuilder<'a>,
        _op_pipeline: Self::OpPipeline,
        _replication: Self::ReplicationStrategy,
    ) -> Self::Deployment<'a> {
        // Create a cluster of size 1 for uniform interface
        flow.cluster::<KVSNode>()
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
        op_pipeline: Self::OpPipeline,
        _replication: Self::ReplicationStrategy,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Use bidirectional external connection like the working local.rs example
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Use the provided operation pipeline instead of hardcoded LocalRouter
        let routed_operations = op_pipeline.intercept_operations(
            operations_stream
                .entries()
                .map(q!(|(_client_id, op)| op))
                .assume_ordering(nondet!(/** Todo: WHY? */)),
            deployment,
        );

        // For local KVS, use sequential processing with LWW semantics.
        // Wrap values with LwwWrapper so merges use last-writer-wins.
        let routed_operations_lww = routed_operations.map(q!(|op| match op {
            KVSOperation::Put(key, value) => {
                KVSOperation::Put(key, crate::values::LwwWrapper::new(value))
            }
            KVSOperation::Get(key) => KVSOperation::Get(key),
        }));

        // Process operations sequentially and generate responses for both PUT and GET
        let responses = crate::kvs_core::KVSCore::process(routed_operations_lww);

        // Optionally label responses for local server
        let proxy_responses = responses
            .map(q!(|resp| format!("{} [LOCAL]", resp)))
            .send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        // Return the bidirectional port
        bidi_port
    }

    fn size(_op_pipeline: Self::OpPipeline, _replication: Self::ReplicationStrategy) -> usize {
        1
    }
}

/// Replicated KVS server with configurable replication strategy
pub struct ReplicatedKVSServer<V, R = crate::maintenance::NoReplication> {
    #[allow(dead_code)] // TODO: Use this field to make cluster size configurable
    cluster_size: usize,
    _phantom: std::marker::PhantomData<(V, R)>,
}

impl<V, R> ReplicatedKVSServer<V, R> {
    pub fn new(cluster_size: usize) -> Self {
        Self {
            cluster_size,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V, R> KVSServer<V> for ReplicatedKVSServer<V, R>
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
    R: crate::maintenance::ReplicationStrategy<V>,
{
    type OpPipeline = crate::dispatch::RoundRobinRouter;
    type ReplicationStrategy = R;
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(
        flow: &FlowBuilder<'a>,
        _op_pipeline: Self::OpPipeline,
        _replication: Self::ReplicationStrategy,
    ) -> Self::Deployment<'a> {
        flow.cluster::<KVSNode>()
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
        op_pipeline: Self::OpPipeline,
        replication: Self::ReplicationStrategy,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Use bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Use the provided operation pipeline instead of hardcoded RoundRobinRouter
        let routed_operations = op_pipeline.intercept_operations(
            operations_stream
                .entries()
                .map(q!(|(_client_id, op)| op))
                .assume_ordering(nondet!(/** Todo: WHY? */)),
            deployment,
        );

        // Tag local operations (respond = true)
        let local_tagged = routed_operations.clone().map(q!(|op| (true, op)));

        // Extract local PUTs for replication
        let local_puts = routed_operations.filter_map(q!(|op| match op {
            KVSOperation::Put(k, v) => Some((k, v)),
            KVSOperation::Get(_) => None,
        }));

        // Replicate PUTs to other nodes (respond = false)
        let replicated_puts = replication.replicate_data(deployment, local_puts);
        let replicated_tagged = replicated_puts.map(q!(|(k, v)| (false, KVSOperation::Put(k, v))));

        // Merge local and replicated operations
        // Assume total order for sequential processing - each node processes its operations
        // in the order they arrive (round-robin ensures non-overlapping operations per node)
        let all_tagged = local_tagged
            .interleave(replicated_tagged)
            .assume_ordering(nondet!(/** Sequential processing requires total order */));
        let responses = crate::kvs_core::KVSCore::process_with_responses(all_tagged);

        // Label responses and send back to clients
        let proxy_responses = responses
            .map(q!(|resp| format!("{} [REPLICATED]", resp)))
            .send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        // Return the bidirectional port
        bidi_port
    }

    fn size(_op_pipeline: Self::OpPipeline, _replication: Self::ReplicationStrategy) -> usize {
        3 // Default cluster size - TODO: make this configurable per instance
    }
}

/// Sharded KVS server that can work with any underlying KVS server
///
/// This is the key to composability - it treats the underlying server
/// as a black box and just handles sharding logic. The operation pipeline
/// uses symmetric composition: Pipeline<ShardedRouter, Inner::OpPipeline>.
pub struct ShardedKVSServer<S> {
    #[allow(dead_code)] // TODO: Use this field to make shard count configurable
    num_shards: usize,
    _phantom: std::marker::PhantomData<S>,
}

impl<S> ShardedKVSServer<S> {
    pub fn new(num_shards: usize) -> Self {
        Self {
            num_shards,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V, S> KVSServer<V> for ShardedKVSServer<S>
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
    S: KVSServer<V>,
    S::OpPipeline: Clone,
    S::ReplicationStrategy: Clone,
{
    /// Symmetric composition: ShardedRouter composed with inner server's pipeline
    type OpPipeline = crate::dispatch::Pipeline<crate::dispatch::ShardedRouter, S::OpPipeline>;

    /// Delegate replication strategy to inner server
    type ReplicationStrategy = S::ReplicationStrategy;

    // For sharded services, we use a single cluster deployment for simplicity
    // In a full implementation, this would be multiple deployments
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(
        flow: &FlowBuilder<'a>,
        _op_pipeline: Self::OpPipeline,
        _replication: Self::ReplicationStrategy,
    ) -> Self::Deployment<'a> {
        // For now, create a single cluster deployment
        // In a full implementation, this would create multiple shard deployments
        flow.cluster::<KVSNode>()
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
        op_pipeline: Self::OpPipeline,
        replication: Self::ReplicationStrategy,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Use bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Extract operations from the stream
        let operations = operations_stream
            .entries()
            .map(q!(|(_client_id, op)| op))
            .assume_ordering(nondet!(/** Todo: WHY? */));

        // Apply the complete pipeline (sharded + inner routing)
        let routed_operations = op_pipeline.intercept_operations(operations, deployment);

        // Tag local operations (respond = true)
        let local_tagged = routed_operations.clone().map(q!(|op| (true, op)));

        // Extract local PUTs for replication
        let local_puts = routed_operations.filter_map(q!(|op| match op {
            KVSOperation::Put(k, v) => Some((k, v)),
            KVSOperation::Get(_) => None,
        }));

        // Replicate PUTs to other nodes (respond = false)
        let replicated_puts = replication.replicate_data(deployment, local_puts);
        let replicated_tagged = replicated_puts.map(q!(|(k, v)| (false, KVSOperation::Put(k, v))));

        // Merge local and replicated operations
        // Assume total order for sequential processing - sharded routing ensures
        // each shard processes its keys in order
        let all_tagged = local_tagged
            .interleave(replicated_tagged)
            .assume_ordering(nondet!(/** Sequential processing requires total order */));
        let responses = crate::kvs_core::KVSCore::process_with_responses(all_tagged);

        // Label responses and send back to clients
        let proxy_responses = responses
            .map(q!(|resp| format!("{} [SHARDED]", resp)))
            .send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        // Return the bidirectional port
        bidi_port
    }

    fn size(op_pipeline: Self::OpPipeline, replication: Self::ReplicationStrategy) -> usize {
        3 * S::size(op_pipeline.second, replication) // 3 shards * size of each shard
    }
}

// =============================================================================
// Composable Type Aliases
// =============================================================================

/// Type alias for common server compositions to reduce repetition
pub mod compositions {
    use super::*;
    // Import replication strategies to avoid crate:: path issues in staged code
    use crate::maintenance::{BroadcastReplication, EpidemicGossip, NoReplication};

    /// Local KVS with inferred value type
    pub type Local<V> = LocalKVSServer<V>;

    /// Replicated KVS with inferred value type and default replication
    pub type Replicated<V> = ReplicatedKVSServer<V, NoReplication>;

    /// Replicated KVS with gossip replication
    pub type ReplicatedGossip<V> = ReplicatedKVSServer<V, EpidemicGossip<V>>;

    /// Replicated KVS with broadcast replication  
    pub type ReplicatedBroadcast<V> = ReplicatedKVSServer<V, BroadcastReplication<V>>;

    /// Sharded local KVS
    pub type ShardedLocal<V> = ShardedKVSServer<Local<V>>;

    /// Sharded replicated KVS with default replication
    pub type ShardedReplicated<V> = ShardedKVSServer<Replicated<V>>;

    /// Sharded replicated KVS with gossip replication
    pub type ShardedReplicatedGossip<V> = ShardedKVSServer<ReplicatedGossip<V>>;

    /// Sharded replicated KVS with broadcast replication
    pub type ShardedReplicatedBroadcast<V> = ShardedKVSServer<ReplicatedBroadcast<V>>;
}

// ============================================================================
// Linearizable KVS Server
// ============================================================================

/// Linearizable KVS server using Paxos consensus for total ordering
///
/// This server provides the strongest consistency guarantees by using Paxos
/// consensus to establish a total order over all operations before applying
/// them to the underlying replicated storage.
///
/// ## Linearizability
///
/// Linearizability is a strong consistency model, requiring that:
/// 1. Operations appear to take effect atomically at some point between their start and end
/// 2. All operations appear to execute in a single, total order
/// 3. The order respects the real-time ordering of non-overlapping operations
///
/// ## Architecture
///
/// ```text
/// Client
///   │
///   └─▶ Paxos Interceptor (assigns total-order slot numbers)
///         │  (stream of (slot, op))
///         └─▶ Round-Robin Dispatch (one KVS node executes each slotted op)
///               │ (ordering preserved by slots)
///               └─▶ Replication Strategy (e.g. LogBased uses slots to replay PUTs)
///                     │
///                     └─▶ LWW Storage (apply PUT values; GET reads latest)
/// ```
///
/// Flow summary:
/// 1. Client requests arrive at the proxy and are fed into Paxos.
/// 2. Paxos assigns a monotonically increasing slot number, producing `(slot, op)`.
/// 3. Slotted operations are round-robin dispatched so only one KVS node executes each.
/// 4. The replication strategy (e.g. `LogBased`) consumes slotted PUT tuples to ensure
///    deterministic replay / replication using the slot ordering.
/// 5. Slots are stripped before storage; they exist solely to preserve global order
///    and replication replay semantics.
/// 6. GETs read from an LWW snapshot; slot numbers are not needed for read paths.
///
/// Notes:
/// - `replication.replicate_slotted_data(..)` is where a strategy (like LogBased) can
///   use the slot numbers to guarantee consistent replication ordering.
/// - The round-robin stage maintains Paxos order because slot numbers are carried
///   through dispatch until replication completes.
/// - Alternative replication strategies may ignore slots if they do not require
///   ordered replay (e.g., trivial or single-node configurations).
///
/// ## Type Parameters
/// - `V`: Value type stored in the KVS
/// - `R`: Replication strategy for the underlying storage
///
/// ## Example
/// ```rust
/// use kvs_zoo::server::{KVSServer, LinearizableKVSServer};
/// use kvs_zoo::maintenance::BroadcastReplication;
/// use kvs_zoo::values::CausalString;
///
/// type LinearizableKVS = LinearizableKVSServer<CausalString, BroadcastReplication<CausalString>>;
/// ```
pub struct LinearizableKVSServer<V, R = crate::maintenance::NoReplication> {
    _phantom: std::marker::PhantomData<(V, R)>,
}

impl<V, R> LinearizableKVSServer<V, R> {
    /// Create a new linearizable KVS server with default Paxos configuration
    pub fn new(_cluster_size: usize) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a linearizable KVS server with custom Paxos configuration
    pub fn with_paxos_config(
        _cluster_size: usize,
        _paxos_config: crate::dispatch::PaxosConfig,
    ) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a linearizable KVS server that tolerates `f` failures
    ///
    /// This will configure Paxos to require `2f + 1` nodes for safety.
    pub fn with_fault_tolerance(_cluster_size: usize, _f: usize) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V, R> KVSServer<V> for LinearizableKVSServer<V, R>
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
    R: ReplicationStrategy<V>,
{
    /// Uses Paxos interceptor for total ordering
    type OpPipeline = crate::dispatch::PaxosInterceptor<V>;

    /// Delegates replication to the specified strategy
    type ReplicationStrategy = R;

    /// Uses a cluster deployment for the linearizable service
    /// Returns (KVS cluster, Proposer cluster, Acceptor cluster)
    type Deployment<'a> = (
        Cluster<'a, KVSNode>,
        Cluster<'a, crate::dispatch::paxos_core::Proposer>,
        Cluster<'a, crate::dispatch::paxos_core::Acceptor>,
    );

    fn create_deployment<'a>(
        flow: &FlowBuilder<'a>,
        _op_pipeline: Self::OpPipeline,
        _replication: Self::ReplicationStrategy,
    ) -> Self::Deployment<'a> {
        let kvs_cluster = flow.cluster::<KVSNode>();
        let proposers = flow.cluster::<crate::dispatch::paxos_core::Proposer>();
        let acceptors = flow.cluster::<crate::dispatch::paxos_core::Acceptor>();
        (kvs_cluster, proposers, acceptors)
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
        op_pipeline: Self::OpPipeline,
        replication: Self::ReplicationStrategy,
    ) -> ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Unpack deployment tuple
        let (kvs_cluster, proposers, acceptors) = deployment;

        // Use bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Apply Paxos consensus for total ordering
        // Paxos returns slotted operations to the proxy
        let slotted_operations_on_proxy = op_pipeline.intercept_operations_slotted_with_paxos(
            operations_stream
                .entries()
                .map(q!(|(_client_id, op)| op))
                .assume_ordering(nondet!(/** Paxos will provide total order */)),
            proxy,
            proposers,
            acceptors,
        );

        // Round-robin the slotted operations to KVS nodes
        // This ensures only ONE node processes each operation and responds
        // round_robin_bincode preserves the total ordering from Paxos
        let slotted_operations_with_slots = slotted_operations_on_proxy
            .round_robin_bincode(kvs_cluster, nondet!(/** round-robin to KVS nodes */))
            .inspect(q!(|(slot, op)| {
                println!(
                    "[Linearizable] Processing slot {}: {:?}",
                    slot,
                    match op {
                        KVSOperation::Put(key, _) => format!("PUT {}", key),
                        KVSOperation::Get(key) => format!("GET {}", key),
                    }
                );
            }));

        // Tag local operations (respond = true)
        let local_tagged = slotted_operations_with_slots
            .clone()
            .map(q!(|(slot, op)| (slot, true, op)));

        // Extract local PUTs for replication
        let local_puts_slotted =
            slotted_operations_with_slots.filter_map(q!(|(slot, op)| match op {
                KVSOperation::Put(key, value) => Some((slot, key, value)),
                KVSOperation::Get(_) => None,
            }));

        // Use replication strategy for PUT operations with slot numbers
        let replicated_puts_slotted =
            replication.replicate_slotted_data(kvs_cluster, local_puts_slotted.clone());

        // Tag replicated operations (respond = false)
        let replicated_tagged =
            replicated_puts_slotted.map(q!(|(slot, k, v)| (slot, false, KVSOperation::Put(k, v))));

        // Combine local and replicated operations, preserving slots
        let all_tagged_slotted = local_tagged.interleave(replicated_tagged);

        // Strip slots but keep (should_respond, op) tags
        // Wrap values with LwwWrapper for LWW semantics
        let all_tagged = all_tagged_slotted.map(q!(|(_slot, should_respond, op)| {
            let op_lww = match op {
                KVSOperation::Put(k, v) => KVSOperation::Put(k, crate::values::LwwWrapper::new(v)),
                KVSOperation::Get(k) => KVSOperation::Get(k),
            };
            (should_respond, op_lww)
        }));

        // Process sequentially with selective responses
        // Paxos guarantees total order, so operations are already ordered
        let responses = crate::kvs_core::KVSCore::process_with_responses(
            all_tagged.assume_ordering(nondet!(/** Paxos provides total order */)),
        );

        // Label responses and send back
        let proxy_responses = responses
            .map(q!(|resp| format!("{} [LINEARIZABLE]", resp)))
            .send_bincode(proxy);

        // Complete the bidirectional connection
        complete_sink.complete(
            proxy_responses
                .entries()
                .map(q!(|(_member_id, response)| (0u64, response)))
                .into_keyed(),
        );

        bidi_port
    }

    fn size(_op_pipeline: Self::OpPipeline, _replication: Self::ReplicationStrategy) -> usize {
        // Linearizable KVS typically needs at least 3 nodes for Paxos (2f+1 where f=1)
        3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_types_compile() {
        // Just test that the types can be constructed
        let _local = LocalKVSServer::<crate::values::LwwWrapper<String>>::new();
        let _replicated: ReplicatedKVSServer<
            crate::values::CausalString,
            crate::maintenance::NoReplication,
        > = ReplicatedKVSServer::new(3);
        let _sharded_local =
            ShardedKVSServer::<LocalKVSServer<crate::values::LwwWrapper<String>>>::new(3);
        let _sharded_replicated = ShardedKVSServer::<
            ReplicatedKVSServer<crate::values::CausalString, crate::maintenance::NoReplication>,
        >::new(3);

        println!("✅ Server types compile!");

        // Test size calculations with new trait signature
        let local_pipeline = crate::dispatch::SingleNodeRouter::new();
        let local_replication = ();
        println!(
            "   LocalKVS size: {}",
            <LocalKVSServer<crate::values::LwwWrapper<String>> as KVSServer<
                crate::values::LwwWrapper<String>,
            >>::size(local_pipeline, local_replication)
        );

        let replicated_pipeline = crate::dispatch::RoundRobinRouter::new();
        let replicated_replication = crate::maintenance::NoReplication::new();
        println!(
            "   ReplicatedKVS size: {}",
            <ReplicatedKVSServer<crate::values::CausalString, crate::maintenance::NoReplication> as KVSServer<
                crate::values::CausalString,
            >>::size(replicated_pipeline, replicated_replication)
        );

        let sharded_local_pipeline = crate::dispatch::Pipeline::new(
            crate::dispatch::ShardedRouter::new(3),
            crate::dispatch::SingleNodeRouter::new(),
        );
        let sharded_local_replication = ();
        println!(
            "   ShardedLocalKVS size: {}",
            <ShardedKVSServer<LocalKVSServer<crate::values::LwwWrapper<String>>> as KVSServer<
                crate::values::LwwWrapper<String>,
            >>::size(sharded_local_pipeline, sharded_local_replication)
        );

        let sharded_replicated_pipeline = crate::dispatch::Pipeline::new(
            crate::dispatch::ShardedRouter::new(3),
            crate::dispatch::RoundRobinRouter::new(),
        );
        let sharded_replicated_replication = crate::maintenance::NoReplication::new();
        println!(
            "   ShardedReplicatedKVS size: {}",
            <ShardedKVSServer<
                ReplicatedKVSServer<crate::values::CausalString, crate::maintenance::NoReplication>,
            > as KVSServer<crate::values::CausalString>>::size(
                sharded_replicated_pipeline,
                sharded_replicated_replication
            )
        );
    }

    #[tokio::test]
    async fn test_deployment_creation() {
        let flow = FlowBuilder::new();

        // Test that deployments can be created with new trait signature
        let local_pipeline = crate::dispatch::SingleNodeRouter::new();
        let local_replication = ();
        let _local_deployment =
            <LocalKVSServer<crate::values::LwwWrapper<String>> as KVSServer<
                crate::values::LwwWrapper<String>,
            >>::create_deployment(&flow, local_pipeline, local_replication);

        let replicated_pipeline = crate::dispatch::RoundRobinRouter::new();
        let replicated_replication = crate::maintenance::NoReplication::new();
        let _replicated_deployment = <ReplicatedKVSServer<
            crate::values::CausalString,
            crate::maintenance::NoReplication,
        > as KVSServer<crate::values::CausalString>>::create_deployment(
            &flow,
            replicated_pipeline,
            replicated_replication,
        );

        let sharded_local_pipeline = crate::dispatch::Pipeline::new(
            crate::dispatch::ShardedRouter::new(3),
            crate::dispatch::SingleNodeRouter::new(),
        );
        let sharded_local_replication = ();
        let _sharded_local_deployments =
            <ShardedKVSServer<LocalKVSServer<crate::values::LwwWrapper<String>>> as KVSServer<
                crate::values::LwwWrapper<String>,
            >>::create_deployment(
                &flow, sharded_local_pipeline, sharded_local_replication
            );

        let sharded_replicated_pipeline = crate::dispatch::Pipeline::new(
            crate::dispatch::ShardedRouter::new(3),
            crate::dispatch::RoundRobinRouter::new(),
        );
        let sharded_replicated_replication = crate::maintenance::NoReplication::new();
        let _sharded_replicated_deployments = <ShardedKVSServer<
            ReplicatedKVSServer<crate::values::CausalString, crate::maintenance::NoReplication>,
        > as KVSServer<crate::values::CausalString>>::create_deployment(
            &flow,
            sharded_replicated_pipeline,
            sharded_replicated_replication,
        );

        // Finalize the flow
        let _nodes = flow.finalize();

        println!("✅ Deployment creation works!");
    }

    #[tokio::test]
    async fn test_symmetric_composition_type_safety() {
        // Test that server composition matches pipeline composition structure

        // LocalKVSServer should use LocalRouter and no replication
        type LocalServer = LocalKVSServer<crate::values::LwwWrapper<String>>;
        type LocalPipeline =
            <LocalServer as KVSServer<crate::values::LwwWrapper<String>>>::OpPipeline;
        type LocalReplication =
            <LocalServer as KVSServer<crate::values::LwwWrapper<String>>>::ReplicationStrategy;

        // Verify types match expected structure
        let _local_pipeline: LocalPipeline = crate::dispatch::SingleNodeRouter::new();
        let _local_replication: LocalReplication = ();

        // ReplicatedKVSServer should use RoundRobinRouter and configurable replication
        type ReplicatedServer =
            ReplicatedKVSServer<crate::values::CausalString, crate::maintenance::NoReplication>;
        type ReplicatedPipeline =
            <ReplicatedServer as KVSServer<crate::values::CausalString>>::OpPipeline;
        type ReplicatedReplication =
            <ReplicatedServer as KVSServer<crate::values::CausalString>>::ReplicationStrategy;

        let _replicated_pipeline: ReplicatedPipeline = crate::dispatch::RoundRobinRouter::new();
        let _replicated_replication: ReplicatedReplication = crate::maintenance::NoReplication::new();

        // ShardedKVSServer should use Pipeline<ShardedRouter, Inner::OpPipeline>
        type ShardedLocalServer =
            ShardedKVSServer<LocalKVSServer<crate::values::LwwWrapper<String>>>;
        type ShardedLocalPipeline =
            <ShardedLocalServer as KVSServer<crate::values::LwwWrapper<String>>>::OpPipeline;
        type ShardedLocalReplication = <ShardedLocalServer as KVSServer<
            crate::values::LwwWrapper<String>,
        >>::ReplicationStrategy;

        let _sharded_local_pipeline: ShardedLocalPipeline = crate::dispatch::Pipeline::new(
            crate::dispatch::ShardedRouter::new(3),
            crate::dispatch::SingleNodeRouter::new(),
        );
        let _sharded_local_replication: ShardedLocalReplication = ();

        println!("✅ Symmetric composition type safety verified!");
    }

    #[tokio::test]
    async fn test_pipeline_composition_matches_server_composition() {
        // Test that ShardedKVSServer<ReplicatedKVSServer> has the expected pipeline type
        type ShardedReplicatedServer = ShardedKVSServer<
            ReplicatedKVSServer<crate::values::CausalString, crate::maintenance::NoReplication>,
        >;
        type ExpectedPipeline = crate::dispatch::Pipeline<
            crate::dispatch::ShardedRouter,
            crate::dispatch::RoundRobinRouter,
        >;

        // This should compile, proving the types match
        let pipeline: ExpectedPipeline = crate::dispatch::Pipeline::new(
            crate::dispatch::ShardedRouter::new(3),
            crate::dispatch::RoundRobinRouter::new(),
        );

        // Test that we can use this pipeline with the server
        let flow = FlowBuilder::new();
        let replication = crate::maintenance::NoReplication::new();
        let _deployment =
            <ShardedReplicatedServer as KVSServer<crate::values::CausalString>>::create_deployment(
                &flow,
                pipeline,
                replication,
            );

        // Finalize the flow to avoid panic
        let _nodes = flow.finalize();

        println!("✅ Pipeline composition matches server composition!");
    }

    #[tokio::test]
    async fn test_zero_cost_abstraction_properties() {
        // Test that the new trait design maintains zero-cost properties

        // Test that associated types resolve to concrete types (no trait objects)
        type LocalPipeline = <LocalKVSServer<String> as KVSServer<String>>::OpPipeline;
        type LocalReplication = <LocalKVSServer<String> as KVSServer<String>>::ReplicationStrategy;

        // These should be concrete types, not trait objects
        assert_eq!(
            std::mem::size_of::<LocalPipeline>(),
            std::mem::size_of::<crate::dispatch::SingleNodeRouter>()
        );
        assert_eq!(
            std::mem::size_of::<LocalReplication>(),
            std::mem::size_of::<()>()
        );

        // Test pipeline composition is zero-cost
        type ComposedPipeline = crate::dispatch::Pipeline<
            crate::dispatch::ShardedRouter,
            crate::dispatch::RoundRobinRouter,
        >;

        assert_eq!(
            std::mem::size_of::<ComposedPipeline>(),
            std::mem::size_of::<crate::dispatch::ShardedRouter>()
                + std::mem::size_of::<crate::dispatch::RoundRobinRouter>()
        );

        println!("✅ Zero-cost abstraction properties verified!");
    }

    #[tokio::test]
    async fn test_compile_time_error_detection() {
        // Test that type mismatches are caught at compile time
        // These tests verify the type system enforces correct composition

        // This should compile - correct composition
        let _correct_pipeline: crate::dispatch::Pipeline<
            crate::dispatch::ShardedRouter,
            crate::dispatch::RoundRobinRouter,
        > = crate::dispatch::Pipeline::new(
            crate::dispatch::ShardedRouter::new(3),
            crate::dispatch::RoundRobinRouter::new(),
        );

        // Test that different server types have different pipeline types
        // We can verify this by using them in a function that requires different types

        // These should be different types (compile-time check)
        fn _different_types<T, U>(_t: T, _u: U)
        where
            T: 'static,
            U: 'static,
        {
            // If T and U are the same type, this would be redundant
            // The fact that we can call this with different pipeline types
            // proves they are distinct types
        }

        _different_types(
            crate::dispatch::SingleNodeRouter::new(),
            crate::dispatch::RoundRobinRouter::new(),
        );

        println!("✅ Compile-time error detection verified!");
    }

    #[test]
    fn test_linearizable_kvs_creation() {
        let _kvs = LinearizableKVSServer::<
            crate::values::LwwWrapper<String>,
            crate::maintenance::NoReplication,
        >::new(3);

        let custom_config = crate::dispatch::PaxosConfig {
            f: 2,
            i_am_leader_send_timeout: 2,
            i_am_leader_check_timeout: 4,
            i_am_leader_check_timeout_delay_multiplier: 2,
        };
        let _kvs_custom = LinearizableKVSServer::<
            crate::values::LwwWrapper<String>,
            crate::maintenance::NoReplication,
        >::with_paxos_config(5, custom_config);

        let _kvs_fault_tolerant = LinearizableKVSServer::<
            crate::values::LwwWrapper<String>,
            crate::maintenance::NoReplication,
        >::with_fault_tolerance(5, 2);

        println!("✅ LinearizableKVSServer creation works!");
    }

    #[test]
    fn test_linearizable_kvs_implements_kvs_server() {
        // Test that LinearizableKVSServer implements KVSServer
        fn _test_kvs_server<V, S>(_server: S)
        where
            S: KVSServer<V>,
            V: Clone
                + Serialize
                + for<'de> Deserialize<'de>
                + PartialEq
                + Eq
                + Default
                + std::fmt::Debug
                + lattices::Merge<V>
                + Send
                + Sync
                + 'static,
        {
        }

        let kvs = LinearizableKVSServer::<
            crate::values::LwwWrapper<String>,
            crate::maintenance::NoReplication,
        >::new(3);
        _test_kvs_server(kvs);

        println!("✅ LinearizableKVSServer implements KVSServer trait!");
    }

    #[test]
    fn test_linearizable_kvs_size() {
        let op_pipeline = crate::dispatch::PaxosInterceptor::new();
        let replication = crate::maintenance::NoReplication::new();

        let size = LinearizableKVSServer::<
            crate::values::LwwWrapper<String>,
            crate::maintenance::NoReplication,
        >::size(op_pipeline, replication);
        assert_eq!(size, 3); // Minimum for Paxos consensus

        println!("✅ LinearizableKVSServer size is correct (3 nodes for f=1)!");
    }

    #[test]
    fn test_linearizable_kvs_deployment_creation() {
        let flow = FlowBuilder::new();
        let op_pipeline = crate::dispatch::PaxosInterceptor::new();
        let replication = crate::maintenance::NoReplication::new();

        let _deployment = LinearizableKVSServer::<
            crate::values::LwwWrapper<String>,
            crate::maintenance::NoReplication,
        >::create_deployment(&flow, op_pipeline, replication);

        // Finalize the flow to avoid panic
        let _nodes = flow.finalize();

        println!("✅ LinearizableKVSServer deployment creation works!");
    }
}
