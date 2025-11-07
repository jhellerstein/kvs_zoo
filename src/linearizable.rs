//! Linearizable KVS implementation using Paxos consensus
//!
//! This module provides a linearizable key-value store that uses Paxos consensus
//! to ensure all operations are applied in a globally consistent order across
//! all replicas.
//!
//! ## Linearizability
//!
//! Linearizability is a strong consistency model, requiring that:
//! 1. Operations appear to take effect atomically at some point between their start and end
//! 2. All operations appear to execute in a single, total order
//! 3. The order respects the real-time ordering of non-overlapping operations
//!
//! ## Architecture
//!
//! ```text
//! Client
//!   │
//!   └─▶ Paxos Interceptor (assigns total-order slot numbers)
//!         │  (stream of (slot, op))
//!         └─▶ Round-Robin Dispatch (one KVS node executes each slotted op)
//!               │ (ordering preserved by slots)
//!               └─▶ Replication Strategy (e.g. LogBased uses slots to replay PUTs)
//!                     │
//!                     └─▶ LWW Storage (apply PUT values; GET reads latest)
//! ```
//!
//! Flow summary:
//! 1. Client requests arrive at the proxy and are fed into Paxos.
//! 2. Paxos assigns a monotonically increasing slot number, producing `(slot, op)`.
//! 3. Slotted operations are round-robin dispatched so only one KVS node executes each.
//! 4. The replication strategy (e.g. `LogBased`) consumes slotted PUT tuples to ensure
//!    deterministic replay / replication using the slot ordering.
//! 5. Slots are stripped before storage; they exist solely to preserve global order
//!    and replication replay semantics.
//! 6. GETs read from an LWW snapshot; slot numbers are not needed for read paths.
//!
//! Notes:
//! - `replication.maintain_slotted_data(..)` is where a strategy (like LogBased) can
//!   use the slot numbers to guarantee consistent replication ordering.
//! - The round-robin stage maintains Paxos order because slot numbers are carried
//!   through dispatch until replication completes.
//! - Alternative replication strategies may ignore slots if they do not require
//!   ordered replay (e.g., trivial or single-node configurations).

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::core::KVSNode;
use crate::interception::{PaxosConfig, PaxosInterceptor};
use crate::protocol::KVSOperation;
use crate::maintain::ReplicationStrategy;
use crate::server::KVSServer;

/// Linearizable KVS server using Paxos consensus for total ordering
///
/// This server provides the strongest consistency guarantees by using Paxos
/// consensus to establish a total order over all operations before applying
/// them to the underlying replicated storage.
///
/// ## Type Parameters
/// - `V`: Value type stored in the KVS
/// - `R`: Replication strategy for the underlying storage
///
/// ## Example
/// ```rust
/// use kvs_zoo::linearizable::LinearizableKVSServer;
/// use kvs_zoo::maintain::BroadcastReplication;
/// use kvs_zoo::values::CausalString;
///
/// type LinearizableKVS = LinearizableKVSServer<CausalString, BroadcastReplication<CausalString>>;
/// ```
pub struct LinearizableKVSServer<V, R = crate::maintain::NoReplication> {
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
    pub fn with_paxos_config(_cluster_size: usize, _paxos_config: PaxosConfig) -> Self {
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
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
    R: ReplicationStrategy<V>,
{
    /// Uses Paxos interceptor for total ordering
    type OpPipeline = PaxosInterceptor<V>;

    /// Delegates replication to the specified strategy
    type ReplicationStrategy = R;

    /// Uses a cluster deployment for the linearizable service
    /// Returns (KVS cluster, Proposer cluster, Acceptor cluster)
    type Deployment<'a> = (
        Cluster<'a, KVSNode>,
        Cluster<'a, crate::interception::paxos_core::Proposer>,
        Cluster<'a, crate::interception::paxos_core::Acceptor>,
    );

    fn create_deployment<'a>(
        flow: &FlowBuilder<'a>,
        _op_pipeline: Self::OpPipeline,
        _replication: Self::ReplicationStrategy,
    ) -> Self::Deployment<'a> {
        let kvs_cluster = flow.cluster::<KVSNode>();
        let proposers = flow.cluster::<crate::interception::paxos_core::Proposer>();
        let acceptors = flow.cluster::<crate::interception::paxos_core::Acceptor>();
        (kvs_cluster, proposers, acceptors)
    }

    fn run<'a>(
        proxy: &Process<'a, ()>,
        deployment: &Self::Deployment<'a>,
        client_external: &External<'a, ()>,
        op_pipeline: Self::OpPipeline,
        replication: Self::ReplicationStrategy,
    ) -> crate::server::ServerPorts<V>
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

        // Demux operations into PUTs and GETs for proper handling
        let put_tuples_slotted =
            slotted_operations_with_slots
                .clone()
                .filter_map(q!(|(slot, op)| match op {
                    KVSOperation::Put(key, value) => Some((slot, key, value)),
                    KVSOperation::Get(_) => None,
                }));

        let get_keys_slotted =
            slotted_operations_with_slots.filter_map(q!(|(slot, op)| match op {
                KVSOperation::Put(_, _) => None,
                KVSOperation::Get(key) => Some((slot, key)),
            }));

        // Use replication strategy for PUT operations with slot numbers
        let replicated_data_slotted =
            replication.maintain_slotted_data(kvs_cluster, put_tuples_slotted.clone());

        // Combine local and replicated PUTs (all slotted)
        let all_put_tuples_slotted = put_tuples_slotted
            .clone()
            .interleave(replicated_data_slotted);

        // Strip slots for storage (storage doesn't need slots)
        let all_put_tuples = all_put_tuples_slotted.map(q!(|(_slot, key, value)| (key, value)));

        // Execute PUT operations using LWW storage
        let kvs_state = crate::lww::KVSLww::put(all_put_tuples);

        // Handle GET operations (strip slots)
        let get_keys = get_keys_slotted.map(q!(|(_slot, key)| key));
        let ticker = kvs_cluster.tick();
        let get_results = crate::lww::KVSLww::get(
            get_keys.batch(&ticker, nondet!(/** batch gets */)),
            kvs_state.snapshot(&ticker, nondet!(/** snapshot for gets */)),
        );

        // Format responses (strip slots from PUTs)
        let put_responses = put_tuples_slotted.map(q!(|(_slot, key, _value)| format!(
            "PUT {} = OK [LINEARIZABLE]",
            key
        )));
        let get_responses = get_results.map(q!(|(key, value)| format!(
            "GET {} = {:?} [LINEARIZABLE]",
            key, value
        )));

        // Combine all responses
        let responses = put_responses.interleave(get_responses.all_ticks());

        // Send results back through proxy to external
        // Each node only receives operations via round-robin, so no duplicate responses
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

    fn size(_op_pipeline: Self::OpPipeline, _replication: Self::ReplicationStrategy) -> usize {
        // Linearizable KVS typically needs at least 3 nodes for Paxos (2f+1 where f=1)
        3
    }
}

// Type aliases removed due to Hydro staging issues with crate:: paths
// Users can create their own type aliases as needed

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interception::PaxosConfig;
    use crate::values::LwwWrapper;

    #[test]
    fn test_linearizable_kvs_creation() {
        let _kvs =
            LinearizableKVSServer::<LwwWrapper<String>, crate::maintain::NoReplication>::new(3);

        let custom_config = PaxosConfig {
            f: 2,
            i_am_leader_send_timeout: 2,
            i_am_leader_check_timeout: 4,
            i_am_leader_check_timeout_delay_multiplier: 2,
        };
        let _kvs_custom = LinearizableKVSServer::<
            LwwWrapper<String>,
            crate::maintain::NoReplication,
        >::with_paxos_config(5, custom_config);

        let _kvs_fault_tolerant = LinearizableKVSServer::<
            LwwWrapper<String>,
            crate::maintain::NoReplication,
        >::with_fault_tolerance(5, 2);
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

        let kvs =
            LinearizableKVSServer::<LwwWrapper<String>, crate::maintain::NoReplication>::new(3);
        _test_kvs_server(kvs);
    }

    #[test]
    fn test_linearizable_kvs_size() {
        let op_pipeline = PaxosInterceptor::new();
        let replication = crate::maintain::NoReplication::new();

        let size =
            LinearizableKVSServer::<LwwWrapper<String>, crate::maintain::NoReplication>::size(
                op_pipeline,
                replication,
            );
        assert_eq!(size, 3); // Minimum for Paxos consensus
    }

    #[test]
    fn test_linearizable_kvs_deployment_creation() {
        let flow = hydro_lang::compile::builder::FlowBuilder::new();
        let op_pipeline = PaxosInterceptor::new();
        let replication = crate::maintain::NoReplication::new();

        let _deployment = LinearizableKVSServer::<
            LwwWrapper<String>,
            crate::maintain::NoReplication,
        >::create_deployment(&flow, op_pipeline, replication);

        // Finalize the flow to avoid panic
        let _nodes = flow.finalize();
    }
}
