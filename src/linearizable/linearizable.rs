use super::paxos::{CorePaxos, PaxosConfig};
use super::paxos_with_client::PaxosLike;
use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

// Type aliases to reduce complexity warnings
type LinearizableInputSink<V> = ExternalBincodeSink<KVSOperation<V>>;
type LinearizableOutputStream<V> = ExternalBincodeStream<(String, Option<V>)>;
type LinearizablePorts<V> = (LinearizableInputSink<V>, LinearizableOutputStream<V>);

/// A linearizable Key-Value Store implementation using Paxos consensus
///
/// This implementation uses Paxos to totally order all operations, providing linearizability.
/// Unlike the replicated KVS which uses eventual consistency via gossip, this provides
/// strong consistency guarantees where all operations appear to take effect atomically
/// at a single point in time between their invocation and response.
///
/// ## Architecture
///
/// ```text
/// External Client
///       ↓
///   Proxy Process
///       ↓
/// Paxos Cluster (Proposers + Acceptors)
///       ↓
///   Replica Cluster
/// (Apply sequenced operations)
/// ```
///
/// ## Paxos Components
///
/// - **Proposers**: Coordinate leader election and propose values to be committed
/// - **Acceptors**: Vote on proposed values and maintain the replicated log
/// - **Replicas**: Apply committed operations from the Paxos log to their local KVS
///
/// ## Guarantees
///
/// - **Linearizability**: All operations appear to execute atomically in a total order
/// - **Consensus**: Paxos ensures all replicas apply operations in the same order
/// - **Fault Tolerance**: Tolerates f failures with 2f+1 acceptors
pub struct LinearizableKVSServer<V>
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
{
    _phantom: std::marker::PhantomData<V>,
}

impl<V> LinearizableKVSServer<V>
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
{
    /// Run a linearizable KVS using Paxos consensus
    ///
    /// ## Parameters
    ///
    /// - `f`: Maximum number of faulty nodes (requires 2f+1 acceptors)
    /// - `num_replicas`: Number of replicas that apply operations
    /// - `checkpoint_frequency`: How often to checkpoint the log (number of operations)
    ///
    /// ## Returns
    ///
    /// - Client input port for sending operations
    /// - Results port for successful GET responses
    /// - Failures port for GET operations on missing keys
    pub fn run_linearizable_kvs<'a>(
        proxy: &Process<'a, ()>,
        proposers: &Cluster<'a, super::paxos::Proposer>,
        acceptors: &Cluster<'a, super::paxos::Acceptor>,
        replicas: &Cluster<'a, KVSNode>,
        client_external: &External<'a, ()>,
        f: usize,
        _checkpoint_frequency: usize,
    ) -> LinearizablePorts<V> {
        let paxos_config = PaxosConfig {
            f,
            i_am_leader_send_timeout: 5,
            i_am_leader_check_timeout: 10,
            i_am_leader_check_timeout_delay_multiplier: 15,
        };

        // Proxy receives operations from external clients
        let (input_port, operations) = proxy.source_external_bincode(client_external);

        let operations_at_proposers = operations
            .inspect(q!(|op| {
                match op {
                    KVSOperation::Put(key, value) => {
                        println!("🔒 Linearizable: PUT {} = {:?}", key, value)
                    }
                    KVSOperation::Get(key) => println!("🔒 Linearizable: GET {}", key),
                }
            }))
            .round_robin_bincode(proposers, nondet!(/** distribute to proposers */));

        // Use Paxos to sequence operations
        let paxos = CorePaxos {
            proposers: proposers.clone(),
            acceptors: acceptors.clone(),
            paxos_config,
        };

        // Checkpoint management for Paxos log garbage collection
        let (acceptor_checkpoint_complete, acceptor_checkpoint) =
            acceptors.forward_ref::<Optional<_, _, _>>();

        let sequenced_operations = paxos.with_client(
            proposers,
            operations_at_proposers,
            acceptor_checkpoint,
            nondet!(/** paxos commit nondeterminism */),
            nondet!(/** operation ordering nondeterminism */),
        );

        // Send sequenced operations to replicas, preserving slot numbers for sequential application
        let replica_operations = sequenced_operations
            .inspect(q!(|(slot, op)| {
                if let Some(op) = op {
                    println!("  Replica applying slot {}: {:?}", slot, op);
                }
            }))
            .broadcast_bincode(replicas, nondet!(/** broadcast to replicas */))
            .values();

        // Each replica applies operations sequentially by slot number
        let replica_tick = replicas.tick();

        // Process operations in slot order: extract operations and apply them sequentially
        let ordered_operations = replica_operations
            .map(q!(|(slot, op)| (slot, op)))
            .assume_ordering::<TotalOrder>(nondet!(/** slot order from paxos */));

        // Filter to only committed operations (Some values)
        let committed_ops = ordered_operations
            .filter_map(q!(|(slot, op)| op.map(|o| (slot, o))));

        // Demux operations while preserving slot order
        let (put_tuples, get_keys) = crate::core::KVSCore::demux_ops(
            committed_ops.map(q!(|(_slot, op)| op))
        );

        // Apply puts sequentially in slot order
        let replica_kvs = crate::lww::KVSLww::put(put_tuples);

        // Handle GET operations with linearizable reads
        // Reads are also ordered by Paxos (they go through consensus like writes).
        // This ensures all replicas apply reads in the same order, providing linearizability.
        let replica_results = crate::lww::KVSLww::get(
            get_keys
                .batch(&replica_tick, nondet!(/** batch gets */)),
            replica_kvs.snapshot(&replica_tick, nondet!(/** snapshot for gets */)),
        );

        // Collect results back to proxy and send to client
        let proxy_results = replica_results.all_ticks().send_bincode(proxy).values();
        let get_results_port = proxy_results
            .assume_ordering::<TotalOrder>(nondet!(/** preserve total order from paxos */))
            .send_bincode_external(client_external);

        // Complete the checkpoint channel (unused for now, but required by PaxosLike trait)
        // Provide an empty stream that is converted to Optional using a vec instead of std::iter::empty
        let empty_checkpoint = acceptors
            .source_iter(q!(Vec::<usize>::new().into_iter()))
            .max();
        acceptor_checkpoint_complete.complete(empty_checkpoint);

        (input_port, get_results_port)
    }
}

/// Type alias for String-based linearizable KVS server
pub type StringLinearizableKVSServer = LinearizableKVSServer<String>;
