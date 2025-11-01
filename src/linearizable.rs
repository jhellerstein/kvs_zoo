use crate::core::KVSCore;
use crate::local::KVSNode;
use crate::paxos::{CorePaxos, PaxosConfig};
use crate::paxos_with_client::PaxosLike;
use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

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
        proposers: &Cluster<'a, crate::paxos::Proposer>,
        acceptors: &Cluster<'a, crate::paxos::Acceptor>,
        replicas: &Cluster<'a, KVSNode>,
        client_external: &External<'a, ()>,
        f: usize,
        _checkpoint_frequency: usize,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, V)>,
        ExternalBincodeStream<String>,
    ) {
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

        // Send sequenced operations to replicas
        let replica_operations = sequenced_operations
            .inspect(q!(|(slot, op)| {
                if let Some(op) = op {
                    println!("  Replica applying slot {}: {:?}", slot, op);
                }
            }))
            .filter_map(q!(|(_slot, op)| op))
            .broadcast_bincode(replicas, nondet!(/** broadcast to replicas */))
            .values();

        // Each replica applies operations to its local KVS using KVSCore
        // We batch operations per tick before applying them
        let replica_tick = replicas.tick();
        let batched_operations = replica_operations
            .clone()
            .batch(&replica_tick, nondet!(/** batch operations */));
        
        // Put operations into the KVS
        let replica_kvs = KVSCore::put(replica_operations.clone());

        // Handle GET operations with linearizable reads
        // The assume_ordering is crucial for linearizability - it ensures reads see 
        // all preceding writes in the total order established by Paxos
        let (replica_results, replica_failures) = KVSCore::get(
            batched_operations.assume_ordering::<TotalOrder>(nondet!(/** total order from paxos */)),
            replica_kvs.snapshot(&replica_tick, nondet!(/** snapshot for gets */)),
        );

        // Collect results back to proxy and send to client
        let proxy_results = replica_results.send_bincode(proxy).values();
        let proxy_failures = replica_failures.send_bincode(proxy).values();

        let get_results_port = proxy_results
            .assume_ordering::<TotalOrder>(nondet!(/** maintain total order for results */))
            .send_bincode_external(client_external);
        let get_fails_port = proxy_failures
            .assume_ordering::<TotalOrder>(nondet!(/** maintain total order for failures */))
            .send_bincode_external(client_external);
        
        // Complete the checkpoint channel (unused for now, but required by PaxosLike trait)
        // Provide an empty stream that is converted to Optional using a vec instead of std::iter::empty
        let empty_checkpoint = acceptors
            .source_iter(q!(Vec::<usize>::new().into_iter()))
            .max();
        acceptor_checkpoint_complete.complete(empty_checkpoint);

        (input_port, get_results_port, get_fails_port)
    }
}

/// Type alias for String-based linearizable KVS server
pub type StringLinearizableKVSServer = LinearizableKVSServer<String>;
