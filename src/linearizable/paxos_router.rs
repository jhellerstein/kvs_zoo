use super::paxos::{Acceptor, CorePaxos, PaxosConfig, Proposer};
use super::paxos_with_client::PaxosLike;
use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use crate::routers::KVSRouter;
use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Trait for routers that need additional clusters beyond the main KVS cluster
pub trait PaxosAwareRouter<V> {
    /// Route operations using Paxos consensus with the provided clusters
    fn route_operations_with_paxos<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        proposers: &Cluster<'a, Proposer>,
        acceptors: &Cluster<'a, Acceptor>,
        replicas: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
}

/// Router that uses Paxos consensus to establish total ordering
/// This provides linearizability by ensuring all operations are totally ordered
/// before being applied to the replicas
pub struct PaxosRouter {
    pub f: usize, // fault tolerance parameter
}

impl PaxosRouter {
    pub fn new(f: usize) -> Self {
        Self { f }
    }
}

impl<V> KVSRouter<V> for PaxosRouter
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + Sync
        + 'static
        + std::fmt::Debug
        + PartialEq
        + Eq,
{
    fn route_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        replicas: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded> {
        // Fallback implementation - just broadcast to replicas
        // The real Paxos implementation is in route_operations_with_paxos
        operations
            .inspect(q!(|op| {
                match op {
                    KVSOperation::Put(key, value) => {
                        println!("🔒 Paxos routing (fallback): PUT {} = {:?}", key, value)
                    }
                    KVSOperation::Get(key) => println!("🔒 Paxos routing (fallback): GET {}", key),
                }
            }))
            .broadcast_bincode(replicas, nondet!(/** fallback broadcast */))
            .assume_ordering::<TotalOrder>(nondet!(/** assume total order */))
    }
}

impl<V> PaxosAwareRouter<V> for PaxosRouter
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + Sync
        + 'static
        + std::fmt::Debug
        + PartialEq
        + Eq,
{
    fn route_operations_with_paxos<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        proposers: &Cluster<'a, Proposer>,
        acceptors: &Cluster<'a, Acceptor>,
        replicas: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let paxos_config = PaxosConfig {
            f: self.f,
            i_am_leader_send_timeout: 5,
            i_am_leader_check_timeout: 10,
            i_am_leader_check_timeout_delay_multiplier: 15,
        };

        // Route operations to proposers first
        let operations_at_proposers = operations
            .inspect(q!(|op| {
                match op {
                    KVSOperation::Put(key, value) => {
                        println!("🔒 Paxos routing: PUT {} = {:?}", key, value)
                    }
                    KVSOperation::Get(key) => println!("🔒 Paxos routing: GET {}", key),
                }
            }))
            .round_robin_bincode(proposers, nondet!(/** distribute to proposers */));

        // Use Paxos to establish total ordering
        let paxos = CorePaxos {
            proposers: proposers.clone(),
            acceptors: acceptors.clone(),
            paxos_config,
        };

        // Checkpoint management for Paxos log garbage collection
        let (_acceptor_checkpoint_complete, acceptor_checkpoint) =
            acceptors.forward_ref::<Optional<_, _, _>>();

        // Run Paxos consensus to sequence operations
        let sequenced_operations = paxos.with_client(
            proposers,
            operations_at_proposers,
            acceptor_checkpoint,
            nondet!(/** paxos commit nondeterminism */),
            nondet!(/** operation ordering nondeterminism */),
        );

        // Extract the operations from the Paxos slots and route to replicas
        sequenced_operations
            .inspect(q!(|(slot, op)| {
                if let Some(op) = op {
                    println!("  Paxos sequenced slot {}: {:?}", slot, op);
                }
            }))
            .filter_map(q!(|(_slot, op)| op))
            .broadcast_bincode(replicas, nondet!(/** broadcast to replicas */))
            .values() // Convert KeyedStream to Stream
            .assume_ordering::<TotalOrder>(nondet!(/** maintain total order from paxos */))
    }
}
