//! Paxos-based operation interceptor for total ordering
//!
//! This module provides a Paxos consensus interceptor that ensures all operations
//! are applied in a globally consistent order across all replicas, providing
//! linearizability guarantees for the KVS.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::protocol::KVSOperation;
use crate::core::KVSNode;
use super::{OpIntercept, paxos_core::{paxos_core, PaxosConfig, PaxosPayload, Proposer, Acceptor}};

/// Wrapper for slot-indexed operations that implements Ord based on slot number
#[derive(Clone, Debug)]
struct SlottedOperation<V> {
    slot: usize,
    operation: Option<KVSOperation<V>>,
}

impl<V> PartialEq for SlottedOperation<V> {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot
    }
}

impl<V> Eq for SlottedOperation<V> {}

impl<V> PartialOrd for SlottedOperation<V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<V> Ord for SlottedOperation<V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.slot.cmp(&other.slot)
    }
}

/// Paxos interceptor that provides total ordering of operations
/// 
/// This interceptor uses the Paxos consensus algorithm to ensure that all operations
/// are totally ordered across all nodes in the cluster, providing linearizability.
#[derive(Clone)]
pub struct PaxosInterceptor<V> {
    config: PaxosConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> PaxosInterceptor<V> {
    /// Create a new Paxos interceptor with default configuration
    pub fn new() -> Self {
        Self {
            config: PaxosConfig {
                f: 1, // Tolerate 1 failure in a 3-node cluster
                i_am_leader_send_timeout: 1,
                i_am_leader_check_timeout: 3,
                i_am_leader_check_timeout_delay_multiplier: 1,
            },
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new Paxos interceptor with custom configuration
    pub fn with_config(config: PaxosConfig) -> Self {
        Self {
            config,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Apply Paxos consensus to a stream of operations
    /// 
    /// This method takes a stream of operations and returns a stream of slot-indexed
    /// operations that are totally ordered by Paxos consensus.
    pub fn apply_with_clusters<'a>(
        &self,
        proposers: &Cluster<'a, Proposer>,
        acceptors: &Cluster<'a, Acceptor>,
        operations: Stream<KVSOperation<V>, Cluster<'a, Proposer>, Unbounded>,
    ) -> Stream<(usize, Option<KVSOperation<V>>), Cluster<'a, Proposer>, Unbounded, hydro_lang::live_collections::stream::NoOrder>
    where
        V: PaxosPayload,
    {
        // Create checkpoint Optional (no checkpointing for now)
        let a_checkpoint = acceptors.source_iter(q!([])).first();
        
        // Call the real Paxos core implementation
        let (_ballots, sequenced_operations) = paxos_core(
            proposers,
            acceptors,
            a_checkpoint,
            |_ballot_stream| operations,
            self.config,
            nondet!(/** leader election non-determinism */),
            nondet!(/** commit non-determinism */),
        );
        
        sequenced_operations
    }
}

impl<V> Default for PaxosInterceptor<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> OpIntercept<V> for PaxosInterceptor<V>
where
    V: PaxosPayload,
{
    fn intercept_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
        flow: &FlowBuilder<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Create Paxos clusters for REAL consensus (not fake enumeration!)
        let (proposers, acceptors, _config) = create_paxos_clusters(flow, 3);
        
        // Broadcast operations to proposers for consensus
        let operations_on_proposers = operations.broadcast_bincode(&proposers, nondet!(/** broadcast to proposers */));
        
        // Apply REAL Paxos consensus using paxos_core
        let sequenced_operations = self.apply_with_clusters(&proposers, &acceptors, operations_on_proposers);
        
        // Sequence the slot-indexed operations to handle out-of-order delivery
        // This is the critical part: Paxos gives us slot numbers but delivery can be out of order
        let tick = proposers.tick();
        
        // Create cycles for buffering out-of-order operations
        let (buffered_operations_complete, buffered_operations) = 
            tick.cycle::<Stream<SlottedOperation<V>, Tick<Cluster<'a, Proposer>>, Bounded>>();
        
        // Convert to SlottedOperation wrapper and batch
        let sorted_operations = sequenced_operations
            .inspect(q!(|(slot, op)| {
                println!("[Paxos] REAL Consensus slot {}: {:?}", 
                    slot,
                    match op {
                        Some(KVSOperation::Put(key, _)) => format!("PUT {}", key),
                        Some(KVSOperation::Get(key)) => format!("GET {}", key),
                        None => "HOLE".to_string(),
                    }
                );
            }))
            .map(q!(|(slot, operation)| SlottedOperation { slot, operation }))
            .batch(&tick, nondet!(/** batch for sequencing */))
            .chain(buffered_operations)
            .sort(); // Now we can sort because SlottedOperation implements Ord
        
        // Track the next expected slot number
        let (next_slot_complete, next_slot) = 
            tick.cycle_with_initial(tick.singleton(q!(0usize)));
        
        // Find the highest contiguous slot we can process
        let next_slot_after_processing = sorted_operations
            .clone()
            .cross_singleton(next_slot.clone())
            .fold(
                q!(|| 0usize),
                q!(|new_next_slot, (slotted_op, next_slot)| {
                    if slotted_op.slot == std::cmp::max(*new_next_slot, next_slot) {
                        *new_next_slot = slotted_op.slot + 1;
                    }
                }),
            );
        
        // Split operations into processable and buffered
        let processable_operations = sorted_operations
            .clone()
            .cross_singleton(next_slot_after_processing.clone())
            .filter(q!(|(slotted_op, highest_slot)| slotted_op.slot < *highest_slot))
            .map(q!(|(slotted_op, _)| slotted_op));
        
        let new_buffered_operations = sorted_operations
            .cross_singleton(next_slot_after_processing.clone())
            .filter(q!(|(slotted_op, highest_slot)| slotted_op.slot > *highest_slot))
            .map(q!(|(slotted_op, _)| slotted_op));
        
        // Complete the cycles
        buffered_operations_complete.complete_next_tick(new_buffered_operations);
        next_slot_complete.complete_next_tick(next_slot_after_processing);
        
        // Extract just the operations in proper sequence, filtering out holes
        let consensus_operations = processable_operations
            .filter_map(q!(|slotted_op| slotted_op.operation))
            .all_ticks();
        
        // Broadcast the consensus-ordered operations to the KVS cluster
        consensus_operations
            .broadcast_bincode(cluster, nondet!(/** broadcast consensus result */))
            .values()
            .assume_ordering(nondet!(/** Paxos provides total ordering */))
    }
}

/// Create Paxos clusters for consensus
/// 
/// This function sets up the necessary clusters for Paxos consensus,
/// including proposers and acceptors.
pub fn create_paxos_clusters<'a>(
    flow: &FlowBuilder<'a>,
    cluster_size: usize,
) -> (
    Cluster<'a, Proposer>,
    Cluster<'a, Acceptor>,
    PaxosConfig,
) {
    // Create proposer cluster
    let proposers = flow.cluster::<Proposer>();

    // Create acceptor cluster  
    let acceptors = flow.cluster::<Acceptor>();

    let config = PaxosConfig {
        f: (cluster_size - 1) / 2, // Standard Paxos fault tolerance
        i_am_leader_send_timeout: 1,
        i_am_leader_check_timeout: 3,
        i_am_leader_check_timeout_delay_multiplier: 1,
    };

    (proposers, acceptors, config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paxos_interceptor_creation() {
        let paxos = PaxosInterceptor::<String>::new();
        assert_eq!(paxos.config.f, 1);
    }
}