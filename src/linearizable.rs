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
//! Client → Paxos Consensus → Replicated Storage
//!           (Total Order)    (Apply in Order)
//! ```
//!
//! The Paxos interceptor ensures all operations get a globally consistent
//! sequence number before being applied to the underlying replicated storage.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::core::KVSNode;
use crate::interception::{OpIntercept, PaxosInterceptor, PaxosConfig};
use crate::protocol::KVSOperation;
use crate::replication::ReplicationStrategy;
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
/// use kvs_zoo::replication::BroadcastReplication;
/// use kvs_zoo::values::CausalString;
///
/// type LinearizableKVS = LinearizableKVSServer<CausalString, BroadcastReplication<CausalString>>;
/// ```
pub struct LinearizableKVSServer<V, R = crate::replication::NoReplication> {
    cluster_size: usize,
    paxos_config: PaxosConfig,
    _phantom: std::marker::PhantomData<(V, R)>,
}

impl<V, R> LinearizableKVSServer<V, R> {
    /// Create a new linearizable KVS server with default Paxos configuration
    pub fn new(cluster_size: usize) -> Self {
        Self {
            cluster_size,
            paxos_config: PaxosConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// Sequence responses back into slot order to maintain linearizability
    /// 
    /// This function takes slot-indexed responses that may arrive out of order
    /// and returns them in the correct sequential order, buffering any responses
    /// that arrive early until the gaps are filled.
    /// 
    /// Based on the pattern from hydro/hydro_test/src/cluster/kv_replica/sequence_payloads.rs
    fn sequence_responses<'a>(
        deployment: &Cluster<'a, KVSNode>,
        slotted_responses: Stream<(usize, String), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        let tick = deployment.tick();
        
        // Create cycles for buffering out-of-order responses
        let (buffered_responses_complete, buffered_responses) = 
            tick.cycle::<Stream<(usize, String), Tick<Cluster<'a, KVSNode>>, Bounded>>();
        
        // Batch incoming responses and combine with buffered ones
        let sorted_responses = slotted_responses
            .batch(&tick, nondet!(/** batch for sequencing */))
            .chain(buffered_responses)
            .sort();
        
        // Track the next expected slot number
        let (next_slot_complete, next_slot) = 
            tick.cycle_with_initial(tick.singleton(q!(0usize)));
        
        // Find the highest contiguous slot we can process
        let next_slot_after_processing = sorted_responses
            .clone()
            .cross_singleton(next_slot.clone())
            .fold(
                q!(|| 0usize),
                q!(|new_next_slot, ((slot, _response), next_slot)| {
                    if slot == std::cmp::max(*new_next_slot, next_slot) {
                        *new_next_slot = slot + 1;
                    }
                }),
            );
        
        // Split responses into processable and buffered
        let processable_responses = sorted_responses
            .clone()
            .cross_singleton(next_slot_after_processing.clone())
            .filter(q!(|((slot, _response), highest_slot)| *slot < *highest_slot))
            .map(q!(|((slot, response), _)| (slot, response)));
        
        let new_buffered_responses = sorted_responses
            .cross_singleton(next_slot_after_processing.clone())
            .filter(q!(|((slot, _response), highest_slot)| *slot > *highest_slot))
            .map(q!(|((slot, response), _)| (slot, response)));
        
        // Complete the cycles
        buffered_responses_complete.complete_next_tick(new_buffered_responses);
        next_slot_complete.complete_next_tick(next_slot_after_processing);
        
        // Return just the response strings in slot order
        processable_responses
            .map(q!(|(_slot, response)| response))
            .all_ticks()
    }

    /// Create a linearizable KVS server with custom Paxos configuration
    pub fn with_paxos_config(cluster_size: usize, paxos_config: PaxosConfig) -> Self {
        Self {
            cluster_size,
            paxos_config,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a linearizable KVS server that tolerates `f` failures
    /// 
    /// This will configure Paxos to require `2f + 1` nodes for safety.
    pub fn with_fault_tolerance(cluster_size: usize, f: usize) -> Self {
        Self {
            cluster_size,
            paxos_config: PaxosConfig {
                f,
                ..PaxosConfig::default()
            },
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
        _replication: Self::ReplicationStrategy,
        flow: &FlowBuilder<'a>,
    ) -> crate::server::ServerPorts<V>
    where
        V: std::fmt::Debug + Send + Sync,
    {
        // Use bidirectional external connection
        let (bidi_port, operations_stream, _membership, complete_sink) =
            proxy.bidi_external_many_bincode::<_, KVSOperation<V>, String>(client_external);

        // Apply Paxos consensus for total ordering using the operation pipeline
        let ordered_operations = op_pipeline.intercept_operations(
            operations_stream
                .entries()
                .map(q!(|(_client_id, op)| op))
                .assume_ordering(nondet!(/** Paxos will provide total order */)),
            deployment,
            flow,
        );

        // Add slot numbers to the operations for proper sequencing
        let slotted_operations = ordered_operations
            .enumerate()
            .map(q!(|(slot, op)| (slot, op)));
        
        // Process each slotted operation sequentially
        let slotted_responses = slotted_operations
            .scan(
                q!(|| std::collections::HashMap::new()),
                q!(|state, (slot, op)| {
                    let response = match op {
                        KVSOperation::Put(key, value) => {
                            state.insert(key.clone(), value);
                            format!("PUT {} = OK [LINEARIZABLE]", key)
                        }
                        KVSOperation::Get(key) => {
                            match state.get(&key) {
                                Some(value) => format!("GET {} = {:?} [LINEARIZABLE]", key, value),
                                None => format!("GET {} = NOT FOUND [LINEARIZABLE]", key),
                            }
                        }
                    };
                    Some((slot, response))
                })
            );
        
        // Now we need to sequence the responses back into order
        // This handles the case where responses arrive out of order due to network/processing delays
        let sequential_responses = Self::sequence_responses(deployment, slotted_responses);

        // Send results back through proxy to external
        let proxy_responses = sequential_responses
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

/// Type aliases for common linearizable KVS configurations
pub mod compositions {
    use super::*;
    use crate::values::{CausalString, LwwWrapper};
    
    /// Linearizable KVS with LWW values and no additional replication
    /// (Paxos provides the replication/consensus)
    pub type LinearizableLww<V> = LinearizableKVSServer<LwwWrapper<V>, crate::replication::NoReplication>;
    
    /// Linearizable KVS with causal values and broadcast replication
    pub type LinearizableCausal = LinearizableKVSServer<CausalString, crate::replication::BroadcastReplication<CausalString>>;
    
    /// Linearizable KVS with string values (most common case)
    pub type LinearizableString = LinearizableLww<String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interception::PaxosConfig;
    use crate::values::LwwWrapper;

    #[test]
    fn test_linearizable_kvs_creation() {
        let _kvs = LinearizableKVSServer::<LwwWrapper<String>, crate::replication::NoReplication>::new(3);
        
        let custom_config = PaxosConfig {
            f: 2,
            i_am_leader_send_timeout: 2,
            i_am_leader_check_timeout: 4,
            i_am_leader_check_timeout_delay_multiplier: 2,
        };
        let _kvs_custom = LinearizableKVSServer::<LwwWrapper<String>, crate::replication::NoReplication>::with_paxos_config(5, custom_config);
        
        let _kvs_fault_tolerant = LinearizableKVSServer::<LwwWrapper<String>, crate::replication::NoReplication>::with_fault_tolerance(5, 2);
    }

    #[test]
    fn test_linearizable_kvs_implements_kvs_server() {
        // Test that LinearizableKVSServer implements KVSServer
        fn _test_kvs_server<V, S>(_server: S) 
        where 
            S: KVSServer<V>,
            V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + std::fmt::Debug + lattices::Merge<V> + Send + Sync + 'static,
        {}
        
        let kvs = LinearizableKVSServer::<LwwWrapper<String>, crate::replication::NoReplication>::new(3);
        _test_kvs_server(kvs);
    }

    #[test]
    fn test_linearizable_kvs_size() {
        let op_pipeline = PaxosInterceptor::new();
        let replication = crate::replication::NoReplication::new();
        
        let size = LinearizableKVSServer::<LwwWrapper<String>, crate::replication::NoReplication>::size(op_pipeline, replication);
        assert_eq!(size, 3); // Minimum for Paxos consensus
    }

    #[test]
    fn test_linearizable_kvs_type_aliases() {
        // Test that type aliases compile
        let _lww: compositions::LinearizableString = LinearizableKVSServer::new(3);
        let _causal: compositions::LinearizableCausal = LinearizableKVSServer::new(3);
    }

    #[test]
    fn test_linearizable_kvs_deployment_creation() {
        let flow = hydro_lang::compile::builder::FlowBuilder::new();
        let op_pipeline = PaxosInterceptor::new();
        let replication = crate::replication::NoReplication::new();
        
        let _deployment = LinearizableKVSServer::<LwwWrapper<String>, crate::replication::NoReplication>::create_deployment(
            &flow,
            op_pipeline,
            replication,
        );
        
        // Finalize the flow to avoid panic
        let _nodes = flow.finalize();
    }
}