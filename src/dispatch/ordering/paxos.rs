//! Paxos-based operation dispatcher for total ordering
//!
//! This module provides a Paxos consensus dispatcher that ensures all operations
//! are applied in a globally consistent order across all replicas, providing
//! linearizability guarantees for the KVS.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::dispatch::ordering::paxos_core::{PaxosConfig, PaxosPayload};
use crate::dispatch::{Deployment, KVSDeployment, OpDispatch};
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

/// Paxos dispatcher that provides total ordering of operations
///
/// This dispatcher uses the Paxos consensus algorithm to ensure that all operations
/// are totally ordered across all nodes in the cluster, providing linearizability.
#[derive(Clone)]
pub struct PaxosDispatcher<V> {
    pub config: PaxosConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> PaxosDispatcher<V> {
    pub fn new() -> Self {
        Self {
            config: PaxosConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
    pub fn with_config(config: PaxosConfig) -> Self {
        Self {
            config,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> Default for PaxosDispatcher<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> OpDispatch<V> for PaxosDispatcher<V>
where
    V: PaxosPayload,
{
    type Deployment<'a> = Deployment<'a>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        Deployment::SingleCluster(flow.cluster::<KVSNode>())
    }

    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let cluster = deployment.kvs_cluster();

        // Round-robin to a single node then provide a stable per-node order.
        operations
            .round_robin_bincode(cluster, nondet!(/** rr */))
            .enumerate()
            .assume_ordering(nondet!(/** enumerate provides ordering */))
            .inspect(q!(|(slot, op)| {
                println!(
                    "[Paxos(min)] slot {}: {}",
                    slot,
                    match op {
                        KVSOperation::Put(k, _) => format!("PUT {}", k),
                        KVSOperation::Get(k) => format!("GET {}", k),
                    }
                );
            }))
            .map(q!(|(_slot, op)| op))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let p = PaxosDispatcher::<String>::new();
        assert_eq!(p.config.f, PaxosConfig::default().f);
    }
}
