//! # Replicated KVS Architecture Example
//! 
//! This example demonstrates a replicated KVS architecture:
//! - **Multi-replica**: Data is replicated across multiple nodes
//! - **Epidemic gossip**: Nodes periodically exchange state for eventual consistency
//! - **Causal consistency**: Uses vector clocks to maintain causal ordering
//! - **Partition tolerance**: Continues operating during network partitions
//! 
//! **Architecture**: Multiple replicas with gossip-based state synchronization
//! **Trade-offs**: High availability and partition tolerance, eventual consistency
//! **Use case**: Social media, content distribution, collaborative editing

mod driver;
use driver::{run_kvs_demo, KVSDemo};
use kvs_zoo::values::{CausalString, generate_causal_operations};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::RoundRobinRouter;

struct ReplicatedDemo;

impl KVSDemo for ReplicatedDemo {
    type Value = CausalString;
    type Storage = kvs_zoo::replicated::KVSReplicatedEpidemic<CausalString>;
    type Router = RoundRobinRouter;

    fn create_router<'a>(&self, _flow: &hydro_lang::compile::builder::FlowBuilder<'a>) -> Self::Router {
        RoundRobinRouter
    }

    fn cluster_size(&self) -> usize {
        3 // 3-node cluster for replication
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Multiple replicas with epidemic gossip\n\
         🔄 Consistency: Causal (with vector clocks)\n\
         🎯 Use case: High availability, partition tolerance"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        generate_causal_operations()
    }

    fn name(&self) -> &'static str {
        "Replicated KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo(ReplicatedDemo).await
}