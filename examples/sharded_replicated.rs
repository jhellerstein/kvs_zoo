//! # Sharded + Replicated KVS Architecture Example
//! 
//! This example demonstrates a hybrid architecture combining sharding and replication:
//! - **Sharding**: Data is partitioned across multiple shards by key hash
//! - **Replication**: Each shard is replicated across multiple nodes
//! - **Best of both worlds**: Scalability from sharding + availability from replication
//! - **Complex coordination**: Requires both routing logic and consensus within shards
//! 
//! **Architecture**: Hash-based routing to replicated shard clusters
//! **Trade-offs**: High scalability and availability, but increased complexity
//! **Use case**: Large-scale systems needing both performance and fault tolerance (databases, caches)

mod driver;
use driver::{run_kvs_demo, KVSDemo};
use kvs_zoo::values::{CausalString, generate_causal_operations};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::ShardedRouter;

struct ShardedReplicatedDemo;

impl KVSDemo for ShardedReplicatedDemo {
    type Value = CausalString;
    type Storage = kvs_zoo::replicated::KVSReplicatedEpidemic<CausalString>;
    type Router = ShardedRouter;

    fn create_router<'a>(&self, _flow: &hydro_lang::compile::builder::FlowBuilder<'a>) -> Self::Router {
        ShardedRouter::new(3)
    }

    fn cluster_size(&self) -> usize {
        3 // 3 nodes that will be sharded and replicated
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Hash-based routing to replicated shard clusters\n\
         🔀 Consistency: Per-shard causal, global eventual\n\
         🎯 Use case: Large-scale systems needing both performance and fault tolerance"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        generate_causal_operations()
    }

    fn name(&self) -> &'static str {
        "Sharded + Replicated KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo(ShardedReplicatedDemo).await
}