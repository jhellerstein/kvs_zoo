//! # Sharded KVS Architecture Example
//! 
//! This example demonstrates a sharded (partitioned) KVS architecture:
//! - **Horizontal partitioning**: Data is split across multiple shards by key hash
//! - **Scalability**: Can handle more data by adding more shards
//! - **Load distribution**: Requests are distributed across shards
//! - **No replication**: Each key exists on exactly one shard (single point of failure)
//! 
//! **Architecture**: Hash-based key routing to independent shard nodes
//! **Trade-offs**: High scalability, but availability depends on all shards being up
//! **Use case**: Large datasets that need to be partitioned (analytics, big data)

mod driver;
use driver::{run_kvs_demo, KVSDemo};
use kvs_zoo::values::{CausalString, generate_causal_operations};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::ShardedRouter;

struct ShardedDemo;

impl KVSDemo for ShardedDemo {
    type Value = CausalString;
    type Storage = kvs_zoo::lww::KVSLww;
    type Router = ShardedRouter;

    fn create_router<'a>(&self, _flow: &hydro_lang::compile::builder::FlowBuilder<'a>) -> Self::Router {
        ShardedRouter::new(3)
    }

    fn cluster_size(&self) -> usize {
        3 // 3 shards
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Hash-based key routing to independent shard nodes\n\
         🔀 Consistency: Per-shard strong, global eventual\n\
         🎯 Use case: Scalability for large datasets"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        generate_causal_operations()
    }

    fn name(&self) -> &'static str {
        "Sharded KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo(ShardedDemo).await
}