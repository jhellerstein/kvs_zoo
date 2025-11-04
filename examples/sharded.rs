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

use futures::{SinkExt, StreamExt};
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use kvs_zoo::driver::KVSDemo;
use kvs_zoo::lww::KVSLww;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::{KVSRouter, ShardedRouter};
use kvs_zoo::run_kvs_demo_impl;

struct ShardedDemo;

impl Default for ShardedDemo {
    fn default() -> Self {
        ShardedDemo
    }
}

impl KVSDemo for ShardedDemo {
    type Value = String;
    type Storage = KVSLww;
    type Router = ShardedRouter;

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        ShardedRouter::new(3) // 3 shards
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
        vec![
            KVSOperation::Put("key1".to_string(), "value1".to_string()),
            KVSOperation::Put("key2".to_string(), "value2".to_string()),
            KVSOperation::Get("key1".to_string()),
            KVSOperation::Get("nonexistent".to_string()),
            KVSOperation::Put("key1".to_string(), "updated_value1".to_string()),
            KVSOperation::Get("key1".to_string()),
        ]
    }

    fn name(&self) -> &'static str {
        "Sharded KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo_impl!(ShardedDemo)
}
