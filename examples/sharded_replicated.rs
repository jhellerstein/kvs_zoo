//! # Sharded + Replicated KVS Architecture Example
//!
//! This example demonstrates a sharded and replicated KVS architecture:
//! - **Shard-aware routing**: Operations routed to primary node of correct shard
//! - **Shard-aware replication**: Replication only within shard boundaries  
//! - **Scalability**: Can handle more data by adding more shards
//! - **High availability**: Can tolerate replica failures within shards
//!
//! **Architecture**: Hash-based key routing with shard-aware broadcast replication
//! **Trade-offs**: True scalability with controlled replication overhead
//! **Use case**: Large-scale applications requiring both scalability and fault tolerance

use futures::{SinkExt, StreamExt};
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use kvs_zoo::driver::KVSDemo;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::replicated::KVSReplicated;
use kvs_zoo::routers::{KVSRouter, ShardAwareBroadcastReplication, ShardedRoundRobin};
use kvs_zoo::run_kvs_demo_impl;
use kvs_zoo::values::LwwWrapper;

struct ShardedReplicatedDemo;

impl Default for ShardedReplicatedDemo {
    fn default() -> Self {
        ShardedReplicatedDemo
    }
}

impl KVSDemo for ShardedReplicatedDemo {
    type Value = LwwWrapper<String>;
    type Storage = KVSReplicated<ShardAwareBroadcastReplication>;
    type Router = ShardedRoundRobin;

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        ShardedRoundRobin::new(3, 3) // 3 shards, 3 replicas each
    }

    fn cluster_size(&self) -> usize {
        9 // 3 shards × 3 replicas per shard
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Hash-based sharding with shard-aware replication\n\
         🔀 Consistency: Per-shard causal, cross-shard eventual\n\
         🛡️ Fault tolerance: Can survive replica failures within shards\n\
         🎯 Use case: Large-scale applications with controlled replication"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        vec![
            KVSOperation::Put("shard_key_0".to_string(), LwwWrapper::new("value_0".to_string())),
            KVSOperation::Put("shard_key_1".to_string(), LwwWrapper::new("value_1".to_string())),
            KVSOperation::Put("shard_key_2".to_string(), LwwWrapper::new("value_2".to_string())),
            KVSOperation::Get("shard_key_0".to_string()),
            KVSOperation::Get("shard_key_1".to_string()),
            KVSOperation::Get("nonexistent".to_string()),
        ]
    }

    fn name(&self) -> &'static str {
        "Sharded + Replicated KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo_impl!(ShardedReplicatedDemo)
}
