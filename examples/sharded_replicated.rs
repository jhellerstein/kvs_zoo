//! # Sharded + Replicated KVS Architecture Example
//!
//! This example demonstrates a sharded and replicated KVS architecture:
//! - **Horizontal partitioning**: Data is split across multiple shards by key hash
//! - **Per-shard replication**: Each shard is replicated for fault tolerance
//! - **Scalability**: Can handle more data by adding more shards
//! - **High availability**: Can tolerate both shard and replica failures
//!
//! **Architecture**: Hash-based key routing with per-shard replication
//! **Trade-offs**: High scalability and availability, but complex consistency model
//! **Use case**: Web-scale applications requiring both scalability and fault tolerance

use futures::{SinkExt, StreamExt};
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use kvs_zoo::driver::KVSDemo;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::replicated::KVSReplicated;
use kvs_zoo::routers::{BroadcastReplication, KVSRouter, ShardedRouter};
use kvs_zoo::run_kvs_demo_impl;
use kvs_zoo::values::CausalString;

/// Type alias for sharded + replicated KVS with broadcast replication
type ShardedReplicatedKVS = KVSReplicated<BroadcastReplication<CausalString>>;

struct ShardedReplicatedDemo;

impl Default for ShardedReplicatedDemo {
    fn default() -> Self {
        ShardedReplicatedDemo
    }
}

impl KVSDemo for ShardedReplicatedDemo {
    type Value = CausalString;
    type Storage = ShardedReplicatedKVS;
    type Router = ShardedRouter;

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        ShardedRouter::new(3) // 3 shards, each replicated
    }

    fn cluster_size(&self) -> usize {
        9 // 3 shards × 3 replicas per shard
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Hash-based sharding with per-shard replication\n\
         🔀 Consistency: Per-shard causal, global eventual\n\
         🛡️ Fault tolerance: Can survive shard and replica failures\n\
         🎯 Use case: Web-scale applications with scalability and availability"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        vec![
            KVSOperation::Put("key1".to_string(), {
                let mut vc = kvs_zoo::values::VCWrapper::new();
                vc.bump("client".to_string());
                CausalString::new(vc, "value1".to_string())
            }),
            KVSOperation::Put("key2".to_string(), {
                let mut vc = kvs_zoo::values::VCWrapper::new();
                vc.bump("client".to_string());
                CausalString::new(vc, "value2".to_string())
            }),
            KVSOperation::Get("key1".to_string()),
            KVSOperation::Get("nonexistent".to_string()),
            KVSOperation::Put("key1".to_string(), {
                let mut vc = kvs_zoo::values::VCWrapper::new();
                vc.bump("client".to_string());
                CausalString::new(vc, "updated_value1".to_string())
            }),
            KVSOperation::Get("key1".to_string()),
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
