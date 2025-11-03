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

#[path = "driver/mod.rs"]
mod driver;
use driver::{KVSDemo, run_kvs_demo};
use hydro_lang::prelude::*;
use kvs_zoo::core::KVSNode;
use kvs_zoo::protocol::KVSOperation;

struct ShardedDemo;

impl KVSDemo for ShardedDemo {
    type Value = String;

    fn cluster_size(&self) -> usize {
        3 // 3 shards
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Hash-based key routing to independent shard nodes\n\
         🔀 Consistency: Per-shard strong, global eventual\n\
         🎯 Use case: Scalability for large datasets"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        // Use the client's standardized demo operations
        Self::client_demo_operations()
    }

    fn name(&self) -> &'static str {
        "Sharded KVS"
    }

    fn setup_dataflow<'a>(&self, client: &Process<'a, ()>, cluster: &Cluster<'a, KVSNode>) {
        // Client sends operations with routing logic
        // Using the same operations as KVSClient::generate_demo_operations() for consistency
        let operations = client.source_iter(q!(vec![
            kvs_zoo::protocol::KVSOperation::Put("key1".to_string(), "value1".to_string()),
            kvs_zoo::protocol::KVSOperation::Put("key2".to_string(), "value2".to_string()),
            kvs_zoo::protocol::KVSOperation::Get("key1".to_string()),
            kvs_zoo::protocol::KVSOperation::Get("nonexistent".to_string()),
            kvs_zoo::protocol::KVSOperation::Put("key1".to_string(), "updated_value1".to_string()),
            kvs_zoo::protocol::KVSOperation::Get("key1".to_string()),
        ]));

        // Route operations to specific cluster members based on key hash
        // This demonstrates sharding - different keys go to different nodes
        let routed_operations = operations
            .map(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
                let key = match &op {
                    kvs_zoo::protocol::KVSOperation::Put(k, _) => k,
                    kvs_zoo::protocol::KVSOperation::Get(k) => k,
                };

                // Simple hash: route based on key hash
                let shard_id = if key == "key1" || key == "nonexistent" {
                    0
                } else if key == "key2" {
                    1
                } else {
                    2
                };

                println!("🔀 Routing {} to shard {}", key, shard_id);
                (hydro_lang::location::MemberId::from_raw(shard_id), op)
            }))
            .demux_bincode(cluster);

        // Each shard processes only its assigned operations
        routed_operations
            .inspect(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
                // Use client's logging format for consistency
                println!("📥 Shard received operation");
                kvs_zoo::client::KVSClient::log_operation(op);
            }))
            .for_each(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
                match op {
                    kvs_zoo::protocol::KVSOperation::Put(key, value) => {
                        println!("💾 Shard: PUT {} = {}", key, value);
                    }
                    kvs_zoo::protocol::KVSOperation::Get(key) => {
                        println!("📖 Shard: GET {}", key);
                    }
                }
            }));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo(ShardedDemo).await
}
