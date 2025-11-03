//! # Local KVS Architecture Example
//!
//! This example demonstrates the simplest KVS architecture:
//! - **Single process**: No distribution, no networking between KVS nodes
//! - **Last-Writer-Wins**: Deterministic conflict resolution
//! - **No replication**: Data exists on one node only
//! - **No sharding**: All data on the same node
//!
//! **Architecture**: Process-to-cluster communication (even for single node)
//! **Use case**: Development, testing, or scenarios where simplicity > availability

#[path = "driver/mod.rs"]
mod driver;
use driver::{KVSDemo, run_kvs_demo};
use hydro_lang::prelude::*;
use kvs_zoo::core::KVSNode;
use kvs_zoo::protocol::KVSOperation;

struct LocalDemo;

impl KVSDemo for LocalDemo {
    type Value = String;

    fn cluster_size(&self) -> usize {
        1 // Single node
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Single process, no networking\n\
         🔒 Consistency: Strong (deterministic)\n\
         🎯 Use case: Development, testing, simple applications"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        // Use the client's standardized demo operations
        Self::client_demo_operations()
    }

    fn name(&self) -> &'static str {
        "Local KVS"
    }

    fn setup_dataflow<'a>(&self, client: &Process<'a, ()>, cluster: &Cluster<'a, KVSNode>) {
        // Client sends operations to the single-node cluster
        // Using the same operations as KVSClient::generate_demo_operations() for consistency
        let operations = client
            .source_iter(q!(vec![
                kvs_zoo::protocol::KVSOperation::Put("key1".to_string(), "value1".to_string()),
                kvs_zoo::protocol::KVSOperation::Put("key2".to_string(), "value2".to_string()),
                kvs_zoo::protocol::KVSOperation::Get("key1".to_string()),
                kvs_zoo::protocol::KVSOperation::Get("nonexistent".to_string()),
                kvs_zoo::protocol::KVSOperation::Put(
                    "key1".to_string(),
                    "updated_value1".to_string()
                ),
                kvs_zoo::protocol::KVSOperation::Get("key1".to_string()),
            ]))
            .round_robin_bincode(cluster, nondet!(/** round robin to cluster members */));

        // Single node processes all operations with client-style logging
        operations
            .inspect(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
                // Use client's logging format for consistency
                println!("📥 Server received operation");
                kvs_zoo::client::KVSClient::log_operation(op);
            }))
            .for_each(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
                match op {
                    kvs_zoo::protocol::KVSOperation::Put(key, value) => {
                        println!("💾 Local KVS: PUT {} = {}", key, value);
                    }
                    kvs_zoo::protocol::KVSOperation::Get(key) => {
                        println!("📖 Local KVS: GET {} (lookup would happen here)", key);
                    }
                }
            }));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo(LocalDemo).await
}
