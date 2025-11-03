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

#[path = "driver/mod.rs"]
mod driver;
use driver::{KVSDemo, run_kvs_demo};
use hydro_lang::prelude::*;
use kvs_zoo::core::KVSNode;
use kvs_zoo::protocol::KVSOperation;

struct ReplicatedDemo;

impl KVSDemo for ReplicatedDemo {
    type Value = String;

    fn cluster_size(&self) -> usize {
        3 // 3-node cluster for replication
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Multiple replicas with epidemic gossip\n\
         🔄 Consistency: Causal (with vector clocks)\n\
         🎯 Use case: High availability, partition tolerance"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        // Use the client's standardized demo operations
        Self::client_demo_operations()
    }

    fn name(&self) -> &'static str {
        "Replicated KVS"
    }

    fn setup_dataflow<'a>(&self, client: &Process<'a, ()>, cluster: &Cluster<'a, KVSNode>) {
        // Client sends operations to all replicas (broadcast for replication)
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
            .broadcast_bincode(cluster, nondet!(/** broadcast to all replicas */));

        // Each node in the cluster processes operations
        // This demonstrates replication - all nodes receive all operations
        operations
            .inspect(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
                // Use client's logging format for consistency
                println!("📥 Replica received operation");
                kvs_zoo::client::KVSClient::log_operation(op);
            }))
            .for_each(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
                match op {
                    kvs_zoo::protocol::KVSOperation::Put(key, value) => {
                        println!(
                            "💾 Replica: PUT {} = {} (replicated to all nodes)",
                            key, value
                        );
                    }
                    kvs_zoo::protocol::KVSOperation::Get(key) => {
                        println!("📖 Replica: GET {} (from any replica)", key);
                    }
                }
            }));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo(ReplicatedDemo).await
}
