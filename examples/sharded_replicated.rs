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

use hydro_deploy::Deployment;
use hydro_lang::prelude::*;
use kvs_zoo::core::KVSNode;

/// Sharded + Replicated KVS with both partitioning and replication
pub fn sharded_replicated_kvs<'a>(
    client: &Process<'a, ()>,
    shard1_cluster: &Cluster<'a, KVSNode>,
    shard2_cluster: &Cluster<'a, KVSNode>,
) {
    // Client sends operations
    let operations = client.source_iter(q!(vec![
        kvs_zoo::protocol::KVSOperation::Put("user_alice".to_string(), "Alice_Data".to_string()), // Shard 1
        kvs_zoo::protocol::KVSOperation::Put("user_bob".to_string(), "Bob_Data".to_string()), // Shard 2
        kvs_zoo::protocol::KVSOperation::Get("user_alice".to_string()),
        kvs_zoo::protocol::KVSOperation::Get("user_bob".to_string()),
        kvs_zoo::protocol::KVSOperation::Put("user_alice".to_string(), "Alice_Updated".to_string()),
    ]));

    // Route to shard 1 (keys containing "alice") and broadcast within shard
    let shard1_ops = operations
        .clone()
        .filter(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
            let key = match op {
                kvs_zoo::protocol::KVSOperation::Put(k, _) => k,
                kvs_zoo::protocol::KVSOperation::Get(k) => k,
            };
            key.contains("alice")
        }))
        .broadcast_bincode(
            shard1_cluster,
            nondet!(/** broadcast to all replicas in shard 1 */),
        ); // Broadcast to all replicas in shard 1

    // Route to shard 2 (keys containing "bob") and broadcast within shard
    let shard2_ops = operations
        .filter(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
            let key = match op {
                kvs_zoo::protocol::KVSOperation::Put(k, _) => k,
                kvs_zoo::protocol::KVSOperation::Get(k) => k,
            };
            key.contains("bob")
        }))
        .broadcast_bincode(
            shard2_cluster,
            nondet!(/** broadcast to all replicas in shard 2 */),
        ); // Broadcast to all replicas in shard 2

    // Shard 1 cluster processes its operations (replicated across all nodes in cluster)
    shard1_ops
        .inspect(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
            println!("📥 Shard1-Cluster received: {:?}", op);
        }))
        .for_each(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
            match op {
                kvs_zoo::protocol::KVSOperation::Put(key, value) => {
                    println!(
                        "💾 Shard1-Cluster: PUT {} = {} (replicated within shard)",
                        key, value
                    );
                }
                kvs_zoo::protocol::KVSOperation::Get(key) => {
                    println!("📖 Shard1-Cluster: GET {} (from any replica in shard)", key);
                }
            }
        }));

    // Shard 2 cluster processes its operations (replicated across all nodes in cluster)
    shard2_ops
        .inspect(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
            println!("📥 Shard2-Cluster received: {:?}", op);
        }))
        .for_each(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
            match op {
                kvs_zoo::protocol::KVSOperation::Put(key, value) => {
                    println!(
                        "💾 Shard2-Cluster: PUT {} = {} (replicated within shard)",
                        key, value
                    );
                }
                kvs_zoo::protocol::KVSOperation::Get(key) => {
                    println!("📖 Shard2-Cluster: GET {} (from any replica in shard)", key);
                }
            }
        }));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running Sharded + Replicated KVS Demo");
    println!("📋 Architecture: Hash-based routing to replicated shard clusters");
    println!("🔀 Consistency: Per-shard causal, global eventual");
    println!("🎯 Use case: Large-scale systems needing both performance and fault tolerance");
    println!();

    let mut deployment = Deployment::new();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let client = flow.process();
    let shard1_cluster = flow.cluster::<KVSNode>();
    let shard2_cluster = flow.cluster::<KVSNode>();

    sharded_replicated_kvs(&client, &shard1_cluster, &shard2_cluster);

    let _nodes = flow
        .with_process(&client, deployment.Localhost())
        .with_cluster(&shard1_cluster, vec![deployment.Localhost(); 2]) // 2 replicas per shard
        .with_cluster(&shard2_cluster, vec![deployment.Localhost(); 2]) // 2 replicas per shard
        .deploy(&mut deployment);

    println!("🚀 Starting deployment (press Ctrl+C to stop)...");
    deployment.run_ctrl_c().await.unwrap();

    Ok(())
}
