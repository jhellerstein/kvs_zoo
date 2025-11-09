//! Sharded + Replicated KVS Example
//!
//! **Configuration:**
//! - Architecture: `ShardedKVSServer<ReplicatedKVSServer<CausalString, BroadcastReplication>>`
//! - Routing: `Pipeline<ShardedRouter, RoundRobinRouter>` (first by shard, then by replica)
//! - Replication: `BroadcastReplication` (within each shard)
//! - Nodes: 3 shards × 3 replicas each = 9 total nodes
//! - Consistency: Per-shard causal (via vector clocks), no cross-shard coordination
//!
//! **What it achieves:**
//! A "best of both" architecture combining data scalability (sharding) with
//! request scalability and fault tolerance (replication). Keys are hash-partitioned
//! across 3 shards, and each shard has 3 replicas synchronized via broadcast replication. This provides
//! cloud-scale capacity (through sharding) and high availability (through replication).
//! Within each shard, causal consistency ensures concurrent writes are preserved
//! via vector clock ordering. Ideal for large-scale production systems requiring
//! both performance and reliability.

// futures traits are used by the shared driver
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, ReplicatedKVSServer, ShardedKVSServer};
use kvs_zoo::values::CausalString;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded + Replicated KVS Demo (broadcast)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    type Inner =
        ReplicatedKVSServer<CausalString, kvs_zoo::maintenance::BroadcastReplication<CausalString>>;
    type Server = ShardedKVSServer<Inner>;
    let pipeline = kvs_zoo::dispatch::Pipeline::new(
        kvs_zoo::dispatch::ShardedRouter::new(3),
        kvs_zoo::dispatch::RoundRobinRouter::new(),
    );
    let replication = kvs_zoo::maintenance::BroadcastReplication::<CausalString>::default();

    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication.clone());
    let port = Server::run(&proxy, &cluster, &client_external, pipeline, replication);

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster, vec![localhost.clone(); 3])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (out, input) = nodes.connect_bincode(port).await;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let ops = kvs_zoo::demo_driver::ops_sharded_replicated_broadcast();
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Sharded+Replicated (broadcast) demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<CausalString>, shards: u64) -> Option<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            let shard_id = hasher.finish() % shards;
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
