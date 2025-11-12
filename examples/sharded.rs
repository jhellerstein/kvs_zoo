//! Sharded KVS Example
//!
//! **Configuration:**
//! - Architecture: Sharded KVS with local nodes
//! - Topology: 3 shards × 1 node each = 3 total nodes
//! - Routing: `ShardedRouter` at cluster level, `SingleNodeRouter` at node level (hash-based partitioning)
//! - Replication: None (each shard is a single local node with no maintenance)
//! - Consistency: Per-shard strong (deterministic), no cross-shard coordination
//!
//! **What it achieves:**
//! Demonstrates horizontal data scalability through hash-based key partitioning. Each
//! key is deterministically routed to one of 3 shards based on its hash, allowing
//! the system to handle larger datasets by distributing load. Shards operate
//! independently with no cross-shard communication, making this a pure partitioning
//! architecture suitable for high-throughput, low-latency workloads where keys are accessed
//! independently.

use futures::{SinkExt, StreamExt};
use kvs_zoo::dispatch::ShardedRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::wire_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;

// Marker type naming this KVS layer
#[derive(Clone)]
struct Shard;

// KVS architecture type: single layer with sharded routing
type ShardedKVS = KVSCluster<Shard, ShardedRouter, (), ()>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded Local KVS Demo");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture
    let kvs_spec = ShardedKVS::new(
        ShardedRouter::new(3), // route to shard by key hash
        (),                    // no maintenance
        (),
    );

    // Wire KVS dataflow (returns cluster handles + I/O port)
    let (layers, port) =
        wire_kvs_dataflow::<LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs_spec);

    // Deploy: 3 shards, 1 node each
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<Shard>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Workload inline
    let ops = vec![
        KVSOperation::Put("user:1".into(), LwwWrapper::new("alice".into())),
        KVSOperation::Put("user:2".into(), LwwWrapper::new("bob".into())),
        KVSOperation::Get("user:1".into()),
        KVSOperation::Get("user:2".into()),
    ];
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ Sharded local demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<LwwWrapper<String>>, shards: u64) -> Option<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
            let shard_id =
                kvs_zoo::dispatch::ShardedRouter::calculate_shard_id(key, shards as usize);
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
