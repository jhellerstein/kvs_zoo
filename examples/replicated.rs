//! Replicated KVS Example
//!
//! **Configuration:**
//! - Architecture: Replicated KVS with epidemic gossip
//! - Topology: 1 cluster of 3 replicas
//! - Cluster Dispatch: `RoundRobinRouter` (load balance across replicas)
//! - Maintenance: `SimpleGossip` for replica coordination
//! - Value Type: `LwwWrapper<String>` (last-writer-wins semantics)
//! - Consistency: Eventual (gossip convergence with LWW merge)
//!
//! **What it achieves:**
//! Demonstrates fault-tolerant replication with maintenance attached at appropriate levels.
//! Uses gossip to propagate updates between replicas and tombstone cleanup for local housekeeping.

use futures::{SinkExt, StreamExt};
use kvs_zoo::dispatch::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::maintenance::SimpleGossip;
use kvs_zoo::server::wire_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;

// Marker type naming this KVS layer
#[derive(Clone)]
struct Replica;

// KVS architecture type: single layer with RoundRobin + Gossip
type ReplicatedKVS = KVSCluster<Replica, RoundRobinRouter, SimpleGossip<LwwWrapper<String>>, ()>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS Demo (gossip)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture
    let kvs_spec = ReplicatedKVS::new(
        RoundRobinRouter::new(),
        SimpleGossip::new(100usize), // 100ms gossip interval
        (),
    );

    // Wire KVS dataflow (returns cluster handles + I/O port)
    let (layers, port) =
        wire_kvs_dataflow::<LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs_spec);

    // Deploy: 3 replicas for the cluster
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<Replica>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Run demo operations
    use kvs_zoo::protocol::KVSOperation as Op;
    let ops = vec![
        Op::Put("alpha".into(), LwwWrapper::new("one".into())),
        Op::Get("alpha".into()),
        Op::Put("beta".into(), LwwWrapper::new("two".into())),
        Op::Get("beta".into()),
    ];
    for (i, op) in ops.into_iter().enumerate() {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
        if i == 0 || i == 2 {
            // brief pause after first PUTs for gossip
            tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        }
    }

    deployment.stop().await?;
    println!("✅ Replicated (gossip) demo complete");
    Ok(())
}
