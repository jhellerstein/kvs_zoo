//! 3-Level Recursive Cluster Example
//!
//! Demonstrates arbitrary-depth cluster nesting with the recursive API:
//! - **Regions** (2): Route by key to region
//! - **Datacenters** (3 per region): Gossip replication within region
//! - **Nodes** (5 per datacenter): Local tombstone cleanup
//!
//! Total: 2 regions × 3 datacenters × 5 nodes = 30 nodes
//!
//! This showcases the power of the recursive cluster spec:
//! - Dispatch chains: Pipeline<RegionRouter, Pipeline<DatacenterRouter, ()>>
//! - Maintenance chains: CombinedMaintenance<Zero, CombinedMaintenance<Gossip, Tombstone>>

use futures::{SinkExt, StreamExt};
use kvs_zoo::cluster_spec_recursive::{KVSCluster, KVSNode};
use kvs_zoo::dispatch::routing::{ShardedRouter, SingleNodeRouter};
use kvs_zoo::maintenance::{SimpleGossip, TombstoneCleanup};
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 3-Level Recursive Cluster Demo\n");
    println!("   Structure: Regions → Datacenters → Nodes");
    println!("   Total: 2 regions × 3 datacenters × 5 nodes = 30 nodes\n");

    // Define the 3-level cluster spec
    let spec = KVSCluster {
        // Level 1: Route to region by key hash
        dispatch: ShardedRouter::new(2),
        maintenance: (), // No cross-region sync
        count: 2, // 2 regions
        each: KVSCluster {
            // Level 2: Route to datacenter (round-robin for simplicity)
            dispatch: ShardedRouter::new(3),
            maintenance: SimpleGossip::<LwwWrapper<String>>::new(200), // Gossip between datacenters
            count: 3, // 3 datacenters per region
            each: KVSNode {
                // Level 3: Actual storage nodes
                dispatch: SingleNodeRouter, // No routing at leaf
                maintenance: TombstoneCleanup::new(5_000), // Local cleanup every 5s
                count: 5, // 5 nodes per datacenter
            },
        },
    };

    println!("📊 Spec stats:");
    println!("   Total nodes: {}", spec.total_nodes());
    println!("   Regions: {}", spec.count);
    println!("   Datacenters per region: {}", spec.each.count);
    println!("   Nodes per datacenter: {}", spec.each.each.count);
    println!();

    // Build the server - dispatch and maintenance chains are built recursively!
    let mut built = spec.build_server::<LwwWrapper<String>>().await?;

    println!("✅ Built 30-node cluster with 3-level recursive spec");
    println!("   Dispatch: Region → Datacenter → Node");
    println!("   Maintenance: () → Gossip → TombstoneCleanup\n");

    built.start().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Run demo operations
    use kvs_zoo::protocol::KVSOperation as Op;
    let (mut out, mut input) = built.take_ports();

    println!("📝 Testing operations across regions...\n");

    // Keys that will likely hash to different regions
    let test_data = vec![
        ("region_a_key", "Value from Region A"),
        ("region_b_key", "Value from Region B"),
        ("another_a", "Another in Region A"),
        ("another_b", "Another in Region B"),
    ];

    for (key, value) in &test_data {
        input
            .send(Op::Put(
                key.to_string(),
                LwwWrapper::new(value.to_string()),
            ))
            .await?;
        if let Some(resp) = out.next().await {
            println!("   {}", resp);
        }
    }

    println!();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    for (key, _) in &test_data {
        input.send(Op::Get(key.to_string())).await?;
        if let Some(resp) = out.next().await {
            println!("   {}", resp);
        }
    }

    println!("\n⏱️  Waiting for gossip propagation (600ms = 3 hops × 200ms)...");
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    println!("\n🔍 Verifying after gossip convergence...\n");

    for (key, _) in &test_data {
        input.send(Op::Get(key.to_string())).await?;
        if let Some(resp) = out.next().await {
            println!("   {}", resp);
        }
    }

    println!("\n✅ 3-Level Recursive demo complete!");
    println!("   The recursive API supports arbitrary nesting depth!");

    Ok(())
}
