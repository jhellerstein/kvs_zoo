//! Replicated KVS Example
//!
//! **Configuration:**
//! - Architecture: Replicated KVS with configurable replication strategies
//! - Routing: `RoundRobinRouter` (load balance across replicas)
//! - Replication: `EpidemicGossip` (Demers-style rumor-mongering)
//! - Value Types:
//!   - `CausalString` (default) - causal consistency with vector clocks
//!   - `LwwWrapper<String>` (--lattice lww) - last-write-wins semantics
//! - Nodes: 3 replicas
//! - Consistency: Eventual (gossip convergence) with causal ordering or LWW merge
//!
//! **What it achieves:**
//! Demonstrates fault-tolerant replication with selectable consistency models like Anna KVS.
//! Uses epidemic gossip to propagate updates between replicas in the background.
//! With causal consistency, concurrent writes are preserved via set union; with
//! LWW, the last write wins. Shows how value semantics change system behavior
//! while using the same replication infrastructure.
//!
//! **Usage:**
//!   cargo run --example replicated               # default: causal
//!   cargo run --example replicated -- --lattice causal
//!   cargo run --example replicated -- --lattice lww

use kvs_zoo::dispatch::RoundRobinRouter;
use kvs_zoo::maintenance::EpidemicGossip;
use kvs_zoo::server::KVSServer;

fn parse_lattice_type() -> String {
    let mut args = std::env::args().skip_while(|a| a != "--lattice");
    let lattice_flag = args.next();
    if lattice_flag.is_none() {
        return "causal".to_string();
    }
    args.next().unwrap_or_else(|| "causal".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let lattice = parse_lattice_type();
    println!("🚀 Replicated KVS Demo (gossip, lattice={})", lattice);

    match lattice.as_str() {
        "causal" => run_causal().await?,
        "lww" => run_lww().await?,
        other => {
            eprintln!("❌ Unknown lattice type '{}'. Use 'causal' or 'lww'.", other);
            std::process::exit(1);
        }
    }

    println!("✅ Replicated (gossip) demo complete");
    Ok(())
}

async fn run_causal() -> Result<(), Box<dyn std::error::Error>> {
    use kvs_zoo::values::CausalString;

    // Server architecture: replicated with causal consistency and gossip
    type Server = KVSServer<
        CausalString,
        RoundRobinRouter,
        EpidemicGossip<CausalString>,
    >;
    
    let (mut deployment, out, input) = Server::builder()
        .with_cluster_size(3)  // 3 replicas
        .build()
        .await?;

    deployment.start().await?;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    kvs_zoo::demo_driver::run_ops(out, input, kvs_zoo::demo_driver::ops_replicated_gossip()).await?;
    Ok(())
}

async fn run_lww() -> Result<(), Box<dyn std::error::Error>> {
    use kvs_zoo::values::LwwWrapper;

    // Server architecture: replicated with LWW semantics and gossip
    type Server = KVSServer<
        LwwWrapper<String>,
        RoundRobinRouter,
        EpidemicGossip<LwwWrapper<String>>,
    >;
    
    let (mut deployment, out, input) = Server::builder()
        .with_cluster_size(3)  // 3 replicas
        .build()
        .await?;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    kvs_zoo::demo_driver::run_ops(out, input, kvs_zoo::demo_driver::ops_local()).await?;
    Ok(())
}
