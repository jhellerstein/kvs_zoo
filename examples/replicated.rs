//! Replicated KVS Example
//!
//! **Configuration:**
//! - Architecture: `ReplicatedKVSServer<V, EpidemicGossip<V>>`
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

use kvs_zoo::server::{KVSServer, ReplicatedKVSServer};

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

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    type Server = ReplicatedKVSServer<
        CausalString,
        kvs_zoo::maintain::EpidemicGossip<CausalString>,
    >;
    let pipeline = kvs_zoo::dispatch::RoundRobinRouter::new();
    let replication = kvs_zoo::maintain::EpidemicGossip::with_config(
        kvs_zoo::maintain::EpidemicGossipConfig::small_cluster(),
    );

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    kvs_zoo::demo_driver::run_ops(out, input, kvs_zoo::demo_driver::ops_replicated_gossip()).await?;
    Ok(())
}

async fn run_lww() -> Result<(), Box<dyn std::error::Error>> {
    use kvs_zoo::values::LwwWrapper;

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    type Server = ReplicatedKVSServer<
        LwwWrapper<String>,
        kvs_zoo::maintain::EpidemicGossip<LwwWrapper<String>>,
    >;
    let pipeline = kvs_zoo::dispatch::RoundRobinRouter::new();
    let replication = kvs_zoo::maintain::EpidemicGossip::with_config(
        kvs_zoo::maintain::EpidemicGossipConfig::small_cluster(),
    );

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    kvs_zoo::demo_driver::run_ops(out, input, kvs_zoo::demo_driver::ops_local()).await?;
    Ok(())
}
