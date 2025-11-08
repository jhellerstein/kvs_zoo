//! KVS Zoo Example
//!
//! A single executable demonstrating multiple KVS variants built from the composable
//! server architecture. Select a variant with `--variant <name>`:
//!
//! Variants:
//!   local                     - Single-node LWW store
//!   replicated-none           - 3-node replicated store (no background replication)
//!   replicated-gossip         - 3-node replicated store with epidemic gossip
//!   replicated-broadcast      - 3-node replicated store with broadcast replication
//!   sharded-local             - 3 shard × 1 node (LWW) = 3 nodes
//!   sharded-replicated-gossip - 3 shards × 3 replicas using causal values
//!   linearizable              - Paxos ordered + LWW write semantics
//!
//! Each variant sends a small script of PUT/GET operations and prints responses.
//!
//! Example:
//!   cargo run --example kvs_zoo -- --variant local
//!   cargo run --example kvs_zoo -- --variant replicated-gossip
//!
//! Notes:
//! - This focuses on demonstrating composition & response formatting.
//! - Networking/deployment provided by Hydro; we sleep briefly after start for readiness.
//! - For simplicity we reuse fixed cluster sizes (3) and shard counts (3).

use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, LocalKVSServer, ReplicatedKVSServer, ShardedKVSServer};
use kvs_zoo::values::{CausalString, LwwWrapper, VCWrapper};
use kvs_zoo::maintain::{EpidemicGossip, BroadcastReplication, NoReplication, LogBased};
use kvs_zoo::dispatch::{SingleNodeRouter, RoundRobinRouter, ShardedRouter, Pipeline, PaxosConfig};

use futures::{StreamExt, SinkExt};

fn parse_variant() -> String {
    let mut args = std::env::args().skip_while(|a| a != "--variant");
    let variant_flag = args.next();
    if variant_flag.is_none() {
        return "local".to_string();
    }
    args.next().unwrap_or_else(|| "local".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // No explicit init required (tests use ctor). Placeholder for consistency.
    let variant = parse_variant();
    println!("🚀 KVS Zoo starting: variant = {}", variant);

    match variant.as_str() {
        "local" => run_local().await?,
        "replicated-none" => run_replicated_none().await?,
        "replicated-gossip" => run_replicated_gossip().await?,
        "replicated-broadcast" => run_replicated_broadcast().await?,
        "sharded-local" => run_sharded_local().await?,
        "sharded-replicated-gossip" => run_sharded_replicated_gossip().await?,
        "linearizable" => run_linearizable().await?,
        other => {
            eprintln!("Unknown variant '{}'. See --help in source header.", other);
        }
    }

    println!("✅ Completed variant: {}", variant);
    Ok(())
}

async fn run_local() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n▶ Running Local LWW KVS");
    let mut deployment = hydro_deploy::Deployment::new();
    let _localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    type Server = LocalKVSServer<LwwWrapper<String>>;
    let pipeline = SingleNodeRouter::new();
    let replication = (); // no-op
    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication);
    let port = Server::run(&proxy, &cluster, &external, pipeline, replication);
    let nodes = flow
        .with_process(&proxy, _localhost.clone())
        .with_cluster(&cluster, vec![_localhost.clone(); 1])
        .with_external(&external, _localhost)
        .deploy(&mut deployment);
    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let ops = vec![
        KVSOperation::Put("alpha".into(), LwwWrapper::new("one".into())),
        KVSOperation::Get("alpha".into()),
        KVSOperation::Put("alpha".into(), LwwWrapper::new("two".into())),
        KVSOperation::Get("alpha".into()),
        KVSOperation::Get("missing".into()),
    ];

    for op in ops { input.send(op).await?; if let Some(resp) = out.next().await { println!("→ {}", resp); } }
    Ok(())
}

async fn run_replicated_none() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n▶ Running Replicated KVS (no replication strategy)");
    let mut deployment = hydro_deploy::Deployment::new();
    let _localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    type Server = ReplicatedKVSServer<CausalString, NoReplication>; // causal values typical for replication
    let pipeline = RoundRobinRouter::new();
    let replication = NoReplication::new();
    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication.clone());
    let port = Server::run(&proxy, &cluster, &external, pipeline, replication.clone());
    let nodes = flow
        .with_process(&proxy, _localhost.clone())
        .with_cluster(&cluster, vec![_localhost.clone(); 1])
        .with_external(&external, _localhost)
        .deploy(&mut deployment);
    deployment.deploy().await?; let (mut out, mut input) = nodes.connect_bincode(port).await; deployment.start().await?; tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Helper to build causal value
    let causal = |node: &str, v: &str| { let mut vc = VCWrapper::new(); vc.bump(node.to_string()); CausalString::new(vc, v.to_string()) };
    let ops = vec![
        KVSOperation::Put("doc".into(), causal("n1", "v1")),
        KVSOperation::Put("doc".into(), causal("n2", "v2")), // concurrent -> set union
        KVSOperation::Get("doc".into()),
    ];
    for op in ops { input.send(op).await?; if let Some(resp) = out.next().await { println!("→ {}", resp); } }
    Ok(())
}

async fn run_replicated_gossip() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n▶ Running Replicated KVS (gossip)");
    let mut deployment = hydro_deploy::Deployment::new(); let _localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new(); let proxy = flow.process::<()>(); let external = flow.external::<()>();

    type Server = ReplicatedKVSServer<CausalString, EpidemicGossip<CausalString>>;
    let pipeline = RoundRobinRouter::new();
    let replication = EpidemicGossip::<CausalString>::default();
    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication.clone());
    let port = Server::run(&proxy, &cluster, &external, pipeline, replication.clone());
    let nodes = flow
        .with_process(&proxy, _localhost.clone())
        .with_cluster(&cluster, vec![_localhost.clone(); 3])
        .with_external(&external, _localhost)
        .deploy(&mut deployment);
    deployment.deploy().await?; let (mut out, mut input) = nodes.connect_bincode(port).await; deployment.start().await?; tokio::time::sleep(std::time::Duration::from_millis(600)).await; // gossip convergence

    let causal = |node: &str, v: &str| { let mut vc = VCWrapper::new(); vc.bump(node.to_string()); CausalString::new(vc, v.to_string()) };
    let ops = vec![
        KVSOperation::Put("gossip".into(), causal("a", "x")),
        KVSOperation::Put("gossip".into(), causal("b", "y")),
        KVSOperation::Get("gossip".into()),
    ];
    for op in ops { input.send(op).await?; if let Some(resp) = out.next().await { println!("→ {}", resp); } }
    Ok(())
}

async fn run_replicated_broadcast() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n▶ Running Replicated KVS (broadcast)");
    let mut deployment = hydro_deploy::Deployment::new(); let _localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new(); let proxy = flow.process::<()>(); let external = flow.external::<()>();

    type Server = ReplicatedKVSServer<CausalString, BroadcastReplication<CausalString>>;
    let pipeline = RoundRobinRouter::new();
    let replication = BroadcastReplication::<CausalString>::default();
    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication.clone());
    let port = Server::run(&proxy, &cluster, &external, pipeline, replication.clone());
    let nodes = flow
        .with_process(&proxy, _localhost.clone())
        .with_cluster(&cluster, vec![_localhost.clone(); 3])
        .with_external(&external, _localhost)
        .deploy(&mut deployment);
    deployment.deploy().await?; let (mut out, mut input) = nodes.connect_bincode(port).await; deployment.start().await?; tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let causal = |node: &str, v: &str| { let mut vc = VCWrapper::new(); vc.bump(node.to_string()); CausalString::new(vc, v.to_string()) };
    let ops = vec![
        KVSOperation::Put("broadcast".into(), causal("a", "x")),
        KVSOperation::Put("broadcast".into(), causal("b", "y")),
        KVSOperation::Get("broadcast".into()),
    ];
    for op in ops { input.send(op).await?; if let Some(resp) = out.next().await { println!("→ {}", resp); } }
    Ok(())
}

async fn run_sharded_local() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n▶ Running Sharded Local KVS");
    let mut deployment = hydro_deploy::Deployment::new(); let _localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new(); let proxy = flow.process::<()>(); let external = flow.external::<()>();

    type Inner = LocalKVSServer<LwwWrapper<String>>;
    type Server = ShardedKVSServer<Inner>;
    let pipeline = Pipeline::new(ShardedRouter::new(3), SingleNodeRouter::new());
    let replication = (); // inner has none
    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication);
    let port = Server::run(&proxy, &cluster, &external, pipeline, replication);
    let nodes = flow
        .with_process(&proxy, _localhost.clone())
        .with_cluster(&cluster, vec![_localhost.clone(); 3])
        .with_external(&external, _localhost)
        .deploy(&mut deployment);
    deployment.deploy().await?; let (mut out, mut input) = nodes.connect_bincode(port).await; deployment.start().await?; tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let ops = vec![
        KVSOperation::Put("user:1".into(), LwwWrapper::new("alice".into())),
        KVSOperation::Put("user:2".into(), LwwWrapper::new("bob".into())),
        KVSOperation::Get("user:1".into()),
        KVSOperation::Get("user:2".into()),
    ];
    for op in ops { input.send(op).await?; if let Some(resp) = out.next().await { println!("→ {}", resp); } }
    Ok(())
}

async fn run_sharded_replicated_gossip() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n▶ Running Sharded Replicated Gossip KVS");
    let mut deployment = hydro_deploy::Deployment::new(); let _localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new(); let proxy = flow.process::<()>(); let external = flow.external::<()>();

    type Inner = ReplicatedKVSServer<CausalString, EpidemicGossip<CausalString>>;
    type Server = ShardedKVSServer<Inner>;
    let pipeline = Pipeline::new(ShardedRouter::new(3), RoundRobinRouter::new());
    let replication = EpidemicGossip::<CausalString>::default();
    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication.clone());
    let port = Server::run(&proxy, &cluster, &external, pipeline, replication.clone());
    let nodes = flow
        .with_process(&proxy, _localhost.clone())
        .with_cluster(&cluster, vec![_localhost.clone(); 3])
        .with_external(&external, _localhost)
        .deploy(&mut deployment);
    deployment.deploy().await?; let (mut out, mut input) = nodes.connect_bincode(port).await; deployment.start().await?; tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    let causal = |node: &str, v: &str| { let mut vc = VCWrapper::new(); vc.bump(node.to_string()); CausalString::new(vc, v.to_string()) };
    let ops = vec![
        KVSOperation::Put("profile:1".into(), causal("a", "x")),
        KVSOperation::Put("profile:1".into(), causal("b", "y")),
        KVSOperation::Get("profile:1".into()),
    ];
    for op in ops { input.send(op).await?; if let Some(resp) = out.next().await { println!("→ {}", resp); } }
    Ok(())
}

async fn run_linearizable() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n▶ Running Linearizable KVS (Paxos + LWW)");
    // For linearizable we rely on the dedicated server implementation
    let mut deployment = hydro_deploy::Deployment::new(); let _localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new(); let proxy = flow.process::<()>(); let external = flow.external::<()>();

    use kvs_zoo::linearizable::LinearizableKVSServer;
    type Server = LinearizableKVSServer<LwwWrapper<String>, LogBased<BroadcastReplication<LwwWrapper<String>>>>;
    let replication = LogBased::new(BroadcastReplication::<LwwWrapper<String>>::default());
    let paxos = kvs_zoo::dispatch::PaxosInterceptor::<LwwWrapper<String>>::new();
    let cluster = Server::create_deployment(&flow, paxos.clone(), replication.clone());
    let port = Server::run(&proxy, &cluster, &external, paxos, replication.clone());
    let nodes = flow
        .with_process(&proxy, _localhost.clone())
        .with_cluster(&cluster.0, vec![_localhost.clone(); 3])
        .with_cluster(&cluster.1, vec![_localhost.clone(); 3])
        .with_cluster(&cluster.2, vec![_localhost.clone(); 3])
        .with_external(&external, _localhost)
        .deploy(&mut deployment);
    deployment.deploy().await?; let (mut out, mut input) = nodes.connect_bincode(port).await; deployment.start().await?; tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let ops = vec![
        KVSOperation::Put("acct".into(), LwwWrapper::new("100".into())),
        KVSOperation::Get("acct".into()),
        KVSOperation::Put("acct".into(), LwwWrapper::new("75".into())),
        KVSOperation::Get("acct".into()),
    ];

    for op in ops { input.send(op).await?; if let Some(resp) = out.next().await { println!("→ {}", resp); } }
    Ok(())
}
