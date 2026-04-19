//! Replicated KVS (RoundRobin + Gossip)

use clap::{Parser, ValueEnum};
use futures::{SinkExt, StreamExt};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow_lattice;
use kvs_zoo::values::{CausalString, VCWrapper};

/// Supported lattice semantics for the replicated example.
#[derive(Clone, Debug, ValueEnum)]
enum LatticeKind {
    Lww,
    Causal,
}

// Marker type naming this KVS layer
#[derive(Clone)]
struct Replica;

// KVS architecture type: single layer with RoundRobin + Gossip
type ReplicatedKVS<V> = KVSCluster<Replica, RoundRobinRouter, SimpleGossip<String, V>, ()>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
    /// Choose lattice semantics for replicated values
    #[clap(long, value_enum, default_value_t = LatticeKind::Causal)]
    lattice: LatticeKind,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    match args.lattice {
        LatticeKind::Lww => run_example::<CausalString>(&args, causal_ops()).await,
        LatticeKind::Causal => run_example::<CausalString>(&args, causal_ops()).await,
    }
}

async fn run_example<V>(
    args: &Args,
    ops: Vec<kvs_zoo::protocol::KVSOperation<String, V>>,
) -> Result<(), Box<dyn std::error::Error>>
where
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + lattices::LatticeFrom<V>
        + lattices::IsBot
        + Send
        + Sync
        + std::hash::Hash
        + 'static,
{
    println!("🚀 Replicated KVS Demo (gossip)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture via defaults; override only the gossip interval
    let mut kvs_spec: ReplicatedKVS<V> = Default::default();
    kvs_spec.after = SimpleGossip::new(100usize); // 100ms gossip interval

    // Build a Hydro graph for the ReplicatedKVS type, return layer handles and client I/O ports
    let (layers, port) =
        plumb_kvs_dataflow_lattice::<String, V, _>(&proxy, &client_external, &mut flow, kvs_spec);

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;

    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: 3 replicas for the cluster
    let nodes = built
        .with_default_optimize()
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

fn lww_ops() -> Vec<kvs_zoo::protocol::KVSOperation<String, String>> {
    use kvs_zoo::protocol::KVSOperation as Op;

    vec![
        Op::Put("alpha".into(), "one".to_string(), 1, None),
        Op::Get("alpha".into(), 2, None),
        Op::Put("beta".into(), "two".to_string(), 3, None),
        Op::Get("beta".into(), 4, None),
    ]
}

fn causal_ops() -> Vec<kvs_zoo::protocol::KVSOperation<String, CausalString>> {
    use kvs_zoo::protocol::KVSOperation as Op;

    fn causal(node: &str, value: &str) -> CausalString {
        let mut vc = VCWrapper::new();
        vc.bump(node.to_string());
        CausalString::new(vc, value.to_string())
    }

    vec![
        Op::Put("alpha".into(), causal("client", "one"), 1, None),
        Op::Get("alpha".into(), 2, None),
        Op::Put("beta".into(), causal("client", "two"), 3, None),
        Op::Get("beta".into(), 4, None),
    ]
}
