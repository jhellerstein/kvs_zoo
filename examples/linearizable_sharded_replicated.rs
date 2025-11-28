//! Linearizable Sharded+Replicated KVS (Paxos → Sharded → RR → Sequenced<Broadcast> → SlotEnforce)

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::{
    BroadcastReplication, SequencedReplication as Sequenced,
};
use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::before_storage::ordering::SlotOrderEnforcer;
use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
use kvs_zoo::before_storage::routing::{RoundRobinRouter, ShardedRouter};
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct OrderedCluster; // Paxos ordering layer
#[derive(Clone)]
struct Shard; // Shard layer
#[derive(Clone)]
struct Replica; // Per-shard replica group
#[derive(Clone)]
struct ReplicaLeaf; // Leaf executor with slot enforcement + responder

// Nested composition:
// - Outer layer: Paxos ordering only (no routing at this layer)
// - Child: Sharded router layer
// - Grandchild: Per-shard replica group (RR) with sequenced broadcast replication
// - Leaf: Slot-enforced apply + responder
// Values: LwwWrapper<String> for linearizable semantics.
type LinearizableShardedReplicatedKVS = KVSCluster<
    OrderedCluster,
    PaxosDispatcher<String, LwwWrapper<String>>,
    (),
    KVSCluster<
        Shard,
        ShardedRouter,
        (),
        KVSCluster<
            Replica,
            RoundRobinRouter,
            Sequenced<BroadcastReplication<String, LwwWrapper<String>>>,
            KVSNode<ReplicaLeaf, SlotOrderEnforcer, Responder>,
        >,
    >,
>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!(
        "🚀 Linearizable Sharded+Replicated KVS Demo (Paxos → Sharded → Sequenced<Broadcast> → SlotEnforce)"
    );

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define the nested KVS architecture via defaults (Paxos → Sharded → RR → Sequenced<Broadcast> → Slot/Responder)
    let kvs_spec: LinearizableShardedReplicatedKVS = Default::default();

    // Plumb full dataflow with external I/O using the standard helper
    let (layers, bidi_port) =
        plumb_kvs_dataflow::<String, LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs_spec);

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;
    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: paxos roles + outer cluster + shard cluster + per-shard replica cluster + leaf
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get_role::<OrderedCluster, kvs_zoo::before_storage::ordering::Proposer>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get_role::<OrderedCluster, kvs_zoo::before_storage::ordering::Acceptor>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<OrderedCluster>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<Shard>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<Replica>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<ReplicaLeaf>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(bidi_port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Demo operations across shards
    let ops = vec![
        KVSOperation::Put("user:alice".into(), LwwWrapper::new("A".into()), 1, None),
        KVSOperation::Put("user:bob".into(), LwwWrapper::new("B".into()), 2, None),
        KVSOperation::Get("user:alice".into(), 3, None),
        KVSOperation::Get("user:bob".into(), 4, None),
    ];
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ Linearizable Sharded+Replicated demo complete");
    Ok(())
}
