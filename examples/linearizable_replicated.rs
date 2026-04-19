//! Linearizable Replicated KVS (Paxos → RR → Broadcast → SlotEnforce)

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::BroadcastOverwrite;
use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::before_storage::Pipeline;
use kvs_zoo::before_storage::ordering::SlotOrderEnforcer;
use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::protocol::KVSOperation;

#[derive(Clone)]
struct OrderedCluster;
#[derive(Clone)]
struct SequenceReplicated;
#[derive(Clone)]
struct ReplicaLeaf;

// Nested composition:
// - Outer layer: Pipeline(Paxos ordering → simple router) with no after_storage at that layer
// - Inner layer: RoundRobin → Broadcast → SlotEnforcer + Responder
// Values: String to deliver linearizable semantics at the API.
type LinearizableReplicatedKVS = KVSCluster<
    OrderedCluster,
    Pipeline<PaxosDispatcher<String, String>, RoundRobinRouter>,
    (),
    KVSCluster<
        SequenceReplicated,
        RoundRobinRouter,
        BroadcastOverwrite<String, String>,
        KVSNode<ReplicaLeaf, SlotOrderEnforcer, Responder>,
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
    println!("🚀 Linearizable Replicated KVS Demo (Paxos → Broadcast → SlotEnforce)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define the nested KVS architecture with synchronous broadcast for snappy local demos
    // (Paxos → RR → Broadcast(synchronous) → Slot/Responder)
    // SlotOrderEnforcer handles ordering; replication is coordination-free
    let inner_after = BroadcastOverwrite::<String, String>::new(
        
    );
    let inner_leaf = kvs_zoo::kvs_layer::KVSNode::<ReplicaLeaf, SlotOrderEnforcer, Responder>::new(
        SlotOrderEnforcer::new(),
        Responder::new(),
    );
    let inner = kvs_zoo::kvs_layer::KVSCluster::<
        SequenceReplicated,
        RoundRobinRouter,
        BroadcastOverwrite<String, String>,
        kvs_zoo::kvs_layer::KVSNode<ReplicaLeaf, SlotOrderEnforcer, Responder>,
    >::new(RoundRobinRouter::new(), inner_after, inner_leaf);

    let kvs_spec: LinearizableReplicatedKVS = kvs_zoo::kvs_layer::KVSCluster::new(
        Pipeline::new(
            PaxosDispatcher::<String, String>::new(),
            RoundRobinRouter::new(),
        ),
        (),
        inner,
    );

    // Plumb full dataflow with external I/O
    // Plumbing detects linearizability via the RequiresLinearizable trait:
    // - PaxosDispatcher implements RequiresLinearizable (establishes total order)
    // - SlotOrderEnforcer implements RequiresLinearizable (enforces sequential execution)
    // - Pipeline propagates the requirement if either component needs it
    // When detected, plumbing preserves TotalOrder through to storage instead of downgrading to NoOrder
    let (layers, bidi_port) = plumb_kvs_dataflow::<String, String, _>(
        &proxy,
        &client_external,
        &mut flow,
        kvs_spec,
    );

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;
    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: outer cluster + Paxos role clusters + inner replicated layer + leaf
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<SequenceReplicated>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
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
            layers.get::<ReplicaLeaf>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(bidi_port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Demo operations
    let ops = vec![
        KVSOperation::Put("acct".into(), "100".to_string(), 1, None),
        KVSOperation::Get("acct".into(), 2, None),
        KVSOperation::Put("acct".into(), "200".to_string(), 3, None),
        KVSOperation::Get("acct".into(), 4, None),
    ];
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ Linearizable Replicated demo complete");
    Ok(())
}
