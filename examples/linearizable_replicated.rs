//! Linearizable Replicated KVS (Paxos → RR → Sequenced<Broadcast> → SlotEnforce)

use futures::{SinkExt, StreamExt};
use kvs_zoo::after_storage::replication::{
    BroadcastReplication, SequencedReplication as Sequenced,
};
use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::before_storage::Pipeline;
use kvs_zoo::before_storage::ordering::SlotOrderEnforcer;
use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;
use kvs_zoo::plumbing::plumb_kvs_dataflow;

#[derive(Clone)]
struct OrderedCluster;
#[derive(Clone)]
struct SequenceReplicated;
#[derive(Clone)]
struct ReplicaLeaf;

// Nested composition:
// - Outer layer: Pipeline(Paxos ordering → simple router) with no after_storage at that layer
// - Inner layer: RoundRobin → Sequenced<Broadcast> → SlotEnforcer + Responder
// Values: LwwWrapper<String> to deliver linearizable semantics at the API.
type LinearizableReplicatedKVS = KVSCluster<
    OrderedCluster,
    Pipeline<PaxosDispatcher<LwwWrapper<String>>, RoundRobinRouter>,
    (),
    KVSCluster<
        SequenceReplicated,
        RoundRobinRouter,
        Sequenced<BroadcastReplication<LwwWrapper<String>>>,
        KVSNode<ReplicaLeaf, SlotOrderEnforcer, Responder>,
    >,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable Replicated KVS Demo (Paxos → Sequenced<Broadcast> → SlotEnforce)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define the nested KVS architecture via defaults (Paxos → RR → Sequenced<Broadcast> → Slot/Responder)
    let kvs_spec: LinearizableReplicatedKVS = Default::default();

    // Wire full dataflow with external I/O using the standard helper (down/up only)
    let (layers, bidi_port) =
        plumb_kvs_dataflow::<LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs_spec);

    // Deploy: outer cluster + Paxos role clusters + inner replicated layer + leaf
    let nodes = flow
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
        KVSOperation::Put("acct".into(), LwwWrapper::new("100".into())),
        KVSOperation::Get("acct".into()),
        KVSOperation::Put("acct".into(), LwwWrapper::new("200".into())),
        KVSOperation::Get("acct".into()),
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
