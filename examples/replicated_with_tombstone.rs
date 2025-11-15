//! Replicated KVS with tombstone deletes (Phase 0 demo)
//!
//! Mirrors the style of existing demos while showcasing Delete + Tomb semantics.
//! Core already emits `DataEvent::Delete` and `MetaEvent::Tomb`; this example
//! focuses on client-observable behavior (Delete → subsequent Get = NOT FOUND).

use futures::{SinkExt, StreamExt};
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::background::TombIndexBackground;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;
use tokio::time::Duration;

#[derive(Clone)]
struct Replica;

type ReplicatedKVS = KVSCluster<
    Replica,
    RoundRobinRouter,
    SimpleGossip<LwwWrapper<String>>,
    (),
    TombIndexBackground,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS Tombstone Demo (gossip)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let mut kvs_spec: ReplicatedKVS = Default::default();
    kvs_spec.after = SimpleGossip::new(100usize); // 100ms gossip interval for visibility
    kvs_spec.background = TombIndexBackground::new()
        .with_logging(true)
        .with_summaries(true);

    let (layers, port) =
        plumb_kvs_dataflow::<LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs_spec);

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
    tokio::time::sleep(Duration::from_millis(500)).await;

    use kvs_zoo::protocol::KVSOperation as Op;
    let ops = vec![
        Op::Put("alpha".into(), LwwWrapper::new("one".into())),
        Op::Get("alpha".into()),
        Op::Delete("alpha".into()),
        Op::Get("alpha".into()),
    ];

    for (i, op) in ops.into_iter().enumerate() {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
        if i == 0 {
            tokio::time::sleep(Duration::from_millis(350)).await;
        }
    }

    tokio::time::sleep(Duration::from_millis(350)).await;

    deployment.stop().await?;
    println!("✅ Replicated tombstone demo complete (meta emission pending)");
    Ok(())
}
