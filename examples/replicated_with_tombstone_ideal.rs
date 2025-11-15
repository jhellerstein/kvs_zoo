//! Replicated KVS with tombstone deletes (idealized ergonomic target)
//!
//! This is the clean target style we want to support. It mirrors `replicated.rs`
//! and adds a Delete demonstrating eventual NOT FOUND plus (future) Tomb meta.
//! Tomb metadata hooks are TODO until DataEvent/MetaEvent wiring is implemented.

use futures::{SinkExt, StreamExt};
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::{DataConsumer, MetaConsumer, DataEvent, MetaEvent};
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct Replica;

// Background stub stage (Phase 0): logs meta tomb events when wired later.
#[derive(Default)]
struct BgTombIndex;
impl DataConsumer<DataEvent<LwwWrapper<String>>> for BgTombIndex {
    fn on_data(&mut self, _ev: &DataEvent<LwwWrapper<String>>) { /* ignore data for now */ }
}
impl MetaConsumer<MetaEvent> for BgTombIndex {
    fn on_meta(&mut self, meta: &MetaEvent) { println!("[background] meta = {:?}", meta); }
}

type ReplicatedKVS = KVSCluster<
    Replica,
    RoundRobinRouter,
    SimpleGossip<LwwWrapper<String>>,
    (),           // no nested child layer
    BgTombIndex,  // background pipeline placeholder
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS Tombstone Demo (idealized)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let mut spec: ReplicatedKVS = Default::default();
    spec.after = SimpleGossip::new(100); // faster gossip for demo
    // Background stage already default-constructed (BgTombIndex)

    let (layers, port) = plumb_kvs_dataflow::<LwwWrapper<String>, _>(&proxy, &client_external, &flow, spec);

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(layers.get::<Replica>(), vec![localhost.clone(), localhost.clone(), localhost.clone()])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    use kvs_zoo::protocol::KVSOperation as Op;
    let ops = vec![
        Op::Put("alpha".into(), LwwWrapper::new("one".into())),
        Op::Get("alpha".into()),
        Op::Delete("alpha".into()), // future: emit MetaEvent::Tomb("alpha") here
        Op::Get("alpha".into()),    // expect NOT FOUND
    ];

    for (i, op) in ops.into_iter().enumerate() {
        input.send(op).await?;
        if let Some(resp) = out.next().await { println!("→ {}", resp); }
        if i == 0 { tokio::time::sleep(std::time::Duration::from_millis(250)).await; }
    }

    // TODO(phase-1): Dispatch MetaEvent::Tomb so BgTombIndex sees it.
    // TODO(phase-2): Replace BgTombIndex with real tomb index & stats.

    deployment.stop().await?;
    println!("✅ Idealized tombstone demo complete (meta pending)");
    Ok(())
}
