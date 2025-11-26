//! Tests for slot buffering at the leaf: GETs should not overtake prior PUTs

use futures::{SinkExt, StreamExt};
// use hydro_lang::prelude::*;
use kvs_zoo::after_storage::replication::{
    BroadcastReplication, SequencedReplication as Sequenced,
};
use kvs_zoo::before_storage::SlotOrderEnforcer;
use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct OrderedCluster;
#[derive(Clone)]
struct ReplicaLeaf;

type LinearizableKVS = KVSCluster<
    OrderedCluster,
    PaxosDispatcher<String, LwwWrapper<String>>,
    Sequenced<BroadcastReplication<String, LwwWrapper<String>>>,
    KVSNode<ReplicaLeaf, SlotOrderEnforcer, ()>,
>;

#[serial_test::serial]
#[test]
fn get_waits_for_prior_put_slot() {
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let kvs = LinearizableKVS::new(
        PaxosDispatcher::new(),
        Sequenced::new(BroadcastReplication::<String, LwwWrapper<String>>::new()),
        KVSNode::<ReplicaLeaf, SlotOrderEnforcer, ()>::new(SlotOrderEnforcer::new(), ()),
    );

    let (layers, bidi_port) =
        plumb_kvs_dataflow::<String, LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs);

    // Responses are already plumbed inside plumb_kvs_dataflow; nothing to add here.

    // Deploy
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<OrderedCluster>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<ReplicaLeaf>(),
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
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        deployment.deploy().await.unwrap();
        let (mut out, mut input) = nodes.connect_bincode(bidi_port).await;
        deployment.start().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        // Send a PUT then a GET; GET must see the PUT (no stale read)
        input
            .send(kvs_zoo::protocol::KVSOperation::Put(
                "x".into(),
                LwwWrapper::new("1".into()),
                Some(1),
            ))
            .await
            .unwrap();
        input
            .send(kvs_zoo::protocol::KVSOperation::Get("x".into(), Some(1)))
            .await
            .unwrap();
        let r1 = out.next().await.unwrap();
        let r2 = out.next().await.unwrap();
        assert!(r1.contains("PUT OK"), "unexpected PUT response: {r1}");
        // Accept either display form (direct value or debug) to avoid brittle formatting assumptions.
        assert!(
            r2.contains("GET = 1") || r2.contains("GET = LwwWrapper"),
            "unexpected GET response: {r2}"
        );

        deployment.stop().await.unwrap();
    });
}
