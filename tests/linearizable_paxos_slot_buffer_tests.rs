//! Tests for slot buffering at the leaf: GETs should not overtake prior PUTs

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*;
use kvs_zoo::before_storage::SlotOrderEnforcer;
use kvs_zoo::before_storage::ordering::paxos::{PaxosConfig, PaxosDispatcher, paxos_order_slotted};
use kvs_zoo::before_storage::ordering::paxos_core::{Acceptor, Proposer};
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode, KVSSpec};
use kvs_zoo::after_storage::replication::{BroadcastReplication, LogBasedDelivery as LogBased};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct ReplicaCluster;
#[derive(Clone)]
struct ReplicaLeaf;

type LinearizableKVS = KVSCluster<
    ReplicaCluster,
    RoundRobinRouter,
    LogBased<BroadcastReplication<LwwWrapper<String>>>,
    KVSNode<ReplicaLeaf, SlotOrderEnforcer, ()>,
>;

#[test]
fn get_waits_for_prior_put_slot() {
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let kvs = LinearizableKVS::new(
        RoundRobinRouter::new(),
    LogBased::new(BroadcastReplication::<LwwWrapper<String>>::new()),
        KVSNode::<ReplicaLeaf, SlotOrderEnforcer, ()>::new(SlotOrderEnforcer::new(), ()),
    );

    // Create clusters
    let mut layers = kvs_zoo::kvs_layer::KVSClusters::new();
    let _entry = kvs.create_clusters(&flow, &mut layers);
    let proposers = flow.cluster::<Proposer>();
    let acceptors = flow.cluster::<Acceptor>();

    // Build a tiny pipeline like in the example
    let (bidi_port, operations_stream, _membership, complete_sink) = proxy
        .bidi_external_many_bincode::<_, KVSOperation<LwwWrapper<String>>, String>(
            &client_external,
        );

    let initial_ops = operations_stream
        .entries()
        .map(q!(|(_client_id, op)| op))
        .assume_ordering(nondet!(/** client ops */));

    let dispatcher = PaxosDispatcher::<LwwWrapper<String>>::with_config(PaxosConfig::default());
    let slotted_ops = paxos_order_slotted(&dispatcher, initial_ops, &proposers, &acceptors);

    // Send slotted ops back to proxy for wiring
    let slotted_ops_process = slotted_ops
        .send_bincode(&proxy)
        .values()
        .assume_ordering(nondet!(/** slotted ops at proxy */));

    // Use two-layer slotted wiring: preserves slots through cluster dispatch/replication to leaf SlotOrderEnforcer
    // Wrap slots into envelopes to preserve metadata through wiring
    let enveloped = slotted_ops_process.map(q!(|(slot, op)| kvs_zoo::protocol::Envelope::new(slot, op)));
    let responses = kvs_zoo::server::wire_two_layer_from_enveloped(&proxy, &layers, &kvs, enveloped);

    let proxy_responses = responses.send_bincode(&proxy);
    complete_sink.complete(
        proxy_responses
            .entries()
            .map(q!(|(_member_id, response)| (0u64, response)))
            .into_keyed(),
    );

    // Deploy
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<ReplicaCluster>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            layers.get::<ReplicaLeaf>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            &proposers,
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            &acceptors,
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
            ))
            .await
            .unwrap();
        input
            .send(kvs_zoo::protocol::KVSOperation::Get("x".into()))
            .await
            .unwrap();
        let r1 = out.next().await.unwrap();
        let r2 = out.next().await.unwrap();
        assert!(r1.contains("PUT x = OK"));
        // Accept either display form (direct value or debug) to avoid brittle formatting assumptions.
        assert!(
            r2.contains("GET x = 1") || r2.contains("GET x = LwwWrapper"),
            "unexpected GET response: {r2}"
        );

        deployment.stop().await.unwrap();
    });
}
