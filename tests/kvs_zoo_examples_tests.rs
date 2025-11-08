//! Tests mirroring the variants demonstrated in `examples/kvs_zoo.rs`.
//! Each test constructs the same server composition and validates representative responses.

use futures::{SinkExt, StreamExt};
use tokio::time::{Duration, sleep, timeout};

use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, LocalKVSServer, ReplicatedKVSServer, ShardedKVSServer};
use kvs_zoo::values::{CausalString, LwwWrapper, VCWrapper};

// Replication strategies
use kvs_zoo::maintain::{BroadcastReplication, EpidemicGossip, LogBased, NoReplication};

// Op interceptors / pipelines
use kvs_zoo::dispatch::{
    PaxosInterceptor, Pipeline, RoundRobinRouter, ShardedRouter, SingleNodeRouter,
};

#[tokio::test]
async fn example_local_lww() {
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    type Server = LocalKVSServer<LwwWrapper<String>>;
    let pipeline = SingleNodeRouter::new();
    let replication = ();
    let cluster = <Server as KVSServer<LwwWrapper<String>>>::create_deployment(
        &flow,
        pipeline.clone(),
        replication,
    );
    let port = <Server as KVSServer<LwwWrapper<String>>>::run(
        &proxy,
        &cluster,
        &external,
        pipeline,
        replication,
    );

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster, vec![localhost.clone(); 1])
        .with_external(&external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await.unwrap();
    sleep(Duration::from_millis(300)).await;

    let ops = vec![
        KVSOperation::Put("alpha".into(), LwwWrapper::new("one".into())),
        KVSOperation::Get("alpha".into()),
    ];

    for op in ops {
        input.send(op).await.unwrap();
        let resp = timeout(Duration::from_secs(2), out.next())
            .await
            .unwrap()
            .unwrap();
        assert!(resp.contains("[LOCAL]"));
    }
}

#[tokio::test]
async fn example_replicated_none() {
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    type Server = ReplicatedKVSServer<CausalString, NoReplication>;
    let pipeline = RoundRobinRouter::new();
    let replication = NoReplication::new();
    let cluster = <Server as KVSServer<CausalString>>::create_deployment(
        &flow,
        pipeline.clone(),
        replication.clone(),
    );
    let port = <Server as KVSServer<CausalString>>::run(
        &proxy,
        &cluster,
        &external,
        pipeline,
        replication.clone(),
    );

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster, vec![localhost.clone(); 3])
        .with_external(&external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await.unwrap();
    sleep(Duration::from_millis(400)).await;

    // helper to create causal values
    let causal = |node: &str, v: &str| {
        let mut vc = VCWrapper::new();
        vc.bump(node.to_string());
        CausalString::new(vc, v.to_string())
    };

    // Two puts + get
    input
        .send(KVSOperation::Put("doc".into(), causal("n1", "x")))
        .await
        .unwrap();
    let r1 = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    assert!(r1.contains("PUT doc = OK") && r1.contains("[REPLICATED]"));

    input
        .send(KVSOperation::Put("doc".into(), causal("n2", "y")))
        .await
        .unwrap();
    let r2 = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    assert!(r2.contains("PUT doc = OK") && r2.contains("[REPLICATED]"));

    input.send(KVSOperation::Get("doc".into())).await.unwrap();
    let r3 = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    // With NoReplication + RoundRobin, the GET may hit a different node and return NOT FOUND.
    // Validate labeling and key presence; allow either NOT FOUND or a value.
    assert!(
        r3.contains("[REPLICATED]")
            && r3.contains("doc")
            && (r3.contains("NOT FOUND") || r3.contains("x") || r3.contains("y"))
    );
}

#[tokio::test]
async fn example_replicated_broadcast() {
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    type Server = ReplicatedKVSServer<CausalString, BroadcastReplication<CausalString>>;
    let pipeline = RoundRobinRouter::new();
    let replication = BroadcastReplication::<CausalString>::default();
    let cluster = <Server as KVSServer<CausalString>>::create_deployment(
        &flow,
        pipeline.clone(),
        replication.clone(),
    );
    let port = <Server as KVSServer<CausalString>>::run(
        &proxy,
        &cluster,
        &external,
        pipeline,
        replication.clone(),
    );

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster, vec![localhost.clone(); 3])
        .with_external(&external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await.unwrap();
    sleep(Duration::from_millis(400)).await;

    let causal = |node: &str, v: &str| {
        let mut vc = VCWrapper::new();
        vc.bump(node.to_string());
        CausalString::new(vc, v.to_string())
    };
    input
        .send(KVSOperation::Put("b".into(), causal("a", "x")))
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    input.send(KVSOperation::Get("b".into())).await.unwrap();
    let r = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    assert!(r.contains("[REPLICATED]") && r.contains("b"));
}

#[tokio::test]
async fn example_sharded_local() {
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    type Inner = LocalKVSServer<LwwWrapper<String>>;
    type Server = ShardedKVSServer<Inner>;
    let pipeline = Pipeline::new(ShardedRouter::new(3), SingleNodeRouter::new());
    let replication = ();
    let cluster = <Server as KVSServer<LwwWrapper<String>>>::create_deployment(
        &flow,
        pipeline.clone(),
        replication,
    );
    let port = <Server as KVSServer<LwwWrapper<String>>>::run(
        &proxy,
        &cluster,
        &external,
        pipeline,
        replication,
    );

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster, vec![localhost.clone(); 3])
        .with_external(&external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    input
        .send(KVSOperation::Put(
            "user:1".into(),
            LwwWrapper::new("alice".into()),
        ))
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    input
        .send(KVSOperation::Get("user:1".into()))
        .await
        .unwrap();
    let r = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    assert!(r.contains("[SHARDED]") && r.contains("user:1"));
}

#[tokio::test]
async fn example_linearizable_paxos_lww() {
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    use kvs_zoo::server::LinearizableKVSServer;
    type Server = LinearizableKVSServer<
        LwwWrapper<String>,
        LogBased<BroadcastReplication<LwwWrapper<String>>>,
    >;
    let paxos = PaxosInterceptor::<LwwWrapper<String>>::new();
    let replication = LogBased::new(BroadcastReplication::<LwwWrapper<String>>::default());
    let cluster = <Server as KVSServer<LwwWrapper<String>>>::create_deployment(
        &flow,
        paxos.clone(),
        replication.clone(),
    );
    let port = <Server as KVSServer<LwwWrapper<String>>>::run(
        &proxy,
        &cluster,
        &external,
        paxos,
        replication.clone(),
    );

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster.0, vec![localhost.clone(); 3])
        .with_cluster(&cluster.1, vec![localhost.clone(); 3])
        .with_cluster(&cluster.2, vec![localhost.clone(); 3])
        .with_external(&external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    input
        .send(KVSOperation::Put(
            "acct".into(),
            LwwWrapper::new("100".into()),
        ))
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(2), out.next())
        .await
        .unwrap()
        .unwrap();
    input.send(KVSOperation::Get("acct".into())).await.unwrap();
    let r = timeout(Duration::from_secs(3), out.next())
        .await
        .unwrap()
        .unwrap();
    assert!(r.contains("[LINEARIZABLE]") && r.contains("acct"));
}

// Gossip-based tests can be flaky due to timing; include but ignore by default.
#[tokio::test]
#[ignore = "Epidemic gossip timing can be flaky in CI"]
async fn example_replicated_gossip() {
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let external = flow.external::<()>();

    type Server = ReplicatedKVSServer<CausalString, EpidemicGossip<CausalString>>;
    let pipeline = RoundRobinRouter::new();
    let replication = EpidemicGossip::<CausalString>::default();
    let cluster = <Server as KVSServer<CausalString>>::create_deployment(
        &flow,
        pipeline.clone(),
        replication.clone(),
    );
    let port = <Server as KVSServer<CausalString>>::run(
        &proxy,
        &cluster,
        &external,
        pipeline,
        replication.clone(),
    );

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster, vec![localhost.clone(); 3])
        .with_external(&external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await.unwrap();
    sleep(Duration::from_millis(700)).await;

    let causal = |node: &str, v: &str| {
        let mut vc = VCWrapper::new();
        vc.bump(node.to_string());
        CausalString::new(vc, v.to_string())
    };
    input
        .send(KVSOperation::Put("g".into(), causal("a", "x")))
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(3), out.next())
        .await
        .unwrap()
        .unwrap();
    input.send(KVSOperation::Get("g".into())).await.unwrap();
    let r = timeout(Duration::from_secs(3), out.next())
        .await
        .unwrap()
        .unwrap();
    assert!(r.contains("[REPLICATED]") && r.contains("g"));
}
