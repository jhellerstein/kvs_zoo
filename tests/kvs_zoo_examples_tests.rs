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
    // Ensure we clean up processes to avoid interference with other tests
    deployment.stop().await.unwrap();
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
    // Ensure we clean up processes to avoid interference with other tests
    deployment.stop().await.unwrap();
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
    // Ensure we clean up processes to avoid interference with other tests
    deployment.stop().await.unwrap();
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
    // Ensure we clean up processes to avoid interference with other tests
    deployment.stop().await.unwrap();
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
    // Small retry to mitigate transient dylib/link/startup races in suite runs
    let mut first_err: Option<String> = None;
    for attempt in 1..=2 {
        match deployment.deploy().await {
            Ok(()) => break,
            Err(e) if attempt == 1 => {
                first_err = Some(format!("{e:?}"));
                // brief backoff before retry
                sleep(Duration::from_millis(300)).await;
            }
            Err(e) => {
                let prev = first_err.unwrap_or_else(|| "<none>".to_string());
                panic!(
                    "Linearizable Paxos deployment failed twice.\n  First error: {prev}\n  Second error: {e:?}\n\nTroubleshooting steps:\n  1. Re-run with environment: RUST_LOG=info RUST_BACKTRACE=full cargo nextest run example_linearizable_paxos_lww --tests\n  2. Inspect per-node startup logs for a panic before readiness. (A 'channel closed' usually means a node process exited.)\n  3. Check for port collisions: leftover processes from previous runs may prevent binding. Kill stray processes (e.g. lsof -iTCP -sTCP:LISTEN | grep kvs_zoo).\n  4. Increase leader election grace period: try sleep 2500ms before first request.\n  5. The suite may be cleaning trybuild artifacts concurrently; a retry usually sidesteps transient dylib/link issues."
                );
            }
        }
    }
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment
        .start()
        .await
        .expect("Failed to start deployment processes. This usually indicates a crash on boot. Check the spawned process logs.");
    sleep(Duration::from_millis(1500)).await; // Wait for Paxos leader election

    input
        .send(KVSOperation::Put(
            "acct".into(),
            LwwWrapper::new("100".into()),
        ))
        .await
        .expect("Failed to send PUT(acct, 100): output channel closed. This implies the server side exited early. Review logs for a panic or configuration error.");
    let _first_resp = match timeout(Duration::from_secs(2), out.next()).await {
        Ok(Some(r)) => r,
        Ok(None) => panic!(
            "Server closed the response stream after PUT(acct, 100) without sending a response. Likely crash after request handling."
        ),
        Err(_) => panic!(
            "Timed out (2s) waiting for response to PUT(acct, 100). Possible causes: Paxos leader not elected yet or slow startup. Try increasing the initial sleep or timeout."
        ),
    };

    input
        .send(KVSOperation::Get("acct".into()))
        .await
        .expect("Failed to send GET(acct): output channel closed. This implies the server side exited early. Review logs for a panic or configuration error.");
    let r = match timeout(Duration::from_secs(3), out.next()).await {
        Ok(Some(r)) => r,
        Ok(None) => panic!(
            "Server closed the response stream before replying to GET(acct). Likely crash during or after Paxos commit."
        ),
        Err(_) => panic!(
            "Timed out (3s) waiting for response to GET(acct). Leader election or log replication may not have completed. Consider increasing the wait-for-leader delay or timeout."
        ),
    };
    assert!(r.contains("[LINEARIZABLE]") && r.contains("acct"));
    // Ensure we clean up processes to avoid interference with other tests
    deployment.stop().await.unwrap();
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
    // Ensure we clean up processes to avoid interference with other tests
    deployment.stop().await.unwrap();
}
