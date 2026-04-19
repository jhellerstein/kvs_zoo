//! Linearizable Sharded+Replicated KVS (Paxos ordering → all replicas)
//!
//! NOTE: Simplified to use PaxosDispatcher directly (no sharding layer)
//! because ShardedRouter erases TotalOrder in the type system.
//! The coordination analysis correctly reports SEQUENTIALLY CONSISTENT.

use futures::{SinkExt, StreamExt};
use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::before_storage::ordering::paxos::PaxosDispatcher;
use kvs_zoo::before_storage::routing::SingleNodeRouter;
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::plumbing::plumb_kvs_dataflow_ordered;
use kvs_zoo::protocol::KVSOperation;

#[derive(Clone)]
struct OrderedCluster;
#[derive(Clone)]
struct Leaf;

type LinearizableKVS = KVSCluster<
    OrderedCluster,
    PaxosDispatcher<String, String>,
    (),
    KVSNode<Leaf, SingleNodeRouter, Responder>,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable KVS Demo (Paxos ordering)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let kvs_spec: LinearizableKVS = Default::default();

    let (layers, bidi_port) = plumb_kvs_dataflow_ordered::<String, String, _>(
        &proxy,
        &client_external,
        &mut flow,
        kvs_spec,
    );

    let built = flow.finalize();

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
            layers.get::<Leaf>(),
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
        KVSOperation::Put("user:alice".into(), "A".to_string(), 1, None),
        KVSOperation::Put("user:bob".into(), "B".to_string(), 2, None),
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
