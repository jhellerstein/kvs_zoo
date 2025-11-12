//! Recursive 3-level KVS (region → datacenter → node)
use futures::{SinkExt, StreamExt};
use kvs_zoo::before_storage::routing::{ShardedRouter, SingleNodeRouter};
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::after_storage::{cleanup::TombstoneCleanup, replication::SimpleGossip};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::wire_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct Region;
#[derive(Clone)]
struct Datacenter;
#[derive(Clone)]
struct Node;

// 3-level architecture: Region -> Datacenter -> Node
type GeoKVS = KVSCluster<
    Region,
    ShardedRouter,
    (),
    KVSCluster<
        Datacenter,
        ShardedRouter,
        SimpleGossip<LwwWrapper<String>>,
        KVSCluster<Node, SingleNodeRouter, TombstoneCleanup, ()>,
    >,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 3-Level Recursive Cluster Demo");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Configure architecture
    let kvs_spec = GeoKVS::new(
        ShardedRouter::new(2), // 2 regions
        (),
        KVSCluster::new(
            ShardedRouter::new(3),       // 3 datacenters per region
            SimpleGossip::new(250usize), // intra-region gossip among datacenters
            KVSCluster::new(
                SingleNodeRouter::new(),
                TombstoneCleanup::new(kvs_zoo::after_storage::cleanup::TombstoneCleanupConfig::default()), // local cleanup config
                (),
            ),
        ),
    );

    // Build a Hydro graph for the GeoKVS type, return layer handles and client I/O ports
    let (layers, port) =
        wire_kvs_dataflow::<LwwWrapper<String>, _>(&proxy, &client_external, &flow, kvs_spec);

    // Deploy clusters per layer
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<Region>(),
            vec![localhost.clone(), localhost.clone()], // 2 regions
        )
        .with_cluster(
            layers.get::<Datacenter>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()], // 3 datacenters
        )
        .with_cluster(
            layers.get::<Node>(),
            vec![
                localhost.clone(),
                localhost.clone(),
                localhost.clone(),
                localhost.clone(),
                localhost.clone(),
            ], // 5 nodes per datacenter (single-node router semantics)
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Demo workload
    let ops = vec![
        KVSOperation::Put("acct:alice".into(), LwwWrapper::new("1".into())),
        KVSOperation::Put("acct:bob".into(), LwwWrapper::new("2".into())),
        KVSOperation::Get("acct:alice".into()),
        KVSOperation::Get("acct:bob".into()),
    ];
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ 3-Level recursive demo complete");
    Ok(())
}
