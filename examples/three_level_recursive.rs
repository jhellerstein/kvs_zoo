//! Recursive 3-level KVS (region → datacenter → node)
use futures::{SinkExt, StreamExt};
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::before_storage::routing::{ShardedRouter, SingleNodeRouter};
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow_lattice;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::CausalWrapper;

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
        SimpleGossip<String, CausalWrapper<String>>,
        KVSCluster<Node, SingleNodeRouter, (), ()>,
    >,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 3-Level Recursive Cluster Demo");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Configure architecture
    let kvs_spec = GeoKVS::new(
        ShardedRouter::new(2), // 2 regions
        (),
        KVSCluster::new(
            ShardedRouter::new(3),       // 3 datacenters per region
            SimpleGossip::new(250usize), // intra-region gossip among datacenters
            KVSCluster::new(SingleNodeRouter::new(), (), ()),
        ),
    );

    // Build a Hydro graph for the GeoKVS type, return layer handles and client I/O ports
    let (layers, port) = plumb_kvs_dataflow_lattice::<String, CausalWrapper<String>, _>(
        &proxy,
        &client_external,
        &mut flow,
        kvs_spec,
    );

    let built = flow.finalize();

    // Deploy clusters per layer
    let nodes = built
        .with_default_optimize()
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
    use kvs_zoo::values::VCWrapper;
    let ops = vec![
        KVSOperation::Put("acct:alice".into(), CausalWrapper::new(VCWrapper::new(), "1".to_string()), 1, None),
        KVSOperation::Put("acct:bob".into(), CausalWrapper::new(VCWrapper::new(), "2".to_string()), 2, None),
        KVSOperation::Get("acct:alice".into(), 3, None),
        KVSOperation::Get("acct:bob".into(), 4, None),
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
