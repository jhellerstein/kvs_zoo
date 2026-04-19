//! Local KVS (single node)

use kvs_zoo::before_storage::routing::SingleNodeRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::plumbing::plumb_kvs_dataflow;

// Marker type for Hydro location type / KVS layer type
#[derive(Clone)]
struct LocalStorage;

// Architecture: single layer, single node
type LocalKVS = KVSCluster<LocalStorage, SingleNodeRouter, (), ()>; // KVSCluster<Marker type, Before, After, Nested layer>

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Local KVS Demo (single node)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Build a Hydro graph for the LocalKVS type, return layer handles and client I/O ports
    let (layers, port) = plumb_kvs_dataflow::<String, String, _>(
        &proxy,
        &client_external,
        &mut flow,
        LocalKVS::default(),
    );

    let built = flow.finalize();

    // Deploy: cluster of 1 node for local storage
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(layers.get::<LocalStorage>(), vec![localhost.clone()])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Run demo operations
    use futures::{SinkExt, StreamExt};
    use kvs_zoo::protocol::KVSOperation as Op;
    
    let ops = vec![
        Op::Put("alpha".into(), "one".to_string(), 1, None),
        Op::Get("alpha".into(), 2, None),
        Op::Put("alpha".into(), "two".to_string(), 3, None),
        Op::Get("alpha".into(), 4, None),
    ];
    
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    println!("✅ Local demo complete");
    deployment.stop().await?;
    Ok(())
}
