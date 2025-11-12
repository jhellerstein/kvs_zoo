//! Local KVS Example
//!
//! **Configuration:**
//! - Architecture: Single node KVS
//! - Topology: 1 node (no sharding, no replication)
//! - Routing: `SingleNodeRouter` (direct to single node)
//! - Replication: None
//! - Consistency: Strong (deterministic single-threaded ordering)
//!
//! **What it achieves:**
//! This is the simplest KVS architecture, serving as a baseline for the composable
//! server framework. All operations execute on a single node with last-write-wins
//! semantics. No networking or replication overhead, making it suitable for
//! development, testing, and simple single-machine applications.

use kvs_zoo::dispatch::SingleNodeRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::server::wire_kvs_dataflow;
use kvs_zoo::values::LwwWrapper;

// Hydro location type = KVS layer type (no duplication!)
#[derive(Clone)]
struct LocalStorage;

// Architecture: single layer, single node
type LocalKVS = KVSCluster<LocalStorage, SingleNodeRouter, (), ()>; // KVSCluster<Marker type, Dispatch, Maintenance, Nested layer>

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Local KVS Demo (single node)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Wire the KVS architecture into dataflow
    let (layers, port) = wire_kvs_dataflow::<LwwWrapper<String>, _>(
        &proxy,
        &client_external,
        &flow,
        LocalKVS::default(),
    );

    // Deploy: cluster of 1 node for local storage
    let nodes = flow
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
        Op::Put("alpha".into(), LwwWrapper::new("one".into())),
        Op::Get("alpha".into()),
        Op::Put("alpha".into(), LwwWrapper::new("two".into())),
        Op::Get("alpha".into()),
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
