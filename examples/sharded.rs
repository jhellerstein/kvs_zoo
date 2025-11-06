//! Sharded KVS examples demonstrating symmetric composition
//! Shows flexible ordering: ShardedRouter.then(RoundRobinRouter) vs alternatives

use futures::{SinkExt, StreamExt};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, LocalKVSServer, ReplicatedKVSServer, ShardedKVSServer};
use kvs_zoo::values::{CausalString, VCWrapper};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded KVS Demo");
    println!("🔀 Demonstrating symmetric composition patterns");
    println!();

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Example 1: Sharded + Local (ShardedRouter.then(LocalRouter))
    println!("📋 Example 1: Sharded Local KVS");
    println!("   Server: ShardedKVSServer<LocalKVSServer>");
    println!("   Pipeline: ShardedRouter.then(LocalRouter)");

    let flow1 = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy1 = flow1.process::<()>();
    let client_external1 = flow1.external::<()>();

    // Symmetric composition: server structure matches pipeline structure
    type ShardedLocal = ShardedKVSServer<LocalKVSServer<String>>;
    let pipeline1 = kvs_zoo::interception::Pipeline::new(
        kvs_zoo::interception::ShardedRouter::new(3),
        kvs_zoo::interception::LocalRouter::new(),
    );
    let replication1 = ();

    let deployment1 = ShardedLocal::create_deployment(&flow1, pipeline1.clone(), replication1);
    let client_port1 = ShardedLocal::run(
        &proxy1,
        &deployment1,
        &client_external1,
        pipeline1,
        replication1,
    );

    let nodes1 = flow1
        .with_process(&proxy1, localhost.clone())
        .with_cluster(&deployment1, vec![localhost.clone(); 3])
        .with_external(&client_external1, localhost.clone())
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut client_out1, mut client_in1) = nodes1.connect_bincode(client_port1).await;
    deployment.start().await?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Sending operations to sharded local KVS...");
    let ops1 = vec![
        KVSOperation::Put("shard1_key".to_string(), "value1".to_string()),
        KVSOperation::Put("shard2_key".to_string(), "value2".to_string()),
        KVSOperation::Get("shard1_key".to_string()),
        KVSOperation::Get("shard2_key".to_string()),
    ];

    for (i, op) in ops1.into_iter().enumerate() {
        println!("  {} {:?}", i + 1, op);
        if let Err(e) = client_in1.send(op).await {
            eprintln!("❌ Error: {}", e);
            break;
        }
        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(500), client_out1.next())
                .await
                .ok()
                .flatten()
        {
            println!("     → {}", response);
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    println!("✅ Sharded local demo completed");
    println!();

    // Example 2: Sharded + Replicated (ShardedRouter.then(RoundRobinRouter))
    println!("📋 Example 2: Sharded Replicated KVS");
    println!("   Server: ShardedKVSServer<ReplicatedKVSServer>");
    println!("   Pipeline: ShardedRouter.then(RoundRobinRouter)");

    let flow2 = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy2 = flow2.process::<()>();
    let client_external2 = flow2.external::<()>();

    // Symmetric composition: more complex nesting
    type ShardedReplicated = ShardedKVSServer<ReplicatedKVSServer<CausalString, kvs_zoo::replication::NoReplication>>;
    let pipeline2 = kvs_zoo::interception::Pipeline::new(
        kvs_zoo::interception::ShardedRouter::new(3),
        kvs_zoo::interception::RoundRobinRouter::new(),
    );
    let replication2 = kvs_zoo::replication::NoReplication::new();

    let deployment2 = ShardedReplicated::create_deployment(&flow2, pipeline2.clone(), replication2.clone());
    let client_port2 = ShardedReplicated::run(
        &proxy2,
        &deployment2,
        &client_external2,
        pipeline2,
        replication2,
    );

    let nodes2 = flow2
        .with_process(&proxy2, localhost.clone())
        .with_cluster(&deployment2, vec![localhost.clone(); 9]) // 3 shards × 3 replicas
        .with_external(&client_external2, localhost.clone())
        .deploy(&mut deployment);

    let (mut client_out2, mut client_in2) = nodes2.connect_bincode(client_port2).await;

    println!("📤 Sending operations to sharded replicated KVS...");
    let ops2 = vec![
        KVSOperation::Put("user:1".to_string(), CausalString::new(VCWrapper::new(), "Alice".to_string())),
        KVSOperation::Put("user:2".to_string(), CausalString::new(VCWrapper::new(), "Bob".to_string())),
        KVSOperation::Put("user:3".to_string(), CausalString::new(VCWrapper::new(), "Charlie".to_string())),
        KVSOperation::Get("user:1".to_string()),
        KVSOperation::Get("user:2".to_string()),
        KVSOperation::Get("user:3".to_string()),
    ];

    for (i, op) in ops2.into_iter().enumerate() {
        println!("  {} {:?}", i + 1, op);
        if let Err(e) = client_in2.send(op).await {
            eprintln!("❌ Error: {}", e);
            break;
        }
        if let Some(response) =
            tokio::time::timeout(std::time::Duration::from_millis(500), client_out2.next())
                .await
                .ok()
                .flatten()
        {
            println!("     → {}", response);
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    println!("✅ Sharded replicated demo completed");
    println!();

    println!("🎓 Composition patterns:");
    println!("   • ShardedKVSServer<LocalKVSServer> → Pipeline<ShardedRouter, LocalRouter>");
    println!("   • ShardedKVSServer<ReplicatedKVSServer> → Pipeline<ShardedRouter, RoundRobinRouter>");
    println!("   • Type system enforces matching structures at compile time");
    println!("   • Zero-cost: all composition resolved at compile time");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())
}
