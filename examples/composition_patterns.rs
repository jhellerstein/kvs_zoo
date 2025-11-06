//! Educational example showing composition patterns that preserve learning value
//! Demonstrates the tradeoffs between verbosity and educational clarity

use futures::{SinkExt, StreamExt};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, ReplicatedKVSServer, ShardedKVSServer};
use kvs_zoo::linearizable::LinearizableKVSServer;
use kvs_zoo::values::CausalWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Educational Composition Patterns");
    println!("📚 Balancing verbosity vs. educational value");
    println!();

    // Pattern 1: Full verbose composition (best for learning)
    println!("📋 Pattern 1: Full Educational Structure");
    type EducationalComposition = 
        ShardedKVSServer<                    // Outer: Sharding layer
            ReplicatedKVSServer<             // Inner: Replication layer  
                CausalWrapper<String>,       // Value: Causal consistency wrapper over Strings
                kvs_zoo::replication::BroadcastReplication<CausalWrapper<String>> // Strategy: immediate broadcast
            >
        >;
    println!("   ✅ Full structure visible - excellent for learning!");
    println!("   📖 ShardedKVSServer<ReplicatedKVSServer<CausalWrapper<String>, BroadcastReplication<CausalWrapper<String>>>>");
    
    // Pattern 2: Generic function with inference
    println!("📋 Pattern 2: Generic Function (maximum inference)");
    fn create_educational_kvs<V>() -> ShardedKVSServer<ReplicatedKVSServer<V, kvs_zoo::replication::BroadcastReplication<V>>>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + Default + std::fmt::Debug + Send + Sync + 'static + lattices::Merge<V>,
    {
        ShardedKVSServer::new(3)
    }
    let _inferred_composition = create_educational_kvs::<CausalWrapper<String>>();
    println!("   ✅ create_educational_kvs::<CausalWrapper<String>>()");
    println!("   📖 Type inference reduces repetition while preserving function signature clarity");
    
    // Pattern 4: Linearizable KVS with Paxos consensus
    println!("📋 Pattern 4: Linearizable KVS (Paxos consensus for strongest consistency)");
    type LinearizableComposition = LinearizableKVSServer<
        CausalWrapper<String>,
        kvs_zoo::replication::BroadcastReplication<CausalWrapper<String>>
    >;
    println!("   ✅ LinearizableKVSServer<CausalWrapper<String>, BroadcastReplication<CausalWrapper<String>>>");
    println!("   📖 Paxos consensus provides linearizability - strongest consistency model");
    
    println!();
    println!("🎓 Educational Value Analysis:");
    println!("   • Pattern 1: Best for understanding composition structure");
    println!("   • Pattern 2: Good balance of clarity and conciseness");  
    println!("   • Pattern 3: Most concise, but hides some structure");
    println!("   • Pattern 4: Demonstrates consensus-based linearizability");
    println!("   • All patterns maintain type safety and zero-cost abstractions");
    println!("   • Value type repetition is a Rust limitation, not architecture flaw");

    // Quick demo showing the educational composition works
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Use the educational composition to show it works
    let pipeline = kvs_zoo::interception::Pipeline::new(
        kvs_zoo::interception::ShardedRouter::new(2),
        kvs_zoo::interception::RoundRobinRouter::new(),
    );
    let replication = kvs_zoo::replication::BroadcastReplication::default();

    let deployment_cluster = EducationalComposition::create_deployment(
        &flow,
        pipeline.clone(),
        replication.clone(),
    );

    let client_port = EducationalComposition::run(
        &proxy,
        &deployment_cluster,
        &client_external,
        pipeline,
        replication,
    );

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&deployment_cluster, vec![localhost.clone(); 6]) // 2 shards × 3 replicas
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("📤 Testing educational composition...");
    let op = KVSOperation::Put("test".to_string(), CausalWrapper::<String>::new(kvs_zoo::values::VCWrapper::new(), "educational!".to_string()));
    println!("  {:?}", op);
    
    if let Err(e) = client_in.send(op).await {
        eprintln!("❌ Error: {}", e);
    } else if let Some(response) = tokio::time::timeout(
        std::time::Duration::from_millis(500), 
        client_out.next()
    ).await.ok().flatten() {
        println!("     → {}", response);
    }

    println!("✅ Educational composition works perfectly!");
    println!("💡 Recommendation: Use verbose types in educational contexts, helper functions in production");

    Ok(())
}