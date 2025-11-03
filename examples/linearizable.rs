use hydro_deploy::Deployment;
use kvs_zoo::linearizable::StringLinearizableKVSServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up localhost deployment
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow with proxy, proposers, acceptors, replicas, and external client
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process();
    let proposers = flow.cluster();
    let acceptors = flow.cluster();
    let replicas = flow.cluster();
    let client_external = flow.external();

    // Build the linearizable KVS with Paxos consensus
    // f=1 means we can tolerate 1 failure (need 2f+1=3 nodes)
    let (_client_input_port, _get_results_port, _get_fails_port) = 
        StringLinearizableKVSServer::run_linearizable_kvs(
            &proxy,
            &proposers,
            &acceptors,
            &replicas,
            &client_external,
            1, // f = 1, so we need 3 acceptors/proposers
            1, // i (unique cluster ID)
        );

    // Deploy to localhost with 3-node clusters
    let _nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&proposers, vec![localhost.clone(); 3])
        .with_cluster(&acceptors, vec![localhost.clone(); 3])
        .with_cluster(&replicas, vec![localhost.clone(); 3])
        .with_external(&client_external, localhost.clone())
        .deploy(&mut deployment);

    deployment.deploy().await?;

    println!("\n========================================");
    println!("  Linearizable KVS Server with Paxos");
    println!("========================================\n");
    
    println!("Deployment Architecture:");
    println!("  📥 Proxy       : 1 node  - Client request handler");
    println!("  🔄 Proposers   : 3 nodes - Paxos proposal layer");
    println!("  📝 Acceptors   : 3 nodes - Paxos acceptance layer");
    println!("  💾 Replicas    : 3 nodes - State machine replicas");
    println!("");
    
    println!("Consensus Configuration:");
    println!("  f = 1 (tolerates 1 failure)");
    println!("  Quorum size = 2 (out of 3 nodes)");
    println!("");
    
    println!("Linearizability Guarantees:");
    println!("  ✓ Total ordering via Paxos consensus");
    println!("  ✓ All operations appear atomic");
    println!("  ✓ Real-time ordering of operations");
    println!("  ✓ Strongest consistency model");
    println!("");
    
    println!("Comparison with Other KVS Variants:");
    println!("  • Local KVS         : Single node, no distribution");
    println!("  • Replicated KVS    : Eventual consistency (gossip)");
    println!("  • Sharded KVS       : Causal consistency (vector clocks)");
    println!("  • Linearizable KVS  : Linearizability (Paxos) ← You are here!");
    println!("");
    
    println!("How it Works:");
    println!("  1. Client sends operation to Proxy");
    println!("  2. Proxy forwards to Proposers (round-robin)");
    println!("  3. Proposers run Paxos consensus with Acceptors");
    println!("  4. Consensus establishes total order");
    println!("  5. Replicas apply operations in order");
    println!("  6. Results return to client");
    println!("");
    
    println!("📊 Server is running. Press Ctrl+C to stop.\n");

    // Keep the deployment running
    tokio::signal::ctrl_c().await?;
    println!("\n✓ Shutting down gracefully...");
    
    Ok(())
}
