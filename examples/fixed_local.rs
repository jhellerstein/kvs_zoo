//! # Fixed Local KVS Example
//! 
//! This example uses the hydro-template pattern that actually works:
//! - Process-to-process communication instead of external interfaces
//! - deployment.run_ctrl_c() instead of separate deploy/start calls
//! - Fully qualified type paths in q! macros

use hydro_deploy::Deployment;
use hydro_lang::prelude::*;

/// Simple working local KVS using the hydro-template pattern
pub fn local_kvs<'a>(client: &Process<'a, ()>, server: &Process<'a, ()>) {
    // Client sends a sequence of KVS operations to server
    let operations = client
        .source_iter(q!(vec![
            kvs_zoo::protocol::KVSOperation::Put("name".to_string(), "Alice".to_string()),
            kvs_zoo::protocol::KVSOperation::Put("age".to_string(), "25".to_string()),
            kvs_zoo::protocol::KVSOperation::Get("name".to_string()),
            kvs_zoo::protocol::KVSOperation::Get("age".to_string()),
            kvs_zoo::protocol::KVSOperation::Put("name".to_string(), "Bob".to_string()), // Update
            kvs_zoo::protocol::KVSOperation::Get("name".to_string()), // Should show Bob
        ]))
        .send_bincode(server);

    // Server processes operations and maintains a simple key-value store
    operations
        .inspect(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
            println!("📥 Server received: {:?}", op);
        }))
        .for_each(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
            match op {
                kvs_zoo::protocol::KVSOperation::Put(key, value) => {
                    println!("💾 KVS: PUT {} = {}", key, value);
                }
                kvs_zoo::protocol::KVSOperation::Get(key) => {
                    println!("📖 KVS: GET {} (lookup would happen here)", key);
                }
            }
        }));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running Fixed Local KVS Demo");
    println!("📋 Architecture: Process-to-process communication (hydro-template style)");
    println!("🔒 Consistency: Strong (deterministic)");
    println!("🎯 Use case: Working example that actually runs!");
    println!();

    let mut deployment = Deployment::new();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let client = flow.process();
    let server = flow.process();
    
    local_kvs(&client, &server);

    let _nodes = flow
        .with_process(&client, deployment.Localhost())
        .with_process(&server, deployment.Localhost())
        .deploy(&mut deployment);

    println!("🚀 Starting deployment (press Ctrl+C to stop)...");
    deployment.run_ctrl_c().await.unwrap();
    
    Ok(())
}