use hydro_deploy::Deployment;
use hydro_lang::prelude::*;
use kvs_zoo::protocol::KVSOperation;

/// Simple working KVS using process-to-process communication (like hydro-template)
pub fn simple_kvs<'a>(client: &Process<'a, ()>, server: &Process<'a, ()>) {
    println!("🔧 Setting up simple KVS dataflow...");
    
    // Client sends operations to server
    let operations = client
        .source_iter(q!(vec![
            kvs_zoo::protocol::KVSOperation::Put("key1".to_string(), "value1".to_string()),
            kvs_zoo::protocol::KVSOperation::Get("key1".to_string()),
            kvs_zoo::protocol::KVSOperation::Put("key2".to_string(), "value2".to_string()),
            kvs_zoo::protocol::KVSOperation::Get("key2".to_string()),
        ]))
        .send_bincode(server);

    // Server processes operations
    operations
        .inspect(q!(|op: &kvs_zoo::protocol::KVSOperation<String>| {
            println!("📥 Server received: {:?}", op);
        }))
        .for_each(q!(|op: kvs_zoo::protocol::KVSOperation<String>| {
            match op {
                kvs_zoo::protocol::KVSOperation::Put(key, value) => {
                    println!("💾 Server: PUT {} = {}", key, value);
                }
                kvs_zoo::protocol::KVSOperation::Get(key) => {
                    println!("📖 Server: GET {}", key);
                }
            }
        }));
    
    println!("✅ Simple KVS dataflow configured");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running Working Local KVS (hydro-template style)");
    
    let mut deployment = Deployment::new();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let client = flow.process();
    let server = flow.process();
    
    simple_kvs(&client, &server);

    let _nodes = flow
        .with_process(&client, deployment.Localhost())
        .with_process(&server, deployment.Localhost())
        .deploy(&mut deployment);

    println!("🚀 Starting deployment...");
    deployment.run_ctrl_c().await.unwrap();
    
    Ok(())
}