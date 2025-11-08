//! Linearizable KVS Example
//!
//! **Configuration:**
//! - Architecture: `LinearizableKVSServer<LwwWrapper<String>, LogBased<BroadcastReplication>>`
//! - Routing: `PaxosInterceptor` (total order via consensus before execution)
//! - Replication: `LogBased<BroadcastReplication>` (replicated write-ahead log)
//! - Nodes: 3 Paxos acceptors + 3 log replicas + 3 KVS replicas = 9 total nodes
//! - Consistency: Linearizable (strongest consistency model)
//!
//! **What it achieves:**
//! Demonstrates strong consistency via Paxos consensus. Every operation goes through
//! a consensus round to establish total order before execution, ensuring that all
//! nodes see operations in the same sequence. The replicated write-ahead log provides
//! durability, and the value semantics (LWW) handle execution. This architecture
//! trades latency for strong consistency guarantees, making it suitable for
//! applications requiring strict ordering (e.g., financial transactions, inventory
//! management) where correctness is more important than raw performance.

// futures traits are used by the shared driver
use kvs_zoo::server::{KVSServer, LinearizableKVSServer};
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable KVS Demo (Paxos, f=1)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    type Server = LinearizableKVSServer<
        LwwWrapper<String>,
        kvs_zoo::maintain::LogBased<kvs_zoo::maintain::BroadcastReplication<LwwWrapper<String>>>,
    >;

    let paxos_cfg = kvs_zoo::dispatch::PaxosConfig {
        f: 1,
        i_am_leader_send_timeout: 1,
        i_am_leader_check_timeout: 3,
        i_am_leader_check_timeout_delay_multiplier: 1,
    };

    let replication = kvs_zoo::maintain::LogBased::new(kvs_zoo::maintain::BroadcastReplication::<
        LwwWrapper<String>,
    >::default());
    let paxos = kvs_zoo::dispatch::PaxosInterceptor::<LwwWrapper<String>>::with_config(paxos_cfg);
    let cluster = Server::create_deployment(&flow, paxos.clone(), replication.clone());
    let port = Server::run(
        &proxy,
        &cluster,
        &client_external,
        paxos,
        replication.clone(),
    );
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster.0, vec![localhost.clone(); 3])
        .with_cluster(&cluster.1, vec![localhost.clone(); 3])
        .with_cluster(&cluster.2, vec![localhost.clone(); 3])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (out, input) = nodes.connect_bincode(port).await;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await; // leader election

    let ops = kvs_zoo::demo_driver::ops_linearizable();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Linearizable demo complete");
    Ok(())
}
