//! Linearizable KVS Example
//!
//! **Architecture:** Paxos consensus with Log-based Broadcast replication
//!
//! **Dispatch:** PaxosDispatcher (total ordering via consensus)
//! **Maintenance:** LogBased<BroadcastReplication> (replicated write-ahead log)
//! **Nodes:** 3 Paxos acceptors + 3 proposers + 3 KVS replicas = 9 total
//! **Consistency:** Linearizable (strongest consistency model)

use kvs_zoo::cluster_spec::{KVSCluster, KVSNode};
use kvs_zoo::dispatch::PaxosDispatcher;
use kvs_zoo::maintenance::{BroadcastReplication, LogBased};
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable KVS Demo (Paxos, f=1)");

    // Runtime configuration
    let paxos_cfg = kvs_zoo::dispatch::PaxosConfig {
        f: 1,
        ..Default::default()
    };

    // Define the cluster topology hierarchically
    let cluster_spec = KVSCluster::new(
        PaxosDispatcher::with_config(paxos_cfg),
        LogBased::<BroadcastReplication<LwwWrapper<String>>>::default(),
        1, // 1 cluster
        KVSNode {
            dispatch: (),
            maintenance: (),
            count: 3, // 3 KVS replicas
        },
    )
    .with_aux1_named(3, "proposers") // 3 Paxos proposers
    .with_aux2_named(3, "acceptors"); // 3 Paxos acceptors

    let mut built = cluster_spec.build_server::<LwwWrapper<String>>().await?;

    built.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await; // leader election

    let ops = kvs_zoo::demo_driver::ops_linearizable();
    let (out, input) = built.take_ports();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Linearizable demo complete");
    Ok(())
}
