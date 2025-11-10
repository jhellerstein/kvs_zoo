//! Linearizable KVS Example
//!
//! **Architecture:** Paxos consensus with Log-based Broadcast replication
//!
//! **Dispatch:** PaxosDispatcher (total ordering via consensus)
//! **Maintenance:** LogBased<BroadcastReplication> (replicated write-ahead log)
//! **Nodes:** 3 Paxos acceptors + 3 proposers + 3 KVS replicas = 9 total
//! **Consistency:** Linearizable (strongest consistency model)

use kvs_zoo::dispatch::PaxosDispatcher;
use kvs_zoo::maintenance::{BroadcastReplication, LogBased};
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::LwwWrapper;

// ====================================================================================
// Server Architecture Declaration
// ====================================================================================
// This type defines the complete system architecture:
// - Value type: LwwWrapper<String> (last-writer-wins semantics)
// - Dispatch: PaxosDispatcher (total ordering via consensus)
// - Maintenance: LogBased<BroadcastReplication> (replicated ordered log)
type Server = KVSServer<
    LwwWrapper<String>,
    PaxosDispatcher<LwwWrapper<String>>,
    LogBased<BroadcastReplication<LwwWrapper<String>>>,
>;
// ====================================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable KVS Demo (Paxos, f=1)");

    // Runtime configuration (only values that need to be configured, not types)
    let paxos_cfg = kvs_zoo::dispatch::PaxosConfig {
        f: 1,
        ..Default::default()
    };
    let dispatch = PaxosDispatcher::with_config(paxos_cfg);
    let maintenance = LogBased::<BroadcastReplication<LwwWrapper<String>>>::default();

    let mut built = Server::builder()
        .with_cluster_size(3) // 3 KVS replicas
        .with_aux1_nodes(3) // 3 Paxos proposers
        .with_aux2_nodes(3) // 3 Paxos acceptors
        .build_with(dispatch, maintenance)
        .await?;

    built.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await; // leader election

    let ops = kvs_zoo::demo_driver::ops_linearizable();
    let (out, input) = built.take_ports();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Linearizable demo complete");
    Ok(())
}
