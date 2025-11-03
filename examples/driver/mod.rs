use hydro_deploy::Deployment;
use hydro_lang::prelude::*;
use kvs_zoo::client::KVSClient;
use kvs_zoo::core::KVSNode;
use kvs_zoo::protocol::KVSOperation;
use serde::{Deserialize, Serialize};

/// Configuration for a KVS demo using the working hydro-template pattern
///
/// This trait provides a unified interface for different KVS architectures:
/// - Local: Single-process KVS
/// - Replicated: Multi-replica with gossip synchronization  
/// - Sharded: Hash-based partitioning across nodes
pub trait KVSDemo {
    /// The value type used in this demo
    type Value: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + Send
        + Sync
        + 'static;

    /// Get the number of cluster nodes needed
    fn cluster_size(&self) -> usize;

    /// Get demo description and architecture info
    fn description(&self) -> &'static str;

    /// Get operations to run in the demo
    #[allow(dead_code)]
    fn operations(&self) -> Vec<KVSOperation<Self::Value>>;

    /// Get demo operations using KVSClient (for String values)
    /// This provides a default implementation that demos can use
    #[allow(dead_code)]
    fn client_demo_operations() -> Vec<KVSOperation<String>>
    where
        Self::Value: From<String>,
    {
        KVSClient::generate_demo_operations()
    }

    /// Log an operation using KVSClient formatting (for String values)
    #[allow(dead_code)]
    fn log_operation(op: &KVSOperation<String>) {
        KVSClient::log_operation(op);
    }

    /// Get the demo name for logging
    fn name(&self) -> &'static str;

    /// Set up the KVS dataflow using the hydro-template pattern
    /// This is where each demo defines its specific architecture
    fn setup_dataflow<'a>(&self, client: &Process<'a, ()>, cluster: &Cluster<'a, KVSNode>);
}

/// Unified driver function that can run any KVS demo using the working pattern
///
/// This driver uses the hydro-template pattern that actually works:
/// - Uses deployment.run_ctrl_c() instead of separate deploy/start calls
/// - Uses process-to-cluster communication with send_bincode()
/// - Handles the dataflow setup through the KVSDemo trait
pub async fn run_kvs_demo<D: KVSDemo>(demo: D) -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running {} Demo", demo.name());
    println!("{}", demo.description());
    println!();

    let mut deployment = Deployment::new();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let client = flow.process();
    let cluster = flow.cluster::<KVSNode>();

    // Set up the dataflow using the demo's specific architecture
    demo.setup_dataflow(&client, &cluster);

    let _nodes = flow
        .with_process(&client, deployment.Localhost())
        .with_cluster(&cluster, vec![deployment.Localhost(); demo.cluster_size()])
        .deploy(&mut deployment);

    println!("🚀 Starting deployment (press Ctrl+C to stop)...");
    deployment.run_ctrl_c().await.unwrap();

    Ok(())
}
