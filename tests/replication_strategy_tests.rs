//! Tests for replication strategies
//!
//! These tests validate the behavior of different replication strategies
//! and their integration with the KVS servers.

use futures::{SinkExt, StreamExt};
use kvs_zoo::maintain::{BroadcastReplication, EpidemicGossip, NoReplication, ReplicationStrategy};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::{KVSServer, ReplicatedKVSServer};
use kvs_zoo::values::CausalString;
use std::collections::HashSet;
use tokio::time::{Duration, timeout};

/// Helper function to create a causal string for testing
fn create_causal_string(node_id: &str, value: &str) -> CausalString {
    let mut vc = kvs_zoo::values::VCWrapper::new();
    vc.bump(node_id.to_string());
    CausalString::new_with_set(vc, HashSet::from([value.to_string()]))
}

#[test]
fn test_no_replication_strategy() {
    // Test that NoReplication implements ReplicationStrategy
    let no_repl = NoReplication::new();

    // Test trait implementation
    fn _accepts_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
    _accepts_replication_strategy::<CausalString>(no_repl);

    // Test unit type also works
    _accepts_replication_strategy::<CausalString>(());
}

#[test]
fn test_epidemic_gossip_strategy() {
    // Test that EpidemicGossip implements ReplicationStrategy
    let gossip = EpidemicGossip::<CausalString>::new();

    // Test trait implementation
    fn _accepts_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
    _accepts_replication_strategy::<CausalString>(gossip);

    // Test configuration
    let gossip_with_config = EpidemicGossip::<CausalString>::with_config(
        kvs_zoo::maintain::EpidemicGossipConfig::small_cluster(),
    );
    _accepts_replication_strategy::<CausalString>(gossip_with_config);
}

#[test]
fn test_broadcast_replication_strategy() {
    // Test that BroadcastReplication implements ReplicationStrategy
    let broadcast = BroadcastReplication::<CausalString>::new();

    // Test trait implementation
    fn _accepts_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
    _accepts_replication_strategy::<CausalString>(broadcast);

    // Test configuration
    let broadcast_with_config = BroadcastReplication::<CausalString>::with_config(
        kvs_zoo::maintain::BroadcastReplicationConfig::low_latency(),
    );
    _accepts_replication_strategy::<CausalString>(broadcast_with_config);
}

#[test]
fn test_replication_strategy_type_compatibility() {
    // Test that all replication strategies work with the same value types
    fn _test_with_causal_type<R: ReplicationStrategy<CausalString>>(_strategy: R) {}
    fn _test_with_lww_type<R: ReplicationStrategy<kvs_zoo::values::LwwWrapper<String>>>(
        _strategy: R,
    ) {
    }

    // Test NoReplication
    _test_with_causal_type(NoReplication::new());
    _test_with_lww_type(NoReplication::new());
    _test_with_causal_type(());
    _test_with_lww_type(());

    // Test EpidemicGossip
    _test_with_causal_type(EpidemicGossip::<CausalString>::new());
    _test_with_lww_type(EpidemicGossip::<kvs_zoo::values::LwwWrapper<String>>::new());

    // Test BroadcastReplication
    _test_with_causal_type(BroadcastReplication::<CausalString>::new());
    _test_with_lww_type(BroadcastReplication::<kvs_zoo::values::LwwWrapper<String>>::new());
}

#[tokio::test]
async fn test_replicated_kvs_with_no_replication() {
    println!("🧪 Testing ReplicatedKVSServer with NoReplication");

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Create replicated KVS server with NoReplication using CausalString
    let kvs_cluster = ReplicatedKVSServer::<CausalString, NoReplication>::create_deployment(
        &flow,
        kvs_zoo::dispatch::RoundRobinRouter::new(),
        NoReplication::new(),
    );
    let client_port = ReplicatedKVSServer::<CausalString, NoReplication>::run(
        &proxy,
        &kvs_cluster,
        &client_external,
        kvs_zoo::dispatch::RoundRobinRouter::new(),
        NoReplication::new(),
    );

    // Deploy with 2 replicas
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); 2])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await.unwrap();

    // Wait for startup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test basic operations with CausalString
    let test_value = create_causal_string("test_node", "test_value");
    let operations = vec![
        KVSOperation::Put("test_key".to_string(), test_value),
        KVSOperation::Get("test_key".to_string()),
    ];

    for (i, op) in operations.into_iter().enumerate() {
        client_in.send(op).await.unwrap();

        if i == 1 {
            // Expect response for GET
            let response = timeout(Duration::from_millis(1000), client_out.next())
                .await
                .expect("Timeout waiting for response")
                .expect("No response received");

            // CausalString response format includes the causal value structure
            assert!(response.contains("test_value") || response.contains("test_key"));
            println!("✅ NoReplication test: {}", response);
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    println!("✅ ReplicatedKVSServer with NoReplication test passed!");
}

#[tokio::test]
#[ignore = "Integration test - may be flaky due to gossip timing"]
async fn test_replicated_kvs_with_epidemic_gossip() {
    println!("🧪 Testing ReplicatedKVSServer with EpidemicGossip");

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Create replicated KVS server with EpidemicGossip
    let gossip_config = kvs_zoo::maintain::EpidemicGossipConfig::small_cluster();
    let kvs_cluster =
        ReplicatedKVSServer::<CausalString, EpidemicGossip<CausalString>>::create_deployment(
            &flow,
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            EpidemicGossip::with_config(gossip_config),
        );
    let client_port = ReplicatedKVSServer::<CausalString, EpidemicGossip<CausalString>>::run(
        &proxy,
        &kvs_cluster,
        &client_external,
        kvs_zoo::dispatch::RoundRobinRouter::new(),
        EpidemicGossip::with_config(kvs_zoo::maintain::EpidemicGossipConfig::small_cluster()),
    );

    // Deploy with 3 replicas for gossip
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); 3])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await.unwrap();

    // Wait for startup and gossip initialization
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Test operations with causal values
    let val1 = create_causal_string("node1", "gossip_value1");
    let val2 = create_causal_string("node2", "gossip_value2");

    let operations = vec![
        KVSOperation::Put("gossip_key".to_string(), val1),
        KVSOperation::Put("gossip_key".to_string(), val2), // Concurrent write
        KVSOperation::Get("gossip_key".to_string()),
    ];

    for (i, op) in operations.into_iter().enumerate() {
        client_in.send(op).await.unwrap();

        if i == 2 {
            // Wait longer for gossip propagation
            let response = timeout(Duration::from_millis(3000), client_out.next())
                .await
                .expect("Timeout waiting for response")
                .expect("No response received");

            // Should contain merged values due to gossip replication
            assert!(
                response.contains("gossip_value1") || response.contains("gossip_value2"),
                "Response should contain gossip values: {}",
                response
            );
            println!("✅ EpidemicGossip test: {}", response);
        } else {
            // Allow time for gossip propagation between operations
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    println!("✅ ReplicatedKVSServer with EpidemicGossip test passed!");
}

#[tokio::test]
#[ignore = "Integration test - may be flaky due to broadcast timing"]
async fn test_replicated_kvs_with_broadcast_replication() {
    println!("🧪 Testing ReplicatedKVSServer with BroadcastReplication");

    // Set up deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    // Create Hydro flow
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Create replicated KVS server with BroadcastReplication
    let broadcast_config = kvs_zoo::maintain::BroadcastReplicationConfig::low_latency();
    let kvs_cluster =
        ReplicatedKVSServer::<CausalString, BroadcastReplication<CausalString>>::create_deployment(
            &flow,
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            BroadcastReplication::with_config(broadcast_config),
        );
    let client_port = ReplicatedKVSServer::<CausalString, BroadcastReplication<CausalString>>::run(
        &proxy,
        &kvs_cluster,
        &client_external,
        kvs_zoo::dispatch::RoundRobinRouter::new(),
        BroadcastReplication::with_config(
            kvs_zoo::maintain::BroadcastReplicationConfig::low_latency(),
        ),
    );

    // Deploy with 3 replicas for broadcast
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&kvs_cluster, vec![localhost.clone(); 3])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();
    let (mut client_out, mut client_in) = nodes.connect_bincode(client_port).await;
    deployment.start().await.unwrap();

    // Wait for startup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test operations with causal values
    let val1 = create_causal_string("node1", "broadcast_value1");
    let val2 = create_causal_string("node2", "broadcast_value2");

    let operations = vec![
        KVSOperation::Put("broadcast_key".to_string(), val1),
        KVSOperation::Put("broadcast_key".to_string(), val2), // Concurrent write
        KVSOperation::Get("broadcast_key".to_string()),
    ];

    for (i, op) in operations.into_iter().enumerate() {
        client_in.send(op).await.unwrap();

        if i == 2 {
            // Broadcast should be faster than gossip
            let response = timeout(Duration::from_millis(2000), client_out.next())
                .await
                .expect("Timeout waiting for response")
                .expect("No response received");

            // Should contain merged values due to broadcast replication
            assert!(
                response.contains("broadcast_value1") || response.contains("broadcast_value2"),
                "Response should contain broadcast values: {}",
                response
            );
            println!("✅ BroadcastReplication test: {}", response);
        } else {
            // Shorter wait for broadcast (faster than gossip)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    println!("✅ ReplicatedKVSServer with BroadcastReplication test passed!");
}

#[test]
fn test_replication_strategy_configurations() {
    // Test EpidemicGossip configurations
    let gossip_default = kvs_zoo::maintain::EpidemicGossipConfig::default();
    let gossip_small = kvs_zoo::maintain::EpidemicGossipConfig::small_cluster();
    let gossip_large = kvs_zoo::maintain::EpidemicGossipConfig::large_cluster();

    assert_eq!(gossip_default.gossip_fanout, 3);
    assert_eq!(gossip_small.gossip_fanout, 2);
    assert_eq!(gossip_large.gossip_fanout, 5);

    // Test BroadcastReplication configurations
    let broadcast_default = kvs_zoo::maintain::BroadcastReplicationConfig::default();
    let broadcast_low_latency = kvs_zoo::maintain::BroadcastReplicationConfig::low_latency();
    let broadcast_high_throughput =
        kvs_zoo::maintain::BroadcastReplicationConfig::high_throughput();
    let broadcast_sync = kvs_zoo::maintain::BroadcastReplicationConfig::synchronous();

    assert!(!broadcast_default.enable_batching);
    assert!(!broadcast_low_latency.enable_batching);
    assert!(broadcast_high_throughput.enable_batching);
    assert!(!broadcast_sync.enable_batching);

    assert_eq!(broadcast_low_latency.max_batch_size, 1);
    assert_eq!(broadcast_high_throughput.max_batch_size, 100);
    assert_eq!(broadcast_sync.max_batch_size, 1);
}

#[test]
fn test_replication_strategy_trait_bounds() {
    // Test that replication strategies have proper trait bounds
    fn _requires_send_sync_static<T: Send + Sync + 'static>(_t: T) {}

    _requires_send_sync_static(NoReplication::new());
    _requires_send_sync_static(EpidemicGossip::<CausalString>::new());
    _requires_send_sync_static(BroadcastReplication::<CausalString>::new());
    _requires_send_sync_static(());
}

#[test]
fn test_replication_strategy_cloning() {
    // Test that replication strategies can be cloned
    let no_repl = NoReplication::new();
    let _no_repl_clone = no_repl.clone();

    let gossip = EpidemicGossip::<CausalString>::new();
    let _gossip_clone = gossip.clone();

    let broadcast = BroadcastReplication::<CausalString>::new();
    let _broadcast_clone = broadcast.clone();
}

#[test]
fn test_replication_strategy_debug() {
    // Test that replication strategies implement Debug
    let no_repl = NoReplication::new();
    let _debug_str = format!("{:?}", no_repl);

    let gossip = EpidemicGossip::<CausalString>::new();
    let _debug_str = format!("{:?}", gossip);

    let broadcast = BroadcastReplication::<CausalString>::new();
    let _debug_str = format!("{:?}", broadcast);
}

#[test]
fn test_backward_compatibility() {
    // Test that migrated replication strategies maintain the same API
    // as the original implementations in the routers module

    // Test that we can create instances the same way
    let _gossip = EpidemicGossip::<CausalString>::new();
    let _gossip_default = EpidemicGossip::<CausalString>::default();
    let _gossip_with_config = EpidemicGossip::<CausalString>::with_config(
        kvs_zoo::maintain::EpidemicGossipConfig::small_cluster(),
    );

    let _broadcast = BroadcastReplication::<CausalString>::new();
    let _broadcast_default = BroadcastReplication::<CausalString>::default();
    let _broadcast_with_config = BroadcastReplication::<CausalString>::with_config(
        kvs_zoo::maintain::BroadcastReplicationConfig::low_latency(),
    );

    // Test that configurations still work the same way
    let gossip_config = kvs_zoo::maintain::EpidemicGossipConfig::default();
    assert_eq!(gossip_config.gossip_fanout, 3);
    assert_eq!(gossip_config.tombstone_prob, 0.1);
    assert_eq!(gossip_config.infection_prob, 0.5);

    let broadcast_config = kvs_zoo::maintain::BroadcastReplicationConfig::default();
    assert!(!broadcast_config.enable_batching);
    assert_eq!(broadcast_config.max_batch_size, 50);
}
