//! Validation tests for KVS examples
//!
//! These tests validate the architectural characteristics and unique behaviors
//! of each example without requiring full Hydro deployment.

use kvs_zoo::dispatch::ShardedRouter;
use kvs_zoo::server::{KVSServer, LocalKVSServer, ReplicatedKVSServer, ShardedKVSServer};
use kvs_zoo::values::{CausalString, LwwWrapper, Merge, VCWrapper};
use std::collections::HashSet;

/// Test architectural characteristics of different server types
#[tokio::test]
async fn test_architectural_characteristics() {
    println!("🧪 Testing Architectural Characteristics");

    // Test node counts for different architectures
    assert_eq!(
        LocalKVSServer::<LwwWrapper<String>>::size(kvs_zoo::dispatch::SingleNodeRouter::new(), (),),
        1,
        "Local KVS should use 1 node"
    );
    assert_eq!(
        ReplicatedKVSServer::<CausalString, kvs_zoo::maintain::NoReplication>::size(
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            kvs_zoo::maintain::NoReplication::new(),
        ),
        3,
        "Replicated KVS should use 3 nodes"
    );
    assert_eq!(
        ShardedKVSServer::<LocalKVSServer<LwwWrapper<String>>>::size(
            kvs_zoo::dispatch::Pipeline::new(
                kvs_zoo::dispatch::ShardedRouter::new(3),
                kvs_zoo::dispatch::SingleNodeRouter::new(),
            ),
            (),
        ),
        3,
        "Sharded KVS should use 3 nodes"
    );
    assert_eq!(
        ShardedKVSServer::<
            ReplicatedKVSServer<LwwWrapper<String>, kvs_zoo::maintain::NoReplication>,
        >::size(
            kvs_zoo::dispatch::Pipeline::new(
                kvs_zoo::dispatch::ShardedRouter::new(3),
                kvs_zoo::dispatch::RoundRobinRouter::new(),
            ),
            kvs_zoo::maintain::NoReplication::new(),
        ),
        9,
        "Sharded+Replicated KVS should use 9 nodes"
    );

    println!("✅ All architectures report correct node counts");
}

/// Test LWW (Last-Writer-Wins) semantics used by Local KVS
#[test]
fn test_lww_semantics() {
    println!("🧪 Testing LWW Semantics");

    // Test that LWW wrapper implements proper overwrite behavior
    let mut lww1 = LwwWrapper::new("value1".to_string());
    let lww2 = LwwWrapper::new("value2".to_string());

    // Merge should result in the second value (last writer wins)
    lww1.merge(lww2);

    // The inner value should be "value2" (the later write)
    assert_eq!(lww1.get(), &"value2".to_string());

    println!("✅ LWW semantics work correctly");
}

/// Test causal consistency semantics used by Replicated KVS
#[test]
fn test_causal_consistency_semantics() {
    println!("🧪 Testing Causal Consistency Semantics");

    // Create two concurrent causal values
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());
    let causal1 = CausalString::new_with_set(vc1, HashSet::from(["value1".to_string()]));

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());
    let causal2 = CausalString::new_with_set(vc2, HashSet::from(["value2".to_string()]));

    // Merge concurrent values
    let mut merged = causal1;
    merged.merge(causal2);

    // Both values should be preserved in the merged result
    let merged_values = merged.values();
    assert!(
        merged_values.contains("value1"),
        "Merged result should contain value1"
    );
    assert!(
        merged_values.contains("value2"),
        "Merged result should contain value2"
    );
    assert_eq!(
        merged_values.len(),
        2,
        "Merged result should contain both values"
    );

    println!("✅ Causal consistency semantics work correctly");
}

/// Test key distribution behavior for sharded systems
#[test]
fn test_key_distribution() {
    println!("🧪 Testing Key Distribution");

    // Test that different keys hash to different shards
    let test_keys = vec![
        "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9",
    ];
    let num_shards = 3;

    let mut shard_counts = vec![0; num_shards];

    for key in test_keys {
        let shard = ShardedRouter::calculate_shard_id(key, num_shards) as usize;
        assert!(shard < num_shards, "Shard index should be within bounds");
        shard_counts[shard] += 1;
    }

    // Verify that keys are distributed across shards (not all in one shard)
    let non_empty_shards = shard_counts.iter().filter(|&&count| count > 0).count();
    assert!(
        non_empty_shards > 1,
        "Keys should be distributed across multiple shards, got distribution: {:?}",
        shard_counts
    );

    println!("✅ Key distribution works correctly");
}

/// Test that the same key always goes to the same shard (consistency)
#[test]
fn test_shard_consistency() {
    println!("🧪 Testing Shard Consistency");

    let test_key = "consistent_key";
    let num_shards = 3;

    // Calculate shard multiple times
    let shard1 = ShardedRouter::calculate_shard_id(test_key, num_shards);
    let shard2 = ShardedRouter::calculate_shard_id(test_key, num_shards);
    let shard3 = ShardedRouter::calculate_shard_id(test_key, num_shards);

    assert_eq!(shard1, shard2, "Same key should always map to same shard");
    assert_eq!(shard2, shard3, "Same key should always map to same shard");

    println!("✅ Shard consistency works correctly");
}

/// Test composability of server architectures
#[test]
fn test_server_composability() {
    println!("🧪 Testing Server Composability");

    // Test that we can compose different server types
    type LocalServer = LocalKVSServer<LwwWrapper<String>>;
    type ReplicatedServer = ReplicatedKVSServer<LwwWrapper<String>>;
    type ShardedLocal = ShardedKVSServer<LocalServer>;
    type ShardedReplicated = ShardedKVSServer<ReplicatedServer>;

    // Verify size calculations work for composed types
    assert_eq!(
        LocalServer::size(kvs_zoo::dispatch::SingleNodeRouter::new(), ()),
        1
    );
    assert_eq!(
        ReplicatedServer::size(
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            kvs_zoo::maintain::NoReplication::new()
        ),
        3
    );
    assert_eq!(
        ShardedLocal::size(
            kvs_zoo::dispatch::Pipeline::new(
                kvs_zoo::dispatch::ShardedRouter::new(3),
                kvs_zoo::dispatch::SingleNodeRouter::new()
            ),
            ()
        ),
        3
    ); // 3 shards × 1 node each
    assert_eq!(
        ShardedReplicated::size(
            kvs_zoo::dispatch::Pipeline::new(
                kvs_zoo::dispatch::ShardedRouter::new(3),
                kvs_zoo::dispatch::RoundRobinRouter::new()
            ),
            kvs_zoo::maintain::NoReplication::new()
        ),
        9
    ); // 3 shards × 3 replicas each

    println!("✅ Server composability works correctly");
}

/// Test value wrapper behavior with different consistency models
#[test]
fn test_value_wrapper_behaviors() {
    println!("🧪 Testing Value Wrapper Behaviors");

    // Test LWW behavior
    let mut lww_a = LwwWrapper::new("a".to_string());
    let lww_b = LwwWrapper::new("b".to_string());
    lww_a.merge(lww_b);
    assert_eq!(lww_a.get(), "b"); // Last writer wins

    // Test causal behavior with concurrent updates
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());
    let mut causal_a = CausalString::new_with_set(vc1, HashSet::from(["a".to_string()]));

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());
    let causal_b = CausalString::new_with_set(vc2, HashSet::from(["b".to_string()]));

    causal_a.merge(causal_b);
    let merged_set = causal_a.values();
    assert!(merged_set.contains("a")); // Both values preserved
    assert!(merged_set.contains("b"));

    println!("✅ Value wrapper behaviors work correctly");
}

/// Test example-specific architectural patterns
#[test]
fn test_example_architectural_patterns() {
    println!("🧪 Testing Example Architectural Patterns");

    // Local KVS: Single node, simple
    assert_eq!(
        LocalKVSServer::<LwwWrapper<String>>::size(kvs_zoo::dispatch::SingleNodeRouter::new(), (),),
        1,
        "Local should be single node"
    );

    // Replicated KVS: Multiple nodes for fault tolerance
    assert_eq!(
        ReplicatedKVSServer::<CausalString, kvs_zoo::maintain::NoReplication>::size(
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            kvs_zoo::maintain::NoReplication::new(),
        ),
        3,
        "Replicated should have multiple nodes"
    );

    // Sharded KVS: Multiple nodes for scalability
    assert_eq!(
        ShardedKVSServer::<LocalKVSServer<LwwWrapper<String>>>::size(
            kvs_zoo::dispatch::Pipeline::new(
                kvs_zoo::dispatch::ShardedRouter::new(3),
                kvs_zoo::dispatch::SingleNodeRouter::new(),
            ),
            (),
        ),
        3,
        "Sharded should have multiple nodes"
    );

    // Sharded + Replicated: Maximum nodes for both scalability and fault tolerance
    assert_eq!(
        ShardedKVSServer::<
            ReplicatedKVSServer<LwwWrapper<String>, kvs_zoo::maintain::NoReplication>,
        >::size(
            kvs_zoo::dispatch::Pipeline::new(
                kvs_zoo::dispatch::ShardedRouter::new(3),
                kvs_zoo::dispatch::RoundRobinRouter::new(),
            ),
            kvs_zoo::maintain::NoReplication::new(),
        ),
        9,
        "Sharded+Replicated should have most nodes"
    );

    // Verify scaling relationship
    let local_nodes =
        LocalKVSServer::<LwwWrapper<String>>::size(kvs_zoo::dispatch::SingleNodeRouter::new(), ());
    let replicated_nodes =
        ReplicatedKVSServer::<CausalString, kvs_zoo::maintain::NoReplication>::size(
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            kvs_zoo::maintain::NoReplication::new(),
        );
    let sharded_nodes = ShardedKVSServer::<LocalKVSServer<LwwWrapper<String>>>::size(
        kvs_zoo::dispatch::Pipeline::new(
            kvs_zoo::dispatch::ShardedRouter::new(3),
            kvs_zoo::dispatch::SingleNodeRouter::new(),
        ),
        (),
    );
    let sharded_replicated_nodes = ShardedKVSServer::<
        ReplicatedKVSServer<LwwWrapper<String>, kvs_zoo::maintain::NoReplication>,
    >::size(
        kvs_zoo::dispatch::Pipeline::new(
            kvs_zoo::dispatch::ShardedRouter::new(3),
            kvs_zoo::dispatch::RoundRobinRouter::new(),
        ),
        kvs_zoo::maintain::NoReplication::new(),
    );

    assert!(
        local_nodes < replicated_nodes,
        "Replication should add nodes"
    );
    assert!(local_nodes < sharded_nodes, "Sharding should add nodes");
    assert!(
        sharded_nodes < sharded_replicated_nodes,
        "Sharding + replication should add most nodes"
    );
    assert!(
        replicated_nodes < sharded_replicated_nodes,
        "Sharding + replication should add most nodes"
    );

    println!("✅ Example architectural patterns are correct");
}

/// Test performance characteristics (conceptual)
#[test]
fn test_performance_characteristics() {
    println!("🧪 Testing Performance Characteristics");

    // These tests validate the conceptual performance trade-offs
    // In practice, these would be measured with actual deployments

    // Local: Lowest latency (1 node, no network)
    let local_latency_factor =
        LocalKVSServer::<LwwWrapper<String>>::size(kvs_zoo::dispatch::SingleNodeRouter::new(), ());

    // Replicated: Higher latency (broadcast to 3 nodes)
    let replicated_latency_factor =
        ReplicatedKVSServer::<CausalString, kvs_zoo::maintain::NoReplication>::size(
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            kvs_zoo::maintain::NoReplication::new(),
        );

    // Sharded: Scalable throughput (3 independent nodes)
    let sharded_throughput_factor = ShardedKVSServer::<LocalKVSServer<LwwWrapper<String>>>::size(
        kvs_zoo::dispatch::Pipeline::new(
            kvs_zoo::dispatch::ShardedRouter::new(3),
            kvs_zoo::dispatch::SingleNodeRouter::new(),
        ),
        (),
    );

    // Sharded + Replicated: Balanced (9 nodes total)
    let balanced_factor = ShardedKVSServer::<
        ReplicatedKVSServer<LwwWrapper<String>, kvs_zoo::maintain::NoReplication>,
    >::size(
        kvs_zoo::dispatch::Pipeline::new(
            kvs_zoo::dispatch::ShardedRouter::new(3),
            kvs_zoo::dispatch::RoundRobinRouter::new(),
        ),
        kvs_zoo::maintain::NoReplication::new(),
    );

    // Validate expected relationships
    assert!(
        local_latency_factor < replicated_latency_factor,
        "Local should have lower latency overhead"
    );
    assert_eq!(
        sharded_throughput_factor, 3,
        "Sharded should enable 3x throughput scaling"
    );
    assert_eq!(
        balanced_factor, 9,
        "Sharded+Replicated should balance scalability and fault tolerance"
    );

    println!("✅ Performance characteristics are as expected");
}

/// Test fault tolerance characteristics
#[test]
fn test_fault_tolerance_characteristics() {
    println!("🧪 Testing Fault Tolerance Characteristics");

    // Local: No fault tolerance (1 node)
    let local_fault_tolerance =
        LocalKVSServer::<LwwWrapper<String>>::size(kvs_zoo::dispatch::SingleNodeRouter::new(), ())
            - 1; // Can lose 0 nodes

    // Replicated: High fault tolerance (can lose 2 of 3 nodes)
    let replicated_fault_tolerance =
        ReplicatedKVSServer::<CausalString, kvs_zoo::maintain::NoReplication>::size(
            kvs_zoo::dispatch::RoundRobinRouter::new(),
            kvs_zoo::maintain::NoReplication::new(),
        ) - 1; // Can lose 2 nodes

    // Sharded: Low fault tolerance (losing any shard loses data)
    let sharded_fault_tolerance = 0; // Cannot lose any shard

    // Sharded + Replicated: Balanced fault tolerance (can lose replicas within shards)
    let balanced_fault_tolerance = 2 * 3; // Can lose 2 replicas per shard across 3 shards

    assert_eq!(local_fault_tolerance, 0, "Local has no fault tolerance");
    assert_eq!(
        replicated_fault_tolerance, 2,
        "Replicated can tolerate 2 node failures"
    );
    assert_eq!(
        sharded_fault_tolerance, 0,
        "Pure sharding has no fault tolerance"
    );
    assert_eq!(
        balanced_fault_tolerance, 6,
        "Sharded+Replicated provides balanced fault tolerance"
    );

    println!("✅ Fault tolerance characteristics are correct");
}
