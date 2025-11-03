/// Tests for Causal Consistency using DomPair<VCWrapper, V>
use kvs_zoo::vector_clock::VCWrapper;
use lattices::set_union::SetUnionHashSet;
use lattices::{DomPair, Merge};

#[test]
fn test_causal_kvs_dom_pair_merge() {
    // Test that DomPair<VCWrapper, SetUnionHashSet<String>> properly implements causal consistency
    // This is a typical use case where concurrent puts are preserved as a set

    // Create two causally ordered updates (node1 at time 1 and time 2)
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());
    let mut set1 = SetUnionHashSet::default();
    set1.as_reveal_mut().insert("first".to_string());
    let value1 = DomPair::new(vc1.clone(), set1);

    let mut vc2 = vc1.clone();
    vc2.bump("node1".to_string());
    let mut set2 = SetUnionHashSet::default();
    set2.as_reveal_mut().insert("second".to_string());
    let value2 = DomPair::new(vc2.clone(), set2);

    // When merging causally ordered updates, the later one should win
    let mut merged = value1.clone();
    merged.merge(value2.clone());
    assert!(merged.as_reveal_ref().1.as_reveal_ref().contains("second"));
    assert!(!merged.as_reveal_ref().1.as_reveal_ref().contains("first"));
    assert_eq!(merged.key.get("node1"), Some(2));

    // Reverse order should also result in the later value
    let mut merged_rev = value2.clone();
    merged_rev.merge(value1.clone());
    assert!(
        merged_rev
            .as_reveal_ref()
            .1
            .as_reveal_ref()
            .contains("second")
    );
    assert!(
        !merged_rev
            .as_reveal_ref()
            .1
            .as_reveal_ref()
            .contains("first")
    );
    assert_eq!(merged_rev.key.get("node1"), Some(2));
}

#[test]
fn test_causal_kvs_concurrent_updates() {
    // Test that concurrent updates from different nodes are handled correctly
    // Using SetUnion: concurrent puts are preserved in the set

    // Node1 makes an update
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());
    let mut set1 = SetUnionHashSet::default();
    set1.as_reveal_mut().insert("from_node1".to_string());
    let value1 = DomPair::new(vc1.clone(), set1);

    // Node2 makes a concurrent update (independent)
    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());
    let mut set2 = SetUnionHashSet::default();
    set2.as_reveal_mut().insert("from_node2".to_string());
    let value2 = DomPair::new(vc2.clone(), set2);

    // Verify they are concurrent
    assert!(vc1.is_concurrent(&vc2));

    // When merging concurrent updates, both clocks and values should merge
    let mut merged = value1.clone();
    let changed = merged.merge(value2.clone());
    assert!(changed); // Should indicate a change occurred

    // The merged clock should have entries for both nodes
    assert_eq!(merged.key.get("node1"), Some(1));
    assert_eq!(merged.key.get("node2"), Some(1));

    // For concurrent SetUnion values, BOTH values are preserved
    let merged_set = merged.as_reveal_ref().1.as_reveal_ref();
    assert!(
        merged_set.contains("from_node1"),
        "Concurrent update from node1 should be preserved"
    );
    assert!(
        merged_set.contains("from_node2"),
        "Concurrent update from node2 should be preserved"
    );
    assert_eq!(
        merged_set.len(),
        2,
        "Both concurrent updates should be in the set"
    );
}

#[test]
fn test_causal_kvs_three_way_merge() {
    // Test a more complex scenario with three nodes
    // Shows how concurrent updates are preserved and merged

    // Initial state
    let vc0 = VCWrapper::new();
    let _value0 = DomPair::new(vc0.clone(), SetUnionHashSet::<String>::default());

    // Node1 updates from initial
    let mut vc1 = vc0.clone();
    vc1.bump("node1".to_string());
    let mut set1 = SetUnionHashSet::default();
    set1.as_reveal_mut().insert("update1".to_string());
    let value1 = DomPair::new(vc1.clone(), set1);

    // Node2 updates from initial (concurrent with node1)
    let mut vc2 = vc0.clone();
    vc2.bump("node2".to_string());
    let mut set2 = SetUnionHashSet::default();
    set2.as_reveal_mut().insert("update2".to_string());
    let value2 = DomPair::new(vc2.clone(), set2);

    // Node3 sees both updates and merges them
    let mut value3 = value1.clone();
    value3.merge(value2.clone());

    // The final clock should have both node1 and node2
    assert_eq!(value3.key.get("node1"), Some(1));
    assert_eq!(value3.key.get("node2"), Some(1));

    // Both concurrent updates should be preserved in the set
    let set3 = value3.as_reveal_ref().1.as_reveal_ref();
    assert!(
        set3.contains("update1"),
        "Node1's update should be preserved"
    );
    assert!(
        set3.contains("update2"),
        "Node2's update should be preserved"
    );

    // Node3 makes its own update
    value3.key.bump("node3".to_string());

    // Now value3 dominates both value1 and value2
    assert!(value1.key.happened_before(&value3.key));
    assert!(value2.key.happened_before(&value3.key));
}
