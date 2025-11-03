/// Tests for Vector Clock implementation
use kvs_zoo::values::vector_clock::VCWrapper;
use lattices::Merge;

#[test]
fn test_new_vector_clock() {
    let vc = VCWrapper::new();
    assert_eq!(vc.get("node1"), None);
}

#[test]
fn test_bump() {
    let mut vc = VCWrapper::new();
    vc.bump("node1".to_string());
    assert_eq!(vc.get("node1"), Some(1));

    vc.bump("node1".to_string());
    assert_eq!(vc.get("node1"), Some(2));

    vc.bump("node2".to_string());
    assert_eq!(vc.get("node2"), Some(1));
}

#[test]
fn test_merge() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());
    vc2.bump("node1".to_string());

    vc1.merge(vc2);
    assert_eq!(vc1.get("node1"), Some(2)); // max(2, 1) = 2
    assert_eq!(vc1.get("node2"), Some(1));
}

#[test]
fn test_happened_before() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = vc1.clone();
    vc2.bump("node1".to_string());

    assert!(vc1.happened_before(&vc2));
    assert!(!vc2.happened_before(&vc1));
}

#[test]
fn test_concurrent() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());

    assert!(vc1.is_concurrent(&vc2));
    assert!(vc2.is_concurrent(&vc1));
}

#[test]
fn test_merge_is_idempotent() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let vc2 = vc1.clone();
    let vc1_before = vc1.clone();

    vc1.merge(vc2);
    assert_eq!(vc1, vc1_before);
}

#[test]
fn test_merge_is_commutative() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());

    let mut result1 = vc1.clone();
    result1.merge(vc2.clone());

    let mut result2 = vc2.clone();
    result2.merge(vc1.clone());

    assert_eq!(result1, result2);
}

#[test]
fn test_merge_is_associative() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());

    let mut vc3 = VCWrapper::new();
    vc3.bump("node3".to_string());

    // (vc1 ∨ vc2) ∨ vc3
    let mut result1 = vc1.clone();
    result1.merge(vc2.clone());
    result1.merge(vc3.clone());

    // vc1 ∨ (vc2 ∨ vc3)
    let mut temp = vc2.clone();
    temp.merge(vc3.clone());
    let mut result2 = vc1.clone();
    result2.merge(temp);

    assert_eq!(result1, result2);
}

#[test]
fn test_missing_entries_are_zero() {
    // Test that missing entries are semantically equivalent to 0
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let vc2 = VCWrapper::new(); // Empty, all entries implicitly 0

    // vc2 has 0 for all nodes, so it should have happened before vc1
    assert!(vc2.happened_before(&vc1));
    assert!(!vc1.happened_before(&vc2));

    // Getting a non-existent node should return None (but semantically it's 0)
    assert_eq!(vc1.get("nonexistent"), None);
    assert_eq!(vc2.get("node1"), None);
}
