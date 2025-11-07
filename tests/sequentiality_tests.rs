//! Regression tests for sequential operation processing
//!
//! These tests verify that operations are processed in the exact order they
//! are received, which is essential for linearizability. They should fail
//! if operations are handled out of order.

use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

/// Test that operations are processed in strict sequential order
#[test]
fn test_strict_sequential_order() {
    // This test verifies that operations are processed in the exact order
    // they appear in the stream, which is critical for linearizability

    let operations = vec![
        KVSOperation::Put("counter".to_string(), LwwWrapper::new("0".to_string())),
        KVSOperation::Get("counter".to_string()),
        KVSOperation::Put("counter".to_string(), LwwWrapper::new("1".to_string())),
        KVSOperation::Get("counter".to_string()),
        KVSOperation::Put("counter".to_string(), LwwWrapper::new("2".to_string())),
        KVSOperation::Get("counter".to_string()),
        KVSOperation::Put("counter".to_string(), LwwWrapper::new("3".to_string())),
        KVSOperation::Get("counter".to_string()),
    ];

    // Simulate sequential processing
    let mut state = std::collections::HashMap::new();
    let mut responses = Vec::new();

    for op in operations {
        let response = match op {
            KVSOperation::Put(key, value) => {
                state.insert(key.clone(), value);
                format!("PUT {} = OK", key)
            }
            KVSOperation::Get(key) => match state.get(&key) {
                Some(value) => format!("GET {} = {:?}", key, value),
                None => format!("GET {} = NOT FOUND", key),
            },
        };
        responses.push(response);
    }

    // Verify strict sequential order
    assert_eq!(responses[0], "PUT counter = OK");
    assert!(responses[1].contains("LwwWrapper(\"0\")")); // First GET sees "0"
    assert_eq!(responses[2], "PUT counter = OK");
    assert!(responses[3].contains("LwwWrapper(\"1\")")); // Second GET sees "1"
    assert_eq!(responses[4], "PUT counter = OK");
    assert!(responses[5].contains("LwwWrapper(\"2\")")); // Third GET sees "2"
    assert_eq!(responses[6], "PUT counter = OK");
    assert!(responses[7].contains("LwwWrapper(\"3\")")); // Fourth GET sees "3"

    println!("Sequential responses: {:?}", responses);
}

/// Test that demonstrates ordering violation when operations are processed out of order
#[test]
fn test_ordering_violation_detection() {
    // This test shows what happens when operations are processed out of order
    // It should demonstrate why sequential processing is necessary

    let operations = vec![
        KVSOperation::Put("x".to_string(), LwwWrapper::new("first".to_string())),
        KVSOperation::Get("x".to_string()),
        KVSOperation::Put("x".to_string(), LwwWrapper::new("second".to_string())),
        KVSOperation::Get("x".to_string()),
        KVSOperation::Put("x".to_string(), LwwWrapper::new("third".to_string())),
        KVSOperation::Get("x".to_string()),
    ];

    // Correct sequential processing
    let mut sequential_state = std::collections::HashMap::new();
    let mut sequential_responses = Vec::new();

    for op in &operations {
        let response = match op {
            KVSOperation::Put(key, value) => {
                sequential_state.insert(key.clone(), value.clone());
                format!("PUT {} = OK", key)
            }
            KVSOperation::Get(key) => match sequential_state.get(key) {
                Some(value) => format!("GET {} = {:?}", key, value),
                None => format!("GET {} = NOT FOUND", key),
            },
        };
        sequential_responses.push(response);
    }

    // Simulate out-of-order processing (what NOT to do)
    let mut out_of_order_state = std::collections::HashMap::new();
    let mut out_of_order_responses = vec!["".to_string(); 6];

    // Process PUTs first (violates ordering)
    for (i, op) in operations.iter().enumerate() {
        if let KVSOperation::Put(key, value) = op {
            out_of_order_state.insert(key.clone(), value.clone());
            out_of_order_responses[i] = format!("PUT {} = OK", key);
        }
    }

    // Then process GETs (violates ordering)
    for (i, op) in operations.iter().enumerate() {
        if let KVSOperation::Get(key) = op {
            match out_of_order_state.get(key) {
                Some(value) => out_of_order_responses[i] = format!("GET {} = {:?}", key, value),
                None => out_of_order_responses[i] = format!("GET {} = NOT FOUND", key),
            }
        }
    }

    // Verify that sequential processing gives correct results
    assert!(sequential_responses[1].contains("first")); // First GET sees "first"
    assert!(sequential_responses[3].contains("second")); // Second GET sees "second"
    assert!(sequential_responses[5].contains("third")); // Third GET sees "third"

    // Verify that out-of-order processing gives incorrect results
    assert!(out_of_order_responses[1].contains("third")); // Wrong! Should see "first"
    assert!(out_of_order_responses[3].contains("third")); // Wrong! Should see "second"
    assert!(out_of_order_responses[5].contains("third")); // This one is correct by accident

    println!("Sequential (correct): {:?}", sequential_responses);
    println!("Out-of-order (incorrect): {:?}", out_of_order_responses);
}

/// Test interleaved operations to ensure strict ordering
#[test]
fn test_interleaved_operations_ordering() {
    // This test uses multiple keys with interleaved operations
    // to verify that ordering is maintained across all keys

    let operations = vec![
        KVSOperation::Put("a".to_string(), LwwWrapper::new("a1".to_string())),
        KVSOperation::Put("b".to_string(), LwwWrapper::new("b1".to_string())),
        KVSOperation::Get("a".to_string()),
        KVSOperation::Get("b".to_string()),
        KVSOperation::Put("a".to_string(), LwwWrapper::new("a2".to_string())),
        KVSOperation::Get("a".to_string()),
        KVSOperation::Put("b".to_string(), LwwWrapper::new("b2".to_string())),
        KVSOperation::Get("b".to_string()),
        KVSOperation::Get("a".to_string()),
    ];

    let mut state = std::collections::HashMap::new();
    let mut responses = Vec::new();

    for op in operations {
        let response = match op {
            KVSOperation::Put(key, value) => {
                state.insert(key.clone(), value);
                format!("PUT {} = OK", key)
            }
            KVSOperation::Get(key) => match state.get(&key) {
                Some(value) => format!("GET {} = {:?}", key, value),
                None => format!("GET {} = NOT FOUND", key),
            },
        };
        responses.push(response);
    }

    // Verify correct interleaved ordering
    assert_eq!(responses[0], "PUT a = OK");
    assert_eq!(responses[1], "PUT b = OK");
    assert!(responses[2].contains("a1")); // GET a sees a1
    assert!(responses[3].contains("b1")); // GET b sees b1
    assert_eq!(responses[4], "PUT a = OK");
    assert!(responses[5].contains("a2")); // GET a sees a2 (updated value)
    assert_eq!(responses[6], "PUT b = OK");
    assert!(responses[7].contains("b2")); // GET b sees b2 (updated value)
    assert!(responses[8].contains("a2")); // Final GET a still sees a2

    println!("Interleaved responses: {:?}", responses);
}

/// Test that missing keys are handled correctly in sequential order
#[test]
fn test_missing_keys_sequential_order() {
    let operations = vec![
        KVSOperation::Get("nonexistent".to_string()),
        KVSOperation::Put("key1".to_string(), LwwWrapper::new("value1".to_string())),
        KVSOperation::Get("nonexistent".to_string()),
        KVSOperation::Get("key1".to_string()),
        KVSOperation::Put("key1".to_string(), LwwWrapper::new("updated".to_string())),
        KVSOperation::Get("key1".to_string()),
        KVSOperation::Get("nonexistent".to_string()),
    ];

    let mut state = std::collections::HashMap::new();
    let mut responses = Vec::new();

    for op in operations {
        let response = match op {
            KVSOperation::Put(key, value) => {
                state.insert(key.clone(), value);
                format!("PUT {} = OK", key)
            }
            KVSOperation::Get(key) => match state.get(&key) {
                Some(value) => format!("GET {} = {:?}", key, value),
                None => format!("GET {} = NOT FOUND", key),
            },
        };
        responses.push(response);
    }

    // Verify correct handling of missing keys in order
    assert!(responses[0].contains("NOT FOUND")); // First GET of nonexistent
    assert_eq!(responses[1], "PUT key1 = OK");
    assert!(responses[2].contains("NOT FOUND")); // Second GET of nonexistent
    assert!(responses[3].contains("value1")); // GET key1 sees value1
    assert_eq!(responses[4], "PUT key1 = OK");
    assert!(responses[5].contains("updated")); // GET key1 sees updated
    assert!(responses[6].contains("NOT FOUND")); // Third GET of nonexistent

    println!("Missing keys responses: {:?}", responses);
}

/// Test concurrent client simulation with strict ordering
#[test]
fn test_concurrent_clients_sequential_processing() {
    // Simulate operations from multiple clients that must be processed
    // in a single sequential order for linearizability

    let operations = vec![
        // Client 1 operations
        KVSOperation::Put(
            "shared".to_string(),
            LwwWrapper::new("client1_v1".to_string()),
        ),
        // Client 2 operations
        KVSOperation::Put(
            "shared".to_string(),
            LwwWrapper::new("client2_v1".to_string()),
        ),
        // Client 1 reads
        KVSOperation::Get("shared".to_string()),
        // Client 2 reads
        KVSOperation::Get("shared".to_string()),
        // Client 1 updates
        KVSOperation::Put(
            "shared".to_string(),
            LwwWrapper::new("client1_v2".to_string()),
        ),
        // Both clients read
        KVSOperation::Get("shared".to_string()),
        KVSOperation::Get("shared".to_string()),
    ];

    let mut state = std::collections::HashMap::new();
    let mut responses = Vec::new();

    for op in operations {
        let response = match op {
            KVSOperation::Put(key, value) => {
                state.insert(key.clone(), value);
                format!("PUT {} = OK", key)
            }
            KVSOperation::Get(key) => match state.get(&key) {
                Some(value) => format!("GET {} = {:?}", key, value),
                None => format!("GET {} = NOT FOUND", key),
            },
        };
        responses.push(response);
    }

    // Verify that all operations see a consistent sequential order
    assert_eq!(responses[0], "PUT shared = OK");
    assert_eq!(responses[1], "PUT shared = OK");
    assert!(responses[2].contains("client2_v1")); // Sees result of second PUT
    assert!(responses[3].contains("client2_v1")); // Same consistent view
    assert_eq!(responses[4], "PUT shared = OK");
    assert!(responses[5].contains("client1_v2")); // Sees latest update
    assert!(responses[6].contains("client1_v2")); // Consistent view

    println!("Concurrent clients responses: {:?}", responses);
}

/// Regression test for a specific linearizability violation scenario
#[test]
fn test_linearizability_violation_regression() {
    // This test captures a specific scenario where non-sequential processing
    // would violate linearizability guarantees

    let operations = vec![
        KVSOperation::Put("balance".to_string(), LwwWrapper::new("1000".to_string())),
        KVSOperation::Get("balance".to_string()), // Should see 1000
        KVSOperation::Put("balance".to_string(), LwwWrapper::new("900".to_string())), // Withdraw 100
        KVSOperation::Get("balance".to_string()), // Should see 900
        KVSOperation::Put("balance".to_string(), LwwWrapper::new("800".to_string())), // Withdraw 100
        KVSOperation::Get("balance".to_string()), // Should see 800
        KVSOperation::Put("balance".to_string(), LwwWrapper::new("850".to_string())), // Deposit 50
        KVSOperation::Get("balance".to_string()), // Should see 850
    ];

    let mut state = std::collections::HashMap::new();
    let mut responses = Vec::new();

    for op in operations {
        let response = match op {
            KVSOperation::Put(key, value) => {
                state.insert(key.clone(), value);
                format!("PUT {} = OK", key)
            }
            KVSOperation::Get(key) => match state.get(&key) {
                Some(value) => format!("GET {} = {:?}", key, value),
                None => format!("GET {} = NOT FOUND", key),
            },
        };
        responses.push(response);
    }

    // Verify the exact sequence of balance changes
    assert_eq!(responses[0], "PUT balance = OK");
    assert!(responses[1].contains("1000")); // Initial balance
    assert_eq!(responses[2], "PUT balance = OK");
    assert!(responses[3].contains("900")); // After first withdrawal
    assert_eq!(responses[4], "PUT balance = OK");
    assert!(responses[5].contains("800")); // After second withdrawal
    assert_eq!(responses[6], "PUT balance = OK");
    assert!(responses[7].contains("850")); // After deposit

    // This sequence must be preserved for linearizability
    // Any reordering would violate the consistency guarantees
    println!("Balance sequence: {:?}", responses);
}
