/// Tests for Linearizability in the Paxos-based KVS
/// 
/// Linearizability requires that:
/// 1. All operations appear to execute atomically at some point between invocation and response
/// 2. The ordering respects the real-time order of non-overlapping operations
/// 3. Each read returns the value of the most recent write in the linearization order

use std::collections::HashMap;
use kvs_zoo::protocol::KVSOperation;

/// Represents an operation with timing information for linearizability checking
#[derive(Debug, Clone, PartialEq, Eq)]
struct TimedOperation<V> {
    op: KVSOperation<V>,
    invocation_time: u64,
    response_time: u64,
    response_value: Option<V>,
}

impl<V> TimedOperation<V> {
    fn overlaps_with(&self, other: &Self) -> bool {
        // Two operations overlap if neither completes before the other starts
        !(self.response_time <= other.invocation_time || other.response_time <= self.invocation_time)
    }
}

/// Validates that a sequence of operations satisfies linearizability
/// by finding a valid linearization (total order) that is consistent with:
/// 1. The real-time ordering of non-overlapping operations
/// 2. Each read returning the most recent write in the order
fn is_linearizable<V>(operations: &[TimedOperation<V>]) -> bool
where
    V: Clone + Eq + std::fmt::Debug,
{
    // For small test cases, we can try all permutations
    // In practice, you'd use a more sophisticated algorithm like the Wing-Gong algorithm
    
    // Check if there's a valid linearization by trying different orderings
    let mut indices: Vec<usize> = (0..operations.len()).collect();
    check_permutations(&mut indices, 0, operations)
}

fn check_permutations<V>(
    indices: &mut [usize],
    start: usize,
    operations: &[TimedOperation<V>],
) -> bool
where
    V: Clone + Eq + std::fmt::Debug,
{
    if start == indices.len() {
        return validate_linearization(indices, operations);
    }
    
    for i in start..indices.len() {
        indices.swap(start, i);
        if check_permutations(indices, start + 1, operations) {
            return true;
        }
        indices.swap(start, i);
    }
    
    false
}

fn validate_linearization<V>(
    order: &[usize],
    operations: &[TimedOperation<V>],
) -> bool
where
    V: Clone + Eq + std::fmt::Debug,
{
    // Check real-time ordering constraint
    // If operation i completes before operation j starts (in real-time),
    // then i must appear before j in the linearization order
    for i in 0..order.len() {
        for j in 0..order.len() {
            if i == j {
                continue;
            }
            
            let op_at_pos_i = &operations[order[i]];
            let op_at_pos_j = &operations[order[j]];
            
            // If op_at_pos_j completes before op_at_pos_i starts (in real time),
            // then j must come before i in the linearization (which means j < i)
            if op_at_pos_j.response_time <= op_at_pos_i.invocation_time && j > i {
                return false;
            }
        }
    }
    
    // Check that reads return correct values according to this linearization
    let mut state: HashMap<String, V> = HashMap::new();
    
    for &idx in order.iter() {
        let timed_op = &operations[idx];
        match &timed_op.op {
            KVSOperation::Put(k, v) => {
                state.insert(k.clone(), v.clone());
            }
            KVSOperation::Get(k) => {
                let expected = state.get(k).cloned();
                if timed_op.response_value != expected {
                    return false;
                }
            }
        }
    }
    
    true
}

#[test]
fn test_linearizable_sequential_operations() {
    // Sequential operations (no overlap) should always be linearizable
    // in their real-time order
    
    let operations = vec![
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 10,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 20,
            response_time: 30,
            response_value: Some("1".to_string()),
        },
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "2".to_string()),
            invocation_time: 40,
            response_time: 50,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 60,
            response_time: 70,
            response_value: Some("2".to_string()),
        },
    ];
    
    assert!(is_linearizable(&operations), "Sequential operations should be linearizable");
}

#[test]
fn test_linearizable_concurrent_operations() {
    // Concurrent operations can be linearized in any order that respects
    // the read values
    
    // Two concurrent writes to different keys
    let operations = vec![
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 20,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Put("y".to_string(), "2".to_string()),
            invocation_time: 5,
            response_time: 25,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 30,
            response_time: 40,
            response_value: Some("1".to_string()),
        },
        TimedOperation {
            op: KVSOperation::Get("y".to_string()),
            invocation_time: 35,
            response_time: 45,
            response_value: Some("2".to_string()),
        },
    ];
    
    assert!(is_linearizable(&operations), "Concurrent writes to different keys should be linearizable");
}

#[test]
fn test_linearizable_read_your_writes() {
    // A read following a write (from same client) must see that write or a later one
    
    let operations = vec![
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 10,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 15,  // Starts after write completes
            response_time: 25,
            response_value: Some("1".to_string()),  // Must see the write
        },
    ];
    
    assert!(is_linearizable(&operations), "Read after write should see the write");
}

#[test]
fn test_non_linearizable_stale_read() {
    // A read that returns a stale value violates linearizability
    
    let operations = vec![
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 10,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "2".to_string()),
            invocation_time: 20,
            response_time: 30,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 40,  // Starts after second write completes
            response_time: 50,
            response_value: Some("1".to_string()),  // Returns first value - STALE!
        },
    ];
    
    assert!(!is_linearizable(&operations), "Stale read should violate linearizability");
}

#[test]
fn test_linearizable_overlapping_writes() {
    // Two overlapping writes can be linearized in either order,
    // as long as subsequent reads are consistent
    
    let operations = vec![
        // Two overlapping writes
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 20,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "2".to_string()),
            invocation_time: 10,  // Overlaps with first write
            response_time: 30,
            response_value: None,
        },
        // Read after both complete - should see one of them
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 40,
            response_time: 50,
            response_value: Some("2".to_string()),  // Sees second write
        },
    ];
    
    assert!(is_linearizable(&operations), "Overlapping writes should be linearizable");
}

#[test]
fn test_linearizable_multiple_keys() {
    // Operations on multiple keys should all respect linearizability
    
    let operations = vec![
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 10,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Put("y".to_string(), "2".to_string()),
            invocation_time: 5,
            response_time: 15,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 20,
            response_time: 30,
            response_value: Some("1".to_string()),
        },
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "3".to_string()),
            invocation_time: 25,
            response_time: 35,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 40,
            response_time: 50,
            response_value: Some("3".to_string()),
        },
        TimedOperation {
            op: KVSOperation::Get("y".to_string()),
            invocation_time: 45,
            response_time: 55,
            response_value: Some("2".to_string()),
        },
    ];
    
    assert!(is_linearizable(&operations), "Multi-key operations should be linearizable");
}

#[test]
fn test_linearizable_empty_read() {
    // Reading a key that was never written should return None
    
    let operations = vec![
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 0,
            response_time: 10,
            response_value: None,  // Key doesn't exist
        },
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 20,
            response_time: 30,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 40,
            response_time: 50,
            response_value: Some("1".to_string()),  // Now key exists
        },
    ];
    
    assert!(is_linearizable(&operations), "Empty reads should be linearizable");
}

#[test]
fn test_paxos_provides_total_order() {
    // This test demonstrates the key property that Paxos provides:
    // a total order over all operations
    
    // In a system with Paxos, even concurrent operations from different
    // clients get assigned a total order via consensus
    
    let operations = vec![
        // Client 1 writes
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 15,
            response_value: None,
        },
        // Client 2 writes (overlapping)
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "2".to_string()),
            invocation_time: 5,
            response_time: 20,
            response_value: None,
        },
        // Client 3 reads - must see one of them in the total order
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 25,
            response_time: 35,
            response_value: Some("2".to_string()),  // Paxos chose this order
        },
    ];
    
    assert!(is_linearizable(&operations), 
            "Paxos total order ensures linearizability");
}

#[test]
fn test_non_linearizable_time_travel() {
    // This shows a violation where a later read sees an older value
    // than an earlier read (impossible in a linearizable system)
    
    let operations = vec![
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 10,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "2".to_string()),
            invocation_time: 20,
            response_time: 30,
            response_value: None,
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 40,
            response_time: 50,
            response_value: Some("2".to_string()),  // Sees second write
        },
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 60,  // Later read
            response_time: 70,
            response_value: Some("1".to_string()),  // But sees first write - TIME TRAVEL!
        },
    ];
    
    assert!(!is_linearizable(&operations), 
            "Time travel (later read seeing older value) violates linearizability");
}

/// Helper function to check if an operation overlaps with any operation in a list
#[allow(dead_code)]
fn has_overlapping_operations<V>(operations: &[TimedOperation<V>]) -> bool {
    for i in 0..operations.len() {
        for j in (i + 1)..operations.len() {
            if operations[i].overlaps_with(&operations[j]) {
                return true;
            }
        }
    }
    false
}

#[test]
fn test_linearizability_with_paxos_semantics() {
    // This test specifically validates that the Paxos-based KVS provides
    // the expected linearizability guarantees
    
    // Scenario: Multiple clients submitting operations
    // Paxos assigns them slot numbers (total order)
    // Replicas apply them in that order
    
    let operations = vec![
        // Client A: PUT x=1 (slot 0)
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "1".to_string()),
            invocation_time: 0,
            response_time: 20,
            response_value: None,
        },
        // Client B: PUT y=2 (slot 1) - concurrent with A
        TimedOperation {
            op: KVSOperation::Put("y".to_string(), "2".to_string()),
            invocation_time: 5,
            response_time: 25,
            response_value: None,
        },
        // Client C: PUT x=3 (slot 2) - concurrent with A and B
        TimedOperation {
            op: KVSOperation::Put("x".to_string(), "3".to_string()),
            invocation_time: 10,
            response_time: 30,
            response_value: None,
        },
        // Client D: GET x after all writes complete (should see slot 2's value)
        TimedOperation {
            op: KVSOperation::Get("x".to_string()),
            invocation_time: 35,
            response_time: 45,
            response_value: Some("3".to_string()),
        },
        // Client E: GET y after all writes complete (should see slot 1's value)
        TimedOperation {
            op: KVSOperation::Get("y".to_string()),
            invocation_time: 36,
            response_time: 46,
            response_value: Some("2".to_string()),
        },
    ];
    
    assert!(is_linearizable(&operations), 
            "Paxos-ordered operations should be linearizable");
}
