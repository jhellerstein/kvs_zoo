//! Utility functions for working with value wrappers in examples and demos

use super::{CausalString, VCWrapper};
use crate::protocol::KVSOperation;

use std::collections::HashSet;

/// Generate demo operations with vector-clock-stamped causal values
///
/// Creates a set of operations that demonstrate causal consistency:
/// - Concurrent writes to the same key (will be merged via set union)
/// - Independent writes to different keys
/// - Read operations to test retrieval
///
/// This is useful for testing and demonstrating causal consistency behavior.
pub fn generate_causal_operations() -> Vec<KVSOperation<CausalString>> {
    // Create vector clocks for different "nodes" (simulating distributed writes)
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());

    let mut vc3 = VCWrapper::new();
    vc3.bump("node3".to_string());

    // Create operations with causal values (keys chosen to likely map to different shards)
    vec![
        // Concurrent writes to the same key (merge via SetUnion)
        KVSOperation::Put(
            "alpha".to_string(),
            CausalString::new_with_set(vc1.clone(), HashSet::from(["a1".to_string()])),
        ),
        KVSOperation::Put(
            "alpha".to_string(),
            CausalString::new_with_set(vc2.clone(), HashSet::from(["a2".to_string()])),
        ),
        // Independent write to a different key
        KVSOperation::Put(
            "beta".to_string(),
            CausalString::new_with_set(vc3.clone(), HashSet::from(["b1".to_string()])),
        ),
        // Reads
        KVSOperation::Get("alpha".to_string()),
        KVSOperation::Get("beta".to_string()),
        KVSOperation::Get("nonexistent".to_string()),
    ]
}

/// Log an operation for demo purposes
///
/// Provides formatted output showing the operation type, key, values,
/// and vector clock information for causal operations.
pub fn log_operation(op: &KVSOperation<CausalString>) {
    match op {
        KVSOperation::Put(k, v) => {
            let (vc, _) = v.as_parts();
            let values: Vec<_> = v.values().iter().collect();
            println!("Client: PUT {} => {:?} (vector clock: {:?})", k, values, vc);
        }
        KVSOperation::Get(k) => {
            println!("Client: GET {}", k);
        }
    }
}
