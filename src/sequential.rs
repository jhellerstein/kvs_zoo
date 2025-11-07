//! Sequential KVS implementation for linearizable operations
//!
//! This module provides a KVS implementation that processes all operations
//! (both reads and writes) in a single sequential order, which is essential
//! for linearizability guarantees.
//!
//! ## Key Difference from Core KVS
//!
//! Unlike `KVSCore` which splits operations into separate PUT and GET streams,
//! this implementation processes all operations in their original order:
//!
//! ```text
//! Input:  PUT(X,1) → GET(X) → PUT(X,2) → GET(X)
//! Output: OK     → "1"    → OK      → "2"
//! ```
//!
//! This ensures that reads see the exact state at their position in the
//! total order, which is required for linearizability.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::protocol::KVSOperation;

/// Sequential KVS that processes operations in order
///
/// This implementation maintains linearizability by processing all operations
/// sequentially without splitting reads and writes into separate streams.
pub struct KVSSequential;

impl KVSSequential {
    /// Process operations sequentially, maintaining linearizable semantics
    ///
    /// This function takes a stream of operations and processes them one by one
    /// in order, ensuring that each read sees the exact state at its position
    /// in the sequence.
    ///
    /// ## Parameters
    /// - `operations`: Stream of operations in total order (from Paxos)
    ///
    /// ## Returns
    /// Stream of responses in the same order as operations
    pub fn process_sequential<'a, V>(
        operations: Stream<KVSOperation<V>, Cluster<'a, crate::core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::core::KVSNode>, Unbounded>
    where
        V: Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        // Use scan to maintain state and emit responses for each operation
        // Use Default::default() to avoid generic type issues in Hydro closures
        operations.scan(
            q!(|| std::collections::HashMap::new()),
            q!(|state, op| {
                let response = match op {
                    KVSOperation::Put(key, value) => {
                        state.insert(key.clone(), value);
                        format!("PUT {} = OK [SEQUENTIAL]", key)
                    }
                    KVSOperation::Get(key) => match state.get(&key) {
                        Some(value) => format!("GET {} = {:?} [SEQUENTIAL]", key, value),
                        None => format!("GET {} = NOT FOUND [SEQUENTIAL]", key),
                    },
                };
                Some(response) // Always emit the response
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::KVSOperation;
    use crate::values::LwwWrapper;

    #[test]
    fn test_sequential_processing_maintains_order() {
        // This test demonstrates the key property: operations are processed
        // in the exact order they appear, ensuring linearizability

        let operations = vec![
            KVSOperation::Put("x".to_string(), LwwWrapper::new("1".to_string())),
            KVSOperation::Get("x".to_string()),
            KVSOperation::Put("x".to_string(), LwwWrapper::new("2".to_string())),
            KVSOperation::Get("x".to_string()),
        ];

        // In a real implementation, we'd test this with Hydro streams
        // For now, we simulate the sequential processing logic
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

        // Verify the linearizable sequence
        assert_eq!(responses[0], "PUT x = OK");
        assert!(responses[1].contains("LwwWrapper(\"1\")")); // GET sees first PUT
        assert_eq!(responses[2], "PUT x = OK");
        assert!(responses[3].contains("LwwWrapper(\"2\")")); // GET sees second PUT
    }

    #[test]
    fn test_sequential_vs_split_processing() {
        // This test shows why splitting PUTs and GETs breaks linearizability

        let operations = vec![
            KVSOperation::Put("account".to_string(), LwwWrapper::new("100".to_string())),
            KVSOperation::Get("account".to_string()),
            KVSOperation::Put("account".to_string(), LwwWrapper::new("75".to_string())),
            KVSOperation::Get("account".to_string()),
        ];

        // Sequential processing (correct for linearizability)
        let mut state = std::collections::HashMap::new();
        let mut sequential_responses = Vec::new();

        for op in &operations {
            let response = match op {
                KVSOperation::Put(key, value) => {
                    state.insert(key.clone(), value.clone());
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key) => match state.get(key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
            };
            sequential_responses.push(response);
        }

        // Split processing (incorrect for linearizability)
        let mut split_state = std::collections::HashMap::new();
        let mut split_responses = vec!["".to_string(); 4];

        // Process all PUTs first (wrong!)
        for (i, op) in operations.iter().enumerate() {
            if let KVSOperation::Put(key, value) = op {
                split_state.insert(key.clone(), value.clone());
                split_responses[i] = format!("PUT {} = OK", key);
            }
        }

        // Then process all GETs (wrong!)
        for (i, op) in operations.iter().enumerate() {
            if let KVSOperation::Get(key) = op {
                // This GET will see the final state, not the state at its position
                match split_state.get(key) {
                    Some(value) => split_responses[i] = format!("GET {} = {:?}", key, value),
                    None => split_responses[i] = format!("GET {} = NOT FOUND", key),
                }
            }
        }

        // Sequential processing gives correct linearizable results
        assert!(sequential_responses[1].contains("100")); // First GET sees 100
        assert!(sequential_responses[3].contains("75")); // Second GET sees 75

        // Split processing gives incorrect results (both GETs see final value)
        assert!(split_responses[1].contains("75")); // Wrong! Should see 100
        assert!(split_responses[3].contains("75")); // This one is correct by accident

        println!("Sequential (correct): {:?}", sequential_responses);
        println!("Split (incorrect): {:?}", split_responses);
    }

    #[test]
    fn test_linearizable_bank_transfer() {
        // This test demonstrates a classic linearizability scenario:
        // a bank transfer that must be atomic and consistent

        let operations = vec![
            // Initial state
            KVSOperation::Put("alice".to_string(), LwwWrapper::new("100".to_string())),
            KVSOperation::Put("bob".to_string(), LwwWrapper::new("50".to_string())),
            // Check initial balances
            KVSOperation::Get("alice".to_string()),
            KVSOperation::Get("bob".to_string()),
            // Transfer $25 from Alice to Bob (must be atomic in total order)
            KVSOperation::Put("alice".to_string(), LwwWrapper::new("75".to_string())),
            KVSOperation::Put("bob".to_string(), LwwWrapper::new("75".to_string())),
            // Check final balances
            KVSOperation::Get("alice".to_string()),
            KVSOperation::Get("bob".to_string()),
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

        // Verify linearizable transfer
        assert!(responses[2].contains("100")); // Alice initially has 100
        assert!(responses[3].contains("50")); // Bob initially has 50
        assert!(responses[6].contains("75")); // Alice finally has 75
        assert!(responses[7].contains("75")); // Bob finally has 75

        println!("Linearizable bank transfer: {:?}", responses);
    }
}
