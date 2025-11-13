//! Core KVS implementation and node marker type
//!
//! This module provides the core per-node KVS implementation. It processes all operations
//! (both reads and writes) in a single sequential order, which is essential
//! for participating in linearizability guarantees. It also defines the KVSNode marker
//! type used for Hydro clusters.

use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::protocol::KVSOperation;

/// Represents an individual KVS node in the cluster
///
/// This is a marker type used with Hydro's `Cluster<KVSNode>` to identify
/// collections of nodes that form a KVS deployment.
pub struct KVSNode {}

/// Core KVS that processes operations in order
pub struct KVSCore;

impl KVSCore {
    /// This function takes a stream of operations and processes them one by one
    /// in order, ensuring that each read sees the exact state at its position
    /// in the sequence. Uses lattice merge semantics for combining values.
    ///
    /// ## Parameters
    /// - `operations`: Stream of operations in total order
    ///
    /// ## Returns
    /// Stream of responses in the same order as operations
    pub fn process<'a, V, L>(
        operations: Stream<KVSOperation<V>, L, Unbounded, TotalOrder>,
    ) -> Stream<String, L, Unbounded, TotalOrder>
    where
        V: Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + Clone + 'a,
    {
        // Use scan to maintain state and emit responses for each operation
        operations.scan(
            q!(|| std::collections::HashMap::new()),
            q!(|state, op| {
                let response = match op {
                    KVSOperation::Put(key, value) => {
                        // Use lattice merge semantics
                        state
                            .entry(key.clone())
                            .and_modify(|existing| {
                                lattices::Merge::merge(existing, value.clone());
                            })
                            .or_insert(value);
                        format!("PUT {} = OK", key)
                    }
                    KVSOperation::Get(key) => match state.get(&key) {
                        Some(value) => format!("GET {} = {}", key, value),
                        None => format!("GET {} = NOT FOUND", key),
                    },
                };
                Some(response) // Always emit the response
            }),
        )
    }

    /// Process operations and also emit a side-channel of applied PUT deltas.
    ///
    /// Returns a tuple (responses, puts) where:
    /// - responses: total order stream of client-visible responses (PUT ack / GET value)
    /// - puts: total order stream of (key, value) for each applied PUT in the same order
    ///
    /// This lets replication strategies operate purely on applied state deltas (after storage)
    /// rather than inspecting upstream operations.
    #[allow(clippy::type_complexity)]
    pub fn process_with_deltas<'a, V, L>(
        operations: Stream<KVSOperation<V>, L, Unbounded, TotalOrder>,
    ) -> (
        Stream<String, L, Unbounded, TotalOrder>,
        Stream<(String, V), L, Unbounded, TotalOrder>,
    )
    where
        V: Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + Clone + 'a,
    {
        let scanned = operations.scan(
            q!(|| (std::collections::HashMap::new(), Vec::new())),
            q!(|state_and_deltas, op| {
                let (state, deltas) = state_and_deltas;
                deltas.clear();
                let response = match op {
                    KVSOperation::Put(key, value) => {
                        state
                            .entry(key.clone())
                            .and_modify(|existing| {
                                lattices::Merge::merge(existing, value.clone());
                            })
                            .or_insert(value.clone());
                        deltas.push((key.clone(), value));
                        format!("PUT {} = OK", key)
                    }
                    KVSOperation::Get(key) => match state.get(&key) {
                        Some(value) => format!("GET {} = {}", key, value),
                        None => format!("GET {} = NOT FOUND", key),
                    },
                };
                Some((response, deltas.clone()))
            }),
        );

        let responses = scanned.clone().map(q!(|(resp, _d)| resp));
        let puts = scanned.flat_map_ordered(q!(|(_resp, deltas)| deltas));
        (responses, puts)
    }

    /// Process operations with selective responses (for replication)
    ///
    /// Takes tagged operations where the boolean indicates whether to generate
    /// a response. This allows distinguishing local operations (respond) from
    /// replicated operations (don't respond).
    ///
    /// ## Parameters
    /// - `tagged_operations`: Stream of (should_respond, operation) pairs
    ///
    /// ## Returns
    /// Stream of responses only for operations marked with should_respond=true
    pub fn process_with_responses<'a, V, L>(
        tagged_operations: Stream<(bool, KVSOperation<V>), L, Unbounded, TotalOrder>,
    ) -> Stream<String, L, Unbounded, TotalOrder>
    where
        V: Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + Clone + 'a,
    {
        // Use scan to maintain state and conditionally emit responses
        tagged_operations
            .scan(
                q!(|| std::collections::HashMap::new()),
                q!(|state, (should_respond, op)| {
                    match op {
                        KVSOperation::Put(key, value) => {
                            // Use lattice merge semantics
                            state
                                .entry(key.clone())
                                .and_modify(|existing| {
                                    lattices::Merge::merge(existing, value.clone());
                                })
                                .or_insert(value);
                            if should_respond {
                                Some(Some(format!("PUT {} = OK", key)))
                            } else {
                                Some(None) // No response for replicated PUTs
                            }
                        }
                        KVSOperation::Get(key) => {
                            // GETs are always local (we don't replicate reads)
                            let response = match state.get(&key) {
                                Some(value) => format!("GET {} = {}", key, value),
                                None => format!("GET {} = NOT FOUND", key),
                            };
                            Some(Some(response))
                        }
                    }
                }),
            )
            .filter_map(q!(|opt| opt)) // Remove None responses
    }
}

impl KVSCore {
    /// Process tagged operations but return the tag alongside the response.
    ///
    /// Tag semantics: `is_replica == true` means the op arrived via replication
    /// and should NOT be ACKed to the client. `is_replica == false` means the op
    /// is the original locally-routed op and SHOULD be ACKed.
    pub fn process_tagged<'a, V>(
        tagged_operations: Stream<
            (bool, KVSOperation<V>),
            Cluster<'a, KVSNode>,
            Unbounded,
            TotalOrder,
        >,
    ) -> Stream<(bool, String), Cluster<'a, KVSNode>, Unbounded, TotalOrder>
    where
        V: Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        let indexed = tagged_operations.enumerate();
        indexed.scan(
            q!(|| (std::collections::HashMap::new(), 0u32)),
            q!(|(state, counter), (_idx, (is_via_replication, op))| {
                let response = match op {
                    KVSOperation::Put(key, value) => {
                        state
                            .entry(key.clone())
                            .and_modify(|existing| {
                                lattices::Merge::merge(existing, value.clone());
                            })
                            .or_insert(value);
                        *counter += 1;
                        format!(
                            "[op{}:rep={}] PUT {} = OK",
                            counter, is_via_replication, key
                        )
                    }
                    KVSOperation::Get(key) => {
                        *counter += 1;
                        match state.get(&key) {
                            Some(value) => format!(
                                "[op{}:rep={}] GET {} = {}",
                                counter, is_via_replication, key, value
                            ),
                            None => format!(
                                "[op{}:rep={}] GET {} = NOT FOUND",
                                counter, is_via_replication, key
                            ),
                        }
                    }
                };
                Some((is_via_replication, response))
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
        assert!(responses[1].contains("1")); // GET sees first PUT
        assert_eq!(responses[2], "PUT x = OK");
        assert!(responses[3].contains("2")); // GET sees second PUT
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
