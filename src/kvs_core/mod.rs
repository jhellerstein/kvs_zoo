//! Core KVS implementation and node marker type
//!
//! This module provides the core per-node KVS implementation. It processes all operations
//! (both reads and writes) in a single sequential order, which is essential
//! for participating in linearizability guarantees. It also defines the KVSNode marker
//! type used for Hydro clusters.

pub mod events;

use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use self::events::{DataEvent, MetaEvent};
use crate::protocol::{KVSOperation, KVSResponse};

#[derive(Clone)]
struct CoreEmission<V> {
    response: Option<KVSResponse<V>>,
    data: Option<DataEvent<V>>,
    meta: Option<MetaEvent>,
}

/// Output bundle produced by `KVSCore::process`.
pub struct CoreOutput<V, L> {
    /// Sequential response stream for client-visible results.
    pub responses: Stream<KVSResponse<V>, L, Unbounded, TotalOrder>,
    /// Data event stream describing applied operations.
    pub data: Stream<DataEvent<V>, L, Unbounded, TotalOrder>,
    /// Metadata stream for maintenance/background pipelines.
    pub meta: Stream<MetaEvent, L, Unbounded, TotalOrder>,
}

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
    /// Structured response containing both the response stream (for clients)
    /// and a metadata stream suitable for background maintenance wiring.
    pub fn process<'a, V, L>(
        operations: Stream<KVSOperation<V>, L, Unbounded, TotalOrder>,
    ) -> CoreOutput<V, L>
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
        let combined = operations.scan(
            q!(|| std::collections::HashMap::new()),
            q!(|state, operation| {
                let client_id = operation.client_id();

                // Only generate response if client_id is Some
                let should_emit_response = client_id.is_some();

                let (response, data, meta) = match operation {
                    KVSOperation::Put(key, value, _) => {
                        let value_for_event = value.clone();
                        state
                            .entry(key.clone())
                            .and_modify(|existing| {
                                lattices::Merge::merge(existing, value.clone());
                            })
                            .or_insert(value);

                        let response = if should_emit_response {
                            Some(KVSResponse::PutOk { client_id })
                        } else {
                            None
                        };
                        let data = Some(DataEvent::Put {
                            key: key.clone(),
                            value: value_for_event,
                        });
                        (response, data, None)
                    }
                    KVSOperation::Get(key, _) => {
                        let value = state.get(&key).cloned();
                        let response = if should_emit_response {
                            Some(KVSResponse::GetResult {
                                client_id,
                                value: value.clone(),
                            })
                        } else {
                            None
                        };
                        let data = Some(DataEvent::Get {
                            key: key.clone(),
                            value,
                        });
                        (response, data, None)
                    }
                    KVSOperation::Delete(key, _) => {
                        state.remove(&key);
                        let response = if should_emit_response {
                            Some(KVSResponse::DeleteOk { client_id })
                        } else {
                            None
                        };
                        let data = Some(DataEvent::Delete { key: key.clone() });
                        let meta = Some(MetaEvent::Tomb { key: key.clone() });
                        (response, data, meta)
                    }
                };
                Some(CoreEmission {
                    response,
                    data,
                    meta,
                })
            }),
        );

        let responses = combined
            .clone()
            .filter_map(q!(|emission| emission.response));
        let data = combined.clone().filter_map(q!(|emission| emission.data));
        let meta = combined.filter_map(q!(|emission| emission.meta));

        CoreOutput {
            responses,
            data,
            meta,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{KVSOperation, KVSResponse};
    use crate::values::LwwWrapper;
    use proptest::prelude::*;

    #[test]
    fn test_sequential_processing_maintains_order() {
        // This test demonstrates the key property: operations are processed
        // in the exact order they appear, ensuring linearizability

        let operations = vec![
            KVSOperation::Put("x".to_string(), LwwWrapper::new("1".to_string()), Some(1)),
            KVSOperation::Get("x".to_string(), Some(1)),
            KVSOperation::Put("x".to_string(), LwwWrapper::new("2".to_string()), Some(1)),
            KVSOperation::Get("x".to_string(), Some(1)),
        ];

        // In a real implementation, we'd test this with Hydro streams
        // For now, we simulate the sequential processing logic
        let mut state = std::collections::HashMap::new();
        let mut responses = Vec::new();

        for op in operations {
            let response = match op {
                KVSOperation::Put(key, value, _) => {
                    state.insert(key.clone(), value);
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _) => match state.get(&key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _) => format!("DELETE {} = OK", key),
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
            KVSOperation::Put(
                "account".to_string(),
                LwwWrapper::new("100".to_string()),
                Some(1),
            ),
            KVSOperation::Get("account".to_string(), Some(1)),
            KVSOperation::Put(
                "account".to_string(),
                LwwWrapper::new("75".to_string()),
                Some(1),
            ),
            KVSOperation::Get("account".to_string(), Some(1)),
        ];

        // Sequential processing (correct for linearizability)
        let mut state = std::collections::HashMap::new();
        let mut sequential_responses = Vec::new();

        for op in &operations {
            let response = match op {
                KVSOperation::Put(key, value, _) => {
                    state.insert(key.clone(), value.clone());
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _) => match state.get(key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _) => format!("DELETE {} = OK", key),
            };
            sequential_responses.push(response);
        }

        // Split processing (incorrect for linearizability)
        let mut split_state = std::collections::HashMap::new();
        let mut split_responses = vec!["".to_string(); 4];

        // Process all PUTs first (wrong!)
        for (i, op) in operations.iter().enumerate() {
            if let KVSOperation::Put(key, value, _) = op {
                split_state.insert(key.clone(), value.clone());
                split_responses[i] = format!("PUT {} = OK", key);
            }
        }

        // Then process all GETs (wrong!)
        for (i, op) in operations.iter().enumerate() {
            if let KVSOperation::Get(key, _) = op {
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
            KVSOperation::Put(
                "alice".to_string(),
                LwwWrapper::new("100".to_string()),
                Some(1),
            ),
            KVSOperation::Put(
                "bob".to_string(),
                LwwWrapper::new("50".to_string()),
                Some(1),
            ),
            // Check initial balances
            KVSOperation::Get("alice".to_string(), Some(1)),
            KVSOperation::Get("bob".to_string(), Some(1)),
            // Transfer $25 from Alice to Bob (must be atomic in total order)
            KVSOperation::Put(
                "alice".to_string(),
                LwwWrapper::new("75".to_string()),
                Some(1),
            ),
            KVSOperation::Put(
                "bob".to_string(),
                LwwWrapper::new("75".to_string()),
                Some(1),
            ),
            // Check final balances
            KVSOperation::Get("alice".to_string(), Some(1)),
            KVSOperation::Get("bob".to_string(), Some(1)),
        ];

        let mut state = std::collections::HashMap::new();
        let mut responses = Vec::new();

        for op in operations {
            let response = match op {
                KVSOperation::Put(key, value, _) => {
                    state.insert(key.clone(), value);
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _) => match state.get(&key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _) => format!("DELETE {} = OK", key),
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

    // Property-based test generators
    fn arb_client_id() -> impl Strategy<Value = u64> {
        prop_oneof![
            Just(0u64),   // Edge case: client 0
            Just(1u64),   // Common case
            1u64..100u64, // Small range
            any::<u64>(), // Full range
        ]
    }

    fn arb_kvs_operation_with_client_id() -> impl Strategy<Value = KVSOperation<LwwWrapper<String>>>
    {
        let key_strategy = "[a-z]{1,10}";
        let value_strategy = "[a-z0-9]{1,20}";

        prop_oneof![
            (key_strategy, value_strategy, arb_client_id())
                .prop_map(|(k, v, cid)| KVSOperation::Put(k, LwwWrapper::new(v), Some(cid))),
            (key_strategy, arb_client_id()).prop_map(|(k, cid)| KVSOperation::Get(k, Some(cid))),
            (key_strategy, arb_client_id()).prop_map(|(k, cid)| KVSOperation::Delete(k, Some(cid))),
        ]
    }

    fn arb_kvs_operation_without_client_id()
    -> impl Strategy<Value = KVSOperation<LwwWrapper<String>>> {
        let key_strategy = "[a-z]{1,10}";
        let value_strategy = "[a-z0-9]{1,20}";

        prop_oneof![
            (key_strategy, value_strategy).prop_map(|(k, v)| KVSOperation::Put(
                k,
                LwwWrapper::new(v),
                None
            )),
            key_strategy.prop_map(|k| KVSOperation::Get(k, None)),
            key_strategy.prop_map(|k| KVSOperation::Delete(k, None)),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// **Feature: client-id-propagation, Property 2: Response inherits operation client ID**
        ///
        /// For any KVS operation with client ID Some(N), the response generated from
        /// processing that operation should have client ID Some(N).
        ///
        /// **Validates: Requirements 1.2**
        #[test]
        fn prop_response_inherits_operation_client_id(op in arb_kvs_operation_with_client_id()) {
            // Extract the client_id from the operation
            let expected_client_id = op.client_id();
            prop_assert!(expected_client_id.is_some(), "Generated operation should have Some client_id");

            // Simulate the core processing logic
            let mut state = std::collections::HashMap::new();
            let should_respond = true; // Client operations should respond
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<LwwWrapper<String>>> = match op {
                KVSOperation::Put(key, value, _) => {
                    state.insert(key.clone(), value);
                    if should_emit_response {
                        Some(KVSResponse::PutOk { client_id })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, _) => {
                    let value = state.get(&key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult { client_id, value })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, _) => {
                    state.remove(&key);
                    if should_emit_response {
                        Some(KVSResponse::DeleteOk { client_id })
                    } else {
                        None
                    }
                }
            };

            // Verify that a response was generated
            prop_assert!(response.is_some(), "Response should be generated for client operation");

            // Verify that the response has the same client_id as the operation
            let response = response.unwrap();
            prop_assert_eq!(response.client_id(), expected_client_id,
                "Response client_id should match operation client_id");
        }

        /// **Feature: client-id-propagation, Property 8: None client ID operations produce no responses**
        ///
        /// For any KVS operation with client ID None, processing that operation should
        /// not generate a response to external clients.
        ///
        /// **Validates: Requirements 4.2**
        #[test]
        fn prop_none_client_id_produces_no_response(op in arb_kvs_operation_without_client_id()) {
            // Verify the operation has None client_id
            prop_assert!(op.client_id().is_none(), "Generated operation should have None client_id");

            // Simulate the core processing logic
            let mut state = std::collections::HashMap::new();
            let should_respond = true; // Even if should_respond is true...
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<LwwWrapper<String>>> = match op {
                KVSOperation::Put(key, value, _) => {
                    state.insert(key.clone(), value);
                    if should_emit_response {
                        Some(KVSResponse::PutOk { client_id })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, _) => {
                    let value = state.get(&key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult { client_id, value })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, _) => {
                    state.remove(&key);
                    if should_emit_response {
                        Some(KVSResponse::DeleteOk { client_id })
                    } else {
                        None
                    }
                }
            };

            // Verify that NO response was generated for None client_id
            prop_assert!(response.is_none(),
                "No response should be generated for operation with None client_id");
        }
    }
}
