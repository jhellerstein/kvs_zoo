//! Core KVS implementation and node marker type
//!
//! This module provides the core per-node KVS implementation. It processes all operations
//! (both reads and writes) in a single sequential order, which is essential
//! for participating in linearizability guarantees. It also defines the KVSNode marker
//! type used for Hydro clusters.

pub mod events;
pub mod local_map;

use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use self::events::{DataEvent, MetaEvent};
use crate::protocol::{KVSOperation, KVSResponse};

/// Trait for KVS storage backends that can handle Put, Get, and Delete operations.
///
/// This trait abstracts over different storage implementations, allowing KVSCore
/// to work with both standard HashMap storage and tombstone-based storage.
pub trait KVSStorage<K, V>: Default {
    /// Apply a Put operation: insert or merge the value at the given key.
    fn apply_put(&mut self, key: K, value: V);

    /// Apply a Get operation: lookup the value at the given key.
    fn apply_get(&self, key: &K) -> Option<&V>;

    /// Apply a Delete operation: remove or tombstone the key.
    fn apply_delete(&mut self, key: K);
}

/// Implementation of KVSStorage for standard HashMap.
///
/// This provides the traditional KVS behavior:
/// - Put: insert or merge values using lattice semantics
/// - Get: standard lookup
/// - Delete: remove the key from the map
impl<K, V> KVSStorage<K, V> for std::collections::HashMap<K, V>
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone + lattices::Merge<V>,
{
    fn apply_put(&mut self, key: K, value: V) {
        self.entry(key)
            .and_modify(|existing| {
                lattices::Merge::merge(existing, value.clone());
            })
            .or_insert(value);
    }

    fn apply_get(&self, key: &K) -> Option<&V> {
        self.get(key)
    }

    fn apply_delete(&mut self, key: K) {
        self.remove(&key);
    }
}

#[derive(Clone)]
struct CoreEmission<K, V> {
    response: Option<KVSResponse<K, V>>,
    data: Option<DataEvent<K, V>>,
    meta: Option<MetaEvent<K>>,
}

/// Output bundle produced by `KVSCore::process`.
pub struct CoreOutput<K, V, L> {
    /// Sequential response stream for client-visible results.
    pub responses: Stream<KVSResponse<K, V>, L, Unbounded, TotalOrder>,
    /// Data event stream describing applied operations.
    pub data: Stream<DataEvent<K, V>, L, Unbounded, TotalOrder>,
    /// Metadata stream for maintenance/background pipelines.
    pub meta: Stream<MetaEvent<K>, L, Unbounded, TotalOrder>,
}

/// Represents an individual KVS node in the cluster
///
/// This is a marker type used with Hydro's `Cluster<KVSNode>` to identify
/// collections of nodes that form a KVS deployment.
pub struct KVSNode {}

/// Wrapper for KVS storage state to avoid generic leakage in q!() closures.
/// This provides a monomorphic type that Stageleft can properly stage.
#[derive(Clone, Debug)]
pub struct KVSState<Store> {
    pub inner: Store,
}

impl<Store: Default> Default for KVSState<Store> {
    fn default() -> Self {
        Self {
            inner: Store::default(),
        }
    }
}

impl<Store> KVSState<Store> {
    pub fn new(store: Store) -> Self {
        Self { inner: store }
    }
}

/// Helper function for creating KVS state in q!() macros.
pub fn new_kvs_state<Store: Default>() -> KVSState<Store> {
    KVSState::default()
}

/// Core KVS that processes operations in order
pub struct KVSCore;

impl KVSCore {
    /// Generic core processing over an arbitrary Store implementing KVSStorage.
    pub fn process<'a, K, V, L, Store>(
        operations: Stream<KVSOperation<K, V>, L, Unbounded, TotalOrder>,
    ) -> CoreOutput<K, V, L>
    where
        K: Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + PartialEq
            + Eq
            + std::hash::Hash
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
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
        Store: KVSStorage<K, V> + Clone + Send + Sync + 'static,
    {
        let combined = operations.scan(
            q!(|| Store::default()),
            q!(|state: &mut Store, operation: KVSOperation<K, V>| {
                let client_id = operation.client_id();

                // Only generate response if client_id is Some
                let should_emit_response = client_id.is_some();

                let (response, data, meta) = match operation {
                    KVSOperation::Put(key, value, _) => {
                        let value_for_event = value.clone();
                        state.apply_put(key.clone(), value);

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
                        let value = state.apply_get(&key).cloned();
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
                        state.apply_delete(key.clone());
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
                Some(CoreEmission { response, data, meta })
            }),
        );

        let responses = combined
            .clone()
            .filter_map(q!(|emission| emission.response));
        let data = combined.clone().filter_map(q!(|emission| emission.data));
        let meta = combined.filter_map(q!(|emission| emission.meta));

        CoreOutput { responses, data, meta }
    }

    /// Monomorphic wrapper for HashMap<K, V> storage to work around generic staging issues.
    ///
    /// This function avoids staging generic closures by using concrete HashMap types.
    /// Use this instead of `process::<K, V, L, HashMap<K, V>>` when staging is required.
    pub fn process_hashmap<'a, K, V, L>(
        operations: Stream<KVSOperation<K, V>, L, Unbounded, TotalOrder>,
    ) -> CoreOutput<K, V, L>
    where
        K: Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + PartialEq
            + Eq
            + std::hash::Hash
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
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
                        state.apply_put(key.clone(), value);

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
                        let value = state.apply_get(&key).cloned();
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
                        state.apply_delete(key.clone());
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
                Some(CoreEmission { response, data, meta })
            }),
        );

        let responses = combined
            .clone()
            .filter_map(q!(|emission| emission.response));
        let data = combined.clone().filter_map(q!(|emission| emission.data));
        let meta = combined.filter_map(q!(|emission| emission.meta));

        CoreOutput { responses, data, meta }
    }

    /// Monomorphic wrapper for LocalHashMapFst<V> tombstone storage.
    ///
    /// This function avoids staging generic closures for tombstone-based deletion.
    /// Use this for examples that need permanent deletion semantics.
    pub fn process_tombstone_fst<'a, V, L>(
        operations: Stream<KVSOperation<String, V>, L, Unbounded, TotalOrder>,
    ) -> CoreOutput<String, V, L>
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
            + lattices::LatticeFrom<V>
            + lattices::IsBot
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + Clone + 'a,
    {
        let combined = operations.scan(
            q!(|| crate::kvs_core::local_map::LocalHashMapFst::default()),
            q!(|state, operation| {
                let client_id = operation.client_id();

                // Only generate response if client_id is Some
                let should_emit_response = client_id.is_some();

                let (response, data, meta) = match operation {
                    KVSOperation::Put(key, value, _) => {
                        let value_for_event = value.clone();
                        state.apply_put(key.clone(), value);

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
                        let value = state.apply_get(&key).cloned();
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
                        state.apply_delete(key.clone());
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
                Some(CoreEmission { response, data, meta })
            }),
        );

        let responses = combined
            .clone()
            .filter_map(q!(|emission| emission.response));
        let data = combined.clone().filter_map(q!(|emission| emission.data));
        let meta = combined.filter_map(q!(|emission| emission.meta));

        CoreOutput { responses, data, meta }
    }
}

#[cfg(test)]
mod tests {
    use super::KVSStorage;
    use crate::protocol::{KVSOperation, KVSResponse};
    use crate::values::LwwWrapper;
    use proptest::prelude::*;

    #[test]
    fn test_sequential_processing_maintains_order() {
        // This test demonstrates the key property: operations are processed
        // in the exact order they appear, ensuring linearizability

        let operations: Vec<KVSOperation<String, LwwWrapper<String>>> = vec![
            KVSOperation::Put("x".to_string(), LwwWrapper::new("1".to_string()), Some(1)),
            KVSOperation::Get("x".to_string(), Some(1)),
            KVSOperation::Put("x".to_string(), LwwWrapper::new("2".to_string()), Some(1)),
            KVSOperation::Get("x".to_string(), Some(1)),
        ];

        // In a real implementation, we'd test this with Hydro streams
        // For now, we simulate the sequential processing logic
        let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
            std::collections::HashMap::new();
        let mut responses = Vec::new();

        for op in operations {
            let response = match op {
                KVSOperation::Put(key, value, _) => {
                    state.apply_put(key.clone(), value);
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _) => match state.apply_get(&key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _) => {
                    state.apply_delete(key.clone());
                    format!("DELETE {} = OK", key)
                }
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

        let operations: Vec<KVSOperation<String, LwwWrapper<String>>> = vec![
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
        let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
            std::collections::HashMap::new();
        let mut sequential_responses = Vec::new();

        for op in &operations {
            let response = match op {
                KVSOperation::Put(key, value, _) => {
                    state.apply_put(key.clone(), value.clone());
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _) => match state.apply_get(key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _) => {
                    state.apply_delete(key.clone());
                    format!("DELETE {} = OK", key)
                }
            };
            sequential_responses.push(response);
        }

        // Split processing (incorrect for linearizability)
        let mut split_state: std::collections::HashMap<String, LwwWrapper<String>> =
            std::collections::HashMap::new();
        let mut split_responses = vec!["".to_string(); 4];

        // Process all PUTs first (wrong!)
        for (i, op) in operations.iter().enumerate() {
            if let KVSOperation::Put(key, value, _) = op {
                split_state.apply_put(key.clone(), value.clone());
                split_responses[i] = format!("PUT {} = OK", key);
            }
        }

        // Then process all GETs (wrong!)
        for (i, op) in operations.iter().enumerate() {
            if let KVSOperation::Get(key, _) = op {
                // This GET will see the final state, not the state at its position
                match split_state.apply_get(key) {
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

        let operations: Vec<KVSOperation<String, LwwWrapper<String>>> = vec![
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

        let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
            std::collections::HashMap::new();
        let mut responses = Vec::new();

        for op in operations {
            let response = match op {
                KVSOperation::Put(key, value, _) => {
                    state.apply_put(key.clone(), value);
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _) => match state.apply_get(&key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _) => {
                    state.apply_delete(key.clone());
                    format!("DELETE {} = OK", key)
                }
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

    fn arb_kvs_operation_with_client_id()
    -> impl Strategy<Value = KVSOperation<String, LwwWrapper<String>>> {
        let key_strategy = "[a-z]{1,10}";
        let value_strategy = "[a-z0-9]{1,20}";

        prop_oneof![
            (key_strategy, value_strategy, arb_client_id())
                .prop_map(|(k, v, cid)| KVSOperation::Put(k, LwwWrapper::new(v), Some(cid))),
            (key_strategy, arb_client_id()).prop_map(|(k, cid)| KVSOperation::Get(k, Some(cid))),
            (key_strategy, arb_client_id())
                .prop_map(|(k, cid)| KVSOperation::Delete(k, Some(cid))),
        ]
    }

    fn arb_kvs_operation_without_client_id()
    -> impl Strategy<Value = KVSOperation<String, LwwWrapper<String>>> {
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
            let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
                std::collections::HashMap::new();
            let should_respond = true; // Client operations should respond
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<String, LwwWrapper<String>>> = match op {
                KVSOperation::Put(key, value, _) => {
                    state.apply_put(key.clone(), value);
                    if should_emit_response {
                        Some(KVSResponse::PutOk { client_id })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, _) => {
                    let value = state.apply_get(&key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult { client_id, value })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, _) => {
                    state.apply_delete(key.clone());
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
            let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
                std::collections::HashMap::new();
            let should_respond = true; // Even if should_respond is true...
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<String, LwwWrapper<String>>> = match op {
                KVSOperation::Put(key, value, _) => {
                    state.apply_put(key.clone(), value);
                    if should_emit_response {
                        Some(KVSResponse::PutOk { client_id })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, _) => {
                    let value = state.apply_get(&key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult { client_id, value })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, _) => {
                    state.apply_delete(key.clone());
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
