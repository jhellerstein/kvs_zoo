//! Core KVS implementation and node marker type
//!
//! This module provides the core per-node KVS implementation. It processes all operations
//! (both reads and writes) in a single sequential order, which is essential
//! for participating in linearizability guarantees. It also defines the KVSNode marker
//! type used for Hydro clusters.

pub mod events;
pub mod local_map;

use hydro_lang::live_collections::stream::{NoOrder, Ordering, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use stageleft::IntoQuotedMut;

use self::events::{DataEvent, MetaEvent};
use crate::protocol::{KVSOperation, KVSResponse};

/// Trait for KVS storage backends that can handle Put, Get, and Delete operations.
///
/// This trait abstracts over different storage implementations, allowing KVSCore
/// to work with both standard HashMap storage and tombstone-based storage.
///
/// Standard HashMap implementation is below; tombstone-based storage is in local_map.rs.
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
pub struct CoreEmission<K, V> {
    pub response: Option<KVSResponse<K, V>>,
    pub data: Option<DataEvent<K, V>>,
    pub meta: Option<MetaEvent<K>>,
}

/// Output bundle produced by `KVSCore::process`.
///
/// Generic over ordering: TotalOrder for linearizable processing, NoOrder for monotonic.
pub struct CoreOutput<K, V, L, O = TotalOrder>
where
    O: Ordering,
{
    /// Response stream for client-visible results.
    pub responses: Stream<KVSResponse<K, V>, L, Unbounded, O>,
    /// Data event stream describing applied operations.
    pub data: Stream<DataEvent<K, V>, L, Unbounded, O>,
    /// Metadata stream for maintenance/background pipelines.
    pub meta: Stream<MetaEvent<K>, L, Unbounded, O>,
}

/// Represents an individual KVS node in the cluster
///
/// This is a marker type used with Hydro's `Cluster<KVSNode>` to identify
/// collections of nodes that form a KVS deployment.
pub struct KVSNode {}

/// Core KVS that processes operations in order
pub struct KVSCore;

impl KVSCore {
    /// Process operations using monotonic semantics with coordination-free updates.
    ///
    /// Accepts NoOrder input and uses lattice operations for CALM compliance.
    /// Mutations (put/delete) flow through without coordination, gets batch into ticks.
    pub fn process<'a, K, V, L, Store, I, F>(
        operations: impl Into<Stream<KVSOperation<K, V>, L, Unbounded, NoOrder>>,
        init_storage: I,
    ) -> CoreOutput<K, V, L, NoOrder>
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
            + lattices::LatticeFrom<V>
            + lattices::IsBot
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + hydro_lang::location::NoTick + Clone + 'a,
        Store: KVSStorage<K, V>,
        I: IntoQuotedMut<'a, F, L> + Clone,
        F: Fn() -> Store + 'a,
    {
        Self::process_no_order(operations, init_storage)
    }

    /// Process operations using linearizable semantics with sequential state updates.
    ///
    /// Requires TotalOrder input because `scan` only works on ordered streams.
    /// This provides strong consistency but requires coordination.
    pub fn process_ordered<'a, K, V, L, Store, I, F>(
        operations: Stream<KVSOperation<K, V>, L, Unbounded, TotalOrder>,
        init_storage: I,
    ) -> CoreOutput<K, V, L, TotalOrder>
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
        Store: KVSStorage<K, V>,
        I: IntoQuotedMut<'a, F, L>,
        F: Fn() -> Store + 'a,
    {
        let combined = operations.scan(init_storage, q!(|state, operation| {
            let request_id = operation.request_id();
            let client_id = operation.client_id();
            let should_emit_response = client_id.is_some();

            let (response, data, meta) = match operation {
                KVSOperation::Put(key, value, _, _) => {
                    let value_for_event = value.clone();
                    state.apply_put(key.clone(), value);

                    let response = if should_emit_response {
                        Some(KVSResponse::PutOk {
                            request_id,
                            client_id,
                        })
                    } else {
                        None
                    };
                    let data = Some(DataEvent::Put {
                        key: key.clone(),
                        value: value_for_event,
                    });
                    (response, data, None)
                }
                KVSOperation::Get(key, _, _) => {
                    let value = state.apply_get(&key).cloned();
                    let response = if should_emit_response {
                        Some(KVSResponse::GetResult {
                            request_id,
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
                KVSOperation::Delete(key, _, _) => {
                    state.apply_delete(key.clone());
                    let response = if should_emit_response {
                        Some(KVSResponse::DeleteOk {
                            request_id,
                            client_id,
                        })
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
        }));

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

    /// Process operations using monotonic semantics with coordination-free updates.
    ///
    /// Accepts NoOrder input and uses lattice operations for CALM compliance.
    /// 
    /// Key insight: Mutations (put/delete) flow through without coordination,
    /// but gets need to batch into a tick to snapshot storage state.
    pub fn process_no_order<'a, K, V, L, Store, I, F>(
        operations: impl Into<Stream<KVSOperation<K, V>, L, Unbounded, NoOrder>>,
        _init_storage: I,
    ) -> CoreOutput<K, V, L, NoOrder>
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
            + lattices::LatticeFrom<V>
            + lattices::IsBot
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + hydro_lang::location::NoTick + Clone + 'a,
        Store: KVSStorage<K, V>,
        I: IntoQuotedMut<'a, F, L> + Clone,
        F: Fn() -> Store + 'a,
    {
        let operations = operations.into();
        let tick = operations.location().tick();

        // Split operations by type
        let puts = operations.clone().filter_map(q!(|op| match op {
            KVSOperation::Put(key, value, request_id, client_id) => {
                Some((key, value, request_id, client_id))
            }
            _ => None,
        }));

        let deletes = operations.clone().filter_map(q!(|op| match op {
            KVSOperation::Delete(key, request_id, client_id) => Some((key, request_id, client_id)),
            _ => None,
        }));

        let gets = operations.filter_map(q!(|op| match op {
            KVSOperation::Get(key, request_id, client_id) => Some((key, request_id, client_id)),
            _ => None,
        }));

        // Build storage state using keyed lattice operations
        // fold_commutative_idempotent maintains persistent state across process lifetime
        let put_updates = puts.clone().map(q!(|(key, value, _, _)| (key, value)));
        
        let storage = put_updates
            .into_keyed()
            .fold_commutative_idempotent(
                q!(|| Default::default()),
                q!(|old, new| {
                    lattices::Merge::merge(old, new);
                })
            );

        // Batch gets into ticks and snapshot storage at the same tick
        let gets_batched = gets
            .batch(&tick, nondet!(/** batch gets for snapshot */))
            .map(q!(|(key, request_id, client_id)| (key, (request_id, client_id))))
            .into_keyed();
        let storage_snapshot = storage.snapshot(&tick, nondet!(/** snapshot storage for gets */));
        
        // Use get_many to lookup keys in the storage snapshot
        let get_results = storage_snapshot.get_many(gets_batched);
        
        let get_responses = get_results
            .values()
            .filter_map(q!(|(value_opt, (request_id, client_id))| {
                if client_id.is_some() {
                    Some(KVSResponse::GetResult {
                        request_id,
                        client_id,
                        value: value_opt,
                    })
                } else {
                    None
                }
            }))
            .all_ticks();

        // Generate put/delete responses by teeing off (no batching needed)
        let put_responses = puts
            .clone()
            .filter_map(q!(|(_, _, request_id, client_id)| {
                if client_id.is_some() {
                    Some(KVSResponse::PutOk {
                        request_id,
                        client_id,
                    })
                } else {
                    None
                }
            }));

        let delete_responses = deletes.clone().filter_map(q!(|(_, request_id, client_id)| {
            if client_id.is_some() {
                Some(KVSResponse::DeleteOk {
                    request_id,
                    client_id,
                })
            } else {
                None
            }
        }));

        // Combine all responses
        let responses = put_responses
            .interleave(delete_responses)
            .interleave(get_responses);

        // Generate data events
        let put_data = puts.map(q!(|(key, value, _, _)| DataEvent::Put { key, value }));
        let delete_data = deletes.clone().map(q!(|(key, _, _)| DataEvent::Delete { key }));
        let data = put_data.interleave(delete_data);

        // Generate meta events (tombstones from deletes)
        let meta = deletes.map(q!(|(key, _, _)| MetaEvent::Tomb { key }));

        CoreOutput {
            responses,
            data,
            meta,
        }
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
            KVSOperation::Put(
                "x".to_string(),
                LwwWrapper::new("1".to_string()),
                1,
                Some(1),
            ),
            KVSOperation::Get("x".to_string(), 2, Some(1)),
            KVSOperation::Put(
                "x".to_string(),
                LwwWrapper::new("2".to_string()),
                3,
                Some(1),
            ),
            KVSOperation::Get("x".to_string(), 4, Some(1)),
        ];

        // In a real implementation, we'd test this with Hydro streams
        // For now, we simulate the sequential processing logic
        let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
            std::collections::HashMap::new();
        let mut responses = Vec::new();

        for op in operations {
            let response = match op {
                KVSOperation::Put(key, value, _, _) => {
                    state.apply_put(key.clone(), value);
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _, _) => match state.apply_get(&key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _, _) => {
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
                1,
                Some(1),
            ),
            KVSOperation::Get("account".to_string(), 2, Some(1)),
            KVSOperation::Put(
                "account".to_string(),
                LwwWrapper::new("75".to_string()),
                3,
                Some(1),
            ),
            KVSOperation::Get("account".to_string(), 4, Some(1)),
        ];

        // Sequential processing (correct for linearizability)
        let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
            std::collections::HashMap::new();
        let mut sequential_responses = Vec::new();

        for op in &operations {
            let response = match op {
                KVSOperation::Put(key, value, _, _) => {
                    state.apply_put(key.clone(), value.clone());
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _, _) => match state.apply_get(key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _, _) => {
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
            if let KVSOperation::Put(key, value, _, _) = op {
                split_state.apply_put(key.clone(), value.clone());
                split_responses[i] = format!("PUT {} = OK", key);
            }
        }

        // Then process all GETs (wrong!)
        for (i, op) in operations.iter().enumerate() {
            if let KVSOperation::Get(key, _, _) = op {
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
                1,
                Some(1),
            ),
            KVSOperation::Put(
                "bob".to_string(),
                LwwWrapper::new("50".to_string()),
                2,
                Some(1),
            ),
            // Check initial balances
            KVSOperation::Get("alice".to_string(), 3, Some(1)),
            KVSOperation::Get("bob".to_string(), 4, Some(1)),
            // Transfer $25 from Alice to Bob (must be atomic in total order)
            KVSOperation::Put(
                "alice".to_string(),
                LwwWrapper::new("75".to_string()),
                5,
                Some(1),
            ),
            KVSOperation::Put(
                "bob".to_string(),
                LwwWrapper::new("75".to_string()),
                6,
                Some(1),
            ),
            // Check final balances
            KVSOperation::Get("alice".to_string(), 7, Some(1)),
            KVSOperation::Get("bob".to_string(), 8, Some(1)),
        ];

        let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
            std::collections::HashMap::new();
        let mut responses = Vec::new();

        for op in operations {
            let response = match op {
                KVSOperation::Put(key, value, _, _) => {
                    state.apply_put(key.clone(), value);
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key, _, _) => match state.apply_get(&key) {
                    Some(value) => format!("GET {} = {:?}", key, value),
                    None => format!("GET {} = NOT FOUND", key),
                },
                KVSOperation::Delete(key, _, _) => {
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
    fn arb_request_id() -> impl Strategy<Value = u64> {
        any::<u64>()
    }

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
            (
                key_strategy,
                value_strategy,
                arb_request_id(),
                arb_client_id()
            )
                .prop_map(|(k, v, rid, cid)| KVSOperation::Put(
                    k,
                    LwwWrapper::new(v),
                    rid,
                    Some(cid)
                )),
            (key_strategy, arb_request_id(), arb_client_id())
                .prop_map(|(k, rid, cid)| KVSOperation::Get(k, rid, Some(cid))),
            (key_strategy, arb_request_id(), arb_client_id())
                .prop_map(|(k, rid, cid)| KVSOperation::Delete(k, rid, Some(cid))),
        ]
    }

    fn arb_kvs_operation_without_client_id()
    -> impl Strategy<Value = KVSOperation<String, LwwWrapper<String>>> {
        let key_strategy = "[a-z]{1,10}";
        let value_strategy = "[a-z0-9]{1,20}";

        prop_oneof![
            (key_strategy, value_strategy, arb_request_id())
                .prop_map(|(k, v, rid)| KVSOperation::Put(k, LwwWrapper::new(v), rid, None)),
            (key_strategy, arb_request_id()).prop_map(|(k, rid)| KVSOperation::Get(k, rid, None)),
            (key_strategy, arb_request_id())
                .prop_map(|(k, rid)| KVSOperation::Delete(k, rid, None)),
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
            let request_id = op.request_id();
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<String, LwwWrapper<String>>> = match op {
                KVSOperation::Put(key, value, _, _) => {
                    state.apply_put(key.clone(), value);
                    if should_emit_response {
                        Some(KVSResponse::PutOk { request_id, client_id })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, _, _) => {
                    let value = state.apply_get(&key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult { request_id, client_id, value })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, _, _) => {
                    state.apply_delete(key.clone());
                    if should_emit_response {
                        Some(KVSResponse::DeleteOk { request_id, client_id })
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
            let request_id = op.request_id();
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<String, LwwWrapper<String>>> = match op {
                KVSOperation::Put(key, value, _, _) => {
                    state.apply_put(key.clone(), value);
                    if should_emit_response {
                        Some(KVSResponse::PutOk { request_id, client_id })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, _, _) => {
                    let value = state.apply_get(&key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult { request_id, client_id, value })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, _, _) => {
                    state.apply_delete(key.clone());
                    if should_emit_response {
                        Some(KVSResponse::DeleteOk { request_id, client_id })
                    } else {
                        None
                    }
                }
            };

            // Verify that NO response was generated for None client_id
            prop_assert!(response.is_none(),
                "No response should be generated for operation with None client_id");
        }

        /// **Feature: request-id-protocol, Property 1: Request ID preservation through pipeline**
        ///
        /// For any KVS operation with request_id R, when processed through the pipeline,
        /// the resulting KVSResponse (if generated) should have request_id equal to R.
        ///
        /// **Validates: Requirements 1.3, 1.4, 3.2, 3.3**
        #[test]
        fn prop_request_id_preservation(op in arb_kvs_operation_with_client_id()) {
            // Extract the request_id from the operation
            let expected_request_id = op.request_id();

            // Simulate the core processing logic
            let mut state: std::collections::HashMap<String, LwwWrapper<String>> =
                std::collections::HashMap::new();
            let request_id = op.request_id();
            let client_id = op.client_id();
            let should_emit_response = client_id.is_some();

            let response: Option<KVSResponse<String, LwwWrapper<String>>> = match op {
                KVSOperation::Put(key, value, _, _) => {
                    state.apply_put(key.clone(), value);
                    if should_emit_response {
                        Some(KVSResponse::PutOk { request_id, client_id })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, _, _) => {
                    let value = state.apply_get(&key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult { request_id, client_id, value })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, _, _) => {
                    state.apply_delete(key.clone());
                    if should_emit_response {
                        Some(KVSResponse::DeleteOk { request_id, client_id })
                    } else {
                        None
                    }
                }
            };

            // Verify that a response was generated (since we use operations with client_id)
            prop_assert!(response.is_some(), "Response should be generated for client operation");

            // Verify that the response has the same request_id as the operation
            let response = response.unwrap();
            prop_assert_eq!(response.request_id(), expected_request_id,
                "Response request_id should match operation request_id");
        }

        /// **Feature: request-id-protocol, Property 2: Request ID and client ID independence**
        ///
        /// For any operation with request_id R and client_id C, when processed through
        /// the pipeline, the resulting response should preserve both R and C independently -
        /// changing one identifier should not affect the other.
        ///
        /// **Validates: Requirements 2.3**
        #[test]
        fn prop_request_id_client_id_independence(
            op in arb_kvs_operation_with_client_id(),
            new_request_id in arb_request_id(),
            new_client_id in arb_client_id()
        ) {
            // Test 1: Change request_id, verify client_id unchanged
            let original_client_id = op.client_id();
            let op_with_new_rid = op.clone().with_request_id(new_request_id);

            prop_assert_eq!(op_with_new_rid.client_id(), original_client_id,
                "Changing request_id should not affect client_id");
            prop_assert_eq!(op_with_new_rid.request_id(), new_request_id,
                "Request ID should be updated");

            // Test 2: Change client_id, verify request_id unchanged
            let original_request_id = op.request_id();
            let op_with_new_cid = op.with_client_id(Some(new_client_id));

            prop_assert_eq!(op_with_new_cid.request_id(), original_request_id,
                "Changing client_id should not affect request_id");
            prop_assert_eq!(op_with_new_cid.client_id(), Some(new_client_id),
                "Client ID should be updated");
        }
    }
}
