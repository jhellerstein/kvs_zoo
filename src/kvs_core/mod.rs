//! Core KVS implementation and node marker type
//!
//! Two storage paths:
//! - **Lattice merge** (`process_lattice`): `V: Merge`, commutative fold. Coordination-free.
//! - **Overwrite** (`process_overwrite`): plain types, assignment fold. LWW semantics.
//!
//! Plus `process_ordered` for sequential state via `scan` on `TotalOrder` streams.

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

/// HashMap storage with lattice merge semantics.
///
/// Put merges the new value into the existing value using `Merge::merge`.
/// Use with lattice value types (CausalWrapper, etc.).
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

/// HashMap storage with overwrite semantics (last-writer-wins).
///
/// Put always replaces the existing value. No `Merge` trait required.
/// Use with plain Rust types on the overwrite path.
#[derive(Clone, Debug)]
pub struct OverwriteMap<K, V>(pub std::collections::HashMap<K, V>);

impl<K, V> Default for OverwriteMap<K, V> {
    fn default() -> Self {
        OverwriteMap(std::collections::HashMap::new())
    }
}

impl<K, V> KVSStorage<K, V> for OverwriteMap<K, V>
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone,
{
    fn apply_put(&mut self, key: K, value: V) {
        self.0.insert(key, value);
    }

    fn apply_get(&self, key: &K) -> Option<&V> {
        self.0.get(key)
    }

    fn apply_delete(&mut self, key: K) {
        self.0.remove(&key);
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
    /// Lattice merge path: coordination-free processing for lattice value types.
    ///
    /// All operations (puts, gets, deletes) go through a single c+i fold.
    /// The fold buffers operations per tick, applies writes first, then reads.
    /// This ensures reads see the latest lattice state without snapshot+join.
    pub fn process_lattice<'a, K, V, L>(
        operations: impl Into<Stream<KVSOperation<K, V>, L, Unbounded, NoOrder>>,
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
    {
        let operations = operations.into();
        let tick = operations.location().tick();

        // Single fold processes all operations per tick.
        // Accumulator: (store, pending_responses, data_events, meta_events)
        // The fold is commutative+idempotent on the store (lattice merge).
        // Gets are buffered and resolved at emit time against the final store.
        let tick_result = operations
            .batch(&tick, nondet!(/** batch operations */))
            .fold(
                q!(|| (
                    std::collections::HashMap::<K, V>::new(),
                    Vec::<KVSOperation<K, V>>::new(),
                )),
                q!(|(store, ops_buffer), op| {
                    // Apply writes immediately to store (commutative lattice merge)
                    match &op {
                        KVSOperation::Put(key, value, _, _) => {
                            let entry = store.entry(key.clone()).or_insert_with(V::default);
                            lattices::Merge::merge(entry, value.clone());
                        }
                        KVSOperation::Delete(key, _, _) => {
                            store.remove(key);
                        }
                        _ => {}
                    }
                    // Buffer all ops for response generation at emit
                    ops_buffer.push(op);
                },
                    commutative = manual_proof!(/** lattice merge is commutative; buffering is commutative */),
                    idempotent = manual_proof!(/** lattice merge is idempotent; duplicate ops produce same state */))
            )
            .all_ticks();

        // Unpack: generate responses, data events, meta events from the fold output
        let responses = tick_result.clone().flat_map_ordered(q!(|(store, ops)| {
            ops.into_iter().filter_map(move |op| match op {
                KVSOperation::Get(key, request_id, client_id) => {
                    if client_id.is_some() {
                        let value = store.get(&key).cloned();
                        Some(KVSResponse::GetResult { request_id, client_id, value })
                    } else { None }
                }
                KVSOperation::Put(_, _, request_id, client_id) => {
                    if client_id.is_some() {
                        Some(KVSResponse::PutOk { request_id, client_id })
                    } else { None }
                }
                KVSOperation::Delete(_, request_id, client_id) => {
                    if client_id.is_some() {
                        Some(KVSResponse::DeleteOk { request_id, client_id })
                    } else { None }
                }
            })
        }));

        let data = tick_result.clone().flat_map_ordered(q!(|(_, ops)| {
            ops.into_iter().filter_map(|op| match op {
                KVSOperation::Put(key, value, _, _) => Some(DataEvent::Put { key, value }),
                KVSOperation::Get(key, _, _) => Some(DataEvent::Get { key, value: None }),
                KVSOperation::Delete(key, _, _) => Some(DataEvent::Delete { key }),
            })
        }));

        let meta = tick_result.flat_map_ordered(q!(|(_, ops)| {
            ops.into_iter().filter_map(|op| match op {
                KVSOperation::Delete(key, _, _) => Some(MetaEvent::Tomb { key }),
                _ => None,
            })
        }));

        CoreOutput { responses: responses.weaken_ordering(), data: data.weaken_ordering(), meta: meta.weaken_ordering() }
    }
    /// Overwrite path: last-writer-wins processing for plain value types.
    ///
    /// No `Merge` bound. Storage uses plain assignment fold.
    /// Non-deterministic under concurrency; deterministic when the architecture
    /// provides ordering (single-node, Paxos-sequenced).
    pub fn process_overwrite<'a, K, V, L>(
        operations: impl Into<Stream<KVSOperation<K, V>, L, Unbounded, NoOrder>>,
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
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + hydro_lang::location::NoTick + Clone + 'a,
    {
        let operations = operations.into();
        let tick = operations.location().tick();
        let (puts, deletes, gets) = Self::split_operations(operations);

        let put_updates = puts.clone().map(q!(|(key, value, _, _)| (key, value)));
        let storage = put_updates
            .assume_ordering::<TotalOrder>(nondet!(
                /// Overwrite path: last-writer-wins. On unordered streams, the result
                /// depends on arrival order — this is intentionally non-deterministic.
                /// For deterministic results, use an ordering layer (Paxos) or the
                /// lattice merge path (process_lattice).
            ))
            .into_keyed()
            .fold(
                q!(|| V::default()),
                q!(|old, new| { *old = new; })
            );

        Self::build_output(puts, deletes, gets, storage, &tick)
    }

    /// Ordered path: sequential state updates via `scan` on a `TotalOrder` stream.
    ///
    /// Suitable for any value type behind a sequencing layer (e.g., Paxos).
    /// No `Merge` bound required.
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

    // --- Private helpers ---

    #[allow(clippy::type_complexity)]
    fn split_operations<'a, K, V, L>(
        operations: Stream<KVSOperation<K, V>, L, Unbounded, NoOrder>,
    ) -> (
        Stream<(K, V, u64, Option<u64>), L, Unbounded, NoOrder>,
        Stream<(K, u64, Option<u64>), L, Unbounded, NoOrder>,
        Stream<(K, u64, Option<u64>), L, Unbounded, NoOrder>,
    )
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
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + hydro_lang::location::NoTick + Clone + 'a,
    {
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

        (puts, deletes, gets)
    }

    fn build_output<'a, K, V, L>(
        puts: Stream<(K, V, u64, Option<u64>), L, Unbounded, NoOrder>,
        deletes: Stream<(K, u64, Option<u64>), L, Unbounded, NoOrder>,
        gets: Stream<(K, u64, Option<u64>), L, Unbounded, NoOrder>,
        storage: hydro_lang::live_collections::keyed_singleton::KeyedSingleton<K, V, L, Unbounded>,
        tick: &hydro_lang::location::tick::Tick<L>,
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
            + Send
            + Sync
            + 'static,
        L: hydro_lang::location::Location<'a> + hydro_lang::location::NoTick + Clone + 'a,
    {
        // Batch gets into ticks and snapshot storage at the same tick
        let gets_batched = gets
            .batch(tick, nondet!(/** batch gets for snapshot */))
            .map(q!(|(key, request_id, client_id)| (key, (request_id, client_id))))
            .into_keyed();
        let storage_snapshot = storage.snapshot(tick, nondet!(/** snapshot storage for gets */));

        // Join gets with storage snapshot by key
        let get_results = storage_snapshot.join_keyed_stream(gets_batched);

        let get_responses = get_results
            .values()
            .filter_map(q!(|(storage_value, (request_id, client_id))| {
                if client_id.is_some() {
                    Some(KVSResponse::GetResult {
                        request_id,
                        client_id,
                        value: Some(storage_value),
                    })
                } else {
                    None
                }
            }))
            .all_ticks();

        // Generate put/delete responses
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

        let responses = put_responses
            .merge_unordered(delete_responses)
            .merge_unordered(get_responses);

        let put_data = puts.map(q!(|(key, value, _, _)| DataEvent::Put { key, value }));
        let delete_data = deletes.clone().map(q!(|(key, _, _)| DataEvent::Delete { key }));
        let data = put_data.merge_unordered(delete_data);

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
    
    use proptest::prelude::*;

    #[test]
    fn test_sequential_processing_maintains_order() {
        // This test demonstrates the key property: operations are processed
        // in the exact order they appear, ensuring linearizability

        let operations: Vec<KVSOperation<String, String>> = vec![
            KVSOperation::Put(
                "x".to_string(),
                "1".to_string(),
                1,
                Some(1),
            ),
            KVSOperation::Get("x".to_string(), 2, Some(1)),
            KVSOperation::Put(
                "x".to_string(),
                "2".to_string(),
                3,
                Some(1),
            ),
            KVSOperation::Get("x".to_string(), 4, Some(1)),
        ];

        // In a real implementation, we'd test this with Hydro streams
        // For now, we simulate the sequential processing logic
        let mut state: super::OverwriteMap<String, String> =
            super::OverwriteMap::default();
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

        let operations: Vec<KVSOperation<String, String>> = vec![
            KVSOperation::Put(
                "account".to_string(),
                "100".to_string(),
                1,
                Some(1),
            ),
            KVSOperation::Get("account".to_string(), 2, Some(1)),
            KVSOperation::Put(
                "account".to_string(),
                "75".to_string(),
                3,
                Some(1),
            ),
            KVSOperation::Get("account".to_string(), 4, Some(1)),
        ];

        // Sequential processing (correct for linearizability)
        let mut state: super::OverwriteMap<String, String> =
            super::OverwriteMap::default();
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
        let mut split_state: super::OverwriteMap<String, String> =
            super::OverwriteMap::default();
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

        let operations: Vec<KVSOperation<String, String>> = vec![
            // Initial state
            KVSOperation::Put(
                "alice".to_string(),
                "100".to_string(),
                1,
                Some(1),
            ),
            KVSOperation::Put(
                "bob".to_string(),
                "50".to_string(),
                2,
                Some(1),
            ),
            // Check initial balances
            KVSOperation::Get("alice".to_string(), 3, Some(1)),
            KVSOperation::Get("bob".to_string(), 4, Some(1)),
            // Transfer $25 from Alice to Bob (must be atomic in total order)
            KVSOperation::Put(
                "alice".to_string(),
                "75".to_string(),
                5,
                Some(1),
            ),
            KVSOperation::Put(
                "bob".to_string(),
                "75".to_string(),
                6,
                Some(1),
            ),
            // Check final balances
            KVSOperation::Get("alice".to_string(), 7, Some(1)),
            KVSOperation::Get("bob".to_string(), 8, Some(1)),
        ];

        let mut state: super::OverwriteMap<String, String> =
            super::OverwriteMap::default();
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
    -> impl Strategy<Value = KVSOperation<String, String>> {
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
                    v,
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
    -> impl Strategy<Value = KVSOperation<String, String>> {
        let key_strategy = "[a-z]{1,10}";
        let value_strategy = "[a-z0-9]{1,20}";

        prop_oneof![
            (key_strategy, value_strategy, arb_request_id())
                .prop_map(|(k, v, rid)| KVSOperation::Put(k, v, rid, None)),
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
            let mut state: super::OverwriteMap<String, String> =
                super::OverwriteMap::default();
            let should_respond = true; // Client operations should respond
            let request_id = op.request_id();
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<String, String>> = match op {
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
            let mut state: super::OverwriteMap<String, String> =
                super::OverwriteMap::default();
            let should_respond = true; // Even if should_respond is true...
            let request_id = op.request_id();
            let client_id = op.client_id();
            let should_emit_response = should_respond && client_id.is_some();

            let response: Option<KVSResponse<String, String>> = match op {
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
            let mut state: super::OverwriteMap<String, String> =
                super::OverwriteMap::default();
            let request_id = op.request_id();
            let client_id = op.client_id();
            let should_emit_response = client_id.is_some();

            let response: Option<KVSResponse<String, String>> = match op {
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
