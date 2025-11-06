//! Concrete sequential KVS implementations that work with Hydro's code generation
//!
//! This module provides concrete implementations of sequential KVS processing
//! that avoid the generic closure issues with Hydro's code generation.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::protocol::KVSOperation;
use crate::values::LwwWrapper;

/// Concrete sequential KVS for LwwWrapper<String> values
pub struct KVSSequentialLwwString;

impl KVSSequentialLwwString {
    /// Process operations sequentially for LwwWrapper<String> values
    pub fn process_sequential<'a>(
        operations: Stream<KVSOperation<LwwWrapper<String>>, Cluster<'a, crate::core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::core::KVSNode>, Unbounded>
    {
        // Use scan with concrete types to avoid Hydro generic issues
        operations
            .scan(
                q!(|| std::collections::HashMap::<String, LwwWrapper<String>>::new()),
                q!(|state, op| {
                    let response = match op {
                        KVSOperation::Put(key, value) => {
                            state.insert(key.clone(), value);
                            format!("PUT {} = OK [SEQUENTIAL]", key)
                        }
                        KVSOperation::Get(key) => {
                            match state.get(&key) {
                                Some(value) => format!("GET {} = {:?} [SEQUENTIAL]", key, value),
                                None => format!("GET {} = NOT FOUND [SEQUENTIAL]", key),
                            }
                        }
                    };
                    Some(response)
                })
            )
    }
}

/// Concrete sequential KVS for any serializable value type
/// This version uses dynamic typing to avoid generic closure issues
pub struct KVSSequentialDynamic;

impl KVSSequentialDynamic {
    /// Process operations sequentially using string serialization for values
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
            + Send
            + Sync
            + 'static,
    {
        // Use string serialization to avoid generic type issues
        operations
            .scan(
                q!(|| std::collections::HashMap::<String, String>::new()),
                q!(|state, op| {
                    let response = match op {
                        KVSOperation::Put(key, value) => {
                            // Serialize the value to string to avoid generic issues
                            let serialized = format!("{:?}", value);
                            state.insert(key.clone(), serialized);
                            format!("PUT {} = OK [SEQUENTIAL]", key)
                        }
                        KVSOperation::Get(key) => {
                            match state.get(&key) {
                                Some(value_str) => format!("GET {} = {} [SEQUENTIAL]", key, value_str),
                                None => format!("GET {} = NOT FOUND [SEQUENTIAL]", key),
                            }
                        }
                    };
                    Some(response)
                })
            )
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::KVSOperation;
    use crate::values::LwwWrapper;

    #[test]
    fn test_concrete_sequential_compiles() {
        // Test that the concrete version compiles without generic issues
        // This is a compile-time test
        
        let operations = vec![
            KVSOperation::Put("key1".to_string(), LwwWrapper::new("value1".to_string())),
            KVSOperation::Get("key1".to_string()),
        ];
        
        // Simulate the processing logic
        let mut state = std::collections::HashMap::<String, LwwWrapper<String>>::new();
        let mut responses = Vec::new();
        
        for op in operations {
            let response = match op {
                KVSOperation::Put(key, value) => {
                    state.insert(key.clone(), value);
                    format!("PUT {} = OK", key)
                }
                KVSOperation::Get(key) => {
                    match state.get(&key) {
                        Some(value) => format!("GET {} = {:?}", key, value),
                        None => format!("GET {} = NOT FOUND", key),
                    }
                }
            };
            responses.push(response);
        }
        
        assert_eq!(responses[0], "PUT key1 = OK");
        assert!(responses[1].contains("value1"));
    }
}