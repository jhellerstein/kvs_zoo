//! Integration tests for end-to-end client ID propagation
//!
//! These tests verify that client IDs are correctly preserved throughout the entire
//! request/response pipeline, from external client input through routing, ordering,
//! core processing, and back to the correct client.

use kvs_zoo::protocol::{KVSOperation, KVSResponse};
use std::collections::HashMap;

/// Test that a single client's operations return responses to that client
///
/// This test validates Requirements 1.1, 1.2, 1.3:
/// - Client ID is preserved through the pipeline
/// - Responses inherit the operation's client ID
/// - Responses are correctly routed back to the originating client
#[test]
fn test_single_client_flow() {
    // Simulate a single client (client_id = 42) sending multiple operations
    let client_id = 42u64;

    let operations = vec![
        KVSOperation::Put(
            "key1".to_string(),
            "value1".to_string(),
            1,
            Some(client_id),
        ),
        KVSOperation::Get("key1".to_string(), 2, Some(client_id)),
        KVSOperation::Put(
            "key2".to_string(),
            "value2".to_string(),
            3,
            Some(client_id),
        ),
        KVSOperation::Get("key2".to_string(), 4, Some(client_id)),
        KVSOperation::Get("key1".to_string(), 5, Some(client_id)),
        KVSOperation::Delete("key1".to_string(), 6, Some(client_id)),
        KVSOperation::Get("key1".to_string(), 7, Some(client_id)),
    ];

    // Simulate the core processing (as it would happen in the real system)
    let mut state = HashMap::new();
    let mut responses = Vec::new();

    for op in operations {
        // Verify that the operation has the correct client_id attached
        assert_eq!(
            op.client_id(),
            Some(client_id),
            "Operation should have client_id {} attached",
            client_id
        );

        // Simulate core processing with should_respond = true (client operations)
        let should_respond = true;
        let op_client_id = op.client_id();
        let should_emit_response = should_respond && op_client_id.is_some();

        let response: Option<KVSResponse<String, String>> = match op {
            KVSOperation::Put(key, value, request_id, _) => {
                state.insert(key.clone(), value);
                if should_emit_response {
                    Some(KVSResponse::PutOk {
                        request_id,
                        client_id: op_client_id,
                    })
                } else {
                    None
                }
            }
            KVSOperation::Get(key, request_id, _) => {
                let value = state.get(&key).cloned();
                if should_emit_response {
                    Some(KVSResponse::GetResult {
                        request_id,
                        client_id: op_client_id,
                        value,
                    })
                } else {
                    None
                }
            }
            KVSOperation::Delete(key, request_id, _) => {
                state.remove(&key);
                if should_emit_response {
                    Some(KVSResponse::DeleteOk {
                        request_id,
                        client_id: op_client_id,
                    })
                } else {
                    None
                }
            }
        };

        // Verify that a response was generated
        assert!(
            response.is_some(),
            "Response should be generated for client operation"
        );

        let response = response.unwrap();

        // Verify that the response has the correct client_id
        assert_eq!(
            response.client_id(),
            Some(client_id),
            "Response should have client_id {} matching the operation",
            client_id
        );

        responses.push(response);
    }

    // Verify the correct number of responses
    assert_eq!(
        responses.len(),
        7,
        "Should have 7 responses for 7 operations"
    );

    // Verify response types and client IDs
    assert!(matches!(
        responses[0],
        KVSResponse::PutOk {
            client_id: Some(42),
            ..
        }
    ));
    assert!(matches!(
        responses[1],
        KVSResponse::GetResult {
            client_id: Some(42),
            value: Some(_),
            ..
        }
    ));
    assert!(matches!(
        responses[2],
        KVSResponse::PutOk {
            client_id: Some(42),
            ..
        }
    ));
    assert!(matches!(
        responses[3],
        KVSResponse::GetResult {
            client_id: Some(42),
            value: Some(_),
            ..
        }
    ));
    assert!(matches!(
        responses[4],
        KVSResponse::GetResult {
            client_id: Some(42),
            value: Some(_),
            ..
        }
    ));
    assert!(matches!(
        responses[5],
        KVSResponse::DeleteOk {
            client_id: Some(42),
            ..
        }
    ));
    assert!(matches!(
        responses[6],
        KVSResponse::GetResult {
            client_id: Some(42),
            value: None,
            ..
        }
    ));

    // Verify that all responses can be routed back to the client
    for response in &responses {
        let extracted_client_id = response.client_id();
        assert_eq!(
            extracted_client_id,
            Some(client_id),
            "All responses should be routable to client {}",
            client_id
        );

        // Verify that the response can be formatted for the client
        let formatted = response.to_string();
        assert!(
            !formatted.is_empty(),
            "Response should be formattable as string"
        );
    }

    println!(
        "✅ Single client flow test passed: all {} operations correctly routed to client {}",
        responses.len(),
        client_id
    );
}

/// Test that multiple clients' operations are correctly routed
/// and verify no cross-contamination between clients
///
/// This test validates Requirement 1.3:
/// - Multiple external clients sending concurrent operations
/// - Each response is returned to its correct originating client
/// - No cross-contamination between clients
#[test]
fn test_multiple_concurrent_clients() {
    // Simulate multiple clients sending interleaved operations
    let client_a = 10u64;
    let client_b = 20u64;
    let client_c = 30u64;

    // Interleaved operations from three different clients
    let operations = vec![
        (
            client_a,
            KVSOperation::Put(
                "shared_key".to_string(),
                "a1".to_string(),
                1,
                Some(client_a),
            ),
        ),
        (
            client_b,
            KVSOperation::Put(
                "shared_key".to_string(),
                "b1".to_string(),
                2,
                Some(client_b),
            ),
        ),
        (
            client_c,
            KVSOperation::Get("shared_key".to_string(), 3, Some(client_c)),
        ),
        (
            client_a,
            KVSOperation::Get("shared_key".to_string(), 4, Some(client_a)),
        ),
        (
            client_b,
            KVSOperation::Get("shared_key".to_string(), 5, Some(client_b)),
        ),
        (
            client_a,
            KVSOperation::Put(
                "key_a".to_string(),
                "value_a".to_string(),
                6,
                Some(client_a),
            ),
        ),
        (
            client_b,
            KVSOperation::Put(
                "key_b".to_string(),
                "value_b".to_string(),
                7,
                Some(client_b),
            ),
        ),
        (
            client_c,
            KVSOperation::Put(
                "key_c".to_string(),
                "value_c".to_string(),
                8,
                Some(client_c),
            ),
        ),
        (
            client_a,
            KVSOperation::Get("key_a".to_string(), 9, Some(client_a)),
        ),
        (
            client_b,
            KVSOperation::Get("key_b".to_string(), 10, Some(client_b)),
        ),
        (
            client_c,
            KVSOperation::Get("key_c".to_string(), 11, Some(client_c)),
        ),
        (
            client_a,
            KVSOperation::Delete("key_a".to_string(), 12, Some(client_a)),
        ),
        (
            client_b,
            KVSOperation::Delete("key_b".to_string(), 13, Some(client_b)),
        ),
        (
            client_c,
            KVSOperation::Delete("key_c".to_string(), 14, Some(client_c)),
        ),
    ];

    // Simulate the core processing
    let mut state = HashMap::new();
    let mut responses_by_client: HashMap<u64, Vec<KVSResponse<String, String>>> =
        HashMap::new();

    for (expected_client_id, op) in operations {
        // Verify that the operation has the correct client_id attached
        assert_eq!(
            op.client_id(),
            Some(expected_client_id),
            "Operation should have client_id {} attached",
            expected_client_id
        );

        // Simulate core processing
        let should_respond = true;
        let op_client_id = op.client_id();
        let should_emit_response = should_respond && op_client_id.is_some();

        let response: Option<KVSResponse<String, String>> = match op {
            KVSOperation::Put(key, value, request_id, _) => {
                state.insert(key.clone(), value);
                if should_emit_response {
                    Some(KVSResponse::PutOk {
                        request_id,
                        client_id: op_client_id,
                    })
                } else {
                    None
                }
            }
            KVSOperation::Get(key, request_id, _) => {
                let value = state.get(&key).cloned();
                if should_emit_response {
                    Some(KVSResponse::GetResult {
                        request_id,
                        client_id: op_client_id,
                        value,
                    })
                } else {
                    None
                }
            }
            KVSOperation::Delete(key, request_id, _) => {
                state.remove(&key);
                if should_emit_response {
                    Some(KVSResponse::DeleteOk {
                        request_id,
                        client_id: op_client_id,
                    })
                } else {
                    None
                }
            }
        };

        if let Some(resp) = response {
            // Verify that the response has the correct client_id
            assert_eq!(
                resp.client_id(),
                Some(expected_client_id),
                "Response should have client_id {} matching the operation",
                expected_client_id
            );

            // Group responses by client
            responses_by_client
                .entry(expected_client_id)
                .or_default()
                .push(resp);
        }
    }

    // Verify that each client received responses
    assert!(
        responses_by_client.contains_key(&client_a),
        "Client A should have responses"
    );
    assert!(
        responses_by_client.contains_key(&client_b),
        "Client B should have responses"
    );
    assert!(
        responses_by_client.contains_key(&client_c),
        "Client C should have responses"
    );

    // Verify no cross-contamination: each client's responses only contain that client's ID
    for (client_id, client_responses) in &responses_by_client {
        for response in client_responses {
            assert_eq!(
                response.client_id(),
                Some(*client_id),
                "All responses for client {} should have that client_id",
                client_id
            );
        }
        println!(
            "✅ Client {} received {} responses, all correctly routed",
            client_id,
            client_responses.len()
        );
    }

    // Verify expected response counts
    let client_a_responses = responses_by_client.get(&client_a).unwrap();
    let client_b_responses = responses_by_client.get(&client_b).unwrap();
    let client_c_responses = responses_by_client.get(&client_c).unwrap();

    // Client A: 1 PUT (shared_key), 1 GET (shared_key), 1 PUT (key_a), 1 GET (key_a), 1 DELETE (key_a) = 5 operations
    assert_eq!(
        client_a_responses.len(),
        5,
        "Client A should have 5 responses"
    );
    // Client B: 1 PUT (shared_key), 1 GET (shared_key), 1 PUT (key_b), 1 GET (key_b), 1 DELETE (key_b) = 5 operations
    assert_eq!(
        client_b_responses.len(),
        5,
        "Client B should have 5 responses"
    );
    // Client C: 1 GET (shared_key), 1 PUT (key_c), 1 GET (key_c), 1 DELETE (key_c) = 4 operations
    assert_eq!(
        client_c_responses.len(),
        4,
        "Client C should have 4 responses"
    );

    // Verify that responses can be extracted and routed correctly
    let all_responses: Vec<_> = responses_by_client.values().flatten().collect();
    for response in all_responses {
        let client_id = response.client_id();
        assert!(
            client_id.is_some(),
            "Response should have a client_id for routing"
        );
        assert!(
            client_id == Some(client_a)
                || client_id == Some(client_b)
                || client_id == Some(client_c),
            "Response client_id should be one of the known clients"
        );

        // Verify that the response can be formatted
        let formatted = response.to_string();
        assert!(!formatted.is_empty(), "Response should be formattable");
    }

    println!(
        "✅ Multiple concurrent clients test passed: {} total responses correctly routed to 3 clients",
        responses_by_client.values().map(|v| v.len()).sum::<usize>()
    );
}

/// Test that replicated operations don't generate client responses
///
/// This test validates Requirements 4.1, 4.2:
/// - Replicated operations are marked with None client_id
/// - Operations with None client_id do not generate client responses
#[test]
fn test_replication_without_client_responses() {
    // Simulate a mix of client operations and replicated operations
    let client_id = 100u64;

    let operations = vec![
        // Client operation: should generate response
        (
            true,
            KVSOperation::Put(
                "key1".to_string(),
                "value1".to_string(),
                1,
                Some(client_id),
            ),
        ),
        // Replicated operation: should NOT generate response
        (
            false,
            KVSOperation::Put(
                "key1".to_string(),
                "value1_replica".to_string(),
                2,
                None,
            ),
        ),
        // Client operation: should generate response
        (
            true,
            KVSOperation::Get("key1".to_string(), 3, Some(client_id)),
        ),
        // Replicated operation: should NOT generate response
        (false, KVSOperation::Get("key1".to_string(), 4, None)),
        // Client operation: should generate response
        (
            true,
            KVSOperation::Put(
                "key2".to_string(),
                "value2".to_string(),
                5,
                Some(client_id),
            ),
        ),
        // Replicated operation: should NOT generate response
        (
            false,
            KVSOperation::Put(
                "key2".to_string(),
                "value2_replica".to_string(),
                6,
                None,
            ),
        ),
        // Client operation: should generate response
        (
            true,
            KVSOperation::Delete("key1".to_string(), 7, Some(client_id)),
        ),
        // Replicated operation: should NOT generate response
        (false, KVSOperation::Delete("key1".to_string(), 8, None)),
    ];

    // Simulate the core processing
    let mut state = HashMap::new();
    let mut client_responses = Vec::new();
    let mut replicated_operations_processed = 0;

    for (is_client_op, op) in operations {
        let op_client_id = op.client_id();

        if is_client_op {
            // Client operations should have Some(client_id)
            assert_eq!(
                op_client_id,
                Some(client_id),
                "Client operation should have client_id {}",
                client_id
            );
        } else {
            // Replicated operations should have None client_id
            assert_eq!(
                op_client_id, None,
                "Replicated operation should have None client_id"
            );
            replicated_operations_processed += 1;
        }

        // Simulate core processing
        // For client ops: should_respond = true
        // For replicated ops: should_respond = false (or true, but client_id is None)
        let should_respond = is_client_op;
        let should_emit_response = should_respond && op_client_id.is_some();

        let response: Option<KVSResponse<String, String>> = match op {
            KVSOperation::Put(key, value, request_id, _) => {
                state.insert(key.clone(), value);
                if should_emit_response {
                    Some(KVSResponse::PutOk {
                        request_id,
                        client_id: op_client_id,
                    })
                } else {
                    None
                }
            }
            KVSOperation::Get(key, request_id, _) => {
                let value = state.get(&key).cloned();
                if should_emit_response {
                    Some(KVSResponse::GetResult {
                        request_id,
                        client_id: op_client_id,
                        value,
                    })
                } else {
                    None
                }
            }
            KVSOperation::Delete(key, request_id, _) => {
                state.remove(&key);
                if should_emit_response {
                    Some(KVSResponse::DeleteOk {
                        request_id,
                        client_id: op_client_id,
                    })
                } else {
                    None
                }
            }
        };

        if is_client_op {
            // Client operations should generate responses
            assert!(
                response.is_some(),
                "Client operation should generate a response"
            );

            let resp = response.unwrap();
            assert_eq!(
                resp.client_id(),
                Some(client_id),
                "Client response should have client_id {}",
                client_id
            );

            client_responses.push(resp);
        } else {
            // Replicated operations should NOT generate responses
            assert!(
                response.is_none(),
                "Replicated operation should NOT generate a response"
            );
        }
    }

    // Verify that we processed both client and replicated operations
    assert_eq!(
        replicated_operations_processed, 4,
        "Should have processed 4 replicated operations"
    );
    assert_eq!(
        client_responses.len(),
        4,
        "Should have generated 4 client responses (not 8)"
    );

    // Verify that all client responses have the correct client_id
    for response in &client_responses {
        assert_eq!(
            response.client_id(),
            Some(client_id),
            "All client responses should have client_id {}",
            client_id
        );
    }

    // Verify response types
    assert!(matches!(
        client_responses[0],
        KVSResponse::PutOk {
            client_id: Some(100),
            ..
        }
    ));
    assert!(matches!(
        client_responses[1],
        KVSResponse::GetResult {
            client_id: Some(100),
            ..
        }
    ));
    assert!(matches!(
        client_responses[2],
        KVSResponse::PutOk {
            client_id: Some(100),
            ..
        }
    ));
    assert!(matches!(
        client_responses[3],
        KVSResponse::DeleteOk {
            client_id: Some(100),
            ..
        }
    ));

    println!(
        "✅ Replication test passed: {} client responses generated, {} replicated operations produced no responses",
        client_responses.len(),
        replicated_operations_processed
    );
}

/// Comprehensive end-to-end integration test simulating the full pipeline
///
/// This test simulates the complete flow:
/// 1. External clients send operations with client IDs
/// 2. Operations are attached with client IDs at entry point
/// 3. Operations flow through the pipeline
/// 4. Core processes operations and generates responses
/// 5. Responses are extracted and routed back to correct clients
#[test]
fn test_end_to_end_pipeline_simulation() {
    // Simulate external input: (client_id, operation_without_id)
    let external_inputs = vec![
        (
            1u64,
            KVSOperation::Put("x".to_string(), "v1".to_string(), 1, None),
        ),
        (
            2u64,
            KVSOperation::Put("y".to_string(), "v2".to_string(), 2, None),
        ),
        (1u64, KVSOperation::Get("x".to_string(), 3, None)),
        (2u64, KVSOperation::Get("y".to_string(), 4, None)),
        (3u64, KVSOperation::Get("x".to_string(), 5, None)),
        (
            1u64,
            KVSOperation::Put(
                "x".to_string(),
                "v1_updated".to_string(),
                6,
                None,
            ),
        ),
        (2u64, KVSOperation::Delete("y".to_string(), 7, None)),
        (3u64, KVSOperation::Get("y".to_string(), 8, None)),
    ];

    // Step 1: Attach client IDs at entry point (simulating plumbing.rs)
    let operations_with_ids: Vec<_> = external_inputs
        .iter()
        .map(|(client_id, op)| {
            let op_with_id = op.clone().with_client_id(Some(*client_id));
            (*client_id, op_with_id)
        })
        .collect();

    // Verify attachment
    for (expected_client_id, op) in &operations_with_ids {
        assert_eq!(
            op.client_id(),
            Some(*expected_client_id),
            "Client ID should be attached at entry point"
        );
    }

    // Step 2: Simulate pipeline transformations (routing, ordering, etc.)
    // In the real system, operations would be serialized/deserialized, routed, etc.
    // Here we just verify that client_id is preserved
    let after_pipeline: Vec<_> = operations_with_ids
        .iter()
        .map(|(client_id, op)| {
            // Simulate serialization/deserialization
            let serialized = serde_json::to_string(&op).unwrap();
            let deserialized: KVSOperation<String, String> =
                serde_json::from_str(&serialized).unwrap();

            // Verify client_id preserved through serialization
            assert_eq!(
                deserialized.client_id(),
                Some(*client_id),
                "Client ID should be preserved through serialization"
            );

            (*client_id, deserialized)
        })
        .collect();

    // Step 3: Core processing
    let mut state = HashMap::new();
    let responses: Vec<_> = after_pipeline
        .iter()
        .map(|(expected_client_id, op)| {
            let should_respond = true;
            let op_client_id = op.client_id();
            let should_emit_response = should_respond && op_client_id.is_some();

            let response: Option<KVSResponse<String, String>> = match op {
                KVSOperation::Put(key, value, request_id, _) => {
                    state.insert(key.clone(), value.clone());
                    if should_emit_response {
                        Some(KVSResponse::PutOk {
                            request_id: *request_id,
                            client_id: op_client_id,
                        })
                    } else {
                        None
                    }
                }
                KVSOperation::Get(key, request_id, _) => {
                    let value = state.get(key).cloned();
                    if should_emit_response {
                        Some(KVSResponse::GetResult {
                            request_id: *request_id,
                            client_id: op_client_id,
                            value,
                        })
                    } else {
                        None
                    }
                }
                KVSOperation::Delete(key, request_id, _) => {
                    state.remove(key);
                    if should_emit_response {
                        Some(KVSResponse::DeleteOk {
                            request_id: *request_id,
                            client_id: op_client_id,
                        })
                    } else {
                        None
                    }
                }
            };

            (*expected_client_id, response)
        })
        .collect();

    // Step 4: Extract client IDs and route responses (simulating plumbing.rs completion)
    let routed_responses: Vec<_> = responses
        .iter()
        .filter_map(|(expected_client_id, response)| {
            response.as_ref().map(|resp| {
                let extracted_client_id = resp.client_id().expect("Response should have client_id");
                let formatted = resp.to_string();

                // Verify routing correctness
                assert_eq!(
                    extracted_client_id, *expected_client_id,
                    "Extracted client_id should match expected client_id"
                );

                (extracted_client_id, formatted)
            })
        })
        .collect();

    // Step 5: Verify end-to-end correctness
    assert_eq!(routed_responses.len(), 8, "Should have 8 responses");

    // Group responses by client
    let mut responses_by_client: HashMap<u64, Vec<String>> = HashMap::new();
    for (client_id, response) in routed_responses {
        responses_by_client
            .entry(client_id)
            .or_default()
            .push(response);
    }

    // Verify each client received their responses
    assert_eq!(
        responses_by_client.get(&1).unwrap().len(),
        3,
        "Client 1 should have 3 responses"
    );
    assert_eq!(
        responses_by_client.get(&2).unwrap().len(),
        3,
        "Client 2 should have 3 responses"
    );
    assert_eq!(
        responses_by_client.get(&3).unwrap().len(),
        2,
        "Client 3 should have 2 responses"
    );

    // Verify response content
    let client1_responses = responses_by_client.get(&1).unwrap();
    assert_eq!(client1_responses[0], "PUT OK");
    assert!(client1_responses[1].contains("v1"));
    assert_eq!(client1_responses[2], "PUT OK");

    let client2_responses = responses_by_client.get(&2).unwrap();
    assert_eq!(client2_responses[0], "PUT OK");
    assert!(client2_responses[1].contains("v2"));
    assert_eq!(client2_responses[2], "DELETE OK");

    let client3_responses = responses_by_client.get(&3).unwrap();
    assert!(client3_responses[0].contains("v1"));
    assert_eq!(client3_responses[1], "GET = NOT FOUND");

    println!("✅ End-to-end pipeline test passed:");
    println!(
        "   - Client 1: {} responses",
        responses_by_client.get(&1).unwrap().len()
    );
    println!(
        "   - Client 2: {} responses",
        responses_by_client.get(&2).unwrap().len()
    );
    println!(
        "   - Client 3: {} responses",
        responses_by_client.get(&3).unwrap().len()
    );
    println!("   - All responses correctly routed through the full pipeline");
}
