/// Tests for KVS protocol operations (KVSOperation, KVSResponse)
use kvs_zoo::protocol::{KVSOperation, KVSResponse};

#[test]
fn test_operation_creation() {
    // Test PUT operation
    let put_op = KVSOperation::Put("key1".to_string(), "value1".to_string(), 1, Some(42));
    assert_eq!(
        put_op,
        KVSOperation::Put("key1".to_string(), "value1".to_string(), 1, Some(42))
    );

    // Test GET operation
    let get_op: KVSOperation<String, String> = KVSOperation::Get("key1".to_string(), 2, None);
    assert_eq!(get_op, KVSOperation::Get("key1".to_string(), 2, None));
}

#[test]
fn test_operation_pattern_matching() {
    let put_op = KVSOperation::Put("key1".to_string(), "value1".to_string(), 1, Some(1));
    match put_op {
        KVSOperation::Put(key, value, request_id, client_id) => {
            assert_eq!(key, "key1");
            assert_eq!(value, "value1");
            assert_eq!(request_id, 1);
            assert_eq!(client_id, Some(1));
        }
        KVSOperation::Get(_, _, _) | KVSOperation::Delete(_, _, _) => panic!("Expected PUT operation"),
    }

    let get_op: KVSOperation<String, String> = KVSOperation::Get("key1".to_string(), 2, Some(2));
    match get_op {
        KVSOperation::Get(key, request_id, client_id) => {
            assert_eq!(key, "key1");
            assert_eq!(request_id, 2);
            assert_eq!(client_id, Some(2));
        }
        KVSOperation::Put(_, _, _, _) | KVSOperation::Delete(_, _, _) => panic!("Expected GET operation"),
    }

    let del_op: KVSOperation<String, String> = KVSOperation::Delete("key1".to_string(), 3, None);
    match del_op {
        KVSOperation::Delete(key, request_id, client_id) => {
            assert_eq!(key, "key1");
            assert_eq!(request_id, 3);
            assert_eq!(client_id, None);
        }
        KVSOperation::Put(_, _, _, _) | KVSOperation::Get(_, _, _) => panic!("Expected DELETE operation"),
    }
}

#[test]
fn test_response_creation() {
    // Test PutOk response
    let put_response: KVSResponse<String, String> = KVSResponse::PutOk { request_id: 1, client_id: Some(1) };
    assert_eq!(put_response, KVSResponse::PutOk { request_id: 1, client_id: Some(1) });

    // Test GetResult responses
    let get_found: KVSResponse<String, String> = KVSResponse::GetResult {
        request_id: 2,
        client_id: Some(2),
        value: Some("value".to_string()),
    };
    assert_eq!(
        get_found,
        KVSResponse::GetResult {
            request_id: 2,
            client_id: Some(2),
            value: Some("value".to_string())
        }
    );

    let get_not_found: KVSResponse<String, String> = KVSResponse::GetResult {
        request_id: 3,
        client_id: None,
        value: None,
    };
    assert_eq!(
        get_not_found,
        KVSResponse::GetResult {
            request_id: 3,
            client_id: None,
            value: None
        }
    );
}

#[test]
fn test_response_pattern_matching() {
    let get_response: KVSResponse<String, String> = KVSResponse::GetResult {
        request_id: 1,
        client_id: Some(1),
        value: Some("value".to_string()),
    };
    match get_response {
        KVSResponse::GetResult {
            request_id,
            value: Some(value),
            client_id,
        } => {
            assert_eq!(request_id, 1);
            assert_eq!(value, "value");
            assert_eq!(client_id, Some(1));
        }
        KVSResponse::GetResult { value: None, .. } => panic!("Expected found value"),
        KVSResponse::PutOk { .. } | KVSResponse::DeleteOk { .. } | KVSResponse::_Phantom(_) => panic!("Expected GET response"),
    }

    let not_found_response: KVSResponse<String, String> = KVSResponse::GetResult {
        request_id: 2,
        client_id: Some(2),
        value: None,
    };
    match not_found_response {
        KVSResponse::GetResult {
            request_id,
            value: None,
            client_id,
        } => {
            assert_eq!(request_id, 2);
            assert_eq!(client_id, Some(2));
        }
        KVSResponse::GetResult { value: Some(_), .. } => panic!("Expected not found"),
        KVSResponse::PutOk { .. } | KVSResponse::DeleteOk { .. } | KVSResponse::_Phantom(_) => panic!("Expected GET response"),
    }
}

// Unit tests for helper methods (Task 1.6)

#[test]
fn test_operation_client_id_extraction() {
    let put_op = KVSOperation::Put("key".to_string(), "value".to_string(), 1, Some(42));
    assert_eq!(put_op.client_id(), Some(42));

    let get_op: KVSOperation<String, String> = KVSOperation::Get("key".to_string(), 2, Some(100));
    assert_eq!(get_op.client_id(), Some(100));

    let del_op: KVSOperation<String, String> = KVSOperation::Delete("key".to_string(), 3, None);
    assert_eq!(del_op.client_id(), None);
}

#[test]
fn test_operation_with_client_id() {
    let put_op = KVSOperation::Put("key".to_string(), "value".to_string(), 1, None);
    let updated = put_op.with_client_id(Some(99));
    assert_eq!(updated.client_id(), Some(99));
    assert_eq!(updated.key(), "key");

    let get_op: KVSOperation<String, String> = KVSOperation::Get("key2".to_string(), 2, Some(1));
    let updated = get_op.with_client_id(Some(2));
    assert_eq!(updated.client_id(), Some(2));
    assert_eq!(updated.key(), "key2");

    let del_op: KVSOperation<String, String> = KVSOperation::Delete("key3".to_string(), 3, Some(5));
    let updated = del_op.with_client_id(None);
    assert_eq!(updated.client_id(), None);
    assert_eq!(updated.key(), "key3");
}

#[test]
fn test_operation_key_extraction() {
    let put_op = KVSOperation::Put("put_key".to_string(), "value".to_string(), 1, Some(1));
    assert_eq!(put_op.key(), "put_key");

    let get_op: KVSOperation<String, String> = KVSOperation::Get("get_key".to_string(), 2, None);
    assert_eq!(get_op.key(), "get_key");

    let del_op: KVSOperation<String, String> = KVSOperation::Delete("del_key".to_string(), 3, Some(2));
    assert_eq!(del_op.key(), "del_key");
}

#[test]
fn test_response_client_id_extraction() {
    let put_response: KVSResponse<String, String> = KVSResponse::PutOk {
        request_id: 1,
        client_id: Some(10),
    };
    assert_eq!(put_response.client_id(), Some(10));

    let del_response: KVSResponse<String, String> = KVSResponse::DeleteOk { request_id: 2, client_id: None };
    assert_eq!(del_response.client_id(), None);

    let get_response: KVSResponse<String, String> = KVSResponse::GetResult {
        request_id: 3,
        client_id: Some(20),
        value: Some("val".to_string()),
    };
    assert_eq!(get_response.client_id(), Some(20));
}

#[test]
fn test_response_display_formatting() {
    let put_response: KVSResponse<String, String> = KVSResponse::PutOk { request_id: 1, client_id: Some(1) };
    assert_eq!(format!("{}", put_response), "PUT OK");

    let del_response: KVSResponse<String, String> = KVSResponse::DeleteOk { request_id: 2, client_id: Some(2) };
    assert_eq!(format!("{}", del_response), "DELETE OK");

    let get_found: KVSResponse<String, String> = KVSResponse::GetResult {
        request_id: 3,
        client_id: Some(3),
        value: Some("myvalue".to_string()),
    };
    assert_eq!(format!("{}", get_found), "GET = myvalue");

    let get_not_found: KVSResponse<String, String> = KVSResponse::GetResult {
        request_id: 4,
        client_id: Some(4),
        value: None,
    };
    assert_eq!(format!("{}", get_not_found), "GET = NOT FOUND");
}

// Property-based tests

use proptest::prelude::*;

// Generators for property-based testing (Task 8.3)

/// Generator for request IDs
fn arb_request_id() -> impl Strategy<Value = u64> {
    any::<u64>()
}

/// Generator for client IDs with edge cases
fn arb_client_id() -> impl Strategy<Value = Option<u64>> {
    prop_oneof![
        Just(None),
        Just(Some(0u64)),              // Edge case: client 0
        Just(Some(1u64)),              // Common case
        (1u64..100u64).prop_map(Some), // Small range
        any::<u64>().prop_map(Some),   // Full range
    ]
}

/// Generator for KVS operations with random request IDs and client IDs
fn arb_kvs_operation() -> impl Strategy<Value = KVSOperation<String, String>> {
    prop_oneof![
        (any::<String>(), any::<String>(), arb_request_id(), arb_client_id())
            .prop_map(|(k, v, rid, cid)| KVSOperation::Put(k, v, rid, cid)),
        (any::<String>(), arb_request_id(), arb_client_id()).prop_map(|(k, rid, cid)| KVSOperation::Get(k, rid, cid)),
        (any::<String>(), arb_request_id(), arb_client_id()).prop_map(|(k, rid, cid)| KVSOperation::Delete(k, rid, cid)),
    ]
}

/// Generator for KVS responses with random request IDs and client IDs
fn arb_kvs_response() -> impl Strategy<Value = KVSResponse<String, String>> {
    prop_oneof![
        (arb_request_id(), arb_client_id()).prop_map(|(rid, cid)| KVSResponse::PutOk { request_id: rid, client_id: cid }),
        (arb_request_id(), arb_client_id()).prop_map(|(rid, cid)| KVSResponse::DeleteOk { request_id: rid, client_id: cid }),
        (arb_request_id(), arb_client_id(), any::<Option<String>>()).prop_map(|(rid, cid, val)| KVSResponse::GetResult {
            request_id: rid,
            client_id: cid,
            value: val
        }),
    ]
}

// **Feature: client-id-propagation, Property 5: Serialization round-trip preserves client ID**
// **Validates: Requirements 2.4**
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_serialization_roundtrip_preserves_client_id(op in arb_kvs_operation()) {
        // Serialize the operation
        let serialized = serde_json::to_string(&op).expect("Serialization should succeed");

        // Deserialize it back
        let deserialized: KVSOperation<String, String> = serde_json::from_str(&serialized)
            .expect("Deserialization should succeed");

        // The client ID should be preserved
        prop_assert_eq!(op.client_id(), deserialized.client_id());

        // The entire operation should be equal
        prop_assert_eq!(op, deserialized);
    }
}

// **Feature: client-id-propagation, Property 4: Client ID attachment at entry point**
// **Validates: Requirements 2.1**
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_client_id_attachment_at_entry(
        op in arb_kvs_operation(),
        new_client_id in any::<u64>()
    ) {
        // Simulate the entry point attachment: (client_id, operation) -> operation.with_client_id(Some(client_id))
        let original_key = op.key().to_string();
        let attached = op.with_client_id(Some(new_client_id));

        // The resulting operation should have the attached client ID
        prop_assert_eq!(attached.client_id(), Some(new_client_id));

        // The key should be preserved
        prop_assert_eq!(attached.key(), &original_key);
    }
}

// **Feature: client-id-propagation, Property 1: Client ID preservation through operation pipeline**
// **Validates: Requirements 1.1**
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_client_id_preservation_through_pipeline(
        op in arb_kvs_operation()
    ) {
        // Simulate pipeline transformations that operations go through
        let original_client_id = op.client_id();
        let original_key = op.key().to_string();

        // 1. Attachment at entry (simulating plumbing attachment)
        let with_id = op.clone().with_client_id(original_client_id);
        prop_assert_eq!(with_id.client_id(), original_client_id);

        // 2. Serialization/deserialization (simulating network transmission)
        let serialized = serde_json::to_string(&with_id).expect("Serialization should succeed");
        let deserialized: KVSOperation<String, String> = serde_json::from_str(&serialized)
            .expect("Deserialization should succeed");
        prop_assert_eq!(deserialized.client_id(), original_client_id);

        // 3. Key extraction (simulating routing)
        prop_assert_eq!(deserialized.key(), &original_key);

        // 4. Client ID extraction (simulating response generation)
        let extracted_id = deserialized.client_id();
        prop_assert_eq!(extracted_id, original_client_id);

        // Throughout all transformations, the client ID should remain unchanged
        prop_assert_eq!(deserialized.client_id(), op.client_id());
    }
}

// **Feature: client-id-propagation, Property 7: Replicated operations have None client ID**
// **Validates: Requirements 4.1**
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_replicated_operations_have_none_client_id(
        key in any::<String>(),
        value in any::<String>(),
        request_id in arb_request_id()
    ) {
        // Simulate the replication process: when a PUT operation is replicated,
        // the replicated operation is created from the (key, value) delta tuple
        // This simulates what happens in cross_layer_flow.rs and plumb_after.rs

        // Create a replicated operation from a PUT delta (as done in the actual code)
        let replicated_op = KVSOperation::Put(key.clone(), value.clone(), request_id, None);

        // Verify that the replicated operation has None client_id
        prop_assert_eq!(
            replicated_op.client_id(),
            None,
            "Replicated operations should have None client_id to prevent client responses"
        );

        // Verify that the key and value are preserved
        match replicated_op {
            KVSOperation::Put(k, v, rid, cid) => {
                prop_assert_eq!(k, key);
                prop_assert_eq!(v, value);
                prop_assert_eq!(rid, request_id);
                prop_assert_eq!(cid, None);
            }
            _ => prop_assert!(false, "Should be a Put operation"),
        }
    }
}

// **Feature: client-id-propagation, Property 3: Multi-client response routing correctness**
// **Validates: Requirements 1.3**
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_multi_client_routing_correctness(
        operations in prop::collection::vec(
            (any::<u64>(), arb_kvs_operation()),
            1..20  // Generate 1-20 operations from different clients
        )
    ) {
        // Simulate multiple clients sending operations concurrently
        // Each operation is a tuple of (external_client_id, operation)

        // Step 1: Attach client IDs to operations (simulating entry point)
        let operations_with_ids: Vec<_> = operations.iter()
            .map(|(client_id, op)| {
                let op_with_id = op.clone().with_client_id(Some(*client_id));
                (*client_id, op_with_id)
            })
            .collect();

        // Step 2: Process each operation and generate responses
        let mut state = std::collections::HashMap::new();
        let responses: Vec<_> = operations_with_ids.iter()
            .map(|(expected_client_id, op)| {
                let request_id = op.request_id();
                let client_id = op.client_id();
                let should_emit_response = client_id.is_some();

                let response: Option<KVSResponse<String, String>> = match op {
                    KVSOperation::Put(key, value, _, _) => {
                        state.insert(key.clone(), value.clone());
                        if should_emit_response {
                            Some(KVSResponse::PutOk { request_id, client_id })
                        } else {
                            None
                        }
                    }
                    KVSOperation::Get(key, _, _) => {
                        let value = state.get(key).cloned();
                        if should_emit_response {
                            Some(KVSResponse::GetResult { request_id, client_id, value })
                        } else {
                            None
                        }
                    }
                    KVSOperation::Delete(key, _, _) => {
                        state.remove(key);
                        if should_emit_response {
                            Some(KVSResponse::DeleteOk { request_id, client_id })
                        } else {
                            None
                        }
                    }
                };

                (*expected_client_id, response)
            })
            .collect();

        // Step 3: Verify routing correctness
        for ((expected_client_id, _op), (_same_client_id, response)) in
            operations_with_ids.iter().zip(responses.iter())
        {
            if let Some(resp) = response {
                // Each response should have the client_id matching its originating operation
                prop_assert_eq!(
                    resp.client_id(),
                    Some(*expected_client_id),
                    "Response client_id should match the originating operation's client_id"
                );
            }
        }

        // Step 4: Verify no cross-contamination between clients
        // Group responses by client_id
        let mut responses_by_client: std::collections::HashMap<u64, Vec<&KVSResponse<String, String>>> =
            std::collections::HashMap::new();

        for (client_id, response) in responses.iter() {
            if let Some(resp) = response {
                responses_by_client
                    .entry(*client_id)
                    .or_default()
                    .push(resp);
            }
        }

        // Verify that each client's responses only contain that client's ID
        for (client_id, client_responses) in responses_by_client.iter() {
            for response in client_responses {
                prop_assert_eq!(
                    response.client_id(),
                    Some(*client_id),
                    "All responses for client {} should have that client_id",
                    client_id
                );
            }
        }
    }
}

// **Feature: client-id-propagation, Property 6: Response client ID preservation through after-storage**
// **Validates: Requirements 3.2**
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_response_preservation_through_after_storage(
        response in arb_kvs_response()
    ) {
        // Simulate responses flowing through after-storage layers
        let original_client_id = response.client_id();
        let original_request_id = response.request_id();

        // 1. Response generated from core
        let from_core = response.clone();
        prop_assert_eq!(from_core.client_id(), original_client_id);
        prop_assert_eq!(from_core.request_id(), original_request_id);

        // 2. Serialization/deserialization (simulating network transmission in replication)
        let serialized = serde_json::to_string(&from_core)
            .expect("Response serialization should succeed");
        let after_network: KVSResponse<String, String> = serde_json::from_str(&serialized)
            .expect("Response deserialization should succeed");
        prop_assert_eq!(
            after_network.client_id(),
            original_client_id,
            "Client ID should be preserved through serialization/deserialization"
        );
        prop_assert_eq!(
            after_network.request_id(),
            original_request_id,
            "Request ID should be preserved through serialization/deserialization"
        );

        // 3. Cloning (simulating broadcast to multiple nodes)
        let cloned = after_network.clone();
        prop_assert_eq!(
            cloned.client_id(),
            original_client_id,
            "Client ID should be preserved through cloning"
        );
        prop_assert_eq!(
            cloned.request_id(),
            original_request_id,
            "Request ID should be preserved through cloning"
        );

        // 4. Pattern matching and reconstruction (simulating responder processing)
        let reconstructed: KVSResponse<String, String> = match cloned {
            KVSResponse::PutOk { request_id, client_id } => {
                KVSResponse::PutOk { request_id, client_id }
            }
            KVSResponse::DeleteOk { request_id, client_id } => {
                KVSResponse::DeleteOk { request_id, client_id }
            }
            KVSResponse::GetResult { request_id, client_id, value } => {
                KVSResponse::GetResult { request_id, client_id, value }
            }
            KVSResponse::_Phantom(_) => unreachable!(),
        };
        prop_assert_eq!(
            reconstructed.client_id(),
            original_client_id,
            "Client ID should be preserved through pattern matching and reconstruction"
        );
        prop_assert_eq!(
            reconstructed.request_id(),
            original_request_id,
            "Request ID should be preserved through pattern matching and reconstruction"
        );

        // 5. Extraction for routing (simulating proxy completion)
        let extracted_id = reconstructed.client_id();
        let extracted_rid = reconstructed.request_id();
        prop_assert_eq!(
            extracted_id,
            original_client_id,
            "Client ID extraction should return the original client ID"
        );
        prop_assert_eq!(
            extracted_rid,
            original_request_id,
            "Request ID extraction should return the original request ID"
        );

        // Throughout all after-storage transformations, both IDs should remain unchanged
        prop_assert_eq!(
            reconstructed.client_id(),
            response.client_id(),
            "Client ID should be preserved through all after-storage layers"
        );
        prop_assert_eq!(
            reconstructed.request_id(),
            response.request_id(),
            "Request ID should be preserved through all after-storage layers"
        );
    }
}
