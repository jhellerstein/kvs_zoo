# Implementation Plan

- [ ] 1. Update KVSOperation and KVSResponse type definitions
  - [ ] 1.1 Add client_id field to KVSOperation enum variants
    - Modify `src/protocol/kvs_ops.rs` to add `Option<u64>` to each variant
    - Update Put variant: `Put(String, V, Option<u64>)`
    - Update Get variant: `Get(String, Option<u64>)`
    - Update Delete variant: `Delete(String, Option<u64>)`
    - _Requirements: 2.1_

  - [ ] 1.2 Implement helper methods for KVSOperation
    - Add `client_id()` method to extract client ID from any variant
    - Add `with_client_id()` method to set/update client ID
    - Add `key()` method to extract key from any variant
    - _Requirements: 2.1, 2.2_

  - [ ] 1.3 Write property test for KVSOperation serialization
    - **Property 5: Serialization round-trip preserves client ID**
    - **Validates: Requirements 2.4**

  - [ ] 1.4 Restructure KVSResponse to include client_id in each variant
    - Change from enum with simple variants to enum with struct variants
    - Add `PutOk { client_id: Option<u64> }`
    - Add `DeleteOk { client_id: Option<u64> }`
    - Add `GetResult { client_id: Option<u64>, value: Option<V> }`
    - _Requirements: 3.1_

  - [ ] 1.5 Implement helper methods for KVSResponse
    - Add `client_id()` method to extract client ID from any variant
    - Implement `Display` trait for formatting responses as strings
    - _Requirements: 3.1, 3.3_

  - [ ] 1.6 Write unit tests for KVSOperation and KVSResponse helpers
    - Test `client_id()` extraction for all variants
    - Test `with_client_id()` updates client ID correctly
    - Test `key()` extraction for all operation types
    - Test `Display` formatting for all response types
    - _Requirements: 2.1, 3.1_

- [ ] 2. Update KVSCore to handle client IDs in operations and responses
  - [ ] 2.1 Modify core processing to extract client IDs from operations
    - Update `process()` method in `src/kvs_core/mod.rs`
    - Extract client_id from envelope.operation using helper method
    - Determine if response should be emitted based on should_respond flag AND client_id presence
    - _Requirements: 1.1, 1.2_

  - [ ] 2.2 Update Put operation handling to generate KVSResponse::PutOk
    - Replace string response with `KVSResponse::PutOk { client_id }`
    - Only generate response if should_emit_response is true
    - _Requirements: 1.2, 3.1_

  - [ ] 2.3 Update Get operation handling to generate KVSResponse::GetResult
    - Replace string response with `KVSResponse::GetResult { client_id, value }`
    - Include the actual value (or None) instead of formatting as string
    - Only generate response if should_emit_response is true
    - _Requirements: 1.2, 3.1_

  - [ ] 2.4 Update Delete operation handling to generate KVSResponse::DeleteOk
    - Replace string response with `KVSResponse::DeleteOk { client_id }`
    - Only generate response if should_emit_response is true
    - _Requirements: 1.2, 3.1_

  - [ ] 2.5 Update CoreOutput type signature
    - Change `responses` field from `Stream<String, ...>` to `Stream<KVSResponse<V>, ...>`
    - Update CoreEmission struct to use `Option<KVSResponse<V>>` instead of `Option<String>`
    - _Requirements: 3.1_

  - [ ]* 2.6 Write property test for response client ID inheritance
    - **Property 2: Response inherits operation client ID**
    - **Validates: Requirements 1.2**

  - [ ]* 2.7 Write property test for None client ID operations
    - **Property 8: None client ID operations produce no responses**
    - **Validates: Requirements 4.2**

- [ ] 3. Update plumbing to attach and extract client IDs
  - [ ] 3.1 Modify plumb_kvs_dataflow to attach client IDs at entry
    - Update `src/plumbing.rs` operations_stream mapping
    - Change from `.map(q!(|(_client_id, op)| op))` to `.map(q!(|(client_id, op)| op.with_client_id(Some(client_id))))`
    - _Requirements: 1.1, 2.1_

  - [ ]* 3.2 Write property test for client ID attachment
    - **Property 4: Client ID attachment at entry point**
    - **Validates: Requirements 2.1**

  - [ ] 3.3 Modify plumb_kvs_dataflow to extract client IDs at exit
    - Update proxy_responses mapping to extract client_id and format response
    - Change from `.map(q!(|(_member_id, response)| (0u64, response)))` to `.filter_map(q!(|(_member_id, response)| response.client_id().map(|cid| (cid, response.to_string()))))`
    - Remove the KVS_STAMP_MEMBER conditional logic (no longer needed)
    - _Requirements: 1.3, 3.3, 3.4_

  - [ ]* 3.4 Write property test for client ID preservation through pipeline
    - **Property 1: Client ID preservation through operation pipeline**
    - **Validates: Requirements 1.1**

  - [ ] 3.4 Update extract_put_deltas to handle new operation structure
    - Update pattern matching to destructure new tuple variants
    - Ensure PUT deltas extraction works with `Put(k, v, _)` pattern
    - _Requirements: 2.2_

- [ ] 4. Update replication logic to mark replicated operations
  - [ ] 4.1 Ensure replicated operations have None client_id
    - Review replication code paths in `src/after_storage/replication/`
    - Verify that operations generated from replication have client_id = None
    - Add explicit `.with_client_id(None)` calls if needed
    - _Requirements: 4.1, 4.2_

  - [ ]* 4.2 Write property test for replicated operation marking
    - **Property 7: Replicated operations have None client ID**
    - **Validates: Requirements 4.1**

- [ ] 5. Update all examples to use new operation and response types
  - [ ] 5.1 Update local_detail.rs
    - Fix operation construction to use new tuple structure
    - Update response handling to work with KVSResponse instead of String
    - Fix the client_id mapping (remove hardcoded 0)
    - _Requirements: 1.1, 1.3_

  - [ ] 5.2 Update local.rs
    - Verify it works with updated plumb_kvs_dataflow
    - Test that responses are correctly routed
    - _Requirements: 1.1, 1.3_

  - [ ] 5.3 Update other examples
    - Update replicated.rs, sharded.rs, and other example files
    - Fix operation construction and response handling
    - _Requirements: 1.1, 1.3_

- [ ] 6. Update all test files to use new types
  - [ ] 6.1 Update protocol_tests.rs
    - Fix KVSOperation construction in tests
    - Update assertions to match new response types
    - _Requirements: 2.1, 3.1_

  - [ ] 6.2 Update linearizability_tests.rs and other test files
    - Fix operation construction throughout test suite
    - Update response assertions
    - _Requirements: 2.1, 3.1_

  - [ ] 6.3 Update kvs_core tests
    - Fix the unit tests in `src/kvs_core/mod.rs`
    - Update operation construction and response assertions
    - _Requirements: 2.1, 3.1_

- [ ] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ]* 8. Add comprehensive property-based tests
  - [ ]* 8.1 Write property test for multi-client routing correctness
    - **Property 3: Multi-client response routing correctness**
    - **Validates: Requirements 1.3**

  - [ ]* 8.2 Write property test for response preservation through after-storage
    - **Property 6: Response client ID preservation through after-storage**
    - **Validates: Requirements 3.2**

  - [ ]* 8.3 Create property test generators module
    - Implement `arb_kvs_operation()` generator
    - Implement `arb_client_id()` generator with edge cases
    - Implement `arb_kvs_response()` generator
    - _Requirements: All testing requirements_

- [ ]* 9. Add integration tests for end-to-end client ID flow
  - [ ]* 9.1 Write integration test for single client flow
    - Test that a single client's operations return responses to that client
    - _Requirements: 1.1, 1.2, 1.3_

  - [ ]* 9.2 Write integration test for multiple concurrent clients
    - Test that multiple clients' operations are correctly routed
    - Verify no cross-contamination between clients
    - _Requirements: 1.3_

  - [ ]* 9.3 Write integration test for replication without client responses
    - Test that replicated operations don't generate client responses
    - _Requirements: 4.1, 4.2_

- [ ] 10. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
