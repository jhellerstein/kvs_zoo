# Design Document: Client ID Propagation

## Overview

This design addresses the bug where external client IDs are erased when operations enter the KVS dataflow pipeline, causing all responses to be incorrectly routed to client 0. The solution threads client IDs through the entire request/response flow by embedding them in the operation and response types.

The key insight is to use an `Option<u64>` for the client ID field: `Some(id)` for client-originated operations that need responses, and `None` for replicated operations that should not generate client responses. This approach elegantly handles both client and replication flows without requiring separate code paths.

## Architecture

### Current Flow (Broken)
```
External Client (id=N) 
  → Proxy receives (client_id, operation)
  → Map erases client_id: |(_client_id, op)| op
  → Operation flows through pipeline
  → Response generated
  → Hardcoded to client 0: |(_, response)| (0u64, response)
  → Wrong client receives response!
```

### New Flow (Fixed)
```
External Client (id=N)
  → Proxy receives (client_id, operation)
  → Attach client_id to operation: |(cid, op)| op.with_client_id(Some(cid))
  → Operation flows through pipeline with embedded client_id
  → Response generated with client_id from operation
  → Extract client_id: |(_, response)| (response.client_id, response.message)
  → Correct client receives response!
```

## Components and Interfaces

### 1. KVSOperation (Modified)

Add a `client_id` field to track the originating external client:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KVSOperation<V> {
    Put(String, V, Option<u64>),    // key, value, client_id
    Get(String, Option<u64>),        // key, client_id
    Delete(String, Option<u64>),     // key, client_id
}
```

**Rationale for Option<u64>:**
- `Some(client_id)`: Client-originated operation that expects a response
- `None`: Replicated operation that should not generate a client response

**Helper methods:**
```rust
impl<V> KVSOperation<V> {
    pub fn client_id(&self) -> Option<u64> { ... }
    pub fn with_client_id(self, client_id: Option<u64>) -> Self { ... }
    pub fn key(&self) -> &str { ... }
}
```

### 2. KVSResponse (Modified)

Add a `client_id` field to track which client should receive the response:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KVSResponse {
    pub client_id: Option<u64>,
    pub message: String,
}
```

**Note:** We change from an enum to a struct with a message field. This simplifies the response handling since the core already generates formatted strings.

### 3. KVSCore (Modified)

Update the core processing logic to:
1. Extract client_id from operations
2. Only generate responses when client_id is Some(_)
3. Include client_id in generated responses

```rust
impl KVSCore {
    pub fn process<'a, V, L>(
        operations: Stream<Envelope<bool, KVSOperation<V>>, L, Unbounded, TotalOrder>,
    ) -> CoreOutput<V, L>
    where ...
    {
        let combined = operations.scan(
            q!(|| std::collections::HashMap::new()),
            q!(|state, envelope| {
                let should_respond = envelope.metadata; // bool flag
                let client_id = envelope.operation.client_id();
                
                // Only generate response if should_respond AND client_id is Some
                let should_emit_response = should_respond && client_id.is_some();
                
                let (response, data, meta) = match envelope.operation {
                    KVSOperation::Put(key, value, _) => {
                        // ... process put ...
                        let response = if should_emit_response {
                            Some(KVSResponse {
                                client_id,
                                message: format!("PUT {} = OK", key),
                            })
                        } else {
                            None
                        };
                        // ... return response, data, meta ...
                    }
                    // ... similar for Get and Delete ...
                };
                // ...
            }),
        );
        // ...
    }
}
```

### 4. Plumbing Functions (Modified)

Update `plumb_kvs_dataflow` to:
1. Attach client IDs when operations enter from external
2. Extract client IDs when completing responses

```rust
pub fn plumb_kvs_dataflow<'a, V, K>(
    proxy: &Process<'a, ()>,
    client_external: &External<'a, ()>,
    flow: &FlowBuilder<'a>,
    mut kvs: K,
) -> (KVSClusters<'a>, ExternalBincodeBidi<...>)
where ...
{
    // ... setup ...
    
    // Attach client IDs to operations
    let initial_ops = operations_stream
        .entries()
        .map(q!(|(client_id, op)| op.with_client_id(Some(client_id))))
        .assume_ordering(nondet!(/** client op stream */));
    
    // ... process through pipeline ...
    
    // Extract client IDs for completion
    let to_complete = proxy_responses
        .entries()
        .filter_map(q!(|(_member_id, response)| {
            response.client_id.map(|cid| (cid, response.message))
        }))
        .into_keyed();
    
    complete_sink.complete(to_complete);
    // ...
}
```

### 5. Example Files (Modified)

Update `local_detail.rs` and similar examples to:
1. Attach client IDs when creating operations
2. Extract client IDs when completing responses

## Data Models

### Operation Flow
```
External Input: (u64, KVSOperation<V>)
  ↓ attach client_id
Internal Operation: KVSOperation<V> with embedded Option<u64>
  ↓ process
Internal Response: KVSResponse with embedded Option<u64>
  ↓ extract client_id
External Output: (u64, String)
```

### Client ID States
- **Some(id)**: Client-originated, expects response to external client `id`
- **None**: Replicated or internal, no external response needed

## Co
rrectness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Client ID preservation through operation pipeline

*For any* KVS operation with a client ID, flowing that operation through routing, ordering, and processing transformations should preserve the client ID unchanged.

**Validates: Requirements 1.1**

### Property 2: Response inherits operation client ID

*For any* KVS operation with client ID `Some(N)`, the response generated from processing that operation should have client ID `Some(N)`.

**Validates: Requirements 1.2**

### Property 3: Multi-client response routing correctness

*For any* set of operations from different client IDs processed concurrently, each response should be routed to the client ID matching its originating operation, with no cross-contamination between clients.

**Validates: Requirements 1.3**

### Property 4: Client ID attachment at entry point

*For any* external input tuple `(client_id, operation)`, the resulting KVS operation after attachment should have `client_id` embedded in its client_id field.

**Validates: Requirements 2.1**

### Property 5: Serialization round-trip preserves client ID

*For any* KVS operation with a client ID, serializing then deserializing the operation should produce an equivalent operation with the same client ID.

**Validates: Requirements 2.4**

### Property 6: Response client ID preservation through after-storage

*For any* response with a client ID, flowing that response through after-storage layers (replication, responders) should preserve the client ID unchanged.

**Validates: Requirements 3.2**

### Property 7: Replicated operations have None client ID

*For any* PUT operation that is replicated to other nodes, the replicated operation should have client ID `None`.

**Validates: Requirements 4.1**

### Property 8: None client ID operations produce no responses

*For any* KVS operation with client ID `None`, processing that operation should not generate a response to external clients.

**Validates: Requirements 4.2**

## Error Handling

### Invalid Client ID Scenarios

1. **Missing client ID on client operation**: Should not occur if attachment logic is correct. If it does, the operation should be rejected or logged as an error.

2. **Response with None client ID reaching complete sink**: The filter_map in plumbing should prevent this. If it occurs, log a warning and drop the response.

3. **Client ID mismatch**: Not applicable since client IDs are carried with operations, not looked up.

### Backward Compatibility

The change to `KVSOperation` and `KVSResponse` types is breaking:
- All existing code that constructs or pattern matches these types must be updated
- Serialized operations from old versions will fail to deserialize
- This is acceptable for an internal system under active development

Migration strategy:
1. Update all operation construction sites to include `None` initially
2. Update pattern matching to handle the new tuple structure
3. Update plumbing to attach real client IDs
4. Update tests to verify client ID propagation

## Testing Strategy

### Unit Testing

Unit tests will verify specific examples and edge cases:

1. **Operation construction**: Test that operations can be created with and without client IDs
2. **Helper methods**: Test `client_id()`, `with_client_id()`, and `key()` methods
3. **Response construction**: Test that responses can be created with client IDs
4. **Edge cases**:
   - Operation with client_id = 0 (valid, should work)
   - Operation with client_id = None (replicated, no response)
   - Empty operation stream
   - Single client, multiple operations

### Property-Based Testing

Property-based tests will verify universal correctness properties across many randomly generated inputs. We will use the `proptest` crate for Rust property-based testing, configured to run a minimum of 100 iterations per property.

Each property-based test will be tagged with a comment explicitly referencing the correctness property from this design document using the format: `**Feature: client-id-propagation, Property {number}: {property_text}**`

Property tests to implement:

1. **Property 1 test**: Generate random operations with random client IDs, flow through mock pipeline transformations, verify client ID unchanged
2. **Property 2 test**: Generate random operations with client IDs, process through core, verify responses have matching client IDs
3. **Property 3 test**: Generate multiple operations from different client IDs, process concurrently, verify each response routes to correct client
4. **Property 4 test**: Generate random (client_id, operation) tuples, apply attachment logic, verify client_id embedded correctly
5. **Property 5 test**: Generate random operations with client IDs, serialize and deserialize, verify client ID preserved
6. **Property 6 test**: Generate random responses with client IDs, flow through mock after-storage layers, verify client ID unchanged
7. **Property 7 test**: Generate random PUT operations, apply replication logic, verify replicated operations have None client ID
8. **Property 8 test**: Generate random operations with None client ID, process through core, verify no responses generated

### Integration Testing

Integration tests will verify the end-to-end flow:

1. **Local example test**: Run `local.rs` and `local_detail.rs` examples, verify responses go to correct clients
2. **Multi-client test**: Simulate multiple external clients sending operations concurrently, verify correct routing
3. **Replication test**: Verify replicated operations don't generate client responses

### Test Generators

For property-based testing, we'll need generators for:

```rust
// Generate random KVS operations with optional client IDs
fn arb_kvs_operation() -> impl Strategy<Value = KVSOperation<String>> {
    prop_oneof![
        (any::<String>(), any::<String>(), any::<Option<u64>>())
            .prop_map(|(k, v, cid)| KVSOperation::Put(k, v, cid)),
        (any::<String>(), any::<Option<u64>>())
            .prop_map(|(k, cid)| KVSOperation::Get(k, cid)),
        (any::<String>(), any::<Option<u64>>())
            .prop_map(|(k, cid)| KVSOperation::Delete(k, cid)),
    ]
}

// Generate random client IDs (including edge cases)
fn arb_client_id() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0u64),           // Edge case: client 0
        Just(1u64),           // Common case
        1u64..100u64,         // Small range
        any::<u64>(),         // Full range
    ]
}

// Generate random responses with client IDs
fn arb_kvs_response() -> impl Strategy<Value = KVSResponse> {
    (any::<Option<u64>>(), any::<String>())
        .prop_map(|(cid, msg)| KVSResponse {
            client_id: cid,
            message: msg,
        })
}
```

## Implementation Notes

### Order of Changes

1. Update `KVSOperation` enum to include `Option<u64>` client_id field
2. Update `KVSResponse` to struct with client_id field
3. Update `KVSCore` to extract and propagate client IDs
4. Update `plumb_kvs_dataflow` to attach and extract client IDs
5. Update all examples to use new operation/response types
6. Update all tests to use new types
7. Add property-based tests for correctness properties

### Files to Modify

- `src/protocol/kvs_ops.rs`: Update KVSOperation and KVSResponse types
- `src/kvs_core/mod.rs`: Update core processing to handle client IDs
- `src/plumbing.rs`: Update plumbing to attach/extract client IDs
- `examples/local_detail.rs`: Update to use new types
- `examples/local.rs`: Update to use new types
- All other examples: Update to use new types
- All test files: Update to use new types

### Compilation Strategy

Since this is a breaking change, we should:
1. Make all type changes first
2. Fix compilation errors systematically
3. Run tests to verify behavior
4. Add new property-based tests

The compiler will guide us to all sites that need updates through type errors.
