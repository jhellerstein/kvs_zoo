# Requirements Document

## Introduction

The KVS system currently erases external client IDs when operations enter the dataflow pipeline, causing all responses to be incorrectly routed back to client 0. This feature will thread external client IDs through the entire request/response pipeline to ensure responses are returned to the correct originating client.

## Glossary

- **External Client ID**: A unique identifier (u64) assigned by the Hydro external connection system to distinguish different external clients connecting to the KVS proxy
- **KVS Operation**: An enum representing client requests (Put, Get, Delete) sent to the key-value store
- **KVS Response**: An enum representing server responses (PutOk, DeleteOk, GetResult) sent back to clients
- **Proxy Process**: The Hydro process that manages bidirectional communication between external clients and the KVS cluster
- **Dataflow Pipeline**: The sequence of transformations that operations undergo from client input through routing, ordering, core processing, and response generation
- **Complete Sink**: The Hydro mechanism that sends responses back to external clients using their client IDs

## Requirements

### Requirement 1

**User Story:** As a KVS system developer, I want external client IDs to be preserved throughout the request/response pipeline, so that responses are correctly routed back to the originating client.

#### Acceptance Criteria

1. WHEN an external client sends a KVS operation THEN the system SHALL preserve the client ID through all dataflow transformations
2. WHEN a KVS operation is processed THEN the system SHALL associate the resulting response with the original client ID
3. WHEN multiple external clients send concurrent operations THEN the system SHALL return each response to its correct originating client
4. WHEN the system completes a response THEN the system SHALL use the preserved client ID rather than a hardcoded value

### Requirement 2

**User Story:** As a KVS system developer, I want KVSOperation to carry client ID information, so that the ID can flow through the pipeline without requiring separate tracking mechanisms.

#### Acceptance Criteria

1. WHEN a KVSOperation is created from external input THEN the system SHALL attach the external client ID to the operation
2. WHEN a KVSOperation flows through routing and ordering layers THEN the system SHALL maintain the client ID as part of the operation data
3. WHEN a KVSOperation is processed by the core THEN the system SHALL extract the client ID for response routing
4. WHEN a KVSOperation is serialized or deserialized THEN the system SHALL preserve the client ID field

### Requirement 3

**User Story:** As a KVS system developer, I want responses to include client ID information, so that the proxy can route them correctly without additional lookups.

#### Acceptance Criteria

1. WHEN the KVS core generates a response THEN the system SHALL include the client ID from the originating operation
2. WHEN responses flow through after-storage layers THEN the system SHALL preserve the client ID
3. WHEN responses reach the proxy process THEN the system SHALL extract the client ID for completion
4. WHEN the complete sink receives a response THEN the system SHALL use the included client ID to route the response

### Requirement 4

**User Story:** As a KVS system developer, I want replicated operations to be distinguishable from client operations, so that only client-originated operations generate responses to external clients.

#### Acceptance Criteria

1. WHEN a PUT operation is replicated to other nodes THEN the system SHALL mark the replicated operation to prevent client response generation
2. WHEN a replicated operation is processed THEN the system SHALL not attempt to send a response to an external client
3. WHEN client operations and replicated operations are combined THEN the system SHALL maintain the distinction between them
4. WHEN generating responses THEN the system SHALL only create responses for client-originated operations with valid client IDs
