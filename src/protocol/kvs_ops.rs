use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// KVS operation types that clients can send to the server
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KVSOperation<K, V> {
    /// Store a key-value pair in the key-value store.
    ///
    /// # Fields
    /// - `K`: The key to store.
    /// - `V`: The value to associate with the key.
    /// - `u64`: Request ID for tracking this specific operation. Used to correlate requests and responses.
    /// - `Option<u64>`: Optional client ID. Use `Some(id)` for client-originated requests; use `None` for replicated/internal operations.
    Put(K, V, u64, Option<u64>),
    /// Retrieve the value associated with a key.
    ///
    /// # Fields
    /// - `K`: The key whose value is to be retrieved.
    /// - `u64`: Request ID for tracking this specific operation. Used to correlate requests and responses.
    /// - `Option<u64>`: Optional client ID. Use `Some(id)` for client-originated requests; use `None` for replicated/internal operations.
    Get(K, u64, Option<u64>),
    /// Tombstone (logically delete) a key from the store.
    ///
    /// # Fields
    /// - `K`: The key to delete.
    /// - `u64`: Request ID for tracking this specific operation. Used to correlate requests and responses.
    /// - `Option<u64>`: Optional client ID. Use `Some(id)` for client-originated requests; use `None` for replicated/internal operations.
    Delete(K, u64, Option<u64>),
}

impl<K, V> KVSOperation<K, V> {
    /// Extract the request ID from the operation
    pub fn request_id(&self) -> u64 {
        match self {
            KVSOperation::Put(_, _, rid, _) => *rid,
            KVSOperation::Get(_, rid, _) => *rid,
            KVSOperation::Delete(_, rid, _) => *rid,
        }
    }

    /// Extract the client ID from the operation
    pub fn client_id(&self) -> Option<u64> {
        match self {
            KVSOperation::Put(_, _, _, cid) => *cid,
            KVSOperation::Get(_, _, cid) => *cid,
            KVSOperation::Delete(_, _, cid) => *cid,
        }
    }

    /// Set or update the request ID for this operation
    pub fn with_request_id(self, request_id: u64) -> Self {
        match self {
            KVSOperation::Put(k, v, _, cid) => KVSOperation::Put(k, v, request_id, cid),
            KVSOperation::Get(k, _, cid) => KVSOperation::Get(k, request_id, cid),
            KVSOperation::Delete(k, _, cid) => KVSOperation::Delete(k, request_id, cid),
        }
    }

    /// Set or update the client ID for this operation
    pub fn with_client_id(self, client_id: Option<u64>) -> Self {
        match self {
            KVSOperation::Put(k, v, rid, _) => KVSOperation::Put(k, v, rid, client_id),
            KVSOperation::Get(k, rid, _) => KVSOperation::Get(k, rid, client_id),
            KVSOperation::Delete(k, rid, _) => KVSOperation::Delete(k, rid, client_id),
        }
    }

    /// Extract the key from the operation
    pub fn key(&self) -> &K {
        match self {
            KVSOperation::Put(k, _, _, _) => k,
            KVSOperation::Get(k, _, _) => k,
            KVSOperation::Delete(k, _, _) => k,
        }
    }
}

/// Response types that the server sends back to clients
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KVSResponse<K, V> {
    /// Successful PUT operation
    PutOk {
        request_id: u64,
        client_id: Option<u64>,
    },
    /// Successful DELETE (tombstone) operation
    DeleteOk {
        request_id: u64,
        client_id: Option<u64>,
    },
    /// GET operation result - Some(value) if found and live, None if not found or tombstoned
    GetResult {
        request_id: u64,
        client_id: Option<u64>,
        value: Option<V>,
    },
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<K>),
}

impl<K, V> KVSResponse<K, V> {
    /// Extract the request ID from the response
    pub fn request_id(&self) -> u64 {
        match self {
            KVSResponse::PutOk { request_id, .. } => *request_id,
            KVSResponse::DeleteOk { request_id, .. } => *request_id,
            KVSResponse::GetResult { request_id, .. } => *request_id,
            KVSResponse::_Phantom(_) => 0,
        }
    }

    /// Extract the client ID from the response
    pub fn client_id(&self) -> Option<u64> {
        match self {
            KVSResponse::PutOk { client_id, .. } => *client_id,
            KVSResponse::DeleteOk { client_id, .. } => *client_id,
            KVSResponse::GetResult { client_id, .. } => *client_id,
            KVSResponse::_Phantom(_) => None,
        }
    }
}

impl<K, V: Display> Display for KVSResponse<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KVSResponse::PutOk { .. } => write!(f, "PUT OK"),
            KVSResponse::DeleteOk { .. } => write!(f, "DELETE OK"),
            KVSResponse::GetResult { value: Some(v), .. } => write!(f, "GET = {}", v),
            KVSResponse::GetResult { value: None, .. } => write!(f, "GET = NOT FOUND"),
            KVSResponse::_Phantom(_) => write!(f, "PHANTOM"),
        }
    }
}
