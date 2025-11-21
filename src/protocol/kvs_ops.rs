use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// KVS operation types that clients can send to the server
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KVSOperation<V> {
    /// Store a key-value pair (key, value, client_id)
    Put(String, V, Option<u64>),
    /// Retrieve the value for a key (key, client_id)
    Get(String, Option<u64>),
    /// Tombstone (logically delete) a key (key, client_id)
    Delete(String, Option<u64>),
}

impl<V> KVSOperation<V> {
    /// Extract the client ID from the operation
    pub fn client_id(&self) -> Option<u64> {
        match self {
            KVSOperation::Put(_, _, cid) => *cid,
            KVSOperation::Get(_, cid) => *cid,
            KVSOperation::Delete(_, cid) => *cid,
        }
    }

    /// Set or update the client ID for this operation
    pub fn with_client_id(self, client_id: Option<u64>) -> Self {
        match self {
            KVSOperation::Put(k, v, _) => KVSOperation::Put(k, v, client_id),
            KVSOperation::Get(k, _) => KVSOperation::Get(k, client_id),
            KVSOperation::Delete(k, _) => KVSOperation::Delete(k, client_id),
        }
    }

    /// Extract the key from the operation
    pub fn key(&self) -> &str {
        match self {
            KVSOperation::Put(k, _, _) => k,
            KVSOperation::Get(k, _) => k,
            KVSOperation::Delete(k, _) => k,
        }
    }
}

/// Response types that the server sends back to clients
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KVSResponse<V> {
    /// Successful PUT operation
    PutOk { client_id: Option<u64> },
    /// Successful DELETE (tombstone) operation
    DeleteOk { client_id: Option<u64> },
    /// GET operation result - Some(value) if found and live, None if not found or tombstoned
    GetResult {
        client_id: Option<u64>,
        value: Option<V>,
    },
}

impl<V> KVSResponse<V> {
    /// Extract the client ID from the response
    pub fn client_id(&self) -> Option<u64> {
        match self {
            KVSResponse::PutOk { client_id } => *client_id,
            KVSResponse::DeleteOk { client_id } => *client_id,
            KVSResponse::GetResult { client_id, .. } => *client_id,
        }
    }
}

impl<V: Display> Display for KVSResponse<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KVSResponse::PutOk { .. } => write!(f, "PUT OK"),
            KVSResponse::DeleteOk { .. } => write!(f, "DELETE OK"),
            KVSResponse::GetResult { value: Some(v), .. } => write!(f, "GET = {}", v),
            KVSResponse::GetResult { value: None, .. } => write!(f, "GET = NOT FOUND"),
        }
    }
}
