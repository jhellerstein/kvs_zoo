use serde::{Deserialize, Serialize};

/// KVS operation types that clients can send to the server
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KVSOperation<V> {
    /// Store a key-value pair
    Put(String, V),
    /// Retrieve the value for a key
    Get(String),
}

/// Response types that the server sends back to clients
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KVSResponse<V> {
    /// Successful PUT operation
    PutOk,
    /// GET operation result - Some(value) if found, None if not found
    GetResult(Option<V>),
}
