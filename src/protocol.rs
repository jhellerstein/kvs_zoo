use serde::{Deserialize, Serialize};

/// Type alias for KVS operations with String values (most common case)
pub type StringKVSOperation = KVSOperation<String>;

/// Type alias for KVS responses with String values (most common case)
pub type StringKVSResponse = KVSResponse<String>;

/// KVS operation types that clients can send to the server
///
/// This enum defines the protocol for client-server communication in the KVS.
/// All operations are serializable for network transmission.
///
/// The value type `V` is generic, allowing the KVS to store different types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KVSOperation<V> {
    /// Store a key-value pair
    Put(String, V),
    /// Retrieve the value for a key
    Get(String),
}

/// Response types that the server sends back to clients
///
/// This enum defines the possible responses from KVS operations.
/// Each operation type has a corresponding response variant.
///
/// The value type `V` is generic, matching the operation type.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KVSResponse<V> {
    /// Successful PUT operation
    PutOk,
    /// GET operation result - Some(value) if found, None if not found
    GetResult(Option<V>),
}

impl<V> KVSOperation<V>
where
    V: std::fmt::Display,
{
    /// Format an operation as a string command
    pub fn to_command(&self) -> String {
        match self {
            KVSOperation::Put(key, value) => format!("PUT {} {}", key, value),
            KVSOperation::Get(key) => format!("GET {}", key),
        }
    }

    /// Get the key being operated on
    pub fn key(&self) -> &str {
        match self {
            KVSOperation::Put(key, _) => key,
            KVSOperation::Get(key) => key,
        }
    }

    /// Check if this is a read operation (doesn't modify state)
    pub fn is_read_only(&self) -> bool {
        matches!(self, KVSOperation::Get(_))
    }

    /// Check if this is a write operation (modifies state)
    pub fn is_write(&self) -> bool {
        matches!(self, KVSOperation::Put(_, _))
    }
}

impl KVSOperation<String> {
    /// Create an operation from a string command
    ///
    /// Parses commands in the format:
    /// - "PUT key value"
    /// - "GET key"
    pub fn from_command(command: &str) -> Option<Self> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        match parts.as_slice() {
            ["PUT", key, value] => Some(KVSOperation::Put(key.to_string(), value.to_string())),
            ["GET", key] => Some(KVSOperation::Get(key.to_string())),
            _ => None,
        }
    }
}

impl<V> KVSResponse<V> {
    /// Check if the response indicates success
    pub fn is_success(&self) -> bool {
        match self {
            KVSResponse::PutOk => true,
            KVSResponse::GetResult(_) => true, // GET is always successful, even if key not found
        }
    }

    /// Get the value from a GET response, if any
    pub fn get_value(&self) -> Option<&V> {
        match self {
            KVSResponse::GetResult(value) => value.as_ref(),
            _ => None,
        }
    }
}
