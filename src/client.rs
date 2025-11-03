use crate::protocol::{KVSOperation, KVSResponse};

/// A KVS client that provides standardized operation generation and logging utilities
///
/// This module serves multiple purposes in the KVS Zoo:
/// - **Demo Operations**: Provides consistent operation sequences for all examples
/// - **Logging Utilities**: Standardized formatting for operations and responses
/// - **Command Parsing**: Converts string commands to KVSOperation objects
/// - **External Client Interface**: Used in linearizable KVS for external process communication
///
/// The client is used across the entire KVS Zoo ecosystem:
/// - All example drivers use the same demo operations for consistency
/// - The linearizable example uses it for external client communication
/// - All examples use its logging utilities for professional output
///
/// The actual networking is handled by Hydro's External interface in examples,
/// while this module focuses on operation generation, parsing, and formatting.
///
/// # Usage Examples
///
/// ```rust
/// use kvs_zoo::client::KVSClient;
/// use kvs_zoo::protocol::KVSOperation;
///
/// // Generate standard demo operations
/// let operations = KVSClient::generate_demo_operations();
///
/// // Parse a command string
/// let op = KVSClient::parse_operation("PUT key1 value1").unwrap();
///
/// // Log an operation with consistent formatting
/// KVSClient::log_operation(&op);
///
/// // Format an operation back to a command string
/// let command = KVSClient::format_operation(&op);
/// ```
pub struct KVSClient;

impl KVSClient {
    /// Generate a sequence of demo operations used across all KVS examples
    ///
    /// This provides the standard operation sequence that all examples use for consistency.
    /// The sequence demonstrates typical KVS usage patterns:
    /// - Initial puts to populate data
    /// - Gets to retrieve existing data  
    /// - Gets for non-existent keys (testing error handling)
    /// - Updates to existing keys
    /// - Gets to verify updates
    ///
    /// Used by: local, replicated, sharded, and linearizable examples
    pub fn generate_demo_operations() -> Vec<KVSOperation<String>> {
        vec![
            KVSOperation::Put("key1".to_string(), "value1".to_string()),
            KVSOperation::Put("key2".to_string(), "value2".to_string()),
            KVSOperation::Get("key1".to_string()),
            KVSOperation::Get("nonexistent".to_string()),
            KVSOperation::Put("key1".to_string(), "updated_value1".to_string()),
            KVSOperation::Get("key1".to_string()),
        ]
    }

    /// Print an operation with standardized formatting used across all examples
    ///
    /// Provides consistent, professional logging output that all KVS examples use.
    /// This ensures a uniform user experience across different architectures.
    pub fn log_operation(op: &KVSOperation<String>) {
        match op {
            KVSOperation::Put(key, value) => println!("Client: Sending PUT {} = {}", key, value),
            KVSOperation::Get(key) => println!("Client: Sending GET {}", key),
        }
    }

    /// Create a client operation from a string command
    ///
    /// This allows for easy creation of operations from user input or configuration
    pub fn parse_operation(command: &str) -> Option<KVSOperation<String>> {
        KVSOperation::from_command(command)
    }

    /// Format an operation as a string command
    pub fn format_operation(op: &KVSOperation<String>) -> String {
        op.to_command()
    }

    /// Generate operations from a list of command strings
    pub fn operations_from_commands(commands: &[&str]) -> Vec<KVSOperation<String>> {
        commands
            .iter()
            .filter_map(|cmd| Self::parse_operation(cmd))
            .collect()
    }

    /// Log a response from the server
    pub fn log_response(response: &KVSResponse<String>) {
        match response {
            KVSResponse::PutOk => println!("Client: Received PUT OK"),
            KVSResponse::GetResult(Some(value)) => println!("Client: Received GET -> {}", value),
            KVSResponse::GetResult(None) => println!("Client: Received GET -> NOT FOUND"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_demo_operations() {
        let operations = KVSClient::generate_demo_operations();
        assert_eq!(operations.len(), 6);

        // Check the sequence of operations
        assert!(
            matches!(operations[0], KVSOperation::Put(ref k, ref v) if k == "key1" && v == "value1")
        );
        assert!(
            matches!(operations[1], KVSOperation::Put(ref k, ref v) if k == "key2" && v == "value2")
        );
        assert!(matches!(operations[2], KVSOperation::Get(ref k) if k == "key1"));
        assert!(matches!(operations[3], KVSOperation::Get(ref k) if k == "nonexistent"));
        assert!(
            matches!(operations[4], KVSOperation::Put(ref k, ref v) if k == "key1" && v == "updated_value1")
        );
        assert!(matches!(operations[5], KVSOperation::Get(ref k) if k == "key1"));
    }

    #[test]
    fn test_parse_operation() {
        // Test PUT operation parsing
        let put_op = KVSClient::parse_operation("PUT key1 value1");
        assert!(matches!(put_op, Some(KVSOperation::Put(k, v)) if k == "key1" && v == "value1"));

        // Test GET operation parsing
        let get_op = KVSClient::parse_operation("GET key1");
        assert!(matches!(get_op, Some(KVSOperation::Get(k)) if k == "key1"));

        // Test invalid operation
        let invalid_op = KVSClient::parse_operation("INVALID command");
        assert!(invalid_op.is_none());
    }

    #[test]
    fn test_format_operation() {
        let put_op = KVSOperation::Put("key1".to_string(), "value1".to_string());
        assert_eq!(KVSClient::format_operation(&put_op), "PUT key1 value1");

        let get_op = KVSOperation::Get("key1".to_string());
        assert_eq!(KVSClient::format_operation(&get_op), "GET key1");
    }

    #[test]
    fn test_operations_from_commands() {
        let commands = vec!["PUT key1 value1", "GET key1", "INVALID", "PUT key2 value2"];
        let operations = KVSClient::operations_from_commands(&commands);

        assert_eq!(operations.len(), 3); // INVALID should be filtered out
        assert!(
            matches!(operations[0], KVSOperation::Put(ref k, ref v) if k == "key1" && v == "value1")
        );
        assert!(matches!(operations[1], KVSOperation::Get(ref k) if k == "key1"));
        assert!(
            matches!(operations[2], KVSOperation::Put(ref k, ref v) if k == "key2" && v == "value2")
        );
    }

    #[test]
    fn test_round_trip_operation_formatting() {
        let original_put = KVSOperation::Put("test_key".to_string(), "test_value".to_string());
        let formatted = KVSClient::format_operation(&original_put);
        let parsed = KVSClient::parse_operation(&formatted).unwrap();
        assert_eq!(original_put, parsed);

        let original_get = KVSOperation::Get("test_key".to_string());
        let formatted = KVSClient::format_operation(&original_get);
        let parsed = KVSClient::parse_operation(&formatted).unwrap();
        assert_eq!(original_get, parsed);
    }
}
