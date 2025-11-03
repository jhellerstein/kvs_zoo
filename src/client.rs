use crate::protocol::{KVSOperation, KVSResponse};

/// A KVS client that handles operation generation and logging
///
/// This demonstrates client-side logic for a KVS system.
/// The actual networking is handled in the examples using Hydro's External interface.
pub struct KVSClient;

impl KVSClient {
    /// Generate a sequence of demo operations for testing
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

    /// Print an operation for logging purposes
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
