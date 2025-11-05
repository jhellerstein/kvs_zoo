/// Tests for KVS protocol operations (KVSOperation, KVSResponse)
use kvs_zoo::protocol::{KVSOperation, KVSResponse};

#[test]
fn test_operation_creation() {
    // Test PUT operation
    let put_op = KVSOperation::Put("key1".to_string(), "value1".to_string());
    assert_eq!(put_op, KVSOperation::Put("key1".to_string(), "value1".to_string()));

    // Test GET operation
    let get_op: KVSOperation<String> = KVSOperation::Get("key1".to_string());
    assert_eq!(get_op, KVSOperation::Get("key1".to_string()));
}

#[test]
fn test_operation_pattern_matching() {
    let put_op = KVSOperation::Put("key1".to_string(), "value1".to_string());
    match put_op {
        KVSOperation::Put(key, value) => {
            assert_eq!(key, "key1");
            assert_eq!(value, "value1");
        }
        KVSOperation::Get(_) => panic!("Expected PUT operation"),
    }

    let get_op: KVSOperation<String> = KVSOperation::Get("key1".to_string());
    match get_op {
        KVSOperation::Get(key) => assert_eq!(key, "key1"),
        KVSOperation::Put(_, _) => panic!("Expected GET operation"),
    }
}

#[test]
fn test_response_creation() {
    // Test PutOk response
    let put_response: KVSResponse<String> = KVSResponse::PutOk;
    assert_eq!(put_response, KVSResponse::PutOk);

    // Test GetResult responses
    let get_found = KVSResponse::GetResult(Some("value".to_string()));
    assert_eq!(get_found, KVSResponse::GetResult(Some("value".to_string())));

    let get_not_found: KVSResponse<String> = KVSResponse::GetResult(None);
    assert_eq!(get_not_found, KVSResponse::GetResult(None));
}

#[test]
fn test_response_pattern_matching() {
    let get_response = KVSResponse::GetResult(Some("value".to_string()));
    match get_response {
        KVSResponse::GetResult(Some(value)) => assert_eq!(value, "value"),
        KVSResponse::GetResult(None) => panic!("Expected found value"),
        KVSResponse::PutOk => panic!("Expected GET response"),
    }

    let not_found_response: KVSResponse<String> = KVSResponse::GetResult(None);
    match not_found_response {
        KVSResponse::GetResult(None) => {}, // Expected
        KVSResponse::GetResult(Some(_)) => panic!("Expected not found"),
        KVSResponse::PutOk => panic!("Expected GET response"),
    }
}
