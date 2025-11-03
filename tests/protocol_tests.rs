/// Tests for KVS protocol operations (KVSOperation, KVSResponse)
use kvs_zoo::protocol::{KVSOperation, KVSResponse};

#[test]
fn test_operation_from_command() {
    assert_eq!(
        KVSOperation::<String>::from_command("PUT key1 value1"),
        Some(KVSOperation::Put("key1".to_string(), "value1".to_string()))
    );
    assert_eq!(
        KVSOperation::<String>::from_command("GET key1"),
        Some(KVSOperation::Get("key1".to_string()))
    );
    assert_eq!(KVSOperation::<String>::from_command("INVALID"), None);
    assert_eq!(KVSOperation::<String>::from_command("DELETE key1"), None); // DELETE no longer supported
}

#[test]
fn test_operation_to_command() {
    let put_op = KVSOperation::Put("key1".to_string(), "value1".to_string());
    assert_eq!(put_op.to_command(), "PUT key1 value1");

    let get_op: KVSOperation<String> = KVSOperation::Get("key1".to_string());
    assert_eq!(get_op.to_command(), "GET key1");
}

#[test]
fn test_operation_properties() {
    let put_op = KVSOperation::Put("key1".to_string(), "value1".to_string());
    assert_eq!(put_op.key(), "key1");
    assert!(!put_op.is_read_only());
    assert!(put_op.is_write());

    let get_op: KVSOperation<String> = KVSOperation::Get("key1".to_string());
    assert_eq!(get_op.key(), "key1");
    assert!(get_op.is_read_only());
    assert!(!get_op.is_write());
}

#[test]
fn test_response_properties() {
    assert!(KVSResponse::<String>::PutOk.is_success());
    assert!(KVSResponse::GetResult(Some("value".to_string())).is_success());
    assert!(KVSResponse::<String>::GetResult(None).is_success());

    let get_response = KVSResponse::GetResult(Some("value".to_string()));
    assert_eq!(get_response.get_value(), Some(&"value".to_string()));

    let not_found_response: KVSResponse<String> = KVSResponse::GetResult(None);
    assert_eq!(not_found_response.get_value(), None);
}
