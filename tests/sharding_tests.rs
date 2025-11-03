/// Tests for sharding hash calculation consistency
use kvs_zoo::sharded::calculate_shard_id;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[test]
fn test_shard_calculation_consistency() {
    // Test that the same key always hashes to the same shard
    let key = "test_key";
    let num_shards = 5;

    let shard1 = calculate_shard_id(key, num_shards);
    let shard2 = calculate_shard_id(key, num_shards);
    let shard3 = calculate_shard_id(key, num_shards);

    assert_eq!(shard1, shard2);
    assert_eq!(shard2, shard3);
    assert!(shard1 < num_shards as u32);
}

#[test]
fn test_shard_distribution() {
    // Test that different keys map to different shards (not a guarantee, but likely)
    let num_shards = 5;
    let keys = vec![
        "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10",
    ];

    let mut shard_counts = vec![0; num_shards];

    for key in keys {
        let shard = calculate_shard_id(key, num_shards);
        assert!(shard < num_shards as u32);
        shard_counts[shard as usize] += 1;
    }

    // Ensure at least some distribution (not all in one shard)
    let unique_shards = shard_counts.iter().filter(|&&count| count > 0).count();
    assert!(
        unique_shards > 1,
        "Keys should distribute across multiple shards"
    );
}

#[test]
fn test_shard_boundary_conditions() {
    // Test with 1 shard (all keys go to shard 0)
    assert_eq!(calculate_shard_id("any_key", 1), 0);

    // Test with 2 shards
    let key = "test";
    let shard = calculate_shard_id(key, 2);
    assert!(shard < 2);

    // Test with large number of shards
    let large_shards = 1000;
    let shard = calculate_shard_id("test", large_shards);
    assert!(shard < large_shards as u32);
}

#[test]
fn test_hash_function_properties() {
    // Test that the hash function used is DefaultHasher
    // This ensures consistency with the calculate_shard_id implementation

    let key = "test_key";

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();

    let num_shards = 10;
    let expected_shard = (hash % num_shards as u64) as u32;
    let actual_shard = calculate_shard_id(key, num_shards);

    assert_eq!(expected_shard, actual_shard);
}

#[test]
fn test_empty_string_sharding() {
    // Test that empty string can be sharded
    let num_shards = 5;
    let shard = calculate_shard_id("", num_shards);
    assert!(shard < num_shards as u32);

    // Should be consistent
    assert_eq!(shard, calculate_shard_id("", num_shards));
}

#[test]
fn test_unicode_key_sharding() {
    // Test that unicode keys work correctly
    let num_shards = 5;
    let keys = vec!["hello", "こんにちは", "你好", "مرحبا", "🚀"];

    for key in keys {
        let shard = calculate_shard_id(key, num_shards);
        assert!(shard < num_shards as u32);
        // Consistency check
        assert_eq!(shard, calculate_shard_id(key, num_shards));
    }
}

#[test]
fn test_similar_keys_different_shards() {
    // Test that similar keys might hash to different shards
    // This is not guaranteed but highly likely with a good hash function
    let num_shards = 10;

    let key1 = "user_123";
    let key2 = "user_124";
    let key3 = "user_125";

    let shard1 = calculate_shard_id(key1, num_shards);
    let shard2 = calculate_shard_id(key2, num_shards);
    let shard3 = calculate_shard_id(key3, num_shards);

    // All should be valid shards
    assert!(shard1 < num_shards as u32);
    assert!(shard2 < num_shards as u32);
    assert!(shard3 < num_shards as u32);

    // With a good hash function and 10 shards, at least one should differ
    // (This test might occasionally fail with very bad luck, but extremely unlikely)
    let all_same = shard1 == shard2 && shard2 == shard3;
    assert!(
        !all_same,
        "Similar keys should likely hash to different shards"
    );
}
