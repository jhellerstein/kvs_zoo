use crate::protocol::KVSOperation;
use crate::vector_clock::VectorClock;
use lattices::{DomPair, set_union::SetUnionHashSet};
use std::collections::HashSet;

/// Causal value: vector clock for causality + a set of strings that merges by union
pub type CausalString = DomPair<VectorClock, SetUnionHashSet<String>>;

/// Generate demo operations with vector-clock-stamped values
pub fn generate_causal_operations() -> Vec<KVSOperation<CausalString>> {
    // Create vector clocks for different "nodes" (simulating distributed writes)
    let mut vc1 = VectorClock::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VectorClock::new();
    vc2.bump("node2".to_string());

    let mut vc3 = VectorClock::new();
    vc3.bump("node3".to_string());

    // Create operations with causal values (keys chosen to likely map to different shards)
    vec![
        // Concurrent writes to the same key (merge via SetUnion)
        KVSOperation::Put(
            "alpha".to_string(),
            DomPair::new(vc1.clone(), SetUnionHashSet::new(HashSet::from(["a1".to_string()]))),
        ),
        KVSOperation::Put(
            "alpha".to_string(),
            DomPair::new(vc2.clone(), SetUnionHashSet::new(HashSet::from(["a2".to_string()]))),
        ),
        // Independent write to a different key
        KVSOperation::Put(
            "beta".to_string(),
            DomPair::new(vc3.clone(), SetUnionHashSet::new(HashSet::from(["b1".to_string()]))),
        ),
        // Reads
        KVSOperation::Get("alpha".to_string()),
        KVSOperation::Get("beta".to_string()),
        KVSOperation::Get("nonexistent".to_string()),
    ]
}

/// Log an operation for demo purposes
pub fn log_operation(op: &KVSOperation<CausalString>) {
    match op {
        KVSOperation::Put(k, v) => {
            let (vc, set) = v.as_reveal_ref();
            let values: Vec<_> = set.as_reveal_ref().iter().collect();
            println!("Client: PUT {} => {:?} (vector clock: {:?})", k, values, vc);
        }
        KVSOperation::Get(k) => {
            println!("Client: GET {}", k);
        }
    }
}
