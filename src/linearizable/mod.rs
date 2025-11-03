//! # Linearizable KVS Implementation
//! 
//! This module contains a linearizable (strongly consistent) key-value store
//! implementation using Paxos consensus. This provides the strongest consistency
//! guarantee where all operations appear to take effect atomically at a single
//! point in time between their invocation and response.
//! 
//! ## Architecture
//! 
//! Unlike the unified API examples (local, replicated, sharded), this implementation
//! uses a more complex multi-cluster architecture:
//! 
//! ```text
//! External Client
//!       ↓
//!   Proxy Process  
//!       ↓
//! Paxos Cluster (Proposers + Acceptors)
//!       ↓
//!   Replica Cluster
//! (Apply sequenced operations)
//! ```
//! 
//! ## Components
//! 
//! - **`paxos.rs`** - Core Multi-Paxos consensus implementation
//! - **`paxos_with_client.rs`** - Client integration for external processes
//! - **`paxos_router.rs`** - Router using Paxos for total ordering
//! - **`linearizable.rs`** - Linearizable KVS built on Paxos
//! 
//! ## Use Cases
//! 
//! - Banking and financial systems
//! - Critical infrastructure requiring strong consistency
//! - Systems where correctness is more important than performance
//! 
//! ## Trade-offs
//! 
//! - ✅ **Linearizability**: Strongest consistency guarantee
//! - ✅ **Fault tolerance**: Tolerates f failures with 2f+1 nodes
//! - ❌ **Latency**: Higher latency due to consensus overhead
//! - ❌ **Complexity**: More complex than eventual/causal consistency
//! 
//! ## Note on Architecture
//! 
//! This implementation intentionally does NOT use the unified driver API
//! from `examples/driver/mod.rs` because consensus algorithms require
//! fundamentally different abstractions than simple replication patterns.

pub mod paxos;
pub mod paxos_with_client;
pub mod paxos_router;
pub mod linearizable;

// Re-export main types for convenience
pub use linearizable::StringLinearizableKVSServer;
pub use paxos::{Acceptor, CorePaxos, PaxosConfig, Proposer};
pub use paxos_router::PaxosRouter;
pub use paxos_with_client::PaxosLike;