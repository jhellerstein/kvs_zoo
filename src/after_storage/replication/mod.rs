//! Replication strategies (after-storage)

pub mod broadcast;
pub mod gossip;
pub use broadcast::*;
pub use gossip::*;
