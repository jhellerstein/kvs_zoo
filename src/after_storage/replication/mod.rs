//! Replication strategies (after-storage)

pub mod broadcast;
pub mod broadcast_overwrite;
pub mod gossip;
pub use broadcast::*;
pub use broadcast_overwrite::*;
pub use gossip::*;
