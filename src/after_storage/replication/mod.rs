//! Replication strategies and wrappers (after-storage)

pub mod broadcast;
pub mod gossip;
pub mod slotted; // slot-aware wrappers kept separate to declutter common case

pub use broadcast::*;
pub use gossip::*;
pub use slotted::{LogBasedDelivery, SlottedBroadcastReplication};
