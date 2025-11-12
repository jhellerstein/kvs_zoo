//! Operation ordering strategies (before-storage)

pub mod paxos_core;
pub mod sequence_payloads;
pub mod slot_enforcer;
pub mod paxos;

pub use paxos::{PaxosConfig, PaxosDispatcher};
pub use paxos_core::{Acceptor, PaxosPayload, Proposer};
pub use sequence_payloads::SequencedPayload;
pub use slot_enforcer::SlotOrderEnforcer;
