//! Operation ordering strategies
//!
//! This module contains dispatchers that establish a total order over operations,
//! ensuring consistency guarantees like linearizability.

pub mod paxos;
pub mod paxos_core;
pub mod sequence_payloads;
pub mod slot_enforcer;

pub use paxos::{PaxosConfig, PaxosDispatcher};
pub use paxos_core::{Acceptor, PaxosPayload, Proposer};
pub use sequence_payloads::SequencedPayload;
pub use slot_enforcer::SlotOrderEnforcer;
