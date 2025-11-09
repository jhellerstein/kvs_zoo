//! Operation ordering strategies
//!
//! This module contains dispatchers that establish a total order over operations,
//! ensuring consistency guarantees like linearizability.

pub mod paxos;
pub mod paxos_core;

pub use paxos::PaxosInterceptor;
pub use paxos_core::PaxosConfig;
