#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

//! Lightweight `PubSub` protocol — `IoT` messaging optimized via topic tree SDF search.

pub mod broker;
pub mod client;
pub mod error;
pub mod filter;
pub mod flags;
pub mod message;
pub mod packet;
pub mod prelude;
pub mod qos;
pub mod retained;
pub mod session;
pub mod topic;
pub mod tree;
pub mod will;
pub mod wire;

#[cfg(test)]
mod integration_tests;

// Backward-compat re-exports.
pub use crate::broker::*;
pub use crate::client::*;
pub use crate::error::*;
pub use crate::filter::*;
pub use crate::flags::*;
pub use crate::message::*;
pub use crate::packet::*;
pub use crate::qos::*;
pub use crate::retained::*;
pub use crate::session::*;
pub use crate::topic::*;
pub use crate::tree::*;
pub use crate::will::*;
