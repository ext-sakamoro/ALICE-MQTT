//! Last-will-and-testament message.

use crate::qos::QoS;

/// A last-will-and-testament message published on unexpected disconnect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WillMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}
