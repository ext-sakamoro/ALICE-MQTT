//! MQTT error types.

use std::fmt;

/// Errors returned by MQTT operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttError {
    InvalidTopic(String),
    InvalidFilter(String),
    InvalidClientId(String),
    InvalidQoS(u8),
    NotConnected(String),
    AlreadyConnected(String),
    SubscriptionNotFound,
    PacketIdRequired,
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic(t) => write!(f, "invalid topic: {t}"),
            Self::InvalidFilter(t) => write!(f, "invalid filter: {t}"),
            Self::InvalidClientId(id) => write!(f, "invalid client id: {id}"),
            Self::InvalidQoS(q) => write!(f, "invalid QoS value: {q}"),
            Self::NotConnected(id) => write!(f, "client not connected: {id}"),
            Self::AlreadyConnected(id) => write!(f, "client already connected: {id}"),
            Self::SubscriptionNotFound => write!(f, "subscription not found"),
            Self::PacketIdRequired => write!(f, "packet id required for QoS > 0"),
        }
    }
}
