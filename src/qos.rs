//! Quality of Service levels.

use crate::error::MqttError;

/// MQTT Quality of Service levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    /// Convert a `u8` to `QoS`.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidQoS` if the value is not 0, 1, or 2.
    pub const fn from_u8(value: u8) -> Result<Self, MqttError> {
        match value {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            _ => Err(MqttError::InvalidQoS(value)),
        }
    }

    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}
