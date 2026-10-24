//! MQTT CONNECT packet flags.

use crate::error::MqttError;
use crate::qos::QoS;

/// MQTT CONNECT packet flags.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ConnectFlags {
    pub clean_session: bool,
    pub will_flag: bool,
    pub will_qos: QoS,
    pub will_retain: bool,
    pub username: bool,
    pub password: bool,
}

impl ConnectFlags {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            clean_session: true,
            will_flag: false,
            will_qos: QoS::AtMostOnce,
            will_retain: false,
            username: false,
            password: false,
        }
    }

    /// Encode flags into a single byte.
    #[must_use]
    pub const fn to_byte(&self) -> u8 {
        let mut byte = 0u8;
        if self.clean_session {
            byte |= 0x02;
        }
        if self.will_flag {
            byte |= 0x04;
        }
        byte |= (self.will_qos.as_u8()) << 3;
        if self.will_retain {
            byte |= 0x20;
        }
        if self.password {
            byte |= 0x40;
        }
        if self.username {
            byte |= 0x80;
        }
        byte
    }

    /// Decode flags from a single byte.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidQoS` if the will `QoS` bits are invalid.
    pub fn from_byte(byte: u8) -> Result<Self, MqttError> {
        let will_qos_val = (byte >> 3) & 0x03;
        Ok(Self {
            clean_session: byte & 0x02 != 0,
            will_flag: byte & 0x04 != 0,
            will_qos: QoS::from_u8(will_qos_val)?,
            will_retain: byte & 0x20 != 0,
            password: byte & 0x40 != 0,
            username: byte & 0x80 != 0,
        })
    }
}

impl Default for ConnectFlags {
    fn default() -> Self {
        Self::new()
    }
}
