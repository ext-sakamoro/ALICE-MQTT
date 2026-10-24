//! MQTT publish message.

use crate::error::MqttError;
use crate::qos::QoS;

/// An MQTT publish message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    pub packet_id: Option<u16>,
}

impl Message {
    /// Create a new message.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::PacketIdRequired` if `QoS` > 0 and `packet_id` is `None`.
    pub fn new(
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
        packet_id: Option<u16>,
    ) -> Result<Self, MqttError> {
        if qos != QoS::AtMostOnce && packet_id.is_none() {
            return Err(MqttError::PacketIdRequired);
        }
        Ok(Self {
            topic,
            payload,
            qos,
            retain,
            packet_id,
        })
    }
}
