//! Validated MQTT client identifier.

use crate::error::MqttError;

/// A validated MQTT client identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId {
    value: String,
}

impl ClientId {
    /// Create a new validated client ID.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidClientId` if the ID is empty, longer than
    /// 23 characters, or contains non-alphanumeric characters.
    pub fn new(id: &str) -> Result<Self, MqttError> {
        Self::validate(id)?;
        Ok(Self {
            value: id.to_string(),
        })
    }

    /// Validate a client ID string.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidClientId` on invalid input.
    pub fn validate(id: &str) -> Result<(), MqttError> {
        if id.is_empty() {
            return Err(MqttError::InvalidClientId("empty client id".to_string()));
        }
        if id.len() > 23 {
            return Err(MqttError::InvalidClientId(format!(
                "client id too long: {} chars (max 23)",
                id.len()
            )));
        }
        if !id.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(MqttError::InvalidClientId(
                "client id must be alphanumeric".to_string(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }
}
