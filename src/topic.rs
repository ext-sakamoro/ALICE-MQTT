//! Validated MQTT topic (no wildcards).

use crate::error::MqttError;
use crate::filter::TopicFilter;

/// A validated MQTT topic (no wildcards).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    pub(crate) path: String,
}

impl Topic {
    /// Create a new validated topic.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidTopic` if the topic is empty, contains
    /// wildcards, or contains null characters.
    pub fn new(path: &str) -> Result<Self, MqttError> {
        Self::validate(path)?;
        Ok(Self {
            path: path.to_string(),
        })
    }

    /// Validate a topic string.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidTopic` on invalid input.
    pub fn validate(path: &str) -> Result<(), MqttError> {
        if path.is_empty() {
            return Err(MqttError::InvalidTopic("empty topic".to_string()));
        }
        if path.contains('\0') {
            return Err(MqttError::InvalidTopic(
                "topic contains null character".to_string(),
            ));
        }
        if path.contains('+') || path.contains('#') {
            return Err(MqttError::InvalidTopic(
                "topic must not contain wildcards".to_string(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Parse the topic into levels split by `/`.
    #[must_use]
    pub fn levels(&self) -> Vec<&str> {
        self.path.split('/').collect()
    }

    /// Check if this topic matches a wildcard filter pattern.
    #[must_use]
    pub fn matches_filter(&self, filter: &TopicFilter) -> bool {
        filter.matches_topic(self)
    }
}
