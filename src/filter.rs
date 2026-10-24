//! Topic filter with wildcards (`+` single-level, `#` multi-level).

use crate::error::MqttError;
use crate::topic::Topic;

/// A topic filter pattern supporting `+` (single-level) and `#` (multi-level)
/// wildcards.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicFilter {
    pattern: String,
}

impl TopicFilter {
    /// Create a new validated topic filter.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidFilter` if the pattern is invalid.
    pub fn new(pattern: &str) -> Result<Self, MqttError> {
        Self::validate(pattern)?;
        Ok(Self {
            pattern: pattern.to_string(),
        })
    }

    /// Validate a topic filter pattern.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidFilter` on invalid input.
    pub fn validate(pattern: &str) -> Result<(), MqttError> {
        if pattern.is_empty() {
            return Err(MqttError::InvalidFilter("empty filter".to_string()));
        }
        if pattern.contains('\0') {
            return Err(MqttError::InvalidFilter(
                "filter contains null character".to_string(),
            ));
        }
        let levels: Vec<&str> = pattern.split('/').collect();
        for (i, level) in levels.iter().enumerate() {
            if *level == "#" && i != levels.len() - 1 {
                return Err(MqttError::InvalidFilter(
                    "# must be the last level".to_string(),
                ));
            }
            if level.contains('#') && *level != "#" {
                return Err(MqttError::InvalidFilter(
                    "# must occupy entire level".to_string(),
                ));
            }
            if level.contains('+') && *level != "+" {
                return Err(MqttError::InvalidFilter(
                    "+ must occupy entire level".to_string(),
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Check if a topic matches this filter.
    #[must_use]
    pub fn matches_topic(&self, topic: &Topic) -> bool {
        let filter_levels: Vec<&str> = self.pattern.split('/').collect();
        let topic_levels: Vec<&str> = topic.path.split('/').collect();
        Self::levels_match(&filter_levels, &topic_levels)
    }

    fn levels_match(filter: &[&str], topic: &[&str]) -> bool {
        let mut fi = 0;
        let mut ti = 0;
        while fi < filter.len() {
            if filter[fi] == "#" {
                return true;
            }
            if ti >= topic.len() {
                return false;
            }
            if filter[fi] != "+" && filter[fi] != topic[ti] {
                return false;
            }
            fi += 1;
            ti += 1;
        }
        ti == topic.len()
    }
}
