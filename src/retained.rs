//! Retained message store.

use std::collections::HashMap;

use crate::filter::TopicFilter;
use crate::message::Message;
use crate::topic::Topic;

/// Stores the last retained message for each topic.
#[derive(Debug, Clone, Default)]
pub struct RetainedStore {
    messages: HashMap<String, Message>,
}

impl RetainedStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            messages: HashMap::new(),
        }
    }

    /// Store a retained message. Empty payload clears the retained message.
    pub fn store(&mut self, message: Message) {
        if message.payload.is_empty() {
            self.messages.remove(&message.topic);
        } else {
            self.messages.insert(message.topic.clone(), message);
        }
    }

    #[must_use]
    pub fn get(&self, topic: &str) -> Option<&Message> {
        self.messages.get(topic)
    }

    pub fn clear(&mut self, topic: &str) {
        self.messages.remove(topic);
    }

    /// Get all retained messages matching a filter.
    #[must_use]
    pub fn match_filter(&self, filter: &TopicFilter) -> Vec<&Message> {
        self.messages
            .values()
            .filter(|msg| Topic::new(&msg.topic).is_ok_and(|topic| filter.matches_topic(&topic)))
            .collect()
    }
}
