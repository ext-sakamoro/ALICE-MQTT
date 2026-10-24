//! Per-client session state.

use std::collections::HashMap;

use crate::filter::TopicFilter;
use crate::message::Message;
use crate::qos::QoS;

/// Per-client session state.
#[derive(Debug, Clone)]
pub struct Session {
    pub client_id: String,
    pub subscriptions: HashMap<TopicFilter, QoS>,
    pub pending_messages: Vec<Message>,
}

impl Session {
    #[must_use]
    pub fn new(client_id: String) -> Self {
        Self {
            client_id,
            subscriptions: HashMap::new(),
            pending_messages: Vec::new(),
        }
    }

    pub fn add_subscription(&mut self, filter: TopicFilter, qos: QoS) {
        self.subscriptions.insert(filter, qos);
    }

    pub fn remove_subscription(&mut self, filter: &TopicFilter) -> bool {
        self.subscriptions.remove(filter).is_some()
    }

    pub fn queue_message(&mut self, message: Message) {
        self.pending_messages.push(message);
    }

    pub fn drain_pending(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.pending_messages)
    }
}
