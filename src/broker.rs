//! In-memory MQTT message broker.

use std::collections::HashMap;

use crate::error::MqttError;
use crate::filter::TopicFilter;
use crate::message::Message;
use crate::qos::QoS;
use crate::retained::RetainedStore;
use crate::session::Session;
use crate::topic::Topic;
use crate::tree::TopicTree;
use crate::will::WillMessage;

/// In-memory MQTT message broker.
#[derive(Debug, Clone, Default)]
pub struct Broker {
    pub sessions: HashMap<String, Session>,
    pub topic_tree: TopicTree,
    pub retained: RetainedStore,
    wills: HashMap<String, WillMessage>,
    next_packet_id: u16,
}

impl Broker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            topic_tree: TopicTree::new(),
            retained: RetainedStore::new(),
            wills: HashMap::new(),
            next_packet_id: 1,
        }
    }

    /// Handle a client connection.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::AlreadyConnected` if the client is already
    /// connected and `clean_session` is false.
    pub fn handle_connect(
        &mut self,
        client_id: String,
        clean_session: bool,
        will: Option<WillMessage>,
    ) -> Result<bool, MqttError> {
        let session_present;
        if clean_session {
            if let Some(old_session) = self.sessions.remove(&client_id) {
                for filter in old_session.subscriptions.keys() {
                    self.topic_tree.unsubscribe(&client_id, filter);
                }
            }
            self.sessions
                .insert(client_id.clone(), Session::new(client_id.clone()));
            session_present = false;
        } else if self.sessions.contains_key(&client_id) {
            session_present = true;
        } else {
            self.sessions
                .insert(client_id.clone(), Session::new(client_id.clone()));
            session_present = false;
        }

        if let Some(w) = will {
            self.wills.insert(client_id, w);
        }

        Ok(session_present)
    }

    /// Handle a client disconnect. If `unexpected` is true, the will message is published.
    pub fn handle_disconnect(&mut self, client_id: &str, unexpected: bool) {
        if unexpected {
            if let Some(will) = self.wills.remove(client_id) {
                let _ = self.publish(&will.topic, &will.payload, will.qos, will.retain);
            }
        } else {
            self.wills.remove(client_id);
        }
    }

    /// Subscribe a connected client to a filter.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::NotConnected` if the client has no session.
    pub fn subscribe(
        &mut self,
        client_id: &str,
        filter: &TopicFilter,
        qos: QoS,
    ) -> Result<(), MqttError> {
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or_else(|| MqttError::NotConnected(client_id.to_owned()))?;
        session.add_subscription(filter.clone(), qos);
        self.topic_tree.subscribe(client_id, filter, qos);

        let retained_msgs: Vec<Message> = self
            .retained
            .match_filter(filter)
            .into_iter()
            .cloned()
            .collect();
        if let Some(session) = self.sessions.get_mut(client_id) {
            for msg in retained_msgs {
                session.queue_message(msg);
            }
        }

        Ok(())
    }

    /// Unsubscribe a connected client from a filter.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::NotConnected` if the client has no session.
    /// Returns `MqttError::SubscriptionNotFound` if the subscription does not exist.
    pub fn unsubscribe(&mut self, client_id: &str, filter: &TopicFilter) -> Result<(), MqttError> {
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or_else(|| MqttError::NotConnected(client_id.to_string()))?;
        if !session.remove_subscription(filter) {
            return Err(MqttError::SubscriptionNotFound);
        }
        self.topic_tree.unsubscribe(client_id, filter);
        Ok(())
    }

    /// Publish a message to all matching subscribers.
    ///
    /// # Errors
    ///
    /// Returns `MqttError::InvalidTopic` if the topic is invalid.
    ///
    /// # Panics
    ///
    /// Panics if a retained message with `QoS` > 0 fails to allocate a packet ID.
    pub fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<Vec<String>, MqttError> {
        let validated = Topic::new(topic)?;

        if retain {
            let packet_id = if qos == QoS::AtMostOnce {
                None
            } else {
                Some(self.allocate_packet_id())
            };
            let retain_msg =
                Message::new(topic.to_owned(), payload.to_owned(), qos, true, packet_id)
                    .expect("packet_id set for qos > 0");
            self.retained.store(retain_msg);
        }

        let subscribers = self.topic_tree.match_subscribers(&validated);
        let mut delivered_to = Vec::new();

        for (client_id, sub_qos) in subscribers {
            let effective_qos = if (qos.as_u8()) < sub_qos.as_u8() {
                qos
            } else {
                sub_qos
            };
            let packet_id = if effective_qos == QoS::AtMostOnce {
                None
            } else {
                Some(self.allocate_packet_id())
            };
            if let Ok(msg) = Message::new(
                topic.to_owned(),
                payload.to_owned(),
                effective_qos,
                false,
                packet_id,
            ) {
                if let Some(session) = self.sessions.get_mut(&client_id) {
                    session.queue_message(msg);
                    delivered_to.push(client_id);
                }
            }
        }

        Ok(delivered_to)
    }

    const fn allocate_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }

    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}
