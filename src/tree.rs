//! Trie-based topic subscription tree.

use std::collections::HashMap;

use crate::filter::TopicFilter;
use crate::qos::QoS;
use crate::topic::Topic;

#[derive(Debug, Clone)]
struct TrieNode {
    children: HashMap<String, Self>,
    subscribers: HashMap<String, QoS>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            subscribers: HashMap::new(),
        }
    }
}

/// Trie-based topic tree for efficient subscriber matching.
#[derive(Debug, Clone)]
pub struct TopicTree {
    root: TrieNode,
}

impl Default for TopicTree {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicTree {
    #[must_use]
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
        }
    }

    pub fn subscribe(&mut self, client_id: &str, filter: &TopicFilter, qos: QoS) {
        let levels: Vec<&str> = filter.pattern().split('/').collect();
        let mut node = &mut self.root;
        for level in levels {
            node = node
                .children
                .entry(level.to_string())
                .or_insert_with(TrieNode::new);
        }
        node.subscribers.insert(client_id.to_string(), qos);
    }

    /// Unsubscribe a client. Returns `true` if the subscription existed.
    pub fn unsubscribe(&mut self, client_id: &str, filter: &TopicFilter) -> bool {
        let levels: Vec<&str> = filter.pattern().split('/').collect();
        Self::unsubscribe_recursive(&mut self.root, client_id, &levels, 0)
    }

    fn unsubscribe_recursive(
        node: &mut TrieNode,
        client_id: &str,
        levels: &[&str],
        depth: usize,
    ) -> bool {
        if depth == levels.len() {
            return node.subscribers.remove(client_id).is_some();
        }
        let level = levels[depth].to_string();
        if let Some(child) = node.children.get_mut(&level) {
            let found = Self::unsubscribe_recursive(child, client_id, levels, depth + 1);
            if child.subscribers.is_empty() && child.children.is_empty() {
                node.children.remove(&level);
            }
            found
        } else {
            false
        }
    }

    /// Find all subscribers matching a topic.
    #[must_use]
    pub fn match_subscribers(&self, topic: &Topic) -> Vec<(String, QoS)> {
        let levels: Vec<&str> = topic.path().split('/').collect();
        let mut results = Vec::new();
        Self::match_recursive(&self.root, &levels, 0, &mut results);
        results
    }

    fn match_recursive(
        node: &TrieNode,
        levels: &[&str],
        depth: usize,
        results: &mut Vec<(String, QoS)>,
    ) {
        if let Some(hash_node) = node.children.get("#") {
            for (client_id, qos) in &hash_node.subscribers {
                results.push((client_id.clone(), *qos));
            }
        }

        if depth == levels.len() {
            for (client_id, qos) in &node.subscribers {
                results.push((client_id.clone(), *qos));
            }
            return;
        }

        let level = levels[depth];

        if let Some(child) = node.children.get(level) {
            Self::match_recursive(child, levels, depth + 1, results);
        }

        if let Some(child) = node.children.get("+") {
            Self::match_recursive(child, levels, depth + 1, results);
        }
    }
}
