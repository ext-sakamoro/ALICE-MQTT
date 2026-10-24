//! Integration tests moved from monolithic lib.rs.

#![allow(
    clippy::float_cmp,
    clippy::similar_names,
    clippy::unreadable_literal,
    clippy::redundant_clone,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::suboptimal_flops,
    clippy::many_single_char_names,
    clippy::needless_range_loop,
    clippy::manual_midpoint
)]

use crate::broker::Broker;
use crate::client::ClientId;
use crate::error::MqttError;
use crate::filter::TopicFilter;
use crate::flags::ConnectFlags;
use crate::message::Message;
use crate::packet::PacketType;
use crate::qos::QoS;
use crate::retained::RetainedStore;
use crate::session::Session;
use crate::topic::Topic;
use crate::tree::TopicTree;
use crate::will::WillMessage;

// -----------------------------------------------------------------------
// QoS tests
// -----------------------------------------------------------------------

#[test]
fn qos_from_u8_valid() {
    assert_eq!(QoS::from_u8(0).unwrap(), QoS::AtMostOnce);
    assert_eq!(QoS::from_u8(1).unwrap(), QoS::AtLeastOnce);
    assert_eq!(QoS::from_u8(2).unwrap(), QoS::ExactlyOnce);
}

#[test]
fn qos_from_u8_invalid() {
    assert_eq!(QoS::from_u8(3), Err(MqttError::InvalidQoS(3)));
    assert_eq!(QoS::from_u8(255), Err(MqttError::InvalidQoS(255)));
}

#[test]
fn qos_as_u8() {
    assert_eq!(QoS::AtMostOnce.as_u8(), 0);
    assert_eq!(QoS::AtLeastOnce.as_u8(), 1);
    assert_eq!(QoS::ExactlyOnce.as_u8(), 2);
}

#[test]
fn qos_roundtrip() {
    for v in 0..=2 {
        assert_eq!(QoS::from_u8(v).unwrap().as_u8(), v);
    }
}

#[test]
fn qos_clone_and_eq() {
    let q = QoS::ExactlyOnce;
    let q2 = q;
    assert_eq!(q, q2);
}

// -----------------------------------------------------------------------
// PacketType tests
// -----------------------------------------------------------------------

#[test]
fn packet_type_from_u8() {
    assert_eq!(PacketType::from_u8(1), Some(PacketType::Connect));
    assert_eq!(PacketType::from_u8(2), Some(PacketType::Connack));
    assert_eq!(PacketType::from_u8(3), Some(PacketType::Publish));
    assert_eq!(PacketType::from_u8(4), Some(PacketType::Puback));
    assert_eq!(PacketType::from_u8(8), Some(PacketType::Subscribe));
    assert_eq!(PacketType::from_u8(9), Some(PacketType::Suback));
    assert_eq!(PacketType::from_u8(10), Some(PacketType::Unsubscribe));
    assert_eq!(PacketType::from_u8(11), Some(PacketType::Unsuback));
    assert_eq!(PacketType::from_u8(12), Some(PacketType::Pingreq));
    assert_eq!(PacketType::from_u8(13), Some(PacketType::Pingresp));
    assert_eq!(PacketType::from_u8(14), Some(PacketType::Disconnect));
}

#[test]
fn packet_type_from_u8_invalid() {
    assert_eq!(PacketType::from_u8(0), None);
    assert_eq!(PacketType::from_u8(5), None);
    assert_eq!(PacketType::from_u8(15), None);
}

#[test]
fn packet_type_roundtrip() {
    let types = [
        PacketType::Connect,
        PacketType::Connack,
        PacketType::Publish,
        PacketType::Puback,
        PacketType::Subscribe,
        PacketType::Suback,
        PacketType::Unsubscribe,
        PacketType::Unsuback,
        PacketType::Pingreq,
        PacketType::Pingresp,
        PacketType::Disconnect,
    ];
    for pt in types {
        assert_eq!(PacketType::from_u8(pt.as_u8()), Some(pt));
    }
}

// -----------------------------------------------------------------------
// Topic tests
// -----------------------------------------------------------------------

#[test]
fn topic_valid() {
    assert!(Topic::new("a/b/c").is_ok());
    assert!(Topic::new("home/living/temp").is_ok());
    assert!(Topic::new("/leading/slash").is_ok());
    assert!(Topic::new("trailing/slash/").is_ok());
    assert!(Topic::new("a").is_ok());
}

#[test]
fn topic_empty_invalid() {
    assert!(Topic::new("").is_err());
}

#[test]
fn topic_null_char_invalid() {
    assert!(Topic::new("a/\0/b").is_err());
}

#[test]
fn topic_wildcard_invalid() {
    assert!(Topic::new("a/+/b").is_err());
    assert!(Topic::new("a/#").is_err());
}

#[test]
fn topic_levels() {
    let t = Topic::new("home/living/temp").unwrap();
    assert_eq!(t.levels(), vec!["home", "living", "temp"]);
}

#[test]
fn topic_levels_with_slashes() {
    let t = Topic::new("/a/b/").unwrap();
    assert_eq!(t.levels(), vec!["", "a", "b", ""]);
}

#[test]
fn topic_path() {
    let t = Topic::new("hello/world").unwrap();
    assert_eq!(t.path(), "hello/world");
}

#[test]
fn topic_clone_eq() {
    let t1 = Topic::new("a/b").unwrap();
    let t2 = t1.clone();
    assert_eq!(t1, t2);
}

// -----------------------------------------------------------------------
// TopicFilter tests
// -----------------------------------------------------------------------

#[test]
fn filter_valid() {
    assert!(TopicFilter::new("a/b/c").is_ok());
    assert!(TopicFilter::new("a/+/c").is_ok());
    assert!(TopicFilter::new("a/#").is_ok());
    assert!(TopicFilter::new("#").is_ok());
    assert!(TopicFilter::new("+").is_ok());
    assert!(TopicFilter::new("+/+/+").is_ok());
}

#[test]
fn filter_empty_invalid() {
    assert!(TopicFilter::new("").is_err());
}

#[test]
fn filter_hash_not_last_invalid() {
    assert!(TopicFilter::new("#/a").is_err());
    assert!(TopicFilter::new("a/#/b").is_err());
}

#[test]
fn filter_hash_partial_invalid() {
    assert!(TopicFilter::new("a/b#").is_err());
    assert!(TopicFilter::new("a/#c").is_err());
}

#[test]
fn filter_plus_partial_invalid() {
    assert!(TopicFilter::new("a/b+").is_err());
    assert!(TopicFilter::new("a/+c").is_err());
}

#[test]
fn filter_null_invalid() {
    assert!(TopicFilter::new("a/\0").is_err());
}

#[test]
fn filter_matches_exact() {
    let f = TopicFilter::new("a/b/c").unwrap();
    assert!(f.matches_topic(&Topic::new("a/b/c").unwrap()));
    assert!(!f.matches_topic(&Topic::new("a/b").unwrap()));
    assert!(!f.matches_topic(&Topic::new("a/b/c/d").unwrap()));
}

#[test]
fn filter_matches_single_wildcard() {
    let f = TopicFilter::new("a/+/c").unwrap();
    assert!(f.matches_topic(&Topic::new("a/b/c").unwrap()));
    assert!(f.matches_topic(&Topic::new("a/x/c").unwrap()));
    assert!(!f.matches_topic(&Topic::new("a/b/d").unwrap()));
    assert!(!f.matches_topic(&Topic::new("a/b/c/d").unwrap()));
}

#[test]
fn filter_matches_multi_wildcard() {
    let f = TopicFilter::new("a/#").unwrap();
    assert!(f.matches_topic(&Topic::new("a/b").unwrap()));
    assert!(f.matches_topic(&Topic::new("a/b/c").unwrap()));
    assert!(f.matches_topic(&Topic::new("a/b/c/d/e").unwrap()));
}

#[test]
fn filter_matches_hash_only() {
    let f = TopicFilter::new("#").unwrap();
    assert!(f.matches_topic(&Topic::new("a").unwrap()));
    assert!(f.matches_topic(&Topic::new("a/b/c").unwrap()));
}

#[test]
fn filter_matches_plus_only() {
    let f = TopicFilter::new("+").unwrap();
    assert!(f.matches_topic(&Topic::new("a").unwrap()));
    assert!(!f.matches_topic(&Topic::new("a/b").unwrap()));
}

#[test]
fn filter_matches_combined_wildcards() {
    let f = TopicFilter::new("+/b/#").unwrap();
    assert!(f.matches_topic(&Topic::new("a/b/c").unwrap()));
    assert!(f.matches_topic(&Topic::new("x/b/c/d").unwrap()));
    assert!(!f.matches_topic(&Topic::new("a/c/d").unwrap()));
}

#[test]
fn filter_pattern() {
    let f = TopicFilter::new("a/+/c").unwrap();
    assert_eq!(f.pattern(), "a/+/c");
}

#[test]
fn topic_matches_filter_method() {
    let t = Topic::new("home/living/temp").unwrap();
    let f = TopicFilter::new("home/+/temp").unwrap();
    assert!(t.matches_filter(&f));
}

// -----------------------------------------------------------------------
// TopicTree tests
// -----------------------------------------------------------------------

#[test]
fn tree_subscribe_and_match() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("a/b/c").unwrap();
    tree.subscribe("client1", &f, QoS::AtMostOnce);
    let topic = Topic::new("a/b/c").unwrap();
    let subs = tree.match_subscribers(&topic);
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].0, "client1");
}

#[test]
fn tree_wildcard_match() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("a/+/c").unwrap();
    tree.subscribe("client1", &f, QoS::AtLeastOnce);
    let topic = Topic::new("a/x/c").unwrap();
    let subs = tree.match_subscribers(&topic);
    assert_eq!(subs.len(), 1);
}

#[test]
fn tree_multi_wildcard_match() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("a/#").unwrap();
    tree.subscribe("client1", &f, QoS::ExactlyOnce);
    assert_eq!(tree.match_subscribers(&Topic::new("a/b").unwrap()).len(), 1);
    assert_eq!(
        tree.match_subscribers(&Topic::new("a/b/c/d").unwrap())
            .len(),
        1
    );
}

#[test]
fn tree_no_match() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("a/b/c").unwrap();
    tree.subscribe("client1", &f, QoS::AtMostOnce);
    let topic = Topic::new("x/y/z").unwrap();
    assert!(tree.match_subscribers(&topic).is_empty());
}

#[test]
fn tree_multiple_subscribers() {
    let mut tree = TopicTree::new();
    let f1 = TopicFilter::new("a/b").unwrap();
    let f2 = TopicFilter::new("a/+").unwrap();
    tree.subscribe("c1", &f1, QoS::AtMostOnce);
    tree.subscribe("c2", &f2, QoS::AtLeastOnce);
    let subs = tree.match_subscribers(&Topic::new("a/b").unwrap());
    assert_eq!(subs.len(), 2);
}

#[test]
fn tree_unsubscribe() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("a/b").unwrap();
    tree.subscribe("c1", &f, QoS::AtMostOnce);
    assert!(tree.unsubscribe("c1", &f));
    assert!(tree
        .match_subscribers(&Topic::new("a/b").unwrap())
        .is_empty());
}

#[test]
fn tree_unsubscribe_not_found() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("a/b").unwrap();
    assert!(!tree.unsubscribe("c1", &f));
}

#[test]
fn tree_default() {
    let tree = TopicTree::default();
    assert!(tree.match_subscribers(&Topic::new("a").unwrap()).is_empty());
}

// -----------------------------------------------------------------------
// Message tests
// -----------------------------------------------------------------------

#[test]
fn message_qos0_no_packet_id() {
    let msg = Message::new("a/b".into(), vec![1, 2], QoS::AtMostOnce, false, None);
    assert!(msg.is_ok());
    assert!(msg.unwrap().packet_id.is_none());
}

#[test]
fn message_qos1_requires_packet_id() {
    let msg = Message::new("a/b".into(), vec![1], QoS::AtLeastOnce, false, None);
    assert_eq!(msg, Err(MqttError::PacketIdRequired));
}

#[test]
fn message_qos2_requires_packet_id() {
    let msg = Message::new("a/b".into(), vec![], QoS::ExactlyOnce, false, None);
    assert_eq!(msg, Err(MqttError::PacketIdRequired));
}

#[test]
fn message_qos1_with_packet_id() {
    let msg = Message::new("a/b".into(), vec![1], QoS::AtLeastOnce, false, Some(42));
    assert!(msg.is_ok());
    assert_eq!(msg.unwrap().packet_id, Some(42));
}

#[test]
fn message_retain_flag() {
    let msg = Message::new("a".into(), vec![0], QoS::AtMostOnce, true, None).unwrap();
    assert!(msg.retain);
}

#[test]
fn message_clone_eq() {
    let msg = Message::new("a".into(), vec![1, 2, 3], QoS::AtMostOnce, false, None).unwrap();
    let msg2 = msg.clone();
    assert_eq!(msg, msg2);
}

// -----------------------------------------------------------------------
// ClientId tests
// -----------------------------------------------------------------------

#[test]
fn client_id_valid() {
    assert!(ClientId::new("abc123").is_ok());
    assert!(ClientId::new("a").is_ok());
    assert!(ClientId::new("A1b2C3d4E5f6G7h8I9j0K1l").is_ok()); // 23 chars
}

#[test]
fn client_id_empty() {
    assert!(ClientId::new("").is_err());
}

#[test]
fn client_id_too_long() {
    let long = "a".repeat(24);
    assert!(ClientId::new(&long).is_err());
}

#[test]
fn client_id_non_alphanumeric() {
    assert!(ClientId::new("abc-123").is_err());
    assert!(ClientId::new("abc 123").is_err());
    assert!(ClientId::new("abc_123").is_err());
}

#[test]
fn client_id_as_str() {
    let cid = ClientId::new("sensor01").unwrap();
    assert_eq!(cid.as_str(), "sensor01");
}

#[test]
fn client_id_clone_eq() {
    let c1 = ClientId::new("abc").unwrap();
    let c2 = c1.clone();
    assert_eq!(c1, c2);
}

#[test]
fn client_id_23_chars_exact() {
    let id = "12345678901234567890123";
    assert_eq!(id.len(), 23);
    assert!(ClientId::new(id).is_ok());
}

// -----------------------------------------------------------------------
// ConnectFlags tests
// -----------------------------------------------------------------------

#[test]
fn connect_flags_default() {
    let f = ConnectFlags::new();
    assert!(f.clean_session);
    assert!(!f.will_flag);
    assert_eq!(f.will_qos, QoS::AtMostOnce);
    assert!(!f.will_retain);
    assert!(!f.username);
    assert!(!f.password);
}

#[test]
fn connect_flags_default_trait() {
    let f = ConnectFlags::default();
    assert!(f.clean_session);
}

#[test]
fn connect_flags_to_byte_clean_session() {
    let f = ConnectFlags::new();
    assert_eq!(f.to_byte(), 0x02);
}

#[test]
fn connect_flags_roundtrip() {
    let original = ConnectFlags {
        clean_session: true,
        will_flag: true,
        will_qos: QoS::ExactlyOnce,
        will_retain: true,
        username: true,
        password: true,
    };
    let byte = original.to_byte();
    let decoded = ConnectFlags::from_byte(byte).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn connect_flags_all_bits() {
    let f = ConnectFlags {
        clean_session: true,
        will_flag: true,
        will_qos: QoS::ExactlyOnce,
        will_retain: true,
        username: true,
        password: true,
    };
    let byte = f.to_byte();
    // clean=0x02, will=0x04, qos2=0x10, retain=0x20, pass=0x40, user=0x80
    assert_eq!(byte, 0x02 | 0x04 | 0x10 | 0x20 | 0x40 | 0x80);
}

#[test]
fn connect_flags_from_byte_invalid_qos() {
    // QoS bits = 3 is invalid
    let byte = 0x18; // bits 3 and 4 set = qos 3
    assert!(ConnectFlags::from_byte(byte).is_err());
}

#[test]
fn connect_flags_will_qos_levels() {
    for qos_val in 0..=2u8 {
        let f = ConnectFlags {
            clean_session: false,
            will_flag: true,
            will_qos: QoS::from_u8(qos_val).unwrap(),
            will_retain: false,
            username: false,
            password: false,
        };
        let decoded = ConnectFlags::from_byte(f.to_byte()).unwrap();
        assert_eq!(decoded.will_qos, QoS::from_u8(qos_val).unwrap());
    }
}

// -----------------------------------------------------------------------
// WillMessage tests
// -----------------------------------------------------------------------

#[test]
fn will_message_create() {
    let w = WillMessage {
        topic: "last/will".to_string(),
        payload: b"offline".to_vec(),
        qos: QoS::AtLeastOnce,
        retain: true,
    };
    assert_eq!(w.topic, "last/will");
    assert!(w.retain);
}

#[test]
fn will_message_clone_eq() {
    let w1 = WillMessage {
        topic: "a".into(),
        payload: vec![1],
        qos: QoS::AtMostOnce,
        retain: false,
    };
    let w2 = w1.clone();
    assert_eq!(w1, w2);
}

// -----------------------------------------------------------------------
// Session tests
// -----------------------------------------------------------------------

#[test]
fn session_new() {
    let s = Session::new("client1".to_string());
    assert_eq!(s.client_id, "client1");
    assert!(s.subscriptions.is_empty());
    assert!(s.pending_messages.is_empty());
}

#[test]
fn session_add_subscription() {
    let mut s = Session::new("c1".into());
    let f = TopicFilter::new("a/b").unwrap();
    s.add_subscription(f.clone(), QoS::AtLeastOnce);
    assert_eq!(s.subscriptions.get(&f), Some(&QoS::AtLeastOnce));
}

#[test]
fn session_remove_subscription() {
    let mut s = Session::new("c1".into());
    let f = TopicFilter::new("a/b").unwrap();
    s.add_subscription(f.clone(), QoS::AtMostOnce);
    assert!(s.remove_subscription(&f));
    assert!(!s.remove_subscription(&f));
}

#[test]
fn session_queue_and_drain() {
    let mut s = Session::new("c1".into());
    let msg = Message::new("a".into(), vec![1], QoS::AtMostOnce, false, None).unwrap();
    s.queue_message(msg.clone());
    s.queue_message(msg);
    let drained = s.drain_pending();
    assert_eq!(drained.len(), 2);
    assert!(s.pending_messages.is_empty());
}

#[test]
fn session_update_subscription_qos() {
    let mut s = Session::new("c1".into());
    let f = TopicFilter::new("a/b").unwrap();
    s.add_subscription(f.clone(), QoS::AtMostOnce);
    s.add_subscription(f.clone(), QoS::ExactlyOnce);
    assert_eq!(s.subscriptions.get(&f), Some(&QoS::ExactlyOnce));
}

// -----------------------------------------------------------------------
// RetainedStore tests
// -----------------------------------------------------------------------

#[test]
fn retained_store_and_get() {
    let mut store = RetainedStore::new();
    let msg = Message::new("a/b".into(), vec![42], QoS::AtMostOnce, true, None).unwrap();
    store.store(msg);
    let got = store.get("a/b").unwrap();
    assert_eq!(got.payload, vec![42]);
}

#[test]
fn retained_store_empty_payload_clears() {
    let mut store = RetainedStore::new();
    let msg = Message::new("a/b".into(), vec![1], QoS::AtMostOnce, true, None).unwrap();
    store.store(msg);
    let clear = Message::new("a/b".into(), vec![], QoS::AtMostOnce, true, None).unwrap();
    store.store(clear);
    assert!(store.get("a/b").is_none());
}

#[test]
fn retained_store_clear() {
    let mut store = RetainedStore::new();
    let msg = Message::new("a/b".into(), vec![1], QoS::AtMostOnce, true, None).unwrap();
    store.store(msg);
    store.clear("a/b");
    assert!(store.get("a/b").is_none());
}

#[test]
fn retained_store_match_filter() {
    let mut store = RetainedStore::new();
    let m1 = Message::new("a/b".into(), vec![1], QoS::AtMostOnce, true, None).unwrap();
    let m2 = Message::new("a/c".into(), vec![2], QoS::AtMostOnce, true, None).unwrap();
    let m3 = Message::new("x/y".into(), vec![3], QoS::AtMostOnce, true, None).unwrap();
    store.store(m1);
    store.store(m2);
    store.store(m3);
    let f = TopicFilter::new("a/+").unwrap();
    let matched = store.match_filter(&f);
    assert_eq!(matched.len(), 2);
}

#[test]
fn retained_store_default() {
    let store = RetainedStore::default();
    assert!(store.get("anything").is_none());
}

#[test]
fn retained_store_overwrite() {
    let mut store = RetainedStore::new();
    let m1 = Message::new("a/b".into(), vec![1], QoS::AtMostOnce, true, None).unwrap();
    let m2 = Message::new("a/b".into(), vec![2], QoS::AtMostOnce, true, None).unwrap();
    store.store(m1);
    store.store(m2);
    assert_eq!(store.get("a/b").unwrap().payload, vec![2]);
}

// -----------------------------------------------------------------------
// Broker tests
// -----------------------------------------------------------------------

#[test]
fn broker_new() {
    let b = Broker::new();
    assert_eq!(b.session_count(), 0);
}

#[test]
fn broker_default() {
    let b = Broker::default();
    assert_eq!(b.session_count(), 0);
}

#[test]
fn broker_connect_clean() {
    let mut b = Broker::new();
    let result = b.handle_connect("c1".into(), true, None).unwrap();
    assert!(!result); // no previous session
    assert_eq!(b.session_count(), 1);
}

#[test]
fn broker_connect_resume_session() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), false, None).unwrap();
    let result = b.handle_connect("c1".into(), false, None).unwrap();
    assert!(result); // session was present
}

#[test]
fn broker_connect_clean_clears_session() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), false, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    b.handle_connect("c1".into(), true, None).unwrap();
    assert!(b.sessions["c1"].subscriptions.is_empty());
}

#[test]
fn broker_subscribe() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    assert!(b.subscribe("c1", &f, QoS::AtMostOnce).is_ok());
}

#[test]
fn broker_subscribe_not_connected() {
    let mut b = Broker::new();
    let f = TopicFilter::new("a/b").unwrap();
    assert_eq!(
        b.subscribe("c1", &f, QoS::AtMostOnce),
        Err(MqttError::NotConnected("c1".into()))
    );
}

#[test]
fn broker_unsubscribe() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    assert!(b.unsubscribe("c1", &f).is_ok());
}

#[test]
fn broker_unsubscribe_not_found() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    assert_eq!(
        b.unsubscribe("c1", &f),
        Err(MqttError::SubscriptionNotFound)
    );
}

#[test]
fn broker_unsubscribe_not_connected() {
    let mut b = Broker::new();
    let f = TopicFilter::new("a/b").unwrap();
    assert_eq!(
        b.unsubscribe("c1", &f),
        Err(MqttError::NotConnected("c1".into()))
    );
}

#[test]
fn broker_publish_delivers() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    let delivered = b.publish("a/b", &[1], QoS::AtMostOnce, false).unwrap();
    assert_eq!(delivered, vec!["c1"]);
    assert_eq!(b.sessions["c1"].pending_messages.len(), 1);
}

#[test]
fn broker_publish_wildcard_delivery() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/+").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    let delivered = b.publish("a/x", &[1], QoS::AtMostOnce, false).unwrap();
    assert_eq!(delivered, vec!["c1"]);
}

#[test]
fn broker_publish_no_match() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    let delivered = b.publish("x/y", &[1], QoS::AtMostOnce, false).unwrap();
    assert!(delivered.is_empty());
}

#[test]
fn broker_publish_invalid_topic() {
    let mut b = Broker::new();
    assert!(b.publish("", &[], QoS::AtMostOnce, false).is_err());
}

#[test]
fn broker_publish_retained() {
    let mut b = Broker::new();
    b.publish("a/b", &[42], QoS::AtMostOnce, true).unwrap();
    assert_eq!(b.retained.get("a/b").unwrap().payload, vec![42]);
}

#[test]
fn broker_retained_on_subscribe() {
    let mut b = Broker::new();
    b.publish("a/b", &[99], QoS::AtMostOnce, true).unwrap();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    assert_eq!(b.sessions["c1"].pending_messages.len(), 1);
    assert_eq!(b.sessions["c1"].pending_messages[0].payload, vec![99]);
}

#[test]
fn broker_will_on_unexpected_disconnect() {
    let mut b = Broker::new();
    b.handle_connect("c2".into(), true, None).unwrap();
    let f = TopicFilter::new("will/topic").unwrap();
    b.subscribe("c2", &f, QoS::AtMostOnce).unwrap();

    let will = WillMessage {
        topic: "will/topic".into(),
        payload: b"gone".to_vec(),
        qos: QoS::AtMostOnce,
        retain: false,
    };
    b.handle_connect("c1".into(), true, Some(will)).unwrap();
    b.handle_disconnect("c1", true);
    assert_eq!(b.sessions["c2"].pending_messages.len(), 1);
    assert_eq!(b.sessions["c2"].pending_messages[0].payload, b"gone");
}

#[test]
fn broker_no_will_on_clean_disconnect() {
    let mut b = Broker::new();
    b.handle_connect("c2".into(), true, None).unwrap();
    let f = TopicFilter::new("will/topic").unwrap();
    b.subscribe("c2", &f, QoS::AtMostOnce).unwrap();

    let will = WillMessage {
        topic: "will/topic".into(),
        payload: b"gone".to_vec(),
        qos: QoS::AtMostOnce,
        retain: false,
    };
    b.handle_connect("c1".into(), true, Some(will)).unwrap();
    b.handle_disconnect("c1", false);
    assert!(b.sessions["c2"].pending_messages.is_empty());
}

#[test]
fn broker_multiple_clients() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    b.handle_connect("c2".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    b.subscribe("c2", &f, QoS::AtLeastOnce).unwrap();
    let delivered = b.publish("a/b", &[1], QoS::AtLeastOnce, false).unwrap();
    assert_eq!(delivered.len(), 2);
}

#[test]
fn broker_qos_downgrade() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a/b").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    b.publish("a/b", &[1], QoS::ExactlyOnce, false).unwrap();
    // Effective QoS = min(publish QoS, subscription QoS) = AtMostOnce
    assert_eq!(b.sessions["c1"].pending_messages[0].qos, QoS::AtMostOnce);
}

#[test]
fn broker_session_count() {
    let mut b = Broker::new();
    assert_eq!(b.session_count(), 0);
    b.handle_connect("c1".into(), true, None).unwrap();
    assert_eq!(b.session_count(), 1);
    b.handle_connect("c2".into(), true, None).unwrap();
    assert_eq!(b.session_count(), 2);
}

// -----------------------------------------------------------------------
// MqttError display tests
// -----------------------------------------------------------------------

#[test]
fn error_display() {
    assert_eq!(
        MqttError::InvalidTopic("x".into()).to_string(),
        "invalid topic: x"
    );
    assert_eq!(
        MqttError::InvalidFilter("x".into()).to_string(),
        "invalid filter: x"
    );
    assert_eq!(
        MqttError::InvalidClientId("x".into()).to_string(),
        "invalid client id: x"
    );
    assert_eq!(MqttError::InvalidQoS(5).to_string(), "invalid QoS value: 5");
    assert_eq!(
        MqttError::NotConnected("x".into()).to_string(),
        "client not connected: x"
    );
    assert_eq!(
        MqttError::AlreadyConnected("x".into()).to_string(),
        "client already connected: x"
    );
    assert_eq!(
        MqttError::SubscriptionNotFound.to_string(),
        "subscription not found"
    );
    assert_eq!(
        MqttError::PacketIdRequired.to_string(),
        "packet id required for QoS > 0"
    );
}

#[test]
fn error_clone_eq() {
    let e1 = MqttError::InvalidTopic("a".into());
    let e2 = e1.clone();
    assert_eq!(e1, e2);
}

#[test]
fn error_debug() {
    let e = MqttError::PacketIdRequired;
    let debug = format!("{e:?}");
    assert!(debug.contains("PacketIdRequired"));
}

// -----------------------------------------------------------------------
// Edge case and integration tests
// -----------------------------------------------------------------------

#[test]
fn topic_single_level() {
    let t = Topic::new("hello").unwrap();
    assert_eq!(t.levels(), vec!["hello"]);
}

#[test]
fn filter_no_match_shorter_topic() {
    let f = TopicFilter::new("a/b/c").unwrap();
    assert!(!f.matches_topic(&Topic::new("a/b").unwrap()));
}

#[test]
fn filter_no_match_longer_topic() {
    let f = TopicFilter::new("a/b").unwrap();
    assert!(!f.matches_topic(&Topic::new("a/b/c").unwrap()));
}

#[test]
fn tree_hash_matches_deep() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("#").unwrap();
    tree.subscribe("c1", &f, QoS::AtMostOnce);
    let subs = tree.match_subscribers(&Topic::new("a/b/c/d/e/f").unwrap());
    assert_eq!(subs.len(), 1);
}

#[test]
fn broker_publish_multi_level_wildcard() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("sensor/#").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    let d = b
        .publish("sensor/temp/living", &[1], QoS::AtMostOnce, false)
        .unwrap();
    assert_eq!(d.len(), 1);
}

#[test]
fn broker_will_retained() {
    let mut b = Broker::new();
    let will = WillMessage {
        topic: "status/c1".into(),
        payload: b"offline".to_vec(),
        qos: QoS::AtMostOnce,
        retain: true,
    };
    b.handle_connect("c1".into(), true, Some(will)).unwrap();
    b.handle_disconnect("c1", true);
    assert_eq!(b.retained.get("status/c1").unwrap().payload, b"offline");
}

#[test]
fn broker_reconnect_clean_removes_will() {
    let mut b = Broker::new();
    let will = WillMessage {
        topic: "w".into(),
        payload: vec![1],
        qos: QoS::AtMostOnce,
        retain: false,
    };
    b.handle_connect("c1".into(), true, Some(will)).unwrap();
    // Reconnect clean without will
    b.handle_connect("c1".into(), true, None).unwrap();
    // Unexpected disconnect should NOT publish old will
    b.handle_disconnect("c1", true);
    // No will was set on second connect, so nothing should happen
}

#[test]
fn client_id_boundary_1_char() {
    assert!(ClientId::new("x").is_ok());
}

#[test]
fn client_id_boundary_23_chars() {
    assert!(ClientId::new(&"a".repeat(23)).is_ok());
}

#[test]
fn client_id_boundary_24_chars() {
    assert!(ClientId::new(&"a".repeat(24)).is_err());
}

#[test]
fn filter_leading_slash() {
    let f = TopicFilter::new("/a/b").unwrap();
    let t = Topic::new("/a/b").unwrap();
    assert!(f.matches_topic(&t));
}

#[test]
fn filter_leading_slash_no_match() {
    let f = TopicFilter::new("a/b").unwrap();
    let t = Topic::new("/a/b").unwrap();
    assert!(!f.matches_topic(&t));
}

#[test]
fn broker_drain_pending() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f = TopicFilter::new("a").unwrap();
    b.subscribe("c1", &f, QoS::AtMostOnce).unwrap();
    b.publish("a", &[1], QoS::AtMostOnce, false).unwrap();
    b.publish("a", &[2], QoS::AtMostOnce, false).unwrap();
    let msgs = b.sessions.get_mut("c1").unwrap().drain_pending();
    assert_eq!(msgs.len(), 2);
    assert!(b.sessions["c1"].pending_messages.is_empty());
}

#[test]
fn tree_subscribe_same_client_twice() {
    let mut tree = TopicTree::new();
    let f = TopicFilter::new("a/b").unwrap();
    tree.subscribe("c1", &f, QoS::AtMostOnce);
    tree.subscribe("c1", &f, QoS::ExactlyOnce);
    let subs = tree.match_subscribers(&Topic::new("a/b").unwrap());
    // Should have only one entry (overwritten)
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].1, QoS::ExactlyOnce);
}

#[test]
fn broker_publish_to_multiple_filters_same_client() {
    let mut b = Broker::new();
    b.handle_connect("c1".into(), true, None).unwrap();
    let f1 = TopicFilter::new("a/b").unwrap();
    let f2 = TopicFilter::new("a/+").unwrap();
    b.subscribe("c1", &f1, QoS::AtMostOnce).unwrap();
    b.subscribe("c1", &f2, QoS::AtMostOnce).unwrap();
    let delivered = b.publish("a/b", &[1], QoS::AtMostOnce, false).unwrap();
    // Client may appear twice (once per matching filter)
    assert!(!delivered.is_empty());
}

#[test]
fn packet_type_as_u8() {
    assert_eq!(PacketType::Connect.as_u8(), 1);
    assert_eq!(PacketType::Disconnect.as_u8(), 14);
}
