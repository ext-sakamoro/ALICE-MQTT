# alice-mqtt

Lightweight PubSub protocol — IoT messaging optimized via topic tree SDF search.

## Features

- **QoS levels**: AtMostOnce (0), AtLeastOnce (1), ExactlyOnce (2)
- **Topic tree**: Trie-based subscriber matching with `+` and `#` wildcard support
- **Retained messages**: Per-topic last-message store
- **Session management**: Clean/persistent sessions with pending message queues
- **Will messages**: Published on unexpected client disconnect
- **Broker**: In-memory message broker with connect, publish, subscribe, unsubscribe
- **Wire format** (`wire`): MQTT 3.1.1 CONNECT / PUBLISH (QoS 0, 1) / PINGREQ / DISCONNECT encoders, CONNACK / PUBACK decoders, `read_packet` with a bounded remaining length, and `publish_qos1` over any `Read + Write` stream
- **Zero dependencies**: No external crates required

## Usage

```rust
use alice_mqtt::{Broker, QoS, Topic, TopicFilter};

let mut broker = Broker::new();
let client_id = "sensor01".to_string();

// Connect
broker.handle_connect(client_id.clone(), true, None).unwrap();

// Subscribe
let filter = TopicFilter::new("home/+/temperature").unwrap();
broker.subscribe(client_id.clone(), filter, QoS::AtLeastOnce).unwrap();

// Publish
let topic = Topic::new("home/living/temperature").unwrap();
broker.publish(topic.path().to_string(), b"23.5".to_vec(), QoS::AtMostOnce, false).unwrap();
```

Publishing one QoS 1 message over a stream (TCP, or TLS around it):

```rust,no_run
use alice_mqtt::wire::{publish_qos1, Connect};
# fn run(stream: &mut std::net::TcpStream) -> Result<(), alice_mqtt::wire::WireError> {
let c = Connect { client_id: "c1", username: Some("user"), password: Some(b"secret"), keep_alive: 10, clean_session: true };
publish_qos1(stream, &c, "device/x/request", b"{}", 1)?; // CONNECT, PUBLISH, wait for PUBACK 1, DISCONNECT
# Ok(()) }
```

## License

`LicenseRef-Proprietary`. The text is [LICENSE-Proprietary](LICENSE-Proprietary).

Enquiries: https://extoria.co.jp/
