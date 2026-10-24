//! MQTT 3.1.1 wire format against the specification's byte layouts
//!
//! oracle: OASIS MQTT Version 3.1.1 — Table 2.4 (remaining length), Figure 3.2 / 3.6-3.10
//! (CONNECT), Figure 3.11-3.13 (CONNACK), Figure 3.14-3.15 (PUBLISH), Figure 3.17-3.18 (PUBACK),
//! Figure 3.33 (PINGREQ), Figure 3.35 (DISCONNECT). The expected bytes are written from those
//! layouts, not produced by the code under test.

use alice_mqtt::wire::{
    connect, decode_remaining_length, disconnect, encode_remaining_length, parse_connack,
    parse_puback, pingreq, publish, publish_qos1, read_packet, Connect, WireError,
    MAX_REMAINING_LENGTH, REPLY_LIMIT,
};
use std::io::{self, Read, Write};

/// Table 2.4: the boundaries of one to four bytes
const TABLE_2_4: [(usize, &[u8]); 8] = [
    (0, &[0x00]),
    (127, &[0x7f]),
    (128, &[0x80, 0x01]),
    (16_383, &[0xff, 0x7f]),
    (16_384, &[0x80, 0x80, 0x01]),
    (2_097_151, &[0xff, 0xff, 0x7f]),
    (2_097_152, &[0x80, 0x80, 0x80, 0x01]),
    (268_435_455, &[0xff, 0xff, 0xff, 0x7f]),
];

#[test]
fn remaining_length_matches_table_2_4_both_ways() {
    for (len, bytes) in TABLE_2_4 {
        let mut out = Vec::new();
        encode_remaining_length(len, &mut out).unwrap();
        assert_eq!(out, bytes, "encode {len}");
        assert_eq!(
            decode_remaining_length(bytes),
            Ok((len, bytes.len())),
            "decode {len}"
        );
    }
    assert_eq!(
        encode_remaining_length(MAX_REMAINING_LENGTH + 1, &mut Vec::new()),
        Err(WireError::TooLarge {
            len: MAX_REMAINING_LENGTH + 1,
            limit: MAX_REMAINING_LENGTH
        })
    );
    assert_eq!(
        decode_remaining_length(&[0x80, 0x80]),
        Err(WireError::Truncated)
    );
    assert_eq!(
        decode_remaining_length(&[0xff, 0xff, 0xff, 0xff, 0x01]),
        Err(WireError::RemainingLengthTooLong)
    );
}

#[test]
fn connect_has_the_specified_layout() {
    let bytes = connect(&Connect {
        client_id: "c1",
        username: Some("bblp"),
        password: Some(b"pw"),
        keep_alive: 10,
        clean_session: true,
    })
    .unwrap();
    let body: Vec<u8> = [
        &[0x00, 0x04, b'M', b'Q', b'T', b'T'][..], // protocol name
        &[0x04],                                   // level 4
        &[0xc2],                                   // user name | password | clean session
        &[0x00, 0x0a],                             // keep alive 10
        &[0x00, 0x02, b'c', b'1'],                 // client id
        &[0x00, 0x04, b'b', b'b', b'l', b'p'],     // user name
        &[0x00, 0x02, b'p', b'w'],                 // password
    ]
    .concat();
    let mut want = vec![0x10, u8::try_from(body.len()).unwrap()];
    want.extend_from_slice(&body);
    assert_eq!(bytes, want);
    // no user name: the flags carry only clean session
    let bare = connect(&Connect {
        client_id: "",
        username: None,
        password: None,
        keep_alive: 0,
        clean_session: true,
    })
    .unwrap();
    assert_eq!(
        bare,
        [0x10, 0x0c, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x00, 0x00, 0x00]
    );
    assert!(matches!(
        connect(&Connect {
            client_id: "x",
            username: None,
            password: Some(b"p"),
            keep_alive: 0,
            clean_session: true
        }),
        Err(WireError::Malformed(_))
    ));
}

#[test]
fn publish_qos0_and_qos1_have_the_specified_layout() {
    assert_eq!(
        publish("a/b", b"hi", None, false).unwrap(),
        [0x30, 0x07, 0x00, 0x03, b'a', b'/', b'b', b'h', b'i']
    );
    assert_eq!(
        publish("a/b", b"hi", Some(10), false).unwrap(),
        [0x32, 0x09, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x0a, b'h', b'i']
    );
    assert_eq!(
        publish("t", b"", None, true).unwrap(),
        [0x31, 0x03, 0x00, 0x01, b't']
    );
    assert_eq!(publish("t", b"", Some(0), false), Err(WireError::PacketId));
}

#[test]
fn control_packets_and_replies() {
    assert_eq!(pingreq(), [0xc0, 0x00]);
    assert_eq!(disconnect(), [0xe0, 0x00]);
    assert_eq!(parse_connack(&[0x00, 0x00]), Ok((false, 0)));
    assert_eq!(parse_connack(&[0x01, 0x05]), Ok((true, 5)));
    assert!(matches!(
        parse_connack(&[0x02, 0x00]),
        Err(WireError::Malformed(_))
    ));
    assert!(matches!(
        parse_connack(&[0x00]),
        Err(WireError::Malformed(_))
    ));
    assert_eq!(parse_puback(&[0x12, 0x34]), Ok(0x1234));
    assert!(matches!(
        parse_puback(&[0x12]),
        Err(WireError::Malformed(_))
    ));
}

#[test]
fn read_packet_bounds_the_length_before_it_sizes_a_buffer() {
    let mut ok = &[0x40, 0x02, 0x00, 0x07][..];
    assert_eq!(read_packet(&mut ok, 64), Ok((0x40, vec![0x00, 0x07])));
    // a claimed length of 268 435 455 with no body: refused at the limit, nothing allocated
    let mut huge = &[0x30, 0xff, 0xff, 0xff, 0x7f][..];
    assert_eq!(
        read_packet(&mut huge, 64),
        Err(WireError::TooLarge {
            len: 268_435_455,
            limit: 64
        })
    );
    let mut five = &[0x30, 0xff, 0xff, 0xff, 0xff, 0x01][..];
    assert_eq!(
        read_packet(&mut five, 64),
        Err(WireError::RemainingLengthTooLong)
    );
    let mut short = &[0x40, 0x02, 0x00][..];
    assert_eq!(read_packet(&mut short, 64), Err(WireError::Truncated));
}

/// A stream that replies with fixed bytes and records what was written
struct Script {
    replies: io::Cursor<Vec<u8>>,
    written: Vec<u8>,
}

impl Script {
    fn new(replies: &[u8]) -> Self {
        Self {
            replies: io::Cursor::new(replies.to_vec()),
            written: Vec::new(),
        }
    }
}

impl Read for Script {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.replies.read(buf)
    }
}

impl Write for Script {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.written.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

const C: Connect<'static> = Connect {
    client_id: "c1",
    username: Some("bblp"),
    password: Some(b"pw"),
    keep_alive: 10,
    clean_session: true,
};

#[test]
fn publish_qos1_sends_connect_publish_disconnect_on_the_specified_replies() {
    let mut s = Script::new(&[0x20, 0x02, 0x00, 0x00, 0x40, 0x02, 0x00, 0x2a]);
    publish_qos1(&mut s, &C, "device/x/request", b"{}", 42).unwrap();
    let mut want = connect(&C).unwrap();
    want.extend(publish("device/x/request", b"{}", Some(42), false).unwrap());
    want.extend(disconnect());
    assert_eq!(s.written, want);
}

#[test]
fn publish_qos1_refuses_any_other_reply() {
    let cases: [(&[u8], WireError); 6] = [
        (&[0x20, 0x02, 0x00, 0x05], WireError::ConnectRefused(5)),
        (
            &[0x30, 0x02, 0x00, 0x00],
            WireError::UnexpectedPacket {
                expected: 2,
                got: 0x30,
            },
        ),
        (
            &[0x20, 0x02, 0x00, 0x00, 0xd0, 0x00],
            WireError::UnexpectedPacket {
                expected: 4,
                got: 0xd0,
            },
        ),
        (
            &[0x20, 0x02, 0x00, 0x00, 0x50, 0x02, 0x00, 0x2a],
            WireError::UnexpectedPacket {
                expected: 4,
                got: 0x50,
            },
        ),
        (
            &[0x20, 0x02, 0x00, 0x00, 0x40, 0x02, 0x00, 0x2b],
            WireError::PacketIdMismatch {
                expected: 42,
                got: 43,
            },
        ),
        (&[0x20, 0x02, 0x00, 0x00], WireError::Truncated),
    ];
    let mut compared = 0;
    for (replies, want) in cases {
        let mut s = Script::new(replies);
        assert_eq!(
            publish_qos1(&mut s, &C, "t", b"p", 42),
            Err(want),
            "{replies:02x?}"
        );
        // no DISCONNECT is sent after a refusal
        assert!(!s.written.ends_with(&disconnect()), "{replies:02x?}");
        compared += 1;
    }
    assert_eq!(compared, 6);
    assert_eq!(
        publish_qos1(&mut Script::new(&[]), &C, "t", b"p", 0),
        Err(WireError::PacketId)
    );
}

/// xorshift64*: a fixed sequence of inputs, the same on every run
fn next(s: &mut u64) -> u64 {
    *s ^= *s >> 12;
    *s ^= *s << 25;
    *s ^= *s >> 27;
    s.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

#[test]
fn the_decoders_never_panic_and_lengths_round_trip_on_random_input() {
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut read = 0;
    for _ in 0..100_000 {
        let n = usize::try_from(next(&mut seed) % 12).unwrap();
        let bytes: Vec<u8> = (0..n).map(|_| next(&mut seed).to_le_bytes()[0]).collect();
        let _ = decode_remaining_length(&bytes);
        let _ = parse_connack(&bytes);
        let _ = parse_puback(&bytes);
        if read_packet(&mut &bytes[..], 64).is_ok() {
            read += 1;
        }
        let len = usize::try_from(next(&mut seed) % (MAX_REMAINING_LENGTH as u64 + 1)).unwrap();
        let mut out = Vec::new();
        encode_remaining_length(len, &mut out).unwrap();
        assert_eq!(decode_remaining_length(&out), Ok((len, out.len())), "{len}");
    }
    // some inputs were whole packets, so the reader's success path was reached too
    assert!(read > 0);
}

/// Yields 0x80 and counts the bytes taken; ends after 1 024 bytes so that a reader that does not
/// stop at the fifth length byte fails (with `Truncated`) instead of running forever
struct Continuations(usize);

impl Read for Continuations {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = buf.len().min(1).min(1024 - self.0);
        buf[..n].fill(0x80);
        self.0 += n;
        Ok(n)
    }
}

#[test]
fn read_packet_stops_at_the_fifth_length_byte() {
    let mut five = &[0x20, 0x80, 0x80, 0x80, 0x80, 0x01][..];
    assert_eq!(
        read_packet(&mut five, 64),
        Err(WireError::RemainingLengthTooLong)
    );
    // an endless stream of continuation bytes: refused after the first byte and four length
    // bytes, without reading on
    let mut endless = Continuations(0);
    assert_eq!(
        read_packet(&mut endless, 64),
        Err(WireError::RemainingLengthTooLong)
    );
    assert_eq!(endless.0, 5);
}

#[test]
fn publish_qos1_refuses_a_reply_that_claims_a_large_length_before_reading_its_body() {
    let mut s = Script::new(&[0x20, 0xff, 0xff, 0xff, 0x7f]);
    assert_eq!(
        publish_qos1(&mut s, &C, "t", b"p", 42),
        Err(WireError::TooLarge {
            len: MAX_REMAINING_LENGTH,
            limit: REPLY_LIMIT
        })
    );
    // only the fixed header was read
    assert_eq!(s.replies.position(), 5);
}
