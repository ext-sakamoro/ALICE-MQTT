#![no_main]
//! read_packet, the CONNACK / PUBACK parsers and the remaining-length decoder on arbitrary bytes:
//! no panic, and a length over the limit never sizes a buffer
use alice_mqtt::wire::{decode_remaining_length, parse_connack, parse_puback, read_packet};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_remaining_length(data);
    let _ = parse_connack(data);
    let _ = parse_puback(data);
    if let Ok((_, body)) = read_packet(&mut &data[..], 64) {
        assert!(body.len() <= 64);
    }
});
