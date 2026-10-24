//! MQTT 3.1.1 wire format for a publishing client (OASIS MQTT Version 3.1.1, sections 2 and 3)
//!
//! The encoders build the packets a client sends (CONNECT, PUBLISH at `QoS` 0 / 1, PINGREQ,
//! DISCONNECT); the decoders read the packets it waits for (CONNACK, PUBACK). [`read_packet`]
//! reads one packet from a stream and bounds the remaining length it reads before sizing a buffer.
//! [`publish_qos1`] runs CONNECT, one `QoS` 1 PUBLISH and DISCONNECT over any `Read + Write` stream
//! (a TLS stream, for example), and refuses any reply other than the one the protocol requires.

use std::io::{self, Read, Write};

/// The largest remaining length the encoding can carry (four bytes, section 2.2.3)
pub const MAX_REMAINING_LENGTH: usize = 268_435_455;

/// Why a packet could not be built, read or accepted
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WireError {
    /// The input ended inside a packet
    Truncated,
    /// The remaining length used more than four bytes
    RemainingLengthTooLong,
    /// The remaining length (read or to write) is over the limit (`len`, `limit`)
    TooLarge {
        /// The length
        len: usize,
        /// The limit it was checked against
        limit: usize,
    },
    /// A string or binary field is longer than 65 535 bytes
    FieldTooLong(usize),
    /// A packet other than the one expected (expected type, received first byte)
    UnexpectedPacket {
        /// The packet type expected
        expected: u8,
        /// The first byte of the packet received
        got: u8,
    },
    /// A packet whose body does not follow the specification
    Malformed(&'static str),
    /// A PUBACK for another packet identifier
    PacketIdMismatch {
        /// The identifier sent
        expected: u16,
        /// The identifier acknowledged
        got: u16,
    },
    /// CONNACK with a non-zero return code (section 3.2.2.3)
    ConnectRefused(u8),
    /// `QoS` 1 needs a non-zero packet identifier, `QoS` 0 has none
    PacketId,
    /// An I/O error of the stream
    Io(io::ErrorKind),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "the input ended inside a packet"),
            Self::RemainingLengthTooLong => {
                write!(f, "the remaining length uses more than four bytes")
            }
            Self::TooLarge { len, limit } => {
                write!(f, "remaining length {len} is over the limit {limit}")
            }
            Self::FieldTooLong(n) => write!(f, "a field of {n} bytes is longer than 65535"),
            Self::UnexpectedPacket { expected, got } => {
                write!(
                    f,
                    "expected packet type {expected}, got first byte {got:#04x}"
                )
            }
            Self::Malformed(why) => write!(f, "malformed packet: {why}"),
            Self::PacketIdMismatch { expected, got } => {
                write!(f, "PUBACK for packet {got}, sent packet {expected}")
            }
            Self::ConnectRefused(rc) => write!(f, "CONNACK return code {rc}"),
            Self::PacketId => write!(f, "QoS 1 needs a non-zero packet id, QoS 0 has none"),
            Self::Io(kind) => write!(f, "stream: {kind}"),
        }
    }
}

impl std::error::Error for WireError {}

impl From<io::Error> for WireError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io(e.kind())
        }
    }
}

/// Appends the remaining length `len` (section 2.2.3: seven bits per byte, low group first)
///
/// # Errors
/// [`WireError::TooLarge`] above [`MAX_REMAINING_LENGTH`].
pub fn encode_remaining_length(len: usize, out: &mut Vec<u8>) -> Result<(), WireError> {
    if len > MAX_REMAINING_LENGTH {
        return Err(WireError::TooLarge {
            len,
            limit: MAX_REMAINING_LENGTH,
        });
    }
    let mut x = len;
    loop {
        // x % 128 < 128, so it fits a byte
        #[allow(clippy::cast_possible_truncation)]
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if x == 0 {
            return Ok(());
        }
    }
}

/// Reads a remaining length from its first bytes: the value and the number of bytes it used
///
/// # Errors
/// [`WireError::Truncated`] when the bytes end inside it, [`WireError::RemainingLengthTooLong`]
/// when a fifth byte would be needed.
pub fn decode_remaining_length(bytes: &[u8]) -> Result<(usize, usize), WireError> {
    let mut value = 0usize;
    for i in 0..4 {
        let b = *bytes.get(i).ok_or(WireError::Truncated)?;
        value |= usize::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(WireError::RemainingLengthTooLong)
}

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_field(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), WireError> {
    let n = u16::try_from(bytes.len()).map_err(|_| WireError::FieldTooLong(bytes.len()))?;
    put_u16(out, n);
    out.extend_from_slice(bytes);
    Ok(())
}

fn packet(first: u8, body: &[u8]) -> Result<Vec<u8>, WireError> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(first);
    encode_remaining_length(body.len(), &mut out)?;
    out.extend_from_slice(body);
    Ok(out)
}

/// The fields of a CONNECT packet a client sends (section 3.1)
#[derive(Debug, Clone, Copy)]
pub struct Connect<'a> {
    /// The client identifier
    pub client_id: &'a str,
    /// The user name, if any
    pub username: Option<&'a str>,
    /// The password, if any (only with a user name, section 3.1.2.9)
    pub password: Option<&'a [u8]>,
    /// Keep alive in seconds (0 turns it off)
    pub keep_alive: u16,
    /// Clean session
    pub clean_session: bool,
}

/// A CONNECT packet (protocol name `MQTT`, level 4)
///
/// # Errors
/// [`WireError::Malformed`] for a password without a user name, [`WireError::FieldTooLong`] for a
/// field over 65 535 bytes.
pub fn connect(c: &Connect<'_>) -> Result<Vec<u8>, WireError> {
    if c.password.is_some() && c.username.is_none() {
        return Err(WireError::Malformed("a password needs a user name"));
    }
    let mut flags = 0u8;
    if c.username.is_some() {
        flags |= 0x80;
    }
    if c.password.is_some() {
        flags |= 0x40;
    }
    if c.clean_session {
        flags |= 0x02;
    }
    let mut body = Vec::new();
    put_field(&mut body, b"MQTT")?;
    body.push(4);
    body.push(flags);
    put_u16(&mut body, c.keep_alive);
    put_field(&mut body, c.client_id.as_bytes())?;
    if let Some(u) = c.username {
        put_field(&mut body, u.as_bytes())?;
    }
    if let Some(p) = c.password {
        put_field(&mut body, p)?;
    }
    packet(0x10, &body)
}

/// A PUBLISH packet: `QoS` 0 without a packet id, `QoS` 1 with a non-zero one (section 3.3)
///
/// # Errors
/// [`WireError::PacketId`] for `QoS` 0 with an id or `QoS` 1 without a non-zero one, and the errors of
/// the fields and the remaining length.
pub fn publish(
    topic: &str,
    payload: &[u8],
    packet_id: Option<u16>,
    retain: bool,
) -> Result<Vec<u8>, WireError> {
    let qos: u8 = match packet_id {
        None => 0,
        Some(0) => return Err(WireError::PacketId),
        Some(_) => 1,
    };
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 4);
    put_field(&mut body, topic.as_bytes())?;
    if let Some(id) = packet_id {
        put_u16(&mut body, id);
    }
    body.extend_from_slice(payload);
    packet(0x30 | (qos << 1) | u8::from(retain), &body)
}

/// PINGREQ (section 3.12)
#[must_use]
pub const fn pingreq() -> [u8; 2] {
    [0xc0, 0x00]
}

/// DISCONNECT (section 3.14)
#[must_use]
pub const fn disconnect() -> [u8; 2] {
    [0xe0, 0x00]
}

/// Reads one packet: its first byte and its body
///
/// The remaining length is accepted only up to `limit` before the body buffer is sized (a length
/// read from the stream is a claim, not a size).
///
/// # Errors
/// [`WireError::Truncated`] when the stream ends inside the packet,
/// [`WireError::RemainingLengthTooLong`], [`WireError::TooLarge`] over `limit`, [`WireError::Io`].
pub fn read_packet<R: Read>(r: &mut R, limit: usize) -> Result<(u8, Vec<u8>), WireError> {
    let mut first = [0u8; 1];
    r.read_exact(&mut first)?;
    let mut len_bytes = Vec::with_capacity(4);
    loop {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        len_bytes.push(b[0]);
        if b[0] & 0x80 == 0 {
            break;
        }
        if len_bytes.len() == 4 {
            return Err(WireError::RemainingLengthTooLong);
        }
    }
    let (claimed, _) = decode_remaining_length(&len_bytes)?;
    // a length read from the stream is a claim, bounded before it sizes the buffer
    let len = alice_serial::bounded::len_at_most(claimed as u64, limit).map_err(|_| {
        WireError::TooLarge {
            len: claimed,
            limit,
        }
    })?;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    Ok((first[0], body))
}

/// The body of a CONNACK: session present and the return code (section 3.2)
///
/// # Errors
/// [`WireError::Malformed`] for a body that is not two bytes or has reserved flag bits set.
pub fn parse_connack(body: &[u8]) -> Result<(bool, u8), WireError> {
    match body {
        [flags, rc] if flags & 0xfe == 0 => Ok((flags & 1 == 1, *rc)),
        [_, _] => Err(WireError::Malformed("CONNACK reserved flag bits set")),
        _ => Err(WireError::Malformed("CONNACK body is not two bytes")),
    }
}

/// The packet identifier of a PUBACK body (section 3.4)
///
/// # Errors
/// [`WireError::Malformed`] for a body that is not two bytes.
pub fn parse_puback(body: &[u8]) -> Result<u16, WireError> {
    match body {
        [hi, lo] => Ok(u16::from_be_bytes([*hi, *lo])),
        _ => Err(WireError::Malformed("PUBACK body is not two bytes")),
    }
}

/// The limit on the remaining length of a reply [`publish_qos1`] reads (CONNACK and PUBACK carry 2)
pub const REPLY_LIMIT: usize = 64;

/// CONNECT, one `QoS` 1 PUBLISH and DISCONNECT over `stream`
///
/// The first reply must be a CONNACK with return code 0 and the second a PUBACK for `packet_id`;
/// anything else is refused (no other packet is expected by a client that subscribed to nothing).
/// A PUBACK says the broker took the message, not that the device acted on it.
///
/// # Errors
/// [`WireError::ConnectRefused`], [`WireError::UnexpectedPacket`], [`WireError::PacketIdMismatch`],
/// [`WireError::Malformed`], [`WireError::PacketId`] for a zero `packet_id`, and the stream's
/// errors.
pub fn publish_qos1<S: Read + Write>(
    stream: &mut S,
    connect_fields: &Connect<'_>,
    topic: &str,
    payload: &[u8],
    packet_id: u16,
) -> Result<(), WireError> {
    if packet_id == 0 {
        return Err(WireError::PacketId);
    }
    stream.write_all(&connect(connect_fields)?)?;
    stream.flush()?;
    let (first, body) = read_packet(stream, REPLY_LIMIT)?;
    if first != 0x20 {
        return Err(WireError::UnexpectedPacket {
            expected: 2,
            got: first,
        });
    }
    let (_, rc) = parse_connack(&body)?;
    if rc != 0 {
        return Err(WireError::ConnectRefused(rc));
    }
    stream.write_all(&publish(topic, payload, Some(packet_id), false)?)?;
    stream.flush()?;
    let (first, body) = read_packet(stream, REPLY_LIMIT)?;
    if first != 0x40 {
        return Err(WireError::UnexpectedPacket {
            expected: 4,
            got: first,
        });
    }
    let got = parse_puback(&body)?;
    if got != packet_id {
        return Err(WireError::PacketIdMismatch {
            expected: packet_id,
            got,
        });
    }
    stream.write_all(&disconnect())?;
    stream.flush()?;
    Ok(())
}
