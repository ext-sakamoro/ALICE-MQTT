# Changelog

## [Unreleased]

### Added

- `wire`: MQTT 3.1.1 の CONNECT / PUBLISH (QoS 0 / 1) / PINGREQ / DISCONNECT の encoder、CONNACK / PUBACK の decoder、残りの長さを上限で検査してから buffer を取る `read_packet` (検査は `alice_serial::bounded::len_at_most`、ALICE-Serial を git の rev で引く)、任意の `Read + Write` の stream で CONNECT → QoS 1 の PUBLISH → PUBACK → DISCONNECT を行う `publish_qos1` (CONNACK の return code が 0 以外、PUBACK 以外の packet、別の packet id は `Err`) 試験 `tests/wire_spec.rs` (仕様の byte 配置を期待値に、乱数入力 10 万件で decoder が panic しない、長さの 5 byte 目と応答の大きな長さの拒否) と `fuzz/` の target `wire_read_packet`

### Changed

- Licence: `MIT` → `LicenseRef-Proprietary`
