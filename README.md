# qmux-rs

[![shiguredo_qmux](https://img.shields.io/crates/v/shiguredo_qmux.svg)](https://crates.io/crates/shiguredo_qmux)
[![Documentation](https://docs.rs/shiguredo_qmux/badge.svg)](https://docs.rs/shiguredo_qmux)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/shiguredo)

> [!WARNING]
> このライブラリは開発中であり、仕様が積極的に変更される場合があります。

## About Shiguredo's open source software

We will not respond to PRs or issues that have not been discussed on Discord. Also, Discord is only available in Japanese.

Please read <https://github.com/shiguredo/oss> before use.

## 時雨堂のオープンソースソフトウェアについて

利用前に <https://github.com/shiguredo/oss> をお読みください。

## 概要

Rust で実装された Sans I/O な QMux (draft-ietf-quic-qmux-02) ライブラリです。QMux は
TLS over TCP や UNIX ソケットのような順序保証された双方向バイトストリーム上で、
QUIC version 1 のストリーム操作とデータグラム操作を提供する互換層です。

`shiguredo_qmux` は下位トランスポートの I/O を一切行いません。読み書きと時刻の供給は
アプリケーションが行います。

## 特徴

- Sans I/O
  - <https://sans-io.readthedocs.io/index.html>
- `no_std` + `alloc` で動作する
- draft-ietf-quic-qmux-02 準拠
- QUIC version 1 のストリーム機構 (RFC 9000 Section 2 から Section 4) とフロー制御
- DATAGRAM 拡張 (RFC 9221) サポート
- ngtcp2/dwnx (QMux の C 実装) との相互接続テストを同梱
- PBT (noprop) と Fuzzing (cargo-fuzz) を同梱

## 構成

| クレート | 役割 |
| --- | --- |
| `shiguredo_qmux` | Sans I/O & no_std + alloc の QMux 実装 |
| `crates/tokio-qmux` | `shiguredo_qmux` の tokio 非同期ラッパー |
| `crates/dwnx-sys` | dwnx の C ソースを clone してビルドし、FFI 宣言を生成する |
| `crates/dwnx-rs` | `dwnx_conn` の安全な Sans I/O ラッパー |
| `crates/tokio-dwnx` | `dwnx-rs` の tokio 非同期ラッパー |
| `interop` | 2 実装を接続する相互接続テスト |

## 使い方

### Sans I/O

```rust
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Timestamp;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection;
use shiguredo_qmux::event::Event;

let now = Timestamp::from_micros(0);
let mut client = Connection::new(Role::Client, Config::default());
let mut server = Connection::new(Role::Server, Config::default());
let mut buf = Vec::new();

// 最初の Record (QX_TRANSPORT_PARAMETERS) を送り、相手に処理させる
assert!(client.poll_transmit(&mut buf, now).expect("transmit succeeds"));
server.receive(&buf, now);

// ピアのトランスポートパラメータを受信したことがイベントで分かる
assert_eq!(server.poll_event(), Some(Event::TransportParametersReceived));
```

### tokio

```rust
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::config::Config;
use tokio_qmux::Connection;

# async fn example() -> Result<(), tokio_qmux::Error> {
let (client_io, server_io) = tokio::io::duplex(64 * 1024);
let client = Connection::new(Role::Client, client_io, Config::default())?;
let server = Connection::new(Role::Server, server_io, Config::default())?;
# let _ = (client, server);
# Ok(())
# }
```

下位トランスポートには接続済みの双方向バイトストリームを渡します。TLS と ALPN の
ネゴシエーションは呼び出し側の責務です。実行可能な最小例は
`crates/tokio-qmux/examples/basic.rs` にあります。

## テスト

```bash
# 全テスト
make test

# 相互接続テスト (shiguredo_qmux と dwnx)
make interop-test

# PBT
make pbt

# Fuzzing (全ターゲットを 30 秒ずつ)
make fuzzing

# Fuzzing ターゲット一覧
make fuzzing-list
```

`dwnx-sys` のビルドには git、C11 コンパイラ、libclang (bindgen 用) が必要です。
macOS では `LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib` を設定します。

詳細は [docs/QMUX.md](./docs/QMUX.md) と [docs/INTEROP.md](./docs/INTEROP.md) を
参照してください。

## 準拠仕様

| 仕様 | 内容 |
| --- | --- |
| [draft-ietf-quic-qmux-02](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/) | QMux |
| [RFC 9000](https://www.rfc-editor.org/rfc/rfc9000) | QUIC version 1 |
| [RFC 9221](https://www.rfc-editor.org/rfc/rfc9221) | QUIC DATAGRAM 拡張 |
| [RFC 7301](https://www.rfc-editor.org/rfc/rfc7301) | TLS ALPN |

## ライセンス

Apache License 2.0

```text
Copyright 2026 Shiguredo Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
