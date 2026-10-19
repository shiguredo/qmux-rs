#![no_std]

//! QMux (draft-ietf-quic-qmux-02) の Sans I/O 実装
//!
//! QMux は TLS over TCP や UNIX ソケットのような、順序保証された双方向の
//! バイトストリーム上で、QUIC version 1 のストリーム操作とデータグラム操作を
//! 提供する互換層である。本クレートは下位トランスポートの I/O を一切行わず、
//! アプリケーションが受け渡すバイト列だけを扱う。
//!
//! 実装している範囲:
//!
//! - QMux Record のエンコード/デコード (draft-ietf-quic-qmux-02 Section 3.2)
//! - QX_TRANSPORT_PARAMETERS / QX_PING フレーム (Section 4.2, Section 4.3)
//! - 利用が許可された QUIC version 1 フレームの判定とデコード (Section 4)
//! - トランスポートパラメータの許可/禁止判定と max_record_size (Section 5)
//! - ストリームの多重化、フロー制御、MAX_STREAMS による同時実行数の制御
//!   (RFC 9000 Section 2 から Section 4 を QMux の順序保証に合わせて適用)
//! - QMux Record の送受信のたびにリセットするアイドルタイムアウト (Section 7.1)
//! - CONNECTION_CLOSE による切断 (Section 7.2) と、ピアが下位トランスポートの
//!   送信側を shutdown したことの通知 (Section 7.3、
//!   [`Connection::peer_shutdown`](crate::connection::Connection::peer_shutdown))
//! - DATAGRAM 拡張 (RFC 9221、Section 9.1)
//! - Section 12.2 の資源消費の監視用カウンタ
//!   ([`TrafficStats`](crate::connection::TrafficStats) と
//!   [`Connection::traffic_stats`](crate::connection::Connection::traffic_stats))
//! - ストリーム数と送信クレジットの照会
//!   ([`Connection::peer_max_streams_bidi`](crate::connection::Connection::peer_max_streams_bidi)
//!   / [`Connection::peer_max_streams_uni`](crate::connection::Connection::peer_max_streams_uni)
//!   / [`Connection::send_credit`](crate::connection::Connection::send_credit)
//!   / [`Connection::stream_send_credit`](crate::connection::Connection::stream_send_credit))
//!
//! 実装していない範囲:
//!
//! - RESET_STREAM_AT 拡張 (Section 9.2) はネゴシエートしない。ピアが
//!   RESET_STREAM_AT フレームを送ってきた場合は未知のフレームタイプとして
//!   FRAME_ENCODING_ERROR で接続を閉じる
//! - 0-RTT (Section 8.2) の remembered transport parameters は扱わない
//! - TLS / TCP の接続管理、ALPN のネゴシエーション (Section 8.1) は下位
//!   トランスポートの責務であり、本クレートの範囲外
//! - Section 10 Version Agility は version 1 の実装に対する規範要求を持たず、
//!   in-band upgrade の方式も未定義のため実装対象外である
//!
//! # エラーの扱い
//!
//! ピアの入力から検出したプロトコル違反は、
//! [`Connection::receive`](crate::connection::Connection::receive) がエラーを
//! 返すのではなく、CONNECTION_CLOSE フレームを送信待ちに追加して
//! [`Event::ConnectionClosed`](crate::event::Event::ConnectionClosed) を通知する。
//!
//! [`crate::codec::error::Error::Protocol`] の契約は、返す API の層で異なる。
//! [`Connection`](crate::connection::Connection) の各メソッドが返す
//! `Error::Protocol` は、未作成のストリームへの操作、終端したストリームへの操作、
//! ストリーム数の上限到達などのローカルの誤用を表し、接続を閉じない。コーデック層の
//! 受信側 API ([`crate::frame::decode`]、[`crate::frame::decode_record`]、
//! [`crate::record::RecordDecoder::poll_record`]、
//! [`crate::transport_params::TransportParameters::decode`] など) が返す
//! `Error::Protocol` は、ピアの入力に起因する接続エラーを表し、呼び出し側が
//! `code` を持つ CONNECTION_CLOSE を送る。
//!
//! 同じ条件でも、送信側と受信側でエラーの意味が異なる。たとえば Frames が空の
//! Record は、送信側の [`crate::record::encode`] ではローカルのエンコード誤りとして
//! [`crate::codec::error::Error::InvalidData`] を返し、受信側の
//! [`crate::record::RecordDecoder::poll_record`] ではピアの違反として
//! `Error::Protocol` (PROTOCOL_VIOLATION) を返す。`max_record_size` の超過も同様に、
//! 送信側は `Error::InvalidData`、受信側は `Error::Protocol`
//! (FRAME_ENCODING_ERROR) である。
//!
//! # 使い方
//!
//! [`Connection`](crate::connection::Connection) に受信バイト列を渡し、
//! [`Connection::poll_event`](crate::connection::Connection::poll_event) で
//! イベントを取り出し、
//! [`Connection::poll_transmit`](crate::connection::Connection::poll_transmit) で
//! 送信バイト列を書き出す。
//! 下位トランスポートへの読み書きと時刻の供給はアプリケーションが行う。
//!
//! ```
//! use shiguredo_qmux::codec::role::Role;
//! use shiguredo_qmux::codec::time::Timestamp;
//! use shiguredo_qmux::config::Config;
//! use shiguredo_qmux::connection::Connection;
//! use shiguredo_qmux::event::Event;
//!
//! let now = Timestamp::from_micros(0);
//! let mut client = Connection::new(Role::Client, Config::default()).expect("テスト用の接続を作成できる");
//! let mut server = Connection::new(Role::Server, Config::default()).expect("テスト用の接続を作成できる");
//! let mut buf = Vec::new();
//!
//! // 最初の Record (QX_TRANSPORT_PARAMETERS) を送り、相手に処理させる
//! assert!(client.poll_transmit(&mut buf, now).expect("transmit succeeds"));
//! server.receive(&buf, now);
//!
//! // ピアのトランスポートパラメータを受信したことがイベントで分かる
//! assert_eq!(server.poll_event(), Some(Event::TransportParametersReceived));
//! ```

#[macro_use]
extern crate alloc;

pub mod codec;
pub mod config;
pub mod connection;
pub mod event;
pub mod frame;
pub mod record;
pub mod stream;
pub mod transport_params;
