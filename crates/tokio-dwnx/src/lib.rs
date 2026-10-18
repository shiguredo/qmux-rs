//! dwnx の tokio ベース非同期ラッパー
//!
//! [`Connection`] は下位トランスポートを所有し、`dwnx-rs` の Sans I/O 接続を
//! バックグラウンドのドライバタスクで駆動する。アプリケーションはストリームの
//! ハンドルと DATAGRAM の API 越しに操作する。
//!
//! DATAGRAM を使うには、双方のトランスポートパラメータで
//! `max_datagram_frame_size` を 0 より大きくする (RFC 9221 Section 3)。
//! 受信した DATAGRAM は保持量の上限を超えると破棄され、破棄した個数は
//! [`Connection::dropped_datagrams`] で観測できる。

#![forbid(unsafe_code)]

mod connection;
mod error;
mod stream;

pub use connection::{Connection, DEFAULT_LINGER};
pub use error::{Error, Result};
pub use stream::{RecvStream, SendStream, StopSendingObserved};

/// 接続のロール
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// クライアント
    Client,
    /// サーバー
    Server,
}
