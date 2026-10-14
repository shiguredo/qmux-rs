//! dwnx の tokio ベース非同期ラッパー
//!
//! [`Connection`] は下位トランスポートを所有し、`dwnx-rs` の Sans I/O 接続を
//! バックグラウンドのドライバタスクで駆動する。

mod connection;
mod error;
mod stream;

pub use connection::Connection;
pub use error::{Error, Result};
pub use stream::{RecvStream, SendStream};

/// 接続のロール
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client,
    Server,
}
