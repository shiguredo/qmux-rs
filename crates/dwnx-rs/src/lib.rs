//! dwnx (QMux の C 実装) の安全な Sans I/O ラッパー
//!
//! [`Connection`] は `dwnx-sys` が提供する `dwnx_conn` を所有し、受信バイト列の
//! 処理、Record の生成、ストリームとタイムアウトの操作を提供する。下位
//! トランスポートの読み書きは行わない。
//!
//! TLS と ALPN のネゴシエーションは呼び出し側の責務である。

mod config;
mod connection;
mod error;
mod event;

pub use config::{MAX_CONFIG_STREAMS, TransportParameters};
pub use connection::{Connection, WriteEvent};
pub use error::Error;
pub use event::Event;
