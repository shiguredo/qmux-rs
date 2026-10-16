#![forbid(unsafe_code)]

//! QMux の tokio ベース非同期ラッパー
//!
//! [`Connection`] は下位トランスポート (TCP、TLS over TCP、UNIX ソケットなど) を
//! 所有し、Sans I/O の [`shiguredo_qmux`] 接続をバックグラウンドのドライバタスクで
//! 駆動する。アプリケーションは [`SendStream`] / [`RecvStream`] と DATAGRAM の
//! ハンドル越しに操作する。
//!
//! 複数タスクからの利用を想定し、待ち受ける系のメソッド ([`Connection::accept_bidirectional`]、
//! [`Connection::accept_unidirectional`]、[`Connection::recv_datagram`]、
//! [`Connection::recv_datagrams`]、[`Connection::recv_stop_sending`]、
//! [`Connection::closed`]) は `&self` を取り、`select!` の腕に置いても安全な
//! cancel safety を持つ。
//!
//! TLS と ALPN のネゴシエーションは呼び出し側の責務である。[`Connection::new`] には
//! 接続済みの双方向バイトストリームを渡す。
//!
//! # QUIC の上で動かす場合との違い
//!
//! QMux は QUIC と API が似ているが、下位が順序保証された単一のバイトストリーム
//! (TCP など) であるため、振る舞いが異なる。
//!
//! - DATAGRAM は信頼的かつ順序どおりに配送される (draft-ietf-quic-qmux-02
//!   Section 1.1)。QUIC の DATAGRAM (RFC 9221) のような非信頼・順不同配送では
//!   ない。ただしアプリケーションが速やかに受け取れない場合は破棄されることが
//!   あり、破棄した個数は [`Connection::dropped_datagrams`] で確認できる
//! - すべてのストリームが 1 本のバイトストリームを共有するため、トランスポート
//!   層の head-of-line blocking は解消されない。ストリーム多重化による恩恵は
//!   QUIC のそれとは異なる
//!
//! ```
//! use shiguredo_qmux::codec::role::Role;
//! use shiguredo_qmux::config::Config;
//! use tokio_qmux::Connection;
//!
//! # async fn example() -> Result<(), tokio_qmux::Error> {
//! let config = Config {
//!     initial_max_data: 64 * 1024,
//!     initial_max_stream_data_bidi_local: 16 * 1024,
//!     initial_max_stream_data_bidi_remote: 16 * 1024,
//!     initial_max_stream_data_uni: 16 * 1024,
//!     initial_max_streams_bidi: 8,
//!     initial_max_streams_uni: 8,
//!     ..Config::default()
//! };
//! let (client_io, server_io) = tokio::io::duplex(64 * 1024);
//! let client = Connection::new(Role::Client, client_io, config.clone())?;
//! let server = Connection::new(Role::Server, server_io, config)?;
//!
//! let (mut send, _recv) = client.open_bidirectional().await?;
//! send.write(b"hello").await?;
//! send.finish().await?;
//!
//! let (_send, mut recv) = server.accept_bidirectional().await?.expect("stream");
//! let mut buf = [0u8; 5];
//! assert_eq!(recv.read(&mut buf).await?, Some(5));
//! assert_eq!(&buf, b"hello");
//! # Ok(())
//! # }
//! ```

mod connection;
mod error;
mod stream;

pub use connection::Connection;
pub use error::{Error, Result};
pub use stream::{RecvStream, SendStream, StopSendingObserved};
