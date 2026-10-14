#![forbid(unsafe_code)]

//! QMux の tokio ベース非同期ラッパー
//!
//! [`Connection`] は下位トランスポート (TCP、TLS over TCP、UNIX ソケットなど) を
//! 所有し、Sans I/O の [`shiguredo_qmux`] 接続をバックグラウンドのドライバタスクで
//! 駆動する。アプリケーションは [`SendStream`] / [`RecvStream`] と DATAGRAM の
//! ハンドル越しに操作する。
//!
//! TLS と ALPN のネゴシエーションは呼び出し側の責務である。`Connection::new` には
//! 接続済みの双方向バイトストリームを渡す。
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
//! let client = Connection::new(Role::Client, client_io, config.clone());
//! let mut server = Connection::new(Role::Server, server_io, config);
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
pub use stream::{RecvStream, SendStream};
