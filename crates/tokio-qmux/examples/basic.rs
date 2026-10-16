//! `tokio::io::duplex` で接続した 2 つの [`tokio_qmux::Connection`] の最小例。
//!
//! 実行: `cargo run -p tokio-qmux --example basic`

use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::config::Config;
use tokio_qmux::Connection;

#[tokio::main]
async fn main() -> Result<(), tokio_qmux::Error> {
    // Config::default() はフロー制御のクレジットが 0 で何も送れないため、
    // ストリームを使う場合は明示的に設定する。
    let config = Config {
        initial_max_data: 64 * 1024,
        initial_max_stream_data_bidi_local: 16 * 1024,
        initial_max_stream_data_bidi_remote: 16 * 1024,
        initial_max_stream_data_uni: 16 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        ..Config::default()
    };
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::new(Role::Client, client_io, config.clone())?;
    let server = Connection::new(Role::Server, server_io, config)?;

    let (mut send, _recv) = client.open_bidirectional().await?;
    send.write(b"hello").await?;
    send.finish().await?;

    let (_send, mut recv) = server
        .accept_bidirectional()
        .await?
        .expect("peer opened a stream");
    let mut buf = [0u8; 5];
    assert_eq!(recv.read(&mut buf).await?, Some(5));
    println!("received: {}", String::from_utf8_lossy(&buf));
    Ok(())
}
