//! tokio-qmux の非同期 I/O 結合テスト
//!
//! `tokio::io::duplex` で接続した 2 つの [`Connection`] を使い、ストリーム、
//! DATAGRAM、切断を検証する。

use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Duration;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::event::CloseReason;
use tokio_qmux::{Connection, RecvStream};

/// テスト用の設定
fn config() -> Config {
    Config {
        max_idle_timeout: Duration::from_secs(30),
        initial_max_data: 64 * 1024,
        initial_max_stream_data_bidi_local: 16 * 1024,
        initial_max_stream_data_bidi_remote: 16 * 1024,
        initial_max_stream_data_uni: 16 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        max_datagram_frame_size: Some(65535),
        ..Config::default()
    }
}

/// 接続の組を作る
fn pair() -> (Connection, Connection) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::new(Role::Client, client_io, config());
    let server = Connection::new(Role::Server, server_io, config());
    (client, server)
}

/// ストリームの終端まで読み取る
async fn read_to_end(recv: &mut RecvStream) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match recv.read(&mut buf).await.expect("読み取りに失敗した") {
            Some(0) => continue,
            Some(len) => data.extend_from_slice(&buf[..len]),
            None => return data,
        }
    }
}

#[tokio::test]
async fn bidirectional_stream_transfers_data() {
    let (client, mut server) = pair();

    let (mut send, _recv) = client
        .open_bidirectional()
        .await
        .expect("双方向ストリームを開ける");
    send.write(b"hello").await.expect("書き込める");
    send.finish().await.expect("FIN を送れる");

    let (_send, mut recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("双方向ストリームが届く");
    assert_eq!(read_to_end(&mut recv).await, b"hello");
}

#[tokio::test]
async fn data_split_across_reads_is_reassembled() {
    let (client, mut server) = pair();

    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    for chunk in [b"abc".as_slice(), b"def", b"ghi"] {
        send.write(chunk).await.expect("書き込める");
    }
    send.finish().await.expect("FIN を送れる");

    let (_send, mut recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");
    assert_eq!(read_to_end(&mut recv).await, b"abcdefghi");
}

#[tokio::test]
async fn unidirectional_stream_transfers_data() {
    let (client, mut server) = pair();

    let mut send = client
        .open_unidirectional()
        .await
        .expect("単方向ストリームを開ける");
    send.write(b"one-way").await.expect("書き込める");
    send.finish().await.expect("FIN を送れる");

    let mut recv = server
        .accept_unidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("単方向ストリームが届く");
    assert_eq!(read_to_end(&mut recv).await, b"one-way");
}

#[tokio::test]
async fn server_can_open_stream_to_client() {
    let (mut client, server) = pair();

    let (mut send, _recv) = server.open_bidirectional().await.expect("開ける");
    send.write(b"from-server").await.expect("書き込める");
    send.finish().await.expect("FIN を送れる");

    let (_send, mut recv) = client
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");
    assert_eq!(read_to_end(&mut recv).await, b"from-server");
}

#[tokio::test]
async fn write_waits_for_flow_control_and_keeps_data() {
    // ピアのストリームクレジットを小さくし、write がフロー制御で保留される経路を
    // 通ってもデータが欠けず、FIN で途中に切断されないことを検証する。
    let mut server_config = config();
    server_config.initial_max_stream_data_bidi_remote = 8;
    server_config.initial_max_data = 8;
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::new(Role::Client, client_io, config());
    let mut server = Connection::new(Role::Server, server_io, server_config);

    // サーバーが読み続けないとクレジットが戻らないため、先に読み取りタスクを立てる。
    let reader = tokio::spawn(async move {
        let (_send, mut recv) = server
            .accept_bidirectional()
            .await
            .expect("accept に失敗しない")
            .expect("ストリームが届く");
        read_to_end(&mut recv).await
    });

    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    let payload: Vec<u8> = (0..1024u32).map(|index| (index % 251) as u8).collect();
    send.write(&payload)
        .await
        .expect("フロー制御を待って書き込める");
    send.finish().await.expect("FIN を送れる");

    let received = reader.await.expect("読み取りタスクが終了する");
    assert_eq!(
        received, payload,
        "フロー制御で分割されてもデータが欠けないこと"
    );
}

#[tokio::test]
async fn datagram_is_delivered() {
    let (client, mut server) = pair();

    client
        .send_datagram(b"datagram")
        .await
        .expect("DATAGRAM を送れる");
    let received = server
        .recv_datagram()
        .await
        .expect("受信に失敗しない")
        .expect("DATAGRAM が届く");
    assert_eq!(received, b"datagram");
}

#[tokio::test]
async fn local_close_is_reported_to_peer() {
    let (client, server) = pair();

    client.close(0x1234, "bye");

    let reason = server.closed().await;
    match reason {
        CloseReason::Peer {
            application,
            error_code,
            ..
        } => {
            assert!(application, "アプリケーションエラーであること");
            assert_eq!(error_code, 0x1234, "エラーコードが一致すること");
        }
        other => panic!("ピアの切断を期待したが {other:?} だった"),
    }
}
