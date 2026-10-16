//! tokio-qmux の非同期 I/O 結合テスト
//!
//! `tokio::io::duplex` で接続した 2 つの [`Connection`] を使い、ストリーム、
//! DATAGRAM、切断を検証する。

use std::sync::Arc;

use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Duration;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::event::CloseReason;
use tokio_qmux::{Connection, Error, RecvStream};

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
    let (client, server) = pair();

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
    let (client, server) = pair();

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
    let (client, server) = pair();

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
    let (client, server) = pair();

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
    let server = Connection::new(Role::Server, server_io, server_config);

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
async fn peer_stop_sending_is_reported_to_send_stream() {
    let (client, server) = pair();

    // クライアントが双方向ストリームを開き、データを送ってサーバーにストリームを作らせる
    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    send.write(b"hello").await.expect("書き込める");
    let (_send, mut recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");

    // サーバーが受信側から送信停止を要求する (STOP_SENDING)
    recv.stop_sending(0x99)
        .await
        .expect("STOP_SENDING を送れる");

    // クライアントの送信側でピアの STOP_SENDING を観測できる
    assert_eq!(
        send.stopped().await.expect("STOP_SENDING を観測できる"),
        0x99,
        "ピアが指定したエラーコードが通知されること"
    );
}

#[tokio::test]
async fn write_after_stop_sending_fails_with_stopped() {
    let (client, server) = pair();

    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    send.write(b"hello").await.expect("書き込める");
    let (_send, mut recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");

    recv.stop_sending(0x7).await.expect("STOP_SENDING を送れる");

    // ピアの送信停止を観測してから、送信側が終端していることを確認する
    assert_eq!(
        send.stopped().await.expect("STOP_SENDING を観測できる"),
        0x7
    );
    match send.write(b"ignored").await {
        Err(Error::Stopped(code)) => assert_eq!(code, 0x7, "エラーコードが一致すること"),
        other => panic!("Stopped を期待したが {other:?} だった"),
    }
}

#[tokio::test]
async fn stop_sending_is_observable_at_connection_level_while_writing() {
    // MOQT のように接続単位で STOP_SENDING を待つ利用側を想定する。送信ストリームを
    // 所有するタスクが write のために &mut を握っていても、別タスクが Arc で共有した
    // Connection の recv_stop_sending を同時に待てることを検証する。
    let (client, server) = pair();
    let client = Arc::new(client);

    // 接続単位の観測タスク。送信ストリームには触れない
    let observer = Arc::clone(&client);
    let observe_task = tokio::spawn(async move {
        observer
            .recv_stop_sending()
            .await
            .expect("受信に失敗しない")
            .expect("STOP_SENDING が届く")
    });

    // 送信側は書き込みのため SendStream を &mut で保持し続ける
    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    send.write(b"hello").await.expect("書き込める");
    let (_send, mut recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");

    recv.stop_sending(0x42)
        .await
        .expect("STOP_SENDING を送れる");

    let observed = observe_task.await.expect("タスクが終了する");
    assert_eq!(observed.id, send.id(), "対象のストリーム ID が一致すること");
    assert_eq!(observed.error_code, 0x42, "エラーコードが一致すること");

    // 送信側は送信停止を検出して次の write が失敗する
    match send.write(b"ignored").await {
        Err(Error::Stopped(code)) => assert_eq!(code, 0x42, "エラーコードが一致すること"),
        other => panic!("Stopped を期待したが {other:?} だった"),
    }
}

#[tokio::test]
async fn datagram_is_delivered() {
    let (client, server) = pair();

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
async fn acceptors_and_datagram_can_be_waited_from_separate_tasks() {
    // MOQT のように双方向ストリーム・単方向ストリーム・DATAGRAM を別々のタスクで
    // 待つ利用側を想定し、Arc で共有した接続に対して &self の accept 系を同時に
    // 待てることを検証する。
    let (client, server) = pair();
    let server = Arc::new(server);

    let bidi_server = Arc::clone(&server);
    let bidi_task = tokio::spawn(async move {
        bidi_server
            .accept_bidirectional()
            .await
            .expect("accept に失敗しない")
            .expect("双方向ストリームが届く")
    });
    let uni_server = Arc::clone(&server);
    let uni_task = tokio::spawn(async move {
        uni_server
            .accept_unidirectional()
            .await
            .expect("accept に失敗しない")
            .expect("単方向ストリームが届く")
    });
    let datagram_server = Arc::clone(&server);
    let datagram_task = tokio::spawn(async move {
        datagram_server
            .recv_datagram()
            .await
            .expect("受信に失敗しない")
            .expect("DATAGRAM が届く")
    });

    let (mut send, _recv) = client
        .open_bidirectional()
        .await
        .expect("双方向ストリームを開ける");
    send.write(b"bidi").await.expect("書き込める");
    send.finish().await.expect("FIN を送れる");
    let mut send_uni = client
        .open_unidirectional()
        .await
        .expect("単方向ストリームを開ける");
    send_uni.write(b"uni").await.expect("書き込める");
    send_uni.finish().await.expect("FIN を送れる");
    client
        .send_datagram(b"dgram")
        .await
        .expect("DATAGRAM を送れる");

    let (_send, mut recv) = bidi_task.await.expect("タスクが終了する");
    assert_eq!(read_to_end(&mut recv).await, b"bidi");
    let mut recv_uni = uni_task.await.expect("タスクが終了する");
    assert_eq!(read_to_end(&mut recv_uni).await, b"uni");
    assert_eq!(datagram_task.await.expect("タスクが終了する"), b"dgram");
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
