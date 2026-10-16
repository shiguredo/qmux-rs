//! tokio-qmux の非同期 I/O 結合テスト
//!
//! `tokio::io::duplex` で接続した 2 つの [`Connection`] を使い、ストリーム、
//! DATAGRAM、切断を検証する。

use std::sync::Arc;

use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Duration;
use shiguredo_qmux::codec::time::Timestamp;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::event::CloseReason;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    let client = Connection::new(Role::Client, client_io, config())
        .expect("テスト用のクライアント接続を作成できる");
    let server = Connection::new(Role::Server, server_io, config())
        .expect("テスト用のサーバー接続を作成できる");
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
    let client = Connection::new(Role::Client, client_io, config())
        .expect("テスト用のクライアント接続を作成できる");
    let server = Connection::new(Role::Server, server_io, server_config)
        .expect("テスト用のサーバー接続を作成できる");

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
async fn peer_transport_parameters_are_readable_after_handshake() {
    let (client, _server) = pair();

    // ストリームを開くと、ピアのトランスポートパラメータを受信した後に完了する
    let (_send, _recv) = client
        .open_bidirectional()
        .await
        .expect("双方向ストリームを開ける");

    let params = client
        .peer_transport_parameters()
        .expect("ピアのトランスポートパラメータを取得できる");
    assert_eq!(
        params.initial_max_data,
        64 * 1024,
        "ピアが広告した initial_max_data が読めること"
    );
}

#[tokio::test]
async fn dropped_datagrams_are_counted() {
    // サーバーが DATAGRAM を読まないまま大量に送り、tokio 側のチャネル上限に
    // あふれた分が破棄数として観測できることを検証する。
    let (client, server) = pair();

    // ハンドシェイクを進める (確立まで DATAGRAM の送信は保留される)
    let _ = client.open_bidirectional().await.expect("開ける");
    for index in 0..300u32 {
        client
            .send_datagram(&index.to_be_bytes())
            .await
            .expect("DATAGRAM を送れる");
    }

    // すべて処理されたことを切断で同期する
    client.close(0, "done");
    server.closed().await;

    assert!(
        server.dropped_datagrams() > 0,
        "チャネル上限を超えた DATAGRAM の破棄数が観測できること"
    );
}

#[tokio::test]
async fn recv_datagrams_returns_arrived_datagrams_together() {
    let (client, server) = pair();
    let _ = client.open_bidirectional().await.expect("開ける");
    for payload in [b"a".as_slice(), b"b", b"c"] {
        client
            .send_datagram(payload)
            .await
            .expect("DATAGRAM を送れる");
    }

    // 配送が完了したことを切断で同期してからまとめて取り出す
    client.close(0, "done");
    server.closed().await;
    let batch = server
        .recv_datagrams()
        .await
        .expect("受信に失敗しない")
        .expect("DATAGRAM が届く");
    assert_eq!(batch.len(), 3, "到着済みの DATAGRAM をまとめて返すこと");
}

#[tokio::test]
async fn invalid_config_is_reported_as_error() {
    // Section 5.2: max_record_size が既定値未満の設定は接続の作成時に拒否する
    let mut invalid = config();
    invalid.max_record_size = 1;
    let (client_io, _server_io) = tokio::io::duplex(64 * 1024);
    match Connection::new(Role::Client, client_io, invalid) {
        Err(Error::Qmux(_)) => {}
        other => panic!("Qmux エラーを期待したが {other:?} だった"),
    }
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

/// 読み取って捨てるだけの生ピアのタスクを立てる
///
/// QMux を実装しないピアを想定し、届いたバイト列を読み捨てるだけで shutdown しない。
/// 送信側の shutdown による EOF を観測したら、その時刻と、ピアの送信側を閉じない
/// よう保持した io を返す。時刻は shutdown がドライバの終了より前に起きることの
/// 検証に使う (duplex ではドライバ終了時の drop でも EOF になるため)。
fn spawn_discarding_peer(
    mut peer_io: tokio::io::DuplexStream,
) -> tokio::task::JoinHandle<(std::time::Instant, tokio::io::DuplexStream)> {
    tokio::spawn(async move {
        let mut scratch = [0u8; 1024];
        loop {
            // QMux を実装しないピアは届いたバイト列を解釈せずに捨てる
            let len = peer_io
                .read(&mut scratch)
                .await
                .expect("生ピアの読み取りが失敗した");
            if len == 0 {
                // 下位トランスポートの送信側が shutdown された (EOF)
                break;
            }
        }
        (std::time::Instant::now(), peer_io)
    })
}

#[tokio::test]
async fn close_ends_driver_when_peer_never_shuts_down() {
    // Section 7.2: 自側から close した場合、ドライバは CONNECTION_CLOSE を送った後で
    // ピアの shutdown を待つ。ピアが shutdown しない限りドライバが終了しない実装では
    // 接続とタスクが残り続けるため、待ち時間には上限 (linger) が必要である。
    //
    // QMux を実装しない生ピアを用意し、ピアが shutdown しない場合でも linger の期限で
    // ドライバが終了し、closed() が自側の close の理由で解決することを検証する
    // (回帰テストの本体)。
    let (peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_millis(200),
    )
    .expect("テスト用のクライアント接続を作成できる");
    // 生ピアは読み取って捨てるだけにし、shutdown しない
    let peer = spawn_discarding_peer(peer_io);

    client.close(0x1234, "bye");

    // 5 秒の guard を置き、実装が壊れている場合にテストがハングしないようにする。
    // ピアが shutdown しないため、linger の上限がなければここは解決しない。
    let close_reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("linger の期限までにドライバが終了すること");
    match close_reason {
        CloseReason::Local {
            application,
            error_code,
            reason,
        } => {
            assert!(application, "アプリケーションエラーであること");
            assert_eq!(error_code, 0x1234, "close に渡したエラーコードが返ること");
            assert_eq!(reason, "bye", "close に渡した理由が返ること");
        }
        other => panic!("自側の close を期待したが {other:?} だった"),
    }

    // ドライバの終了時に下位トランスポートの送信側が閉じられ、ピアが EOF を観測して
    // 終了すること。ピアの io はテストの最後まで保持し、ピアの送信側を閉じない
    // (ピアの EOF と linger の期限を混同しないため)。
    let (_eof_at, _peer_io) = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
}

#[tokio::test]
async fn close_ends_driver_without_waiting_when_linger_is_zero() {
    // linger = ZERO は「送信側の shutdown を試みた後、ピアの shutdown を待たない」
    // ことを意味する。ピアが shutdown しない場合でも closed() が解決することを検証する。
    let (peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client =
        Connection::with_linger(Role::Client, client_io, config(), std::time::Duration::ZERO)
            .expect("テスト用のクライアント接続を作成できる");
    let peer = spawn_discarding_peer(peer_io);

    client.close(0x1234, "bye");

    // 5 秒の guard を置き、実装が壊れている場合にテストがハングしないようにする
    let reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("linger が ZERO の場合は期限を待たずにドライバが終了すること");
    match reason {
        CloseReason::Local { error_code, .. } => {
            assert_eq!(error_code, 0x1234, "close に渡したエラーコードが返ること");
        }
        other => panic!("自側の close を期待したが {other:?} だった"),
    }

    // ドライバの終了時に下位トランスポートの送信側が閉じられ、ピアが EOF を観測して
    // 終了すること
    let (_eof_at, _peer_io) = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
}

#[tokio::test]
async fn peer_shutdown_ends_driver_before_linger_expires() {
    // Section 7.2: ピアが送信側を shutdown すれば、ドライバは linger の期限を待たずに
    // 終了する。linger を 30 秒にして、ピアの shutdown が期限より先にドライバを
    // 終了させることを検証する。
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    // pair() は既定 linger の Connection::new を使うため、client だけ with_linger で
    // 作り直す (server は pair() と同じ構成)
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_secs(30),
    )
    .expect("テスト用のクライアント接続を作成できる");
    let server = Connection::new(Role::Server, server_io, config())
        .expect("テスト用のサーバー接続を作成できる");

    client.close(0x1234, "bye");

    // 30 秒の期限を待たず、ピアの shutdown で 2 秒以内にドライバが終了すること
    let reason = tokio::time::timeout(std::time::Duration::from_secs(2), client.closed())
        .await
        .expect("ピアの shutdown により linger の期限を待たずにドライバが終了すること");
    match reason {
        CloseReason::Local { error_code, .. } => {
            assert_eq!(error_code, 0x1234, "close に渡したエラーコードが返ること");
        }
        other => panic!("自側の close を期待したが {other:?} だった"),
    }

    // close がピアへ届き、ピア側はピア起因の切断として観測すること
    let server_reason = tokio::time::timeout(std::time::Duration::from_secs(5), server.closed())
        .await
        .expect("ピアが close を観測して終了すること");
    match server_reason {
        CloseReason::Peer { error_code, .. } => {
            assert_eq!(
                error_code, 0x1234,
                "ピアが受け取ったエラーコードが一致すること"
            );
        }
        other => panic!("ピアの切断を期待したが {other:?} だった"),
    }
}

#[tokio::test]
async fn close_ends_driver_while_write_is_blocked() {
    // Section 7.2: ピアが読み取りを止めて送信が詰まっている場合でも、close 要求は
    // write の詰まりに埋もれず、linger の期限でドライバが終了することを検証する。
    //
    // QMux を実装しない生ピアを Sans I/O の Connection (Role::Server) でハンドシェイク
    // だけ済ませ、その後は読み取らないようにする。クライアントが 64 KiB を書くと
    // 小さな duplex のバッファが埋まり、ドライバの write が詰まる。
    let (mut peer_io, client_io) = tokio::io::duplex(1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_millis(200),
    )
    .expect("テスト用のクライアント接続を作成できる");

    // 生ピアのハンドシェイク。クライアントの QX_TRANSPORT_PARAMETERS を処理し、
    // 自分のトランスポートパラメータを返すところまで進める
    let peer = tokio::spawn(async move {
        let mut peer = shiguredo_qmux::connection::Connection::new(Role::Server, config())
            .expect("テスト用のピア接続を作成できる");
        let now = Timestamp::from_micros(0);
        let mut scratch = [0u8; 1024];
        let mut output = Vec::new();
        while peer.peer_transport_parameters().is_none() {
            let len = peer_io
                .read(&mut scratch)
                .await
                .expect("生ピアがクライアントの送信を読み取れる");
            assert!(len > 0, "生ピアが EOF を観測しないこと");
            peer.receive(&scratch[..len], now);
            // poll_transmit は Vec に追記するため、呼ぶ前に clear する
            loop {
                output.clear();
                if !peer
                    .poll_transmit(&mut output, now)
                    .expect("送信する Record を取り出せる")
                {
                    break;
                }
                peer_io
                    .write_all(&output)
                    .await
                    .expect("生ピアがクライアントへ書き込める");
            }
        }
        // ここから先は読み取らない。詰まった write を作るため、io はテスト側へ渡して
        // 保持する
        peer_io
    });

    // ハンドシェイクが済むまで待つ (ピアはこの後読み取らない)
    let _peer_io = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアがハンドシェイクを終えること")
        .expect("ピアのタスクが終了すること");

    // クライアントは 64 KiB のストリームデータを書き、ピアが読み取らないためドライバの
    // write が詰まる。write の完了は待たず、結果も使わない
    let client = Arc::new(client);
    let writer_client = Arc::clone(&client);
    let writer = tokio::spawn(async move {
        let (mut send, _recv) = writer_client
            .open_bidirectional()
            .await
            .expect("双方向ストリームを開ける");
        let payload = vec![0u8; 64 * 1024];
        send.write(&payload).await
    });

    // ドライバの write が詰まるのを待つ。ピアが読み取らないため write は完了しない
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !writer.is_finished(),
        "ピアが読み取らないため 64 KiB の write は完了しないこと"
    );

    // write が詰まっていても close 要求が処理されること
    client.close(0x1234, "bye");

    // 5 秒の guard を置き、実装が壊れている場合にテストがハングしないようにする
    let reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("write が詰まっていても linger の期限までにドライバが終了すること");
    match reason {
        CloseReason::Local { error_code, .. } => {
            assert_eq!(error_code, 0x1234, "close に渡したエラーコードが返ること");
        }
        other => panic!("自側の close を期待したが {other:?} だった"),
    }

    // 詰まった write のタスクは結果を使わないため終了させる
    writer.abort();
    let _ = writer.await;
}

#[tokio::test]
async fn write_side_is_shut_down_before_linger_expires() {
    // Section 7.2: 自側の close の後、ドライバは CONNECTION_CLOSE を送ってから下位
    // トランスポートの送信側を graceful に shutdown する。ドライバの終了時に io を
    // drop するだけでもピアは EOF を観測するため、EOF の有無だけでは shutdown を
    // 検証できない。ここでは「ピアの EOF がドライバの終了より 1 秒以上早いこと」で
    // shutdown を判別する。linger を 2 秒にしているため、shutdown が実装されていれば
    // EOF は close の直後に届き、ドライバは期限の約 2 秒後に終了する。
    let (peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_secs(2),
    )
    .expect("テスト用のクライアント接続を作成できる");
    // 生ピアは読み取って捨てるだけにし、shutdown しない
    let peer = spawn_discarding_peer(peer_io);

    // close() を呼ぶ直前を起点に、EOF の観測時刻と closed() の解決時刻を測る
    let close_started = std::time::Instant::now();
    client.close(0x1234, "bye");

    // 5 秒の guard を置き、実装が壊れている場合にテストがハングしないようにする
    let close_reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("linger の期限までにドライバが終了すること");
    let driver_elapsed = close_started.elapsed();
    match close_reason {
        CloseReason::Local { error_code, .. } => {
            assert_eq!(error_code, 0x1234, "close に渡したエラーコードが返ること");
        }
        other => panic!("自側の close を期待したが {other:?} だった"),
    }

    // ピアが EOF を観測した時刻。io はテストの最後まで保持し、ピアの送信側を閉じない
    let (eof_at, _peer_io) = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
    let eof_elapsed = eof_at.duration_since(close_started);

    assert!(
        eof_elapsed + std::time::Duration::from_secs(1) < driver_elapsed,
        "送信側の shutdown がドライバの終了より前にピアへ届くこと: \
         EOF {eof_elapsed:?} / ドライバ終了 {driver_elapsed:?}"
    );
}

#[tokio::test]
async fn max_linger_does_not_panic_and_peer_shutdown_ends_driver() {
    // linger に Duration::MAX を渡しても、期限の計算で桁あふれ (panic) しないことを
    // 検証する。期限を Instant::now() + linger で計算する実装ではドライバが panic し、
    // 理由を通知せずに終了するため closed() が CloseReason::TransportClosed になる。
    // ピアが shutdown するため、期限を待たずに CloseReason::Local で解決すること。
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    // pair() は既定 linger の Connection::new を使うため、client だけ with_linger で
    // 作り直す (server は pair() と同じ構成)
    let client =
        Connection::with_linger(Role::Client, client_io, config(), std::time::Duration::MAX)
            .expect("テスト用のクライアント接続を作成できる");
    let server = Connection::new(Role::Server, server_io, config())
        .expect("テスト用のサーバー接続を作成できる");

    client.close(0x1234, "bye");

    // 2 秒の guard を置き、ピアの shutdown で期限を待たずに終了することを確認する
    let close_reason = tokio::time::timeout(std::time::Duration::from_secs(2), client.closed())
        .await
        .expect("ピアの shutdown により期限を待たずにドライバが終了すること");
    match close_reason {
        CloseReason::Local { error_code, .. } => {
            assert_eq!(error_code, 0x1234, "close に渡したエラーコードが返ること");
        }
        other => panic!("自側の close を期待したが {other:?} だった"),
    }

    // close がピアへ届き、ピア側はピア起因の切断として観測すること
    let server_reason = tokio::time::timeout(std::time::Duration::from_secs(5), server.closed())
        .await
        .expect("ピアが close を観測して終了すること");
    match server_reason {
        CloseReason::Peer { error_code, .. } => {
            assert_eq!(
                error_code, 0x1234,
                "ピアが受け取ったエラーコードが一致すること"
            );
        }
        other => panic!("ピアの切断を期待したが {other:?} だった"),
    }
}
