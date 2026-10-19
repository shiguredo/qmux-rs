//! tokio-qmux の非同期 I/O 結合テスト
//!
//! `tokio::io::duplex` で接続した 2 つの [`Connection`] を使い、ストリーム、
//! DATAGRAM、切断を検証する。

use std::sync::Arc;

use shiguredo_qmux::codec::error::Error as QmuxError;
use shiguredo_qmux::codec::frame::Frame as QuicFrame;
use shiguredo_qmux::codec::frame::max_streams::MaxStreams;
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Duration;
use shiguredo_qmux::codec::time::Timestamp;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::event::CloseReason;
use shiguredo_qmux::frame::QmuxFrame;
use shiguredo_qmux::record;
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
async fn open_waits_for_max_streams_when_peer_limit_is_zero() {
    // ピアが初期上限 0 を広告した場合でも、open_bidirectional は一時エラーを
    // 返さず、ピアが MAX_STREAMS で上限を増やすまで待つ。ドライバは open 要求を
    // 保留し、Event::StreamsAvailable の処理時に再試行する。
    //
    // ピアは Sans I/O の接続でハンドシェイクを済ませ、クライアントが上限 0 で
    // open を試みたこと (STREAMS_BLOCKED の受信) を確認してから MAX_STREAMS を
    // 1 つだけ送る。
    let mut peer_config = config();
    peer_config.initial_max_streams_bidi = 0;
    let (mut peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Arc::new(
        Connection::new(Role::Client, client_io, config())
            .expect("テスト用のクライアント接続を作成できる"),
    );

    let peer = tokio::spawn(async move {
        let mut peer = shiguredo_qmux::connection::Connection::new(Role::Server, peer_config)
            .expect("テスト用のピア接続を作成できる");
        handshake_sans_io_peer(&mut peer_io, &mut peer).await;

        // クライアントの open が上限 0 で失敗すると、STREAMS_BLOCKED を含む Record が
        // 届く。これを受信した時点で open が保留されたことが分かる。
        let now = Timestamp::from_micros(0);
        let mut scratch = [0u8; 1024];
        let len = peer_io
            .read(&mut scratch)
            .await
            .expect("ピアがクライアントの送信を読み取れる");
        assert!(len > 0, "ストリーム上限のブロック通知が届くこと");
        peer.receive(&scratch[..len], now);

        // 上限を 1 に増やす MAX_STREAMS を送る (RFC 9000 Section 19.11)
        let frame = QmuxFrame::Quic(QuicFrame::MaxStreams(MaxStreams {
            bidirectional: true,
            maximum_streams: 1,
        }));
        let mut encoded = Vec::new();
        frame.encode(&mut encoded);
        let mut record_bytes = Vec::new();
        record::encode(&encoded, record::DEFAULT_MAX_RECORD_SIZE, &mut record_bytes)
            .expect("MAX_STREAMS の Record をエンコードできる");
        peer_io
            .write_all(&record_bytes)
            .await
            .expect("ピアが MAX_STREAMS を送信できる");

        // クライアントが開いたストリームのデータを読み取る
        let len = peer_io
            .read(&mut scratch)
            .await
            .expect("ピアがクライアントの送信を読み取れる");
        assert!(len > 0, "クライアントが開いたストリームへ書き込むこと");
        peer.receive(&scratch[..len], now);
        peer_io
    });

    let opener = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.open_bidirectional().await }
    });

    let (mut send, _recv) = tokio::time::timeout(std::time::Duration::from_secs(5), opener)
        .await
        .expect("MAX_STREAMS の受信で open が解決すること")
        .expect("open のタスクが終了すること")
        .expect("ストリームを開けること");
    send.write(b"opened")
        .await
        .expect("開いたストリームへ書き込める");
    send.finish().await.expect("FIN を送れる");

    tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアのタスクが終了すること")
        .expect("ピアのタスクが異常終了しないこと");
}

#[tokio::test]
async fn pending_open_fails_when_connection_closes() {
    // ストリーム上限待ちで保留している open は、接続が閉じた時点で
    // Error::ConnectionClosed として失敗する。ドライバの linger の期限まで
    // 呼び出し側を待たせない。
    let mut peer_config = config();
    peer_config.initial_max_streams_bidi = 0;
    let (mut peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Arc::new(
        Connection::with_linger(
            Role::Client,
            client_io,
            config(),
            std::time::Duration::from_millis(200),
        )
        .expect("テスト用のクライアント接続を作成できる"),
    );

    let peer = tokio::spawn(async move {
        let mut peer = shiguredo_qmux::connection::Connection::new(Role::Server, peer_config)
            .expect("テスト用のピア接続を作成できる");
        handshake_sans_io_peer(&mut peer_io, &mut peer).await;
        // クライアントの open が上限 0 で失敗し、STREAMS_BLOCKED が届くまで待つ
        let mut scratch = [0u8; 1024];
        let len = peer_io
            .read(&mut scratch)
            .await
            .expect("ピアがクライアントの送信を読み取れる");
        assert!(len > 0, "ストリーム上限のブロック通知が届くこと");
        peer_io
    });

    let opener = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.open_bidirectional().await }
    });

    // STREAMS_BLOCKED の受信で open が保留されたことを確認する
    let _peer_io = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("クライアントが open を試みること")
        .expect("ピアのタスクが終了すること");

    // 接続を閉じると、保留中の open は ConnectionClosed で失敗する
    client.close(0, "bye").expect("close 要求を送れる");
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), opener)
        .await
        .expect("保留中の open が接続の閉鎖で失敗すること")
        .expect("open のタスクが終了すること")
        .expect_err("ストリームは開けない");
    assert!(
        matches!(error, Error::ConnectionClosed),
        "ConnectionClosed になること: {error:?}"
    );
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
    client.close(0, "done").expect("close 要求を送れる");
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
    client.close(0, "done").expect("close 要求を送れる");
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

    client.close(0x1234, "bye").expect("close 要求を送れる");

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

/// Sans I/O のピアをハンドシェイクまで進める
///
/// `peer_io` で接続した Sans I/O の `peer` にクライアントの Record を処理させ、
/// ピアのトランスポートパラメータを受信するまで自分の Record を書き出す。
async fn handshake_sans_io_peer(
    peer_io: &mut tokio::io::DuplexStream,
    peer: &mut shiguredo_qmux::connection::Connection,
) {
    let now = Timestamp::from_micros(0);
    let mut scratch = [0u8; 1024];
    let mut output = Vec::new();
    while peer.peer_transport_parameters().is_none() {
        let len = peer_io
            .read(&mut scratch)
            .await
            .expect("ピアがクライアントの送信を読み取れる");
        assert!(len > 0, "ピアが EOF を観測しないこと");
        peer.receive(&scratch[..len], now);
        // poll_transmit は Vec に追記するため、呼ぶ前に clear する
        output.clear();
        while peer
            .poll_transmit(&mut output, now)
            .expect("送信する Record を取り出せる")
        {}
        if !output.is_empty() {
            peer_io
                .write_all(&output)
                .await
                .expect("ピアがクライアントへ書き込める");
        }
    }
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

    client.close(0x1234, "bye").expect("close 要求を送れる");

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

    client.close(0x1234, "bye").expect("close 要求を送れる");

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

    client.close(0x1234, "bye").expect("close 要求を送れる");

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
async fn peer_transport_eof_is_reported_as_peer_shutdown() {
    // Section 7.3: 下位トランスポートの読み取りで EOF (ピアの送信側 shutdown) を
    // 観測した場合、ドライバは自側の送信側を graceful に shutdown し、切断理由を
    // CloseReason::PeerShutdown として通知する。読み取りエラー (Section 7.4) の
    // TransportClosed とは区別する。
    let (mut peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_secs(300),
    )
    .expect("テスト用のクライアント接続を作成できる");

    let peer = tokio::spawn(async move {
        let mut peer = shiguredo_qmux::connection::Connection::new(Role::Server, config())
            .expect("テスト用のピア接続を作成できる");
        handshake_sans_io_peer(&mut peer_io, &mut peer).await;

        // QMux フレームを送らずに送信側だけを shutdown する (Section 7.3 の観測事象)
        peer_io
            .shutdown()
            .await
            .expect("ピアが送信側を shutdown できる");
        // クライアントが送信側を shutdown するまで読み捨てる
        let mut scratch = [0u8; 1024];
        loop {
            let len = peer_io
                .read(&mut scratch)
                .await
                .expect("ピアの読み取りが失敗した");
            if len == 0 {
                break;
            }
        }
        peer_io
    });

    let reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("ピアの送信側 shutdown を検知してドライバが終了すること");
    assert_eq!(
        reason,
        CloseReason::PeerShutdown,
        "ピアの送信側 shutdown として通知されること"
    );

    // 自側の送信側も shutdown され、ピアが EOF を観測して終了すること
    tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
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
    let mut peer_io = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
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

    // ドライバが write を開始したことを、ピア側で最初のバイト列を読み取れることで
    // 確認する。ピアはここで読み取りをやめるため、64 KiB の write は完了しない。
    // 時間に頼らず「まだ完了していない」ことを確かめるための同期である。
    let mut probe = [0u8; 1024];
    let received =
        tokio::time::timeout(std::time::Duration::from_secs(5), peer_io.read(&mut probe))
            .await
            .expect("ドライバが write を開始すること")
            .expect("ピアがクライアントの送信を読み取れること");
    assert!(received > 0, "ドライバがストリームデータを送信していること");
    assert!(
        !writer.is_finished(),
        "ピアが読み取らないため 64 KiB の write は完了しないこと"
    );

    // write が詰まっていても close 要求が処理されること
    client.close(0x1234, "bye").expect("close 要求を送れる");

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
    // 検証できない。ここでは linger をテストの guard より十分に大きくし、ピアが EOF を
    // 観測した時点でドライバがまだ終了していないこと (ピアの shutdown を待っている
    // こと) で shutdown を判別する。時間の差の測定には頼らない。
    let (peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_secs(300),
    )
    .expect("テスト用のクライアント接続を作成できる");
    // 生ピアは読み取って捨てるだけにし、shutdown しない
    let peer = spawn_discarding_peer(peer_io);

    client.close(0x1234, "bye").expect("close 要求を送れる");

    // 送信側の shutdown が実装されていれば、ピアは linger の期限を待たずに EOF を
    // 観測する。実装されていない場合、EOF はドライバが終了する 300 秒後になるため
    // この guard で失敗する。
    let (_eof_at, _peer_io) = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("送信側の shutdown によりピアが EOF を観測すること")
        .expect("ピアのタスクが終了すること");
    // ピアはまだ送信側を shutdown していないため、ドライバは linger の上限まで
    // 待っており、終了していない
    assert!(
        !client.is_closed(),
        "ピアの shutdown を待つ間はドライバが終了しないこと"
    );

    // テストの終了時に Connection をドロップすると、ドライバは linger を待たずに
    // 終了する
    drop(client);
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

    client.close(0x1234, "bye").expect("close 要求を送れる");

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

#[tokio::test]
async fn reset_propagates_to_peer_and_read_returns_reset() {
    // RFC 9000 Section 3.5: RESET_STREAM を送ると、ピアの受信側は
    // Error::Reset として観測する。書き込み途中のデータがあっても通知される。
    let (client, server) = pair();

    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    send.write(b"partial").await.expect("書き込める");
    send.reset(0x55).await.expect("リセットできる");

    let (_send, mut recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");
    // 送信途中のデータはリセットより先に読める。リセットまで読み続ける。
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut buf = [0u8; 16];
        loop {
            match recv.read(&mut buf).await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("リセットの前に終端した"),
                Err(error) => return error,
            }
        }
    })
    .await
    .expect("リセットの通知が届くこと");
    match error {
        Error::Reset(code) => assert_eq!(code, 0x55, "エラーコードが一致すること"),
        other => panic!("Reset を期待したが {other:?} だった"),
    }

    // リセットで送信側は終端するため、以降の書き込みは受け付けない
    assert!(
        matches!(send.write(b"x").await, Err(Error::StreamFinished)),
        "終端したストリームへの書き込みは StreamFinished になること"
    );
}

#[tokio::test]
async fn ping_response_is_reported_by_recv_ping_ack() {
    // Section 4.3: QX_PING の要求にピアが応答し、応答のシーケンス番号を
    // アプリケーションが観測できること。
    let (client, _server) = pair();

    let sequence = client.ping().await.expect("QX_PING を送れる");
    let acknowledged =
        tokio::time::timeout(std::time::Duration::from_secs(5), client.recv_ping_ack())
            .await
            .expect("応答が届くこと")
            .expect("受信に失敗しない")
            .expect("応答が観測できること");
    assert_eq!(
        acknowledged, sequence,
        "送信したシーケンス番号への応答が返ること"
    );
}

#[tokio::test]
async fn dropping_recv_stream_sends_stop_sending() {
    // RFC 9000 Section 3.5: アプリケーションが受信側を破棄したら、ドライバは
    // STOP_SENDING (NO_ERROR) を送り、ピアの送信側を終端させる。追加のデータを
    // 待たずに送られること (アイドル中のストリームでも通知されること) を確認する。
    let (client, server) = pair();

    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    send.write(b"hello").await.expect("書き込める");
    let (_send, recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");

    // 受信側を破棄するだけで、ドライバがピアへ STOP_SENDING を送る
    let id = recv.id();
    drop(recv);

    let observed = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.recv_stop_sending(),
    )
    .await
    .expect("STOP_SENDING が届くこと")
    .expect("受信に失敗しない")
    .expect("STOP_SENDING が観測できること");
    assert_eq!(observed.id, id, "対象のストリーム ID が一致すること");
    assert_eq!(observed.error_code, 0, "エラーコードが 0 であること");
}

#[tokio::test]
async fn reset_is_not_lost_when_recv_event_channel_is_full() {
    // アプリケーションが読み取らないまま受信イベントチャネルが満杯になっても、
    // リセットは失われず、溜まったデータを読み切った後に Error::Reset として
    // 観測できること (制御信号を破棄しない実装の回帰テスト)。
    let mut server_config = config();
    // アプリケーションが読まなくてもクライアントが書き切れるようクレジットを上げる
    server_config.initial_max_stream_data_bidi_remote = 256 * 1024;
    server_config.initial_max_data = 1 << 20;
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::new(Role::Client, client_io, config())
        .expect("テスト用のクライアント接続を作成できる");
    let server = Connection::new(Role::Server, server_io, server_config)
        .expect("テスト用のサーバー接続を作成できる");

    // チャネルの容量 (16) を超える回数だけ書き込み、サーバーは accept だけして読まない
    let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
    let payload = vec![0x5au8; 512];
    for _ in 0..32 {
        send.write(&payload)
            .await
            .expect("フロー制御の範囲で書き込める");
    }
    let (_send, mut recv) = server
        .accept_bidirectional()
        .await
        .expect("accept に失敗しない")
        .expect("ストリームが届く");
    send.reset(0x77).await.expect("リセットできる");

    // 溜まったデータを読み切り、その後にリセットを観測する
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut received = 0usize;
        let mut buf = [0u8; 1024];
        loop {
            match recv.read(&mut buf).await {
                Ok(Some(len)) => received += len,
                Ok(None) => panic!("リセットの前に終端した"),
                Err(error) => return (received, error),
            }
        }
    })
    .await
    .expect("リセットの通知が届くこと");
    assert!(
        outcome.0 > 0,
        "リセットまでに届いたデータを読み取れること: {} バイト",
        outcome.0
    );
    match outcome.1 {
        Error::Reset(code) => assert_eq!(code, 0x77, "リセットのエラーコードが一致すること"),
        other => panic!("Reset を期待したが {other:?} だった"),
    }
}

#[tokio::test]
async fn peer_streams_are_kept_until_accepted() {
    // accept の受け渡しチャネル (容量 16) より多くストリームを開かれても、
    // ストリームを破棄せず、accept した順にすべて受け取れること。
    let mut server_config = config();
    server_config.initial_max_streams_bidi = 32;
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::new(Role::Client, client_io, config())
        .expect("テスト用のクライアント接続を作成できる");
    let server = Connection::new(Role::Server, server_io, server_config)
        .expect("テスト用のサーバー接続を作成できる");

    // 20 本のストリームを開いて、それぞれ識別用の 1 バイトを送る
    let mut sends = Vec::new();
    for index in 0..20u8 {
        let (mut send, _recv) = client.open_bidirectional().await.expect("開ける");
        send.write(&[index]).await.expect("書き込める");
        sends.push(send);
    }

    // accept しないまま 20 本が届くのを待ち、その後すべて受け取れることを確認する
    let mut received = Vec::new();
    for _ in 0..20 {
        let (_send, mut recv) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.accept_bidirectional(),
        )
        .await
        .expect("accept が進むこと")
        .expect("accept に失敗しない")
        .expect("ストリームが届く");
        let mut buf = [0u8; 1];
        let len = tokio::time::timeout(std::time::Duration::from_secs(5), recv.read(&mut buf))
            .await
            .expect("データが届くこと")
            .expect("読み取りに失敗しない")
            .expect("データがあること");
        assert_eq!(len, 1, "1 バイト読み取れること");
        received.push(buf[0]);
    }
    assert_eq!(
        received,
        (0..20u8).collect::<Vec<_>>(),
        "accept した順にすべてのストリームを受け取れること"
    );
    assert_eq!(sends.len(), 20, "クライアント側の送信側を保持できること");
}

#[tokio::test]
async fn accept_progresses_without_reads_when_accept_channel_is_full() {
    // accept の受け渡しチャネル (容量 16) を超えてストリームが届いた場合でも、
    // アプリが accept を繰り返すだけで残りが配送されること。ストリームを読まないと
    // ドライバは起床しないため、配送待ちキューに残った分を配送するきっかけが
    // accept 側にあることを検証する (回帰テスト)。
    //
    // ピアは Sans I/O の実装で駆動し、20 本を開いて 1 バイトずつ送った後に一切
    // フレームを送らない。これにより、配送のきっかけが accept 側にあることを
    // 確かめられる。
    let mut server_config = config();
    server_config.initial_max_streams_bidi = 32;
    let (mut peer_io, server_io) = tokio::io::duplex(64 * 1024);
    let server = Connection::new(Role::Server, server_io, server_config)
        .expect("テスト用のサーバー接続を作成できる");

    // ピアはハンドシェイクを済ませ、20 本の双方向ストリームに 1 バイトずつ送る
    let mut peer = shiguredo_qmux::connection::Connection::new(Role::Client, config())
        .expect("テスト用のピア接続を作成できる");
    let now = Timestamp::from_micros(0);
    let mut scratch = [0u8; 1024];
    let mut output = Vec::new();
    while peer.peer_transport_parameters().is_none() {
        let len = peer_io
            .read(&mut scratch)
            .await
            .expect("ピアがサーバーの送信を読み取れる");
        assert!(len > 0, "ピアが EOF を観測しないこと");
        peer.receive(&scratch[..len], now);
        output.clear();
        while peer
            .poll_transmit(&mut output, now)
            .expect("送信する Record を取り出せる")
        {}
        if !output.is_empty() {
            peer_io
                .write_all(&output)
                .await
                .expect("ピアがサーバーへ書き込める");
        }
    }
    output.clear();
    for index in 0..20u8 {
        let id = peer.open_bidi().expect("ピアが双方向ストリームを開ける");
        peer.write(id, &[index], false).expect("ピアが書き込める");
    }
    while peer
        .poll_transmit(&mut output, now)
        .expect("送信する Record を取り出せる")
    {}
    peer_io
        .write_all(&output)
        .await
        .expect("ピアがサーバーへ書き込める");

    // サーバーは accept だけを繰り返す。読み取りもハンドルの破棄もしない
    // (受信側を破棄するとドライバが起こされてしまうため、すべて保持する)
    let mut accepted = Vec::new();
    for _ in 0..20 {
        let handles = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.accept_bidirectional(),
        )
        .await
        .expect("読み取らなくても accept が進むこと")
        .expect("accept に失敗しない")
        .expect("ストリームが届く");
        accepted.push(handles);
    }

    let mut ids: Vec<u64> = accepted
        .iter()
        .map(|(_, recv)| recv.id().as_u64())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 20, "20 本すべてが重複なく届くこと");
    // ピアは accept の間も一切フレームを送っていない (配送のきっかけは accept 側)
    assert!(
        peer.peer_transport_parameters().is_some(),
        "ピアがサーバーのパラメータを受信済みであること"
    );
}

#[tokio::test]
async fn receive_and_accept_progress_while_write_is_blocked() {
    // ピアが下位トランスポートを読まなくなり、自側の write が詰まっている間も、
    // 受信・accept・ピアの切断の検知が進むこと (書き込みをメイン select に
    // 統合した実装の回帰テスト)。
    let (mut peer_io, client_io) = tokio::io::duplex(1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_secs(1),
    )
    .expect("テスト用のクライアント接続を作成できる");

    // 生ピアはハンドシェイクだけ済ませ、その後は読み取らない
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
            while peer
                .poll_transmit(&mut output, now)
                .expect("送信する Record を取り出せる")
            {}
            if !output.is_empty() {
                peer_io
                    .write_all(&output)
                    .await
                    .expect("生ピアがクライアントへ書き込める");
                output.clear();
            }
        }
        (peer_io, peer)
    });

    // ハンドシェイクが済むまで待ち、ピアの io と Sans I/O 接続をテスト側で保持する
    let (mut peer_io, mut peer) = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアがハンドシェイクを終えること")
        .expect("ピアのタスクが終了すること");

    // ピアが読み取らないため、クライアントの write が詰まる
    let client = Arc::new(client);
    let blocked_client = Arc::clone(&client);
    let blocked = tokio::spawn(async move {
        let (mut send, _recv) = blocked_client
            .open_bidirectional()
            .await
            .expect("双方向ストリームを開ける");
        let payload = vec![0u8; 64 * 1024];
        send.write(&payload).await
    });
    // ドライバが write を開始したことを、ピア側で最初のバイト列を読み取れることで
    // 確認する。ピアはここで読み取りをやめるため、64 KiB の write は完了しない。
    let mut probe = [0u8; 1024];
    let received =
        tokio::time::timeout(std::time::Duration::from_secs(5), peer_io.read(&mut probe))
            .await
            .expect("ドライバが write を開始すること")
            .expect("ピアがクライアントの送信を読み取れること");
    assert!(received > 0, "ドライバがストリームデータを送信していること");
    assert!(
        !blocked.is_finished(),
        "ピアが読み取らないため write は完了しないこと"
    );

    // 詰まっている間も、ピアが開いたストリームを受信して accept できる
    let now = Timestamp::from_micros(0);
    let id = peer.open_bidi().expect("ピアが双方向ストリームを開ける");
    peer.write(id, b"progress", true).expect("ピアが書き込める");
    let mut output = Vec::new();
    while peer
        .poll_transmit(&mut output, now)
        .expect("送信する Record を取り出せる")
    {}
    peer_io
        .write_all(&output)
        .await
        .expect("ピアがクライアントへ書き込める");

    let (_send, mut recv) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.accept_bidirectional(),
    )
    .await
    .expect("write が詰まっていても accept が進むこと")
    .expect("accept に失敗しない")
    .expect("ストリームが届く");
    let data = tokio::time::timeout(std::time::Duration::from_secs(5), read_to_end(&mut recv))
        .await
        .expect("write が詰まっていても受信できること");
    assert_eq!(data, b"progress", "受信したデータが一致すること");

    // ピアが下位トランスポートの送信側を shutdown すると、ドライバは Section 7.3 の
    // ピア shutdown として扱い、自側の送信側を shutdown してから終了する。write が
    // 詰まっているため送信側の shutdown は linger の期限まで待つが、終了理由は
    // 下位トランスポートの閉塞 (Section 7.4) ではなく PeerShutdown になる。
    peer_io
        .shutdown()
        .await
        .expect("ピアが送信側を shutdown できる");
    let reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("ピアの送信側 shutdown を検知してドライバが終了すること");
    assert_eq!(
        reason,
        CloseReason::PeerShutdown,
        "ピアの送信側 shutdown として通知されること"
    );

    blocked.abort();
    let _ = blocked.await;
}

#[tokio::test]
async fn idle_timeout_shuts_down_write_side_before_driver_ends() {
    // Section 7.1 / Section 7.2: アイドルタイムアウトでも、ドライバは下位
    // トランスポートの送信側を graceful に shutdown し、ピアの送信側 shutdown を
    // linger の上限まで待つ (自側の close と同じ扱い)。
    //
    // ピアは EOF を観測した時点でいったん止まり、テストが合図するまで自分の送信側を
    // shutdown しない。時刻の差ではなく「EOF の後にまだドライバが終了していないこと」
    // と「ピアの shutdown で終了すること」で待ち合わせを検証する。
    let mut client_config = config();
    client_config.max_idle_timeout = Duration::from_millis(300);
    let (mut peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        client_config,
        // ピアが shutdown しない間はドライバが待ち続けることを明確にするため、
        // テストの guard より十分に大きい上限にする
        std::time::Duration::from_secs(300),
    )
    .expect("テスト用のクライアント接続を作成できる");

    let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut scratch = [0u8; 1024];
        loop {
            let len = peer_io
                .read(&mut scratch)
                .await
                .expect("ピアの読み取りが失敗した");
            if len == 0 {
                // クライアントの送信側が shutdown された (EOF)
                break;
            }
        }
        // 送信側の shutdown を観測したことをテストへ通知し、合図を待つ
        eof_tx.send(()).expect("テストが EOF を待っている");
        release_rx.await.expect("テストが shutdown を指示する");
        peer_io
            .shutdown()
            .await
            .expect("ピアが送信側を shutdown できる");
        peer_io
    });

    tokio::time::timeout(std::time::Duration::from_secs(5), eof_rx)
        .await
        .expect("アイドルタイムアウトで送信側が shutdown されること")
        .expect("テストが EOF の通知を受け取れること");
    assert!(
        !client.is_closed(),
        "ピアの shutdown を待つ間はドライバが終了しないこと"
    );

    release_tx.send(()).expect("ピアが shutdown を待っている");
    let reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("ピアの shutdown でドライバが終了すること");
    assert_eq!(
        reason,
        CloseReason::IdleTimeout,
        "アイドルタイムアウトとして通知されること"
    );

    tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアのタスクが終了すること")
        .expect("ピアのタスクが異常終了しないこと");
}

#[tokio::test]
async fn idle_timeout_does_not_send_unsent_record() {
    // Section 7.2: アイドルタイムアウトが発火した時点でまだ 1 バイトも書き出して
    // いない Record は破棄し、QMux フレームを送らずに送信側を shutdown する。
    //
    // 容量 0 の duplex は書き込みを一切受け付けないため、ドライバは Record を
    // 生成しても written == 0 のままになる。ここで未送信 Record を破棄して
    // shutdown しなければ、ドライバは書き込みの完了を待ち続けて linger の期限まで
    // 終了しない。
    let mut client_config = config();
    client_config.max_idle_timeout = Duration::from_millis(300);
    let (mut peer_io, client_io) = tokio::io::duplex(0);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        client_config,
        std::time::Duration::from_secs(300),
    )
    .expect("テスト用のクライアント接続を作成できる");

    let peer = tokio::spawn(async move {
        let mut scratch = [0u8; 1024];
        let mut received = 0usize;
        loop {
            let len = peer_io
                .read(&mut scratch)
                .await
                .expect("ピアの読み取りが失敗した");
            if len == 0 {
                // クライアントの送信側が shutdown された。クライアントが読む側にも
                // EOF を届けるため、ピアも送信側を shutdown する
                peer_io
                    .shutdown()
                    .await
                    .expect("ピアが送信側を shutdown できる");
                break;
            }
            received += len;
        }
        (received, peer_io)
    });

    // 300 秒の linger を待たずに終了すること。未送信 Record を破棄して shutdown
    // すれば、ピアの shutdown でドライバが終了する。
    let reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("未送信 Record を破棄してドライバが終了すること");
    assert_eq!(
        reason,
        CloseReason::IdleTimeout,
        "アイドルタイムアウトとして通知されること"
    );

    let (received, _peer_io) = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
    assert_eq!(
        received, 0,
        "アイドルタイムアウト後に QMux フレームを送らないこと"
    );
}

#[tokio::test]
async fn peer_close_shuts_down_write_side_before_driver_ends() {
    // Section 7.3: ピア起因の切断でも、ドライバは終了前に送信側を graceful に
    // shutdown する。ピアが EOF を観測する時刻がドライバの終了より前であることで
    // 判別する (終了時の drop だけでも EOF は観測できるため、順序で判別する)。
    let (mut peer_io, client_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(
        Role::Client,
        client_io,
        config(),
        std::time::Duration::from_secs(2),
    )
    .expect("テスト用のクライアント接続を作成できる");

    // 生ピアはハンドシェイクの後に切断し、クライアントの shutdown を観測する
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
            while peer
                .poll_transmit(&mut output, now)
                .expect("送信する Record を取り出せる")
            {}
            if !output.is_empty() {
                peer_io
                    .write_all(&output)
                    .await
                    .expect("生ピアがクライアントへ書き込める");
                output.clear();
            }
        }

        // ピアから切断する (CONNECTION_CLOSE を送る)
        peer.close(0x99, "peer closes")
            .expect("ピアが接続を閉じられる");
        while peer
            .poll_transmit(&mut output, now)
            .expect("送信する Record を取り出せる")
        {}
        peer_io
            .write_all(&output)
            .await
            .expect("生ピアがクライアントへ書き込める");

        // クライアントが送信側を shutdown するまで読み捨てる
        let eof_at = loop {
            let len = peer_io
                .read(&mut scratch)
                .await
                .expect("生ピアの読み取りが失敗した");
            if len == 0 {
                break std::time::Instant::now();
            }
        };
        (eof_at, peer_io)
    });

    let started = std::time::Instant::now();
    let reason = tokio::time::timeout(std::time::Duration::from_secs(5), client.closed())
        .await
        .expect("ピアの切断でドライバが終了すること");
    let driver_elapsed = started.elapsed();
    match reason {
        CloseReason::Peer { error_code, .. } => {
            assert_eq!(error_code, 0x99, "ピアのエラーコードが返ること");
        }
        other => panic!("ピアの切断を期待したが {other:?} だった"),
    }

    // ピアが EOF を観測した時刻がドライバの終了より前であること。終了時の drop で
    // 閉じる実装では、EOF の観測がドライバの終了より後になる。
    let (eof_at, _peer_io) = tokio::time::timeout(std::time::Duration::from_secs(5), peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
    let eof_elapsed = eof_at.duration_since(started);
    assert!(
        eof_elapsed <= driver_elapsed,
        "送信側の shutdown がドライバの終了より前にピアへ届くこと: \
         EOF {eof_elapsed:?} / ドライバ終了 {driver_elapsed:?}"
    );
}

#[tokio::test]
async fn send_datagram_fails_when_peer_does_not_support_it() {
    // RFC 9221 Section 3: DATAGRAM を広告していないピアへは送れず、失敗が
    // 呼び出し側へ返ること。
    let mut server_config = config();
    server_config.max_datagram_frame_size = None;
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::new(Role::Client, client_io, config())
        .expect("テスト用のクライアント接続を作成できる");
    let _server = Connection::new(Role::Server, server_io, server_config)
        .expect("テスト用のサーバー接続を作成できる");

    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.send_datagram(b"x"),
    )
    .await
    .expect("送信が完了すること")
    .expect_err("DATAGRAM 非対応のピアへは送れない");
    assert!(
        matches!(error, Error::Qmux(QmuxError::InvalidData(_))),
        "ローカルのエラーとして返ること: {error:?}"
    );
}

#[tokio::test]
async fn send_datagram_fails_when_larger_than_peer_limit() {
    // RFC 9221 Section 3: ピアが広告した max_datagram_frame_size を超える
    // DATAGRAM は送れず、失敗が呼び出し側へ返ること。
    let mut server_config = config();
    server_config.max_datagram_frame_size = Some(8);
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::new(Role::Client, client_io, config())
        .expect("テスト用のクライアント接続を作成できる");
    let _server = Connection::new(Role::Server, server_io, server_config)
        .expect("テスト用のサーバー接続を作成できる");

    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.send_datagram(&[0u8; 32]),
    )
    .await
    .expect("送信が完了すること")
    .expect_err("上限を超える DATAGRAM は送れない");
    assert!(
        matches!(error, Error::Qmux(QmuxError::InvalidData(_))),
        "ローカルのエラーとして返ること: {error:?}"
    );
}

#[tokio::test]
async fn close_with_invalid_error_code_returns_error() {
    // RFC 9000 Section 19.19: アプリケーションエラーコードは可変長整数として
    // 表現できなければならない。2^62-1 を超える値は要求として受け付けず、
    // ローカルのエラーとして返すこと。
    let (client, _server) = pair();
    let error = client
        .close(u64::MAX, "too large")
        .expect_err("範囲外のエラーコードは受け付けない");
    assert!(
        matches!(error, Error::Qmux(QmuxError::InvalidData(_))),
        "ローカルのエラーとして返ること: {error:?}"
    );
    assert!(!client.is_closed(), "要求を送らないため接続は閉じないこと");
}
