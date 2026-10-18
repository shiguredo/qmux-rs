//! tokio-dwnx 接続のテスト
//!
//! `tokio::io::duplex` で 2 つの接続を作り、実際の dwnx を使って非同期の
//! データ転送と終了処理を検証する。テストが停止しないよう、待ち合わせには
//! 必ず guard timeout を置く。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use dwnx_rs::Connection as DwnxConnection;
use dwnx_rs::Event as DwnxEvent;
use dwnx_rs::TransportParameters;
use tokio_dwnx::{Connection, DEFAULT_LINGER, RecvStream, Role};

/// 待ち合わせの上限
const GUARD: Duration = Duration::from_secs(5);

/// テストで使う linger の上限
///
/// ピアが shutdown しない場合のテストを短く終わらせるため、既定値より短くする。
const TEST_LINGER: Duration = Duration::from_millis(100);

/// 送信側の shutdown を判別するテストで使う linger の上限
///
/// shutdown を送らない実装では、ピアが EOF を観測するのはこの期限の満了時点に
/// なる。期限を長くとることで、shutdown が期限を待たずに届くことを判別できる。
const SHUTDOWN_LINGER: Duration = Duration::from_secs(2);

/// テスト用の設定
///
/// アイドルタイムアウトは 30 秒にして、テスト中に満了しないようにする。
fn test_config() -> TransportParameters {
    TransportParameters {
        initial_max_stream_data_bidi_local: 64 * 1024,
        initial_max_stream_data_bidi_remote: 64 * 1024,
        initial_max_stream_data_uni: 64 * 1024,
        initial_max_data: 256 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        max_idle_timeout: 30_000_000_000,
        ..TransportParameters::default()
    }
}

/// duplex で接続した 2 つの dwnx 接続を作る
fn connect(linger: Duration) -> (Connection, Connection) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, test_config(), linger)
        .expect("クライアントを作成できる");
    let server = Connection::with_linger(Role::Server, server_io, test_config(), linger)
        .expect("サーバーを作成できる");
    (client, server)
}

/// ストリームを終端まで読み取る
async fn read_to_end(recv: &mut RecvStream) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let read = tokio::time::timeout(GUARD, recv.read(&mut buf))
            .await
            .expect("guard の時間内に読み取れる")
            .expect("読み取りに失敗しない");
        match read {
            // 0 バイトの読み取りはデータがないことを表すため読み直す
            Some(0) => continue,
            Some(len) => data.extend_from_slice(&buf[..len]),
            None => return data,
        }
    }
}

/// 指定した長さのデータを読み取る
///
/// FIN を待たずに読み取れるため、送信側がまだストリームを閉じていない
/// テストで使う。
async fn read_len(recv: &mut RecvStream, len: usize) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    while data.len() < len {
        let read = tokio::time::timeout(GUARD, recv.read(&mut buf))
            .await
            .expect("guard の時間内に読み取れる")
            .expect("読み取りに失敗しない");
        match read {
            Some(0) => continue,
            Some(read_len) => data.extend_from_slice(&buf[..read_len]),
            None => break,
        }
    }
    data
}

/// 接続を閉じてドライバの終了を待つ
async fn close_and_wait(connection: &Connection, error_code: u64, reason: &str) {
    connection.close(error_code, reason);
    tokio::time::timeout(GUARD, connection.closed())
        .await
        .expect("guard の時間内に閉じる");
}

#[tokio::test]
async fn bidirectional_data_transfer() {
    let (client, server) = connect(DEFAULT_LINGER);

    // クライアントが双方向ストリームを開いてデータを送る
    let (mut send, mut recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    send.write(b"hello from client")
        .await
        .expect("クライアントが書き込める");
    send.finish().await.expect("クライアントが FIN を送れる");

    // サーバー側でストリームを受け取り、データを読む
    let (mut server_send, mut server_recv) =
        tokio::time::timeout(GUARD, server.accept_bidirectional())
            .await
            .expect("guard の時間内に accept できる")
            .expect("ストリームが届く");
    let data = read_to_end(&mut server_recv).await;
    assert_eq!(
        data, b"hello from client",
        "サーバーがクライアントのデータを受け取ること"
    );

    // サーバーがエコーを返す
    server_send
        .write(&data)
        .await
        .expect("サーバーが書き込める");
    server_send.finish().await.expect("サーバーが FIN を送れる");
    let echoed = read_to_end(&mut recv).await;
    assert_eq!(echoed, data, "クライアントがエコーを受け取ること");

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn data_is_split_across_records() {
    // 1 つの Record (16382 バイト) に収まらない大きさのデータを送る
    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let (client, server) = connect(DEFAULT_LINGER);

    let (mut send, _recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    send.write(&payload).await.expect("データを書き込める");
    send.finish().await.expect("FIN を送れる");

    let (_, mut server_recv) = tokio::time::timeout(GUARD, server.accept_bidirectional())
        .await
        .expect("guard の時間内に accept できる")
        .expect("ストリームが届く");

    // 小さなバッファで繰り返し読み、複数回に分けて届くことを確認する
    let mut data = Vec::new();
    let mut buf = [0u8; 1024];
    let mut reads = 0;
    loop {
        let read = tokio::time::timeout(GUARD, server_recv.read(&mut buf))
            .await
            .expect("guard の時間内に読み取れる")
            .expect("読み取りに失敗しない");
        match read {
            Some(0) => continue,
            Some(len) => {
                data.extend_from_slice(&buf[..len]);
                reads += 1;
            }
            None => break,
        }
    }
    assert_eq!(
        data.len(),
        payload.len(),
        "受け取るデータの長さが一致すること"
    );
    assert_eq!(data, payload, "受け取るデータが一致すること");
    assert!(reads > 1, "データが複数回に分けて届くこと");

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn idle_timeout_resolves_closed() {
    // アイドルタイムアウトを 100 ミリ秒にする
    let params = TransportParameters {
        max_idle_timeout: 100 * 1_000_000,
        ..test_config()
    };
    // ピアは応答しないため、アイドルタイムアウトだけが接続を終わらせる
    let (io, _peer) = tokio::io::duplex(64 * 1024);
    let connection = Connection::new(Role::Client, io, params).expect("クライアントを作成できる");

    tokio::time::timeout(GUARD, connection.closed())
        .await
        .expect("アイドルタイムアウトで closed() が解決すること");
    assert!(
        connection.is_closed(),
        "アイドルタイムアウトで閉じたことが分かること"
    );
}

#[tokio::test]
async fn close_waits_for_peer_shutdown_within_linger() {
    // ピアが送信側を shutdown しない場合でも、linger の上限で終了すること
    let (io, _peer) = tokio::io::duplex(64 * 1024);
    let connection = Connection::with_linger(Role::Client, io, test_config(), TEST_LINGER)
        .expect("クライアントを作成できる");

    connection.close(0, "bye");
    tokio::time::timeout(GUARD, connection.closed())
        .await
        .expect("ピアが shutdown しなくても linger の上限で終了すること");
}

#[tokio::test]
async fn close_finishes_when_peer_stops_reading() {
    // ピアが読み取らないため CONNECTION_CLOSE を書き切れない状況でも、
    // linger の上限でドライバが終了すること
    let (io, _peer) = tokio::io::duplex(64);
    let connection = Connection::with_linger(Role::Client, io, test_config(), TEST_LINGER)
        .expect("クライアントを作成できる");

    // 書き切れない大きさの理由句を渡す
    connection.close(0, &"x".repeat(8000));
    tokio::time::timeout(GUARD, connection.closed())
        .await
        .expect("送信が詰まっても linger の上限で終了すること");
}

#[tokio::test]
async fn close_truncates_long_reason() {
    // Record の上限を超える理由句を渡しても、接続が終了すること
    let (client, server) = connect(TEST_LINGER);
    client.close(1, &"あ".repeat(20_000));
    server.close(1, &"い".repeat(20_000));

    tokio::time::timeout(GUARD, client.closed())
        .await
        .expect("理由句が長くてもクライアントが終了すること");
    tokio::time::timeout(GUARD, server.closed())
        .await
        .expect("理由句が長くてもサーバーが終了すること");
}

#[tokio::test]
async fn invalid_config_is_rejected() {
    // dwnx が受け付けない設定は、接続を作る前にエラーになること
    let cases: [(&str, TransportParameters); 4] = [
        (
            "max_record_size が既定値未満",
            TransportParameters {
                max_record_size: 0,
                ..test_config()
            },
        ),
        (
            "max_record_size が既定値超",
            TransportParameters {
                // dwnx はアプリケーションの指定を既定値へ強制するため、
                // 既定値以外は設定の誤りとして拒否する
                max_record_size: 65535,
                ..test_config()
            },
        ),
        (
            "max_idle_timeout が UINT64_MAX",
            TransportParameters {
                max_idle_timeout: u64::MAX,
                ..test_config()
            },
        ),
        (
            "initial_max_data が DWNX_MAX_VARINT 超",
            TransportParameters {
                initial_max_data: u64::MAX,
                ..test_config()
            },
        ),
    ];

    for (name, params) in cases {
        let (io, _peer) = tokio::io::duplex(1024);
        let result = Connection::new(Role::Client, io, params);
        assert!(result.is_err(), "{name} の接続作成がエラーになること");
    }
}

#[tokio::test]
async fn stop_sending_is_notified_and_write_fails() {
    let (client, server) = connect(DEFAULT_LINGER);

    // クライアントがストリームを開いてデータを送る
    let (mut send, _recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    send.write(b"hello").await.expect("データを書き込める");

    // サーバーがストリームを受け取り、受信側を閉じて STOP_SENDING を送る
    let (_, mut server_recv) = tokio::time::timeout(GUARD, server.accept_bidirectional())
        .await
        .expect("guard の時間内に accept できる")
        .expect("ストリームが届く");
    let data = read_len(&mut server_recv, 5).await;
    assert_eq!(data, b"hello", "サーバーがデータを受け取ること");
    server_recv
        .stop_sending(9)
        .await
        .expect("STOP_SENDING を送れる");

    // クライアントが STOP_SENDING を観測する
    let observed = tokio::time::timeout(GUARD, client.recv_stop_sending())
        .await
        .expect("guard の時間内に通知が届く")
        .expect("STOP_SENDING が届く");
    assert_eq!(observed.id, send.id(), "ストリーム ID が一致すること");
    assert_eq!(observed.error_code, 9, "エラーコードが一致すること");

    // 送信側が閉じているため、以後の書き込みはエラーになる
    let result = tokio::time::timeout(GUARD, send.write(b"more"))
        .await
        .expect("guard の時間内に応答が返る");
    assert!(result.is_err(), "送信停止後の書き込みがエラーになること");

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn reset_notifies_peer() {
    let (client, server) = connect(DEFAULT_LINGER);

    // クライアントがストリームを開いてデータを送る
    let (mut send, _recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    send.write(b"hello").await.expect("データを書き込める");

    let (_, mut server_recv) = tokio::time::timeout(GUARD, server.accept_bidirectional())
        .await
        .expect("guard の時間内に accept できる")
        .expect("ストリームが届く");
    assert_eq!(
        read_len(&mut server_recv, 5).await,
        b"hello",
        "サーバーがデータを受け取ること"
    );

    // クライアントが送信側をリセットすると、サーバーの読み取りがエラーになる
    send.reset(3).await.expect("RESET_STREAM を送れる");
    let mut buf = [0u8; 64];
    let result = tokio::time::timeout(GUARD, server_recv.read(&mut buf))
        .await
        .expect("guard の時間内に応答が返る");
    match result {
        Err(tokio_dwnx::Error::Reset(code)) => {
            assert_eq!(code, 3, "リセットのエラーコードが一致すること");
        }
        other => panic!("リセットが通知されない: {other:?}"),
    }

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn dropping_connection_ends_driver() {
    // Connection をドロップすると、ドライバが下位トランスポートを閉じて終了すること
    let (client, server) = connect(DEFAULT_LINGER);
    let (mut send, _recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    send.write(b"hello").await.expect("データを書き込める");

    drop(client);
    drop(send);

    // サーバー側は下位トランスポートの終了を観測して閉じる
    tokio::time::timeout(GUARD, server.closed())
        .await
        .expect("クライアントのドロップでサーバーが閉じること");
}

#[tokio::test]
async fn peer_close_is_observed() {
    let (client, server) = connect(DEFAULT_LINGER);

    // サーバーが閉じると、クライアントがそれを観測すること
    server.close(7, "bye");
    tokio::time::timeout(GUARD, client.closed())
        .await
        .expect("ピアの close でクライアントが閉じること");
    assert!(client.is_closed(), "ピアの close で閉じたことが分かること");
}

#[tokio::test]
async fn transport_end_is_observed() {
    // 下位トランスポートの終了を接続が観測すること
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let connection =
        Connection::new(Role::Client, io, test_config()).expect("クライアントを作成できる");

    // ピア側を閉じると読み取りが終端に達する
    peer.shutdown().await.expect("shutdown できる");
    drop(peer);

    tokio::time::timeout(GUARD, connection.closed())
        .await
        .expect("下位トランスポートの終了で closed() が解決すること");
}

#[tokio::test]
async fn deferred_delivery_resumes_while_app_reads() {
    // アプリケーションが読み続けている限り、追加のコマンドを送らなくても
    // 配送待ちに積まれたデータが欠けずに全部届くこと (F-2 / D-1)
    const TOTAL: usize = 4 * 1024 * 1024;

    let (client, server) = connect(DEFAULT_LINGER);
    let (mut send, mut recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    // ピアにストリームを見せるため 1 バイトだけ送る
    send.write(b"x").await.expect("データを書き込める");

    let (mut server_send, _server_recv) =
        tokio::time::timeout(GUARD, server.accept_bidirectional())
            .await
            .expect("guard の時間内に accept できる")
            .expect("ストリームが届く");

    // サーバーは固定量を送り切って FIN を送る
    let writer = tokio::spawn(async move {
        let chunk = vec![0x5au8; 16 * 1024];
        let mut sent = 0;
        while sent < TOTAL {
            let len = server_send
                .write(&chunk)
                .await
                .expect("サーバーが書き込める");
            sent += len;
        }
        server_send.finish().await.expect("サーバーが FIN を送れる");
    });

    // クライアントはまず読まない。これで配送待ちのキューが埋まる
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !writer.is_finished(),
        "読まない間はフロー制御でサーバーの送信が止まること"
    );

    // 以後は読み続けるだけにする。追加のコマンドは送らない
    let data = read_to_end(&mut recv).await;
    assert_eq!(
        data.len(),
        TOTAL,
        "読み続けている限り全データが届くこと (配送待ちの再開)"
    );
    assert!(
        data.iter().all(|byte| *byte == 0x5a),
        "受け取るデータが一致すること"
    );
    tokio::time::timeout(GUARD, writer)
        .await
        .expect("guard の時間内にサーバーが送り切る")
        .expect("サーバーのタスクが終了する");

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn deferred_delivery_is_bounded_by_flow_control() {
    // アプリケーションが読まない間は、配送待ちのキューがフロー制御で有界になり
    // (クレジットは配送できた時点でしか返らない)、読み始めると欠けずに届くこと (F-2 / D-2)
    const STREAM_WINDOW: usize = 32 * 1024;

    let client_config = TransportParameters {
        initial_max_stream_data_bidi_local: STREAM_WINDOW as u64,
        initial_max_stream_data_bidi_remote: STREAM_WINDOW as u64,
        initial_max_data: 8 * STREAM_WINDOW as u64,
        ..test_config()
    };
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, client_config, TEST_LINGER)
        .expect("クライアントを作成できる");
    let server = Connection::with_linger(Role::Server, server_io, test_config(), TEST_LINGER)
        .expect("サーバーを作成できる");

    let (mut send, mut recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    send.write(b"x").await.expect("データを書き込める");

    let (mut server_send, _server_recv) =
        tokio::time::timeout(GUARD, server.accept_bidirectional())
            .await
            .expect("guard の時間内に accept できる")
            .expect("ストリームが届く");

    // サーバーは受け付けられる限り送り続け、受け付けられた量を数える
    let pushed = Arc::new(AtomicUsize::new(0));
    let pushed_writer = Arc::clone(&pushed);
    let writer = tokio::spawn(async move {
        let chunk = vec![0x5au8; 16 * 1024];
        while let Ok(len) = server_send.write(&chunk).await {
            pushed_writer.fetch_add(len, Ordering::SeqCst);
        }
    });

    // クライアントは読まない。ウィンドウとチャネルの容量を超えた分は
    // フロー制御で止まる
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let pushed_first = pushed.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let pushed_second = pushed.load(Ordering::SeqCst);

    assert_eq!(
        pushed_second, pushed_first,
        "読まない間は受け付けられる量が増え続けないこと (フロー制御で有界)"
    );
    assert!(
        pushed_first < 4 * 1024 * 1024,
        "受け付けられる量がウィンドウとチャネル容量の範囲に収まること (実際は {pushed_first} バイト)"
    );

    // 送信を止めてから読み始める。配送待ちに積まれた分も欠けずに届くこと
    writer.abort();
    let _ = writer.await;
    let total = pushed.load(Ordering::SeqCst);

    let mut received = 0;
    let mut buf = [0u8; 4096];
    while received < total {
        let read = tokio::time::timeout(GUARD, recv.read(&mut buf))
            .await
            .expect("guard の時間内に読み取れる")
            .expect("読み取りに失敗しない");
        match read {
            Some(0) => continue,
            Some(len) => {
                assert!(
                    buf[..len].iter().all(|byte| *byte == 0x5a),
                    "受け取るデータが一致すること"
                );
                received += len;
            }
            None => break,
        }
    }
    assert!(
        received >= total,
        "送信側が受け付けたデータが欠けずに届くこと (受け付け {total} バイト、受信 {received} バイト)"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn idle_timeout_resolves_while_write_is_blocked() {
    // ピアが読み取りを止めて書き込みが詰まっている間も、アイドルタイムアウトで
    // ドライバが終了すること (I-14: 書き込みをメインループの select! に統合)
    let client_config = TransportParameters {
        max_idle_timeout: 300 * 1_000_000,
        ..test_config()
    };
    let peer_config = TransportParameters {
        max_idle_timeout: 0,
        ..test_config()
    };

    // 下位トランスポートのバッファを小さくし、ピアが読み取らないとすぐ詰まるようにする
    let (client_io, peer_io) = tokio::io::duplex(4096);
    let client = Connection::with_linger(Role::Client, client_io, client_config, TEST_LINGER)
        .expect("クライアントを作成できる");

    // ピアはハンドシェイクだけ手動で済ませ、以後は読み取らない
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut io = peer_io;
        let mut peer = DwnxConnection::server(&peer_config).expect("ピアを作成できる");
        let mut buf = [0u8; 4096];
        let mut out = Vec::new();
        let mut ready_tx = Some(ready_tx);
        loop {
            match io.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(len) => {
                    if peer.read(&buf[..len], 0).is_err() {
                        return;
                    }
                    loop {
                        out.clear();
                        match peer.poll_transmit(&mut out, 0) {
                            Ok(true) => {
                                if io.write_all(&out).await.is_err() {
                                    return;
                                }
                            }
                            Ok(false) => break,
                            Err(_) => return,
                        }
                    }
                    if let Some(tx) = ready_tx.take() {
                        let _ = tx.send(());
                        // ここから先は読み取らない。クライアントの書き込みを詰まらせる
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
            }
        }
    });
    ready_rx.await.expect("ピアがハンドシェイクを終える");

    // ピアの受信ウィンドウより小さく、下位トランスポートのバッファより大きい量を書き、
    // 書き込みを詰まらせる
    let (mut send, _recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    let writer = tokio::spawn(async move { send.write(&vec![0x5au8; 32 * 1024]).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !writer.is_finished(),
        "ピアが読み取らないため書き込みは完了しないこと"
    );

    // 書き込みが詰まっていてもアイドルタイムアウトで終了すること
    tokio::time::timeout(GUARD, client.closed())
        .await
        .expect("書き込みが詰まっていてもアイドルタイムアウトで closed() が解決すること");

    writer.abort();
    peer.abort();
}

#[tokio::test]
async fn transport_end_is_observed_while_write_is_blocked() {
    // 書き込みが詰まっている間も、下位トランスポートの終了を検知して
    // ドライバが終了すること (I-14)
    let (client_io, server_io) = tokio::io::duplex(4096);
    let client = Connection::with_linger(Role::Client, client_io, test_config(), TEST_LINGER)
        .expect("クライアントを作成できる");
    let server = Connection::with_linger(Role::Server, server_io, test_config(), TEST_LINGER)
        .expect("サーバーを作成できる");

    let (mut send, _recv) = tokio::time::timeout(GUARD, client.open_bidirectional())
        .await
        .expect("guard の時間内に開ける")
        .expect("ストリームを開ける");
    // ピアにストリームを見せるため 1 バイトだけ送る
    send.write(b"x").await.expect("データを書き込める");
    // サーバーは受け取るが、アプリケーションは読まない
    let (_, _server_recv) = tokio::time::timeout(GUARD, server.accept_bidirectional())
        .await
        .expect("guard の時間内に accept できる")
        .expect("ストリームが届く");

    // 受信側が読まないため、フロー制御で書き込みが詰まる。受信側のチャネルが
    // 埋まるまでは受け付けられるため、それを十分に超える量を書く
    let writer = tokio::spawn(async move { send.write(&vec![0x5au8; 4 * 1024 * 1024]).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !writer.is_finished(),
        "受信側が読まないため書き込みは完了しないこと"
    );

    // ピアが下位トランスポートを閉じると、書き込みが詰まっていても終了すること
    drop(server);
    tokio::time::timeout(GUARD, client.closed())
        .await
        .expect("書き込みが詰まっていても下位トランスポートの終了で closed() が解決すること");

    writer.abort();
}

#[tokio::test]
async fn accept_progresses_when_accept_channel_is_full() {
    // accept チャネル (容量 16) が埋まっても、アプリケーションが accept するだけで
    // 保留中のストリームが渡されること (D-4)。ピアはストリームを開いた後は
    // 何も送らないため、アプリケーションからの通知だけがドライバの起床要因になる。
    const STREAMS: usize = 20;

    // ピアが開けるストリーム数を accept チャネルの容量より大きくする
    let client_config = TransportParameters {
        initial_max_streams_bidi: 32,
        ..test_config()
    };
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, client_config, TEST_LINGER)
        .expect("クライアントを作成できる");
    let server = Connection::with_linger(Role::Server, server_io, test_config(), TEST_LINGER)
        .expect("サーバーを作成できる");

    // サーバーが accept チャネルの容量を超える数の双方向ストリームを開く
    let mut server_streams = Vec::new();
    for _ in 0..STREAMS {
        let (mut send, recv) = tokio::time::timeout(GUARD, server.open_bidirectional())
            .await
            .expect("guard の時間内に開ける")
            .expect("サーバーがストリームを開ける");
        send.write(b"x").await.expect("サーバーが書き込める");
        server_streams.push((send, recv));
    }

    // ドライバがストリームを登録し終えて待機状態に入るまで待つ
    tokio::time::sleep(Duration::from_millis(200)).await;

    // アプリケーションは accept だけを行う。ハンドルはドロップせずに保持する
    let mut client_streams = Vec::new();
    for _ in 0..STREAMS {
        let accepted = tokio::time::timeout(GUARD, client.accept_bidirectional())
            .await
            .expect("guard の時間内に accept できる")
            .expect("保留中のストリームが渡されること");
        client_streams.push(accepted);
    }
    assert_eq!(
        client_streams.len(),
        STREAMS,
        "チャネルの容量を超える数のストリームを受け取れること"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn stop_sending_progresses_when_channel_is_full() {
    // STOP_SENDING の通知チャネル (容量 16) が埋まっても、アプリケーションが
    // 通知を受け取るだけで保留中の通知が渡されること (D-4 と同じ起床の必要性)
    const STREAMS: usize = 20;

    let client_config = TransportParameters {
        initial_max_streams_bidi: 32,
        ..test_config()
    };
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, client_config, TEST_LINGER)
        .expect("クライアントを作成できる");
    let server = Connection::with_linger(Role::Server, server_io, test_config(), TEST_LINGER)
        .expect("サーバーを作成できる");

    // サーバーがストリームを開いて 1 バイトずつ送る
    let mut server_streams = Vec::new();
    for _ in 0..STREAMS {
        let (mut send, recv) = tokio::time::timeout(GUARD, server.open_bidirectional())
            .await
            .expect("guard の時間内に開ける")
            .expect("サーバーがストリームを開ける");
        send.write(b"x").await.expect("サーバーが書き込める");
        server_streams.push((send, recv));
    }

    // クライアントがすべて受け取り、ハンドルを保持する
    let mut client_streams = Vec::new();
    for _ in 0..STREAMS {
        let accepted = tokio::time::timeout(GUARD, client.accept_bidirectional())
            .await
            .expect("guard の時間内に accept できる")
            .expect("ストリームが届く");
        client_streams.push(accepted);
    }

    // サーバーがすべてのストリームで STOP_SENDING を送る
    for (_, recv) in server_streams.iter_mut() {
        recv.stop_sending(9).await.expect("STOP_SENDING を送れる");
    }

    // ドライバが通知を登録し終えて待機状態に入るまで待つ
    tokio::time::sleep(Duration::from_millis(200)).await;

    // アプリケーションは通知を受け取るだけを行う
    let mut observed = Vec::new();
    for _ in 0..STREAMS {
        let notification = tokio::time::timeout(GUARD, client.recv_stop_sending())
            .await
            .expect("guard の時間内に通知が届く")
            .expect("保留中の通知が渡されること");
        assert_eq!(notification.error_code, 9, "エラーコードが一致すること");
        observed.push(notification);
    }
    assert_eq!(
        observed.len(),
        STREAMS,
        "チャネルの容量を超える数の通知を受け取れること"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

/// ピアを手動で駆動し、ハンドシェイクの後に CONNECTION_CLOSE を送る
///
/// 戻り値は自分 (ピア) が EOF を観測した時刻である。クライアントが送信側を
/// shutdown したことを、ドライバの終了との順序で判別するために使う。
async fn drive_peer_that_closes(
    mut io: tokio::io::DuplexStream,
    config: TransportParameters,
) -> std::time::Instant {
    let mut peer = DwnxConnection::server(&config).expect("ピアを作成できる");
    let mut buf = [0u8; 4096];
    let mut out = Vec::new();
    let mut established = false;
    while !established {
        let len = io.read(&mut buf).await.expect("ピアが読み取れる");
        assert!(len > 0, "ハンドシェイク中に EOF を観測しないこと");
        peer.read(&buf[..len], 0)
            .expect("ピアが Record を処理できる");
        loop {
            out.clear();
            match peer.poll_transmit(&mut out, 0) {
                Ok(true) => io.write_all(&out).await.expect("ピアが書き込める"),
                Ok(false) => break,
                Err(error) => panic!("ピアが Record を生成できない: {error}"),
            }
        }
        while let Some(event) = peer.poll_event() {
            if matches!(event, DwnxEvent::TransportParametersReceived(_)) {
                established = true;
            }
        }
    }

    // ピアから切断する (CONNECTION_CLOSE を送る)
    peer.close(0x99, b"peer closes")
        .expect("ピアが接続を閉じられる");
    loop {
        out.clear();
        match peer.poll_transmit(&mut out, 0) {
            Ok(true) => io.write_all(&out).await.expect("ピアが書き込める"),
            Ok(false) => break,
            Err(error) => panic!("ピアが Record を生成できない: {error}"),
        }
    }

    // クライアントが送信側を shutdown するまで読み捨てる
    loop {
        let len = io.read(&mut buf).await.expect("ピアが読み取れる");
        if len == 0 {
            return std::time::Instant::now();
        }
    }
}

#[tokio::test]
async fn idle_timeout_shuts_down_write_side_before_driver_ends() {
    // Section 7.1 / 7.2: アイドルタイムアウトでも、ドライバは終了前に下位
    // トランスポートの送信側を graceful に shutdown する。ピアが EOF を観測する
    // 時刻がドライバの終了より前であることで判別する (終了時の drop でも EOF は
    // 観測できるため、順序で判別する)。
    let client_config = TransportParameters {
        max_idle_timeout: 300 * 1_000_000,
        ..test_config()
    };
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, client_config, SHUTDOWN_LINGER)
        .expect("クライアントを作成できる");

    // 生ピアは読み取って捨てるだけにし、shutdown しない
    let peer = tokio::spawn(async move {
        let mut io = peer_io;
        let mut buf = [0u8; 4096];
        loop {
            match io.read(&mut buf).await {
                Ok(0) => return std::time::Instant::now(),
                Ok(_) => {}
                Err(error) => panic!("ピアの読み取りが失敗した: {error}"),
            }
        }
    });

    let started = std::time::Instant::now();
    tokio::time::timeout(GUARD, client.closed())
        .await
        .expect("アイドルタイムアウトでドライバが終了すること");
    let driver_elapsed = started.elapsed();

    let eof_at = tokio::time::timeout(GUARD, peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
    let eof_elapsed = eof_at.duration_since(started);
    assert!(
        eof_elapsed <= driver_elapsed,
        "送信側の shutdown がドライバの終了より前にピアへ届くこと: \
         EOF {eof_elapsed:?} / ドライバ終了 {driver_elapsed:?}"
    );
    // linger の期限まで待たずに shutdown が届くこと。shutdown を送らない実装では、
    // ピアが EOF を観測するのはドライバが linger の期限で終了する時点になる。
    assert!(
        eof_elapsed < Duration::from_secs(1),
        "shutdown が linger ({SHUTDOWN_LINGER:?}) の期限を待たずに届くこと: EOF {eof_elapsed:?}"
    );
}

#[tokio::test]
async fn peer_close_shuts_down_write_side_before_driver_ends() {
    // Section 7.3: ピア起因の切断でも、ドライバは終了前に送信側を graceful に
    // shutdown する。判別方法はアイドルタイムアウトの場合と同じである。
    let peer_config = TransportParameters {
        max_idle_timeout: 0,
        ..test_config()
    };
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, test_config(), SHUTDOWN_LINGER)
        .expect("クライアントを作成できる");
    let peer = tokio::spawn(drive_peer_that_closes(peer_io, peer_config));

    let started = std::time::Instant::now();
    tokio::time::timeout(GUARD, client.closed())
        .await
        .expect("ピアの切断でドライバが終了すること");
    let driver_elapsed = started.elapsed();

    let eof_at = tokio::time::timeout(GUARD, peer)
        .await
        .expect("ピアが EOF を観測して終了すること")
        .expect("ピアのタスクが終了すること");
    let eof_elapsed = eof_at.duration_since(started);
    assert!(
        eof_elapsed <= driver_elapsed,
        "送信側の shutdown がドライバの終了より前にピアへ届くこと: \
         EOF {eof_elapsed:?} / ドライバ終了 {driver_elapsed:?}"
    );
    // linger の期限まで待たずに shutdown が届くこと (アイドルタイムアウトの場合と同じ)
    assert!(
        eof_elapsed < Duration::from_secs(1),
        "shutdown が linger ({SHUTDOWN_LINGER:?}) の期限を待たずに届くこと: EOF {eof_elapsed:?}"
    );
}

/// DATAGRAM を有効にしたテスト用の設定
///
/// `max_datagram_frame_size` は受信を受け付ける DATAGRAM フレームの最大サイズで
/// あり、0 は DATAGRAM 拡張の非サポートを表す (RFC 9221 Section 3)。ローカルの
/// Record 容量より大きい値にして、Record 容量の検査を検証できるようにする。
fn datagram_config() -> TransportParameters {
    TransportParameters {
        max_datagram_frame_size: 65535,
        ..test_config()
    }
}

/// DATAGRAM を有効にした接続の組を作る
fn connect_datagram() -> (Connection, Connection) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, datagram_config(), TEST_LINGER)
        .expect("クライアントを作成できる");
    let server = Connection::with_linger(Role::Server, server_io, datagram_config(), TEST_LINGER)
        .expect("サーバーを作成できる");
    (client, server)
}

#[tokio::test]
async fn datagram_roundtrip() {
    let (client, server) = connect_datagram();

    // クライアントからサーバーへ DATAGRAM が届くこと
    tokio::time::timeout(GUARD, client.send_datagram(b"hello datagram"))
        .await
        .expect("guard の時間内に送信が完了する")
        .expect("DATAGRAM を送れる");
    let received = tokio::time::timeout(GUARD, server.recv_datagram())
        .await
        .expect("guard の時間内に DATAGRAM が届く")
        .expect("受信に失敗しない")
        .expect("DATAGRAM が届く");
    assert_eq!(
        received, b"hello datagram",
        "クライアントが送信した DATAGRAM がサーバーへ届くこと"
    );

    // サーバーからクライアントへも DATAGRAM が届くこと
    tokio::time::timeout(GUARD, server.send_datagram(b"pong datagram"))
        .await
        .expect("guard の時間内に送信が完了する")
        .expect("DATAGRAM を送れる");
    let received = tokio::time::timeout(GUARD, client.recv_datagram())
        .await
        .expect("guard の時間内に DATAGRAM が届く")
        .expect("受信に失敗しない")
        .expect("DATAGRAM が届く");
    assert_eq!(
        received, b"pong datagram",
        "サーバーが送信した DATAGRAM がクライアントへ届くこと"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn multiple_datagrams_and_empty_datagram_are_received_in_order() {
    // 空の DATAGRAM は 0 バイトの DATAGRAM フレームとして送れる
    // (RFC 9221 Section 4)
    let (client, server) = connect_datagram();

    let payloads: [&[u8]; 3] = [b"first", b"", b"third"];
    for payload in payloads {
        tokio::time::timeout(GUARD, client.send_datagram(payload))
            .await
            .expect("guard の時間内に送信が完了する")
            .expect("DATAGRAM を送れる");
    }

    // ドライバが受信イベントをチャネルへ移し終えるまで待つ。DATAGRAM は
    // フロー制御されないため、受信側のドライバとアプリケーションの進み方は
    // 非同期である。
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 到着済みの DATAGRAM をまとめて取り出せること
    let batch = tokio::time::timeout(GUARD, server.recv_datagrams())
        .await
        .expect("guard の時間内に DATAGRAM が届く")
        .expect("受信に失敗しない")
        .expect("DATAGRAM が届く");
    assert_eq!(
        batch,
        payloads
            .iter()
            .map(|payload| payload.to_vec())
            .collect::<Vec<_>>(),
        "複数の DATAGRAM が積んだ順に届き、空の DATAGRAM も届くこと"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn send_datagram_fails_when_peer_does_not_support_datagram() {
    // ピアが DATAGRAM 拡張を広告していない場合はエラーになり、接続は閉じない
    // (RFC 9221 Section 3)
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, datagram_config(), TEST_LINGER)
        .expect("クライアントを作成できる");
    // サーバーは既定値 (max_datagram_frame_size = 0) のままにする
    let server = Connection::with_linger(Role::Server, server_io, test_config(), TEST_LINGER)
        .expect("サーバーを作成できる");

    let result = tokio::time::timeout(GUARD, client.send_datagram(b"unsupported"))
        .await
        .expect("guard の時間内に応答が返る");
    assert!(
        matches!(&result, Err(tokio_dwnx::Error::Dwnx(_))),
        "DATAGRAM 非対応のピアへの送信がエラーになること: {result:?}"
    );
    assert!(
        !client.is_closed(),
        "DATAGRAM 非対応は接続の終了ではないこと"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn send_datagram_fails_when_datagram_is_too_large() {
    // フレーム全体 (タイプと Length を含む) がピアの max_datagram_frame_size を
    // 超える場合はエラーになり、接続は閉じない
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::with_linger(Role::Client, client_io, datagram_config(), TEST_LINGER)
        .expect("クライアントを作成できる");
    let server_config = TransportParameters {
        max_datagram_frame_size: 100,
        ..test_config()
    };
    let server = Connection::with_linger(Role::Server, server_io, server_config, TEST_LINGER)
        .expect("サーバーを作成できる");

    let payload = vec![0x5au8; 200];
    let result = tokio::time::timeout(GUARD, client.send_datagram(&payload))
        .await
        .expect("guard の時間内に応答が返る");
    assert!(
        matches!(&result, Err(tokio_dwnx::Error::Dwnx(_))),
        "ピアの上限を超える DATAGRAM の送信がエラーになること: {result:?}"
    );
    assert!(!client.is_closed(), "サイズ超過は接続の終了ではないこと");

    // 上限内の DATAGRAM は引き続き送れること
    tokio::time::timeout(GUARD, client.send_datagram(b"ok"))
        .await
        .expect("guard の時間内に送信が完了する")
        .expect("上限内の DATAGRAM を送れる");

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn dropped_datagrams_are_counted() {
    // 受信チャネルの容量を超える数の DATAGRAM を、アプリケーションが受信しない
    // 間に送ると、あふれた分が破棄され、個数が観測できること
    const TOTAL: usize = 100;
    // ドライバの受信チャネルの容量 (DATAGRAM_CAPACITY) と同じ値
    const CAPACITY: u64 = 64;

    let (client, server) = connect_datagram();
    for _ in 0..TOTAL {
        tokio::time::timeout(GUARD, client.send_datagram(b"x"))
            .await
            .expect("guard の時間内に送信が完了する")
            .expect("DATAGRAM を送れる");
    }

    // ドライバが受信イベントをチャネルへ移すまで待ち、破棄数を観測する。
    // アプリケーションが受信しないため、チャネルに入るのは容量の 64 個だけで、
    // 残りはすべて破棄される。
    let expected = TOTAL as u64 - CAPACITY;
    let mut dropped = 0;
    for _ in 0..50 {
        dropped = server.dropped_datagrams();
        if dropped == expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        dropped, expected,
        "受信しない間にあふれた DATAGRAM が破棄され、個数が観測できること"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}

#[tokio::test]
async fn send_datagram_fails_when_datagram_exceeds_record_capacity() {
    // ピアが十分大きい max_datagram_frame_size (65535) を広告していても、
    // ローカルの Record 容量 (DWNX_DEFAULT_MAX_RECORD_SIZE = 16382 バイト。
    // フレームに使えるのは Record 長の 2 バイトを除いた 16380 バイト) を
    // 超える DATAGRAM は送信できない。事前検査が無いと dwnx が NOBUF を
    // 握りつぶし、send_datagram が永久に完了しない。
    let (client, server) = connect_datagram();

    let payload = vec![0x5au8; 16378];
    let result = tokio::time::timeout(GUARD, client.send_datagram(&payload))
        .await
        .expect("guard の時間内に応答が返ること (永久に待たない)");
    assert!(
        matches!(&result, Err(tokio_dwnx::Error::Dwnx(_))),
        "Record 容量を超える DATAGRAM の送信がエラーになること: {result:?}"
    );
    assert!(
        !client.is_closed(),
        "Record 容量超過は接続の終了ではないこと"
    );

    // 上限内の DATAGRAM は引き続き送れること
    tokio::time::timeout(GUARD, client.send_datagram(b"fits"))
        .await
        .expect("guard の時間内に送信が完了する")
        .expect("上限内の DATAGRAM を送れる");
    let received = tokio::time::timeout(GUARD, server.recv_datagram())
        .await
        .expect("guard の時間内に DATAGRAM が届く")
        .expect("受信に失敗しない")
        .expect("DATAGRAM が届く");
    assert_eq!(
        received, b"fits",
        "失敗の後でも上限内の DATAGRAM が届くこと"
    );

    close_and_wait(&client, 0, "done").await;
    close_and_wait(&server, 0, "done").await;
}
