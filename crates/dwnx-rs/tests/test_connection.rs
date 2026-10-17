//! dwnx 接続 (Sans I/O) のテスト
//!
//! 2 つの [`Connection`] の間で Record をそのまま受け渡し、実際の dwnx を
//! 使ってストリームとタイムアウトの振る舞いを検証する。下位トランスポートは
//! 使わないため、モックは不要である。

use dwnx_rs::{Connection, Event, MAX_CONFIG_STREAMS, TransportParameters, WriteEvent};
use dwnx_sys as sys;

/// テストで使う時刻 (ナノ秒)
const NOW: u64 = 1_000_000_000;

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

/// 接続の組を作る
fn connect() -> (Connection, Connection) {
    let client = Connection::client(&test_config()).expect("クライアントを作成できる");
    let server = Connection::server(&test_config()).expect("サーバーを作成できる");
    (client, server)
}

/// `from` の送信待ち Record をすべて `to` へ渡す
///
/// 送信できる Record が続く限り繰り返す。上限を設けて、送信が止まらない場合に
/// テストが終わらないことを防ぐ。
fn transfer(from: &mut Connection, to: &mut Connection, now: u64) -> bool {
    let mut moved = false;
    for _ in 0..1024 {
        let mut out = Vec::new();
        let more = from.poll_transmit(&mut out, now).expect("送信できる");
        if !more {
            break;
        }
        to.read(&out, now).expect("読み取れる");
        moved = true;
    }
    moved
}

/// 両方向の送信待ち Record がなくなるまで往復させる
fn pump(client: &mut Connection, server: &mut Connection, now: u64) {
    for _ in 0..64 {
        let mut moved = false;
        moved |= transfer(client, server, now);
        moved |= transfer(server, client, now);
        if !moved {
            return;
        }
    }
    panic!("Record の往復が収束しない");
}

/// 接続が通知したイベントをすべて取り出す
fn drain_events(connection: &mut Connection) -> Vec<Event> {
    let mut events = Vec::new();
    while let Some(event) = connection.poll_event() {
        events.push(event);
    }
    events
}

/// ピアのトランスポートパラメータを受信したイベントを取り出して捨てる
fn take_transport_parameters(events: &mut Vec<Event>) -> bool {
    let before = events.len();
    events.retain(|event| !matches!(event, Event::TransportParametersReceived(_)));
    before != events.len()
}

/// ストリームのデータを連結し、FIN を観測したかどうかを返す
fn collect_stream_data(events: &[Event], id: i64) -> (Vec<u8>, bool) {
    let mut data = Vec::new();
    let mut fin = false;
    for event in events {
        if let Event::StreamData {
            id: event_id,
            data: chunk,
            fin: event_fin,
            ..
        } = event
            && *event_id == id
        {
            data.extend_from_slice(chunk);
            fin |= *event_fin;
        }
    }
    (data, fin)
}

/// 指定したストリームのデータイベントの数を数える
fn count_data_events(events: &[Event], id: i64) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, Event::StreamData { id: event_id, .. } if *event_id == id))
        .count()
}

#[test]
fn client_sends_stream_to_server() {
    let (mut client, mut server) = connect();
    pump(&mut client, &mut server, NOW);

    // ハンドシェイクで相互のトランスポートパラメータが届くこと
    let mut client_events = drain_events(&mut client);
    let mut server_events = drain_events(&mut server);
    assert!(
        take_transport_parameters(&mut client_events),
        "クライアントがピアのトランスポートパラメータを受け取ること"
    );
    assert!(
        take_transport_parameters(&mut server_events),
        "サーバーがピアのトランスポートパラメータを受け取ること"
    );

    // クライアントが双方向ストリームを開いてデータと FIN を送る
    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(client.write(id, b"hello", false), "データを積めること");
    assert!(client.write(id, b"", true), "FIN を積めること");
    pump(&mut client, &mut server, NOW);

    // 送信が完了したことが WriteEvent で通知されること
    let mut written = 0;
    while let Some(event) = client.poll_write_event() {
        match event {
            WriteEvent::Written { stream_id } => {
                assert_eq!(stream_id, id, "完了が通知されるストリームが一致すること");
                written += 1;
            }
            WriteEvent::Failed { error, .. } => panic!("送信に失敗した: {error}"),
        }
    }
    assert_eq!(written, 2, "積んだ 2 つの書き込みが完了すること");

    // サーバー側でストリームの open とデータと FIN を観測する
    let events = drain_events(&mut server);
    assert!(
        events.contains(&Event::StreamOpened { id }),
        "サーバーがストリームの open を観測すること"
    );
    let (data, fin) = collect_stream_data(&events, id);
    assert_eq!(data, b"hello", "サーバーが送信したデータを受け取ること");
    assert!(fin, "サーバーが FIN を観測すること");
}

#[test]
fn server_sends_stream_to_client() {
    let (mut client, mut server) = connect();
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    // サーバーが開いた双方向ストリームでも同じようにデータが届くこと
    let id = server.open_bidi().expect("ストリームを開ける");
    assert!(server.write(id, b"pong", true), "データと FIN を積めること");
    pump(&mut client, &mut server, NOW);

    let events = drain_events(&mut client);
    let (data, fin) = collect_stream_data(&events, id);
    assert_eq!(data, b"pong", "クライアントが送信したデータを受け取ること");
    assert!(fin, "クライアントが FIN を観測すること");
}

#[test]
fn large_payload_is_split_into_records() {
    // 1 つの Record (16382 バイト) に収まらない大きさのデータを送る
    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let (mut client, mut server) = connect();
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(client.write(id, &payload, true), "データを積めること");
    pump(&mut client, &mut server, NOW);

    let events = drain_events(&mut server);
    let (data, fin) = collect_stream_data(&events, id);
    assert_eq!(
        data.len(),
        payload.len(),
        "受け取るデータの長さが一致すること"
    );
    assert_eq!(data, payload, "受け取るデータが一致すること");
    assert!(fin, "分割された最後のデータで FIN を観測すること");
    assert!(
        count_data_events(&events, id) > 1,
        "データが複数の Record に分割されて届くこと"
    );
}

#[test]
fn write_to_unknown_stream_is_reported_as_failure() {
    let mut client = Connection::client(&test_config()).expect("クライアントを作成できる");

    // 開いていないストリームへの書き込みは、送信時に dwnx が拒否する
    assert!(client.write(0, b"data", false), "書き込みを積めること");
    let mut out = Vec::new();
    let result = client.poll_transmit(&mut out, NOW);
    assert!(result.is_err(), "STREAM_NOT_FOUND はエラーになること");

    match client.poll_write_event() {
        Some(WriteEvent::Failed { stream_id, error }) => {
            assert_eq!(stream_id, 0, "失敗したストリームが一致すること");
            assert_eq!(
                error.code,
                sys::DWNX_ERR_STREAM_NOT_FOUND,
                "dwnx の STREAM_NOT_FOUND が返ること"
            );
        }
        other => panic!("失敗が通知されない: {other:?}"),
    }
}

#[test]
fn idle_timeout_closes_connection() {
    // アイドルタイムアウトを 10 ミリ秒にして、タイマーを進める
    let params = TransportParameters {
        max_idle_timeout: 10 * 1_000_000,
        ..test_config()
    };
    let mut client = Connection::client(&params).expect("クライアントを作成できる");
    let mut server = Connection::server(&params).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);

    let expiry = client.next_timeout().expect("タイマーが設定されている");
    assert!(
        client.handle_timeout(expiry + 1).is_ok(),
        "タイムアウトを処理できること"
    );
    assert!(client.is_closed(), "アイドルタイムアウトで接続が閉じること");
    assert!(
        drain_events(&mut client).contains(&Event::ConnectionClosed),
        "ConnectionClosed が通知されること"
    );
}

#[test]
fn close_writes_connection_close_record() {
    let (mut client, mut server) = connect();
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    client
        .close(0x100, "goodbye".as_bytes())
        .expect("接続を閉じられる");
    let mut out = Vec::new();
    assert!(
        client.poll_transmit(&mut out, NOW).expect("送信できる"),
        "CONNECTION_CLOSE が書き出されること"
    );
    assert!(client.is_closed(), "CONNECTION_CLOSE の送信で閉じること");

    // ピアは CONNECTION_CLOSE を認識し、接続が閉じる
    server
        .read(&out, NOW)
        .expect("ピアが CONNECTION_CLOSE を処理できる");
    assert!(
        server.is_closed(),
        "ピアが CONNECTION_CLOSE で閉じたことになる"
    );
    assert!(
        drain_events(&mut server).contains(&Event::ConnectionClosed),
        "ピアに ConnectionClosed が通知されること"
    );
}

#[test]
fn close_truncates_reason_that_does_not_fit() {
    let mut client = Connection::client(&test_config()).expect("クライアントを作成できる");

    // Record の上限 (16382 バイト) を大きく超える理由句を渡す
    let reason = "あ".repeat(20_000);
    client
        .close(1, reason.as_bytes())
        .expect("理由句を切り詰めて閉じられる");

    let mut out = Vec::new();
    assert!(
        client.poll_transmit(&mut out, NOW).expect("送信できる"),
        "切り詰めた CONNECTION_CLOSE が書き出されること"
    );
    assert!(
        out.len() <= sys::DWNX_DEFAULT_MAX_RECORD_SIZE as usize,
        "Record が上限に収まること"
    );
    assert!(client.is_closed(), "閉じたことになる");
}

#[test]
fn invalid_config_is_rejected() {
    // dwnx の conn_new の assert と同じ条件を Rust 側で検査し、プロセスを
    // abort させずにエラーを返すこと
    let cases: [(&str, TransportParameters); 6] = [
        (
            "max_record_size が既定値未満",
            TransportParameters {
                max_record_size: 0,
                ..test_config()
            },
        ),
        (
            "max_record_size が DWNX_MAX_VARINT 超",
            TransportParameters {
                max_record_size: u64::MAX,
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
        (
            "initial_max_streams_bidi が DWNX_MAX_VARINT 超",
            TransportParameters {
                initial_max_streams_bidi: u64::MAX,
                ..test_config()
            },
        ),
        (
            "initial_max_stream_data_uni が DWNX_MAX_VARINT 超",
            TransportParameters {
                initial_max_stream_data_uni: u64::MAX,
                ..test_config()
            },
        ),
    ];

    for (name, params) in cases {
        assert!(params.validate().is_err(), "{name} が検証で弾かれること");
        assert!(
            Connection::client(&params).is_err(),
            "{name} のクライアント作成がエラーになること"
        );
        assert!(
            Connection::server(&params).is_err(),
            "{name} のサーバー作成がエラーになること"
        );
    }

    assert!(
        test_config().validate().is_ok(),
        "既定の設定が検証を通ること"
    );
}

#[test]
fn stop_sending_is_notified() {
    let (mut client, mut server) = connect();
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    // サーバーがストリームを認識するまでデータを送る
    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(client.write(id, b"data", false), "データを積めること");
    pump(&mut client, &mut server, NOW);
    assert!(
        drain_events(&mut server).contains(&Event::StreamOpened { id }),
        "サーバーがストリームを認識すること"
    );

    // サーバーが受信側を閉じると STOP_SENDING が届く
    server.shutdown_read(id, 7).expect("STOP_SENDING を送れる");
    pump(&mut client, &mut server, NOW);

    let events = drain_events(&mut client);
    assert!(
        events.contains(&Event::RecvStopSending { id, error_code: 7 }),
        "STOP_SENDING が通知されること"
    );
}

#[test]
fn stream_reset_is_notified() {
    let (mut client, mut server) = connect();
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    // サーバーがストリームを認識するまでデータを送る
    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(client.write(id, b"data", false), "データを積めること");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    // クライアントが送信側をリセットすると、サーバーに RESET_STREAM が届く
    client.shutdown_write(id, 3).expect("RESET_STREAM を送れる");
    pump(&mut client, &mut server, NOW);

    let events = drain_events(&mut server);
    assert!(
        events.contains(&Event::StreamReset {
            id,
            final_size: 4,
            error_code: 3,
        }),
        "RESET_STREAM が通知されること"
    );
}

#[test]
fn config_stream_limit_is_enforced() {
    // ストリーム数の実用上限 (shiguredo_qmux の MAX_CONFIG_STREAMS と同じ値) を
    // 超える設定を拒否し、上限ちょうどは受け付けること (資源枯渇対策)
    let at_limit = TransportParameters {
        initial_max_streams_bidi: MAX_CONFIG_STREAMS,
        initial_max_streams_uni: MAX_CONFIG_STREAMS,
        ..test_config()
    };
    assert!(
        at_limit.validate().is_ok(),
        "上限ちょうどの設定が検証を通ること"
    );
    assert!(
        Connection::client(&at_limit).is_ok(),
        "上限ちょうどの設定で接続を作成できること"
    );

    let cases: [(&str, TransportParameters); 2] = [
        (
            "initial_max_streams_bidi",
            TransportParameters {
                initial_max_streams_bidi: MAX_CONFIG_STREAMS + 1,
                ..test_config()
            },
        ),
        (
            "initial_max_streams_uni",
            TransportParameters {
                initial_max_streams_uni: MAX_CONFIG_STREAMS + 1,
                ..test_config()
            },
        ),
    ];
    for (name, params) in cases {
        assert!(
            params.validate().is_err(),
            "{name} が上限を超えたら弾かれること"
        );
        assert!(
            Connection::client(&params).is_err(),
            "{name} が上限を超えたら接続を作成できないこと"
        );
        assert!(
            Connection::server(&params).is_err(),
            "{name} が上限を超えたらサーバーを作成できないこと"
        );
    }
}
