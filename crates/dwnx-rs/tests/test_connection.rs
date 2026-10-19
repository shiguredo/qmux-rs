//! dwnx 接続 (Sans I/O) のテスト
//!
//! 2 つの [`Connection`] の間で Record をそのまま受け渡し、実際の dwnx を
//! 使ってストリームとタイムアウトの振る舞いを検証する。下位トランスポートは
//! 使わないため、モックは不要である。

use dwnx_rs::{
    Connection, DatagramEvent, Event, MAX_CONFIG_STREAMS, MAX_PENDING_DATAGRAM_BYTES,
    MAX_PENDING_DATAGRAMS, TransportParameters, WriteEvent,
};
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

/// DATAGRAM を有効にしたテスト用の設定
///
/// `max_datagram_frame_size` は受信を受け付ける DATAGRAM フレームの最大サイズで
/// あり、0 は DATAGRAM 拡張の非サポートを表す (RFC 9221 Section 3)。DATAGRAM を
/// 有効にするには 0 より大きい値を指定する必要がある。
fn datagram_config(max_datagram_frame_size: u64) -> TransportParameters {
    TransportParameters {
        max_datagram_frame_size,
        ..test_config()
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

/// DATAGRAM イベントのデータを順番に取り出す
fn collect_datagrams(events: &[Event]) -> Vec<Vec<u8>> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Datagram { data } => Some(data.clone()),
            _ => None,
        })
        .collect()
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
    let cases: [(&str, TransportParameters); 8] = [
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
        (
            "max_datagram_frame_size が DWNX_MAX_VARINT 超",
            TransportParameters {
                max_datagram_frame_size: u64::MAX,
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

#[test]
fn datagram_transport_parameter_is_transferred() {
    // DATAGRAM 拡張は max_datagram_frame_size の交換でネゴシエートする
    // (RFC 9221 Section 3)。自分が広告した値がピアに届くことと、既定値 0 が
    // DATAGRAM の非サポートとして伝わることを検証する。
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&test_config()).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);

    // クライアントが広告した値がサーバーへ届くこと
    let mut client_params = None;
    for event in drain_events(&mut server) {
        if let Event::TransportParametersReceived(params) = event {
            client_params = Some(params);
        }
    }
    let client_params = client_params.expect("クライアントのトランスポートパラメータが届く");
    assert_eq!(
        client_params.max_datagram_frame_size, 4096,
        "広告した max_datagram_frame_size がピアへ届くこと"
    );

    // サーバーは既定値 (0) を広告しており、DATAGRAM の非サポートとして届くこと
    let mut server_params = None;
    for event in drain_events(&mut client) {
        if let Event::TransportParametersReceived(params) = event {
            server_params = Some(params);
        }
    }
    let server_params = server_params.expect("サーバーのトランスポートパラメータが届く");
    assert_eq!(
        server_params.max_datagram_frame_size, 0,
        "DATAGRAM を無効にした接続の max_datagram_frame_size が 0 として届くこと"
    );
}

#[test]
fn client_sends_datagram_to_server() {
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(4096)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    assert!(
        client.send_datagram(b"hello datagram"),
        "DATAGRAM を送信待ちに積めること"
    );
    pump(&mut client, &mut server, NOW);

    // 送信側では DATAGRAM が Record に収められたことが通知されること
    match client.poll_datagram_event() {
        Some(DatagramEvent::Accepted) => {}
        other => panic!("DATAGRAM の完了が通知されない: {other:?}"),
    }
    assert!(
        client.poll_datagram_event().is_none(),
        "余分な DATAGRAM の通知が無いこと"
    );

    // 受信側ではデータがそのまま届くこと
    let datagrams = collect_datagrams(&drain_events(&mut server));
    assert_eq!(
        datagrams,
        vec![b"hello datagram".to_vec()],
        "送信した DATAGRAM が届くこと"
    );
}

#[test]
fn server_sends_datagram_to_client() {
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(4096)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    // 双方向のどちらからでも DATAGRAM を送れること
    assert!(
        server.send_datagram(b"pong datagram"),
        "サーバーが DATAGRAM を送信待ちに積めること"
    );
    pump(&mut client, &mut server, NOW);

    match server.poll_datagram_event() {
        Some(DatagramEvent::Accepted) => {}
        other => panic!("DATAGRAM の完了が通知されない: {other:?}"),
    }
    let datagrams = collect_datagrams(&drain_events(&mut client));
    assert_eq!(
        datagrams,
        vec![b"pong datagram".to_vec()],
        "サーバーが送信した DATAGRAM がクライアントへ届くこと"
    );
}

#[test]
fn multiple_datagrams_and_empty_datagram_are_transferred() {
    // 空の DATAGRAM は 0 バイトの DATAGRAM フレームとして送れる
    // (RFC 9221 Section 4)
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(4096)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    let payloads: [&[u8]; 3] = [b"first", b"", b"third"];
    for payload in payloads {
        assert!(
            client.send_datagram(payload),
            "DATAGRAM を送信待ちに積めること"
        );
    }
    pump(&mut client, &mut server, NOW);

    // 積んだ数だけ完了が通知されること
    let mut accepted = 0;
    while let Some(event) = client.poll_datagram_event() {
        match event {
            DatagramEvent::Accepted => accepted += 1,
            DatagramEvent::Failed { error } => panic!("DATAGRAM の送信に失敗した: {error}"),
        }
    }
    assert_eq!(
        accepted, 3,
        "積んだ 3 つの DATAGRAM すべての完了が通知されること"
    );

    // 複数の DATAGRAM が積んだ順に届き、空の DATAGRAM も 1 つのイベントになること
    let datagrams = collect_datagrams(&drain_events(&mut server));
    assert_eq!(
        datagrams,
        vec![b"first".to_vec(), Vec::new(), b"third".to_vec()],
        "複数の DATAGRAM が積んだ順に届き、空の DATAGRAM も届くこと"
    );
}

#[test]
fn partially_delivered_datagram_is_assembled() {
    // dwnx は入力バイト列がフレームの途中で区切られると、1 つの DATAGRAM を
    // 複数のチャンクとして recv_datagram コールバックへ通知する。dwnx-rs が
    // チャンクを連結して 1 つの Event::Datagram にすることを、Record を
    // 1 バイトずつ読ませて検証する。
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(4096)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    let payload = b"partially delivered datagram";
    assert!(
        client.send_datagram(payload),
        "DATAGRAM を送信待ちに積めること"
    );
    let mut record = Vec::new();
    assert!(
        client.poll_transmit(&mut record, NOW).expect("送信できる"),
        "DATAGRAM を含む Record が書き出されること"
    );
    assert!(
        record.len() > 1,
        "1 バイトずつ読める大きさの Record であること"
    );

    // Record を 1 バイトずつ渡し、DATAGRAM が部分配送される状況を作る
    for byte in &record {
        server
            .read(core::slice::from_ref(byte), NOW)
            .expect("1 バイトずつ読み取れる");
    }

    let events = drain_events(&mut server);
    assert_eq!(
        collect_datagrams(&events),
        vec![payload.to_vec()],
        "部分配送された DATAGRAM が 1 つのイベントに組み立てられること"
    );
    assert!(
        matches!(client.poll_datagram_event(), Some(DatagramEvent::Accepted)),
        "部分配送されても送信側の完了は 1 回だけ通知されること"
    );
}

#[test]
fn datagram_to_peer_without_support_is_reported_as_failure() {
    // ピアが DATAGRAM 拡張を広告していない (max_datagram_frame_size が 0) 場合、
    // 送信は DWNX_ERR_INVALID_STATE で失敗するが接続は閉じない
    // (RFC 9221 Section 3、dwnx.h の dwnx_conn_writev_datagram を参照)。
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&test_config()).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    assert!(
        client.send_datagram(b"unsupported"),
        "DATAGRAM を送信待ちに積めること"
    );
    // DATAGRAM は失敗するが、制御フレームだけの Record が出ることはある
    let mut out = Vec::new();
    for _ in 0..16 {
        if !client.poll_transmit(&mut out, NOW).expect("送信できる") {
            break;
        }
    }

    match client.poll_datagram_event() {
        Some(DatagramEvent::Failed { error }) => {
            assert_eq!(
                error.code,
                sys::DWNX_ERR_INVALID_STATE,
                "ピア非対応は DWNX_ERR_INVALID_STATE として通知されること"
            );
        }
        other => panic!("DATAGRAM の失敗が通知されない: {other:?}"),
    }
    assert!(
        !client.is_closed(),
        "DATAGRAM 非対応は接続の終了ではないこと"
    );

    // 接続は引き続き使える (ストリームは DATAGRAM と独立している)
    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(
        client.write(id, b"still alive", false),
        "データを積めること"
    );
    pump(&mut client, &mut server, NOW);
    let (data, _fin) = collect_stream_data(&drain_events(&mut server), id);
    assert_eq!(
        data, b"still alive",
        "DATAGRAM 非対応のピアともストリームは使えること"
    );
}

#[test]
fn datagram_larger_than_peer_limit_is_reported_as_failure() {
    // フレーム全体 (タイプと Length を含む) がピアの max_datagram_frame_size を
    // 超える場合、送信は DWNX_ERR_INVALID_ARGUMENT で失敗するが接続は閉じない。
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(100)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    let payload = vec![0x5au8; 200];
    assert!(
        client.send_datagram(&payload),
        "DATAGRAM を送信待ちに積めること"
    );
    let mut out = Vec::new();
    for _ in 0..16 {
        if !client.poll_transmit(&mut out, NOW).expect("送信できる") {
            break;
        }
    }

    match client.poll_datagram_event() {
        Some(DatagramEvent::Failed { error }) => {
            assert_eq!(
                error.code,
                sys::DWNX_ERR_INVALID_ARGUMENT,
                "サイズ超過は DWNX_ERR_INVALID_ARGUMENT として通知されること"
            );
        }
        other => panic!("DATAGRAM の失敗が通知されない: {other:?}"),
    }
    assert!(!client.is_closed(), "サイズ超過は接続の終了ではないこと");
    assert!(
        collect_datagrams(&drain_events(&mut server)).is_empty(),
        "大きすぎる DATAGRAM はピアへ届かないこと"
    );
}

#[test]
fn datagram_at_record_capacity_limit_is_sent() {
    // dwnx-rs の送信バッファは DWNX_DEFAULT_MAX_RECORD_SIZE (16382) バイトで、
    // dwnx の Record 符号化器が先頭 2 バイトを Record 長に使うため、フレームに
    // 使えるのは 16380 バイトである。DATAGRAM フレームはタイプ 1 バイト +
    // Length 2 バイト + ペイロードなので、ペイロード 16377 バイトが上限になる。
    let mut client = Connection::client(&datagram_config(65535)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(65535)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    let payload = vec![0xa5u8; 16377];
    assert!(
        client.send_datagram(&payload),
        "上限ちょうどの DATAGRAM を送信待ちに積めること"
    );
    pump(&mut client, &mut server, NOW);

    match client.poll_datagram_event() {
        Some(DatagramEvent::Accepted) => {}
        other => panic!("上限ちょうどの DATAGRAM の完了が通知されない: {other:?}"),
    }
    let datagrams = collect_datagrams(&drain_events(&mut server));
    assert_eq!(
        datagrams.len(),
        1,
        "上限ちょうどの DATAGRAM が 1 つ届くこと"
    );
    assert_eq!(
        datagrams[0], payload,
        "上限ちょうどの DATAGRAM のデータが一致すること"
    );
}

#[test]
fn datagram_exceeding_record_capacity_is_reported_as_failure() {
    // 上限を 1 バイト超えるペイロード 16378 バイトは、ピアが十分大きい
    // max_datagram_frame_size を広告していても、ローカルの Record 容量に収まらない。
    // dwnx はこの場合の DWNX_ERR_NOBUF を握りつぶして何も返さないため、
    // dwnx-rs が事前に検査して失敗を通知する。通知しないと送信待ちが永久に残る。
    let mut client = Connection::client(&datagram_config(65535)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(65535)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    let payload = vec![0x5au8; 16378];
    assert!(
        client.send_datagram(&payload),
        "収まらない DATAGRAM も失敗として通知するため true を返すこと"
    );

    // 送信できる Record が無いため、poll_transmit はすぐ false を返すこと。
    // 以前はここで永久に false を返し続け、失敗も通知されなかった。
    let mut out = Vec::new();
    assert!(
        !client.poll_transmit(&mut out, NOW).expect("送信できる"),
        "Record に収まらない DATAGRAM は送信されないこと"
    );

    match client.poll_datagram_event() {
        Some(DatagramEvent::Failed { error }) => {
            assert_eq!(
                error.code,
                sys::DWNX_ERR_INVALID_ARGUMENT,
                "ローカルの Record 容量超過は DWNX_ERR_INVALID_ARGUMENT として通知されること"
            );
        }
        other => panic!("DATAGRAM の失敗が通知されない: {other:?}"),
    }
    assert!(
        !client.is_closed(),
        "ローカルの Record 容量超過は接続の終了ではないこと"
    );
    assert!(
        collect_datagrams(&drain_events(&mut server)).is_empty(),
        "収まらない DATAGRAM はピアへ届かないこと"
    );

    // 接続は引き続き使える。上限内の DATAGRAM は送れる
    assert!(
        client.send_datagram(b"fits"),
        "上限内の DATAGRAM を送信待ちに積めること"
    );
    pump(&mut client, &mut server, NOW);
    assert_eq!(
        collect_datagrams(&drain_events(&mut server)),
        vec![b"fits".to_vec()],
        "上限内の DATAGRAM は失敗の後でも送れること"
    );
}

#[test]
fn datagram_failure_keeps_submission_order() {
    // 送信待ちの DATAGRAM の完了通知は、積んだ順に取り出せること。
    // 失敗を即座に先頭へ積むと、tokio-dwnx の ack の対応付けがずれる。
    let mut client = Connection::client(&datagram_config(65535)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(65535)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    assert!(
        client.send_datagram(b"first"),
        "1 つ目の DATAGRAM を積めること"
    );
    assert!(
        client.send_datagram(&vec![0u8; 16378]),
        "収まらない 2 つ目の DATAGRAM を積めること"
    );
    pump(&mut client, &mut server, NOW);

    match client.poll_datagram_event() {
        Some(DatagramEvent::Accepted) => {}
        other => panic!("1 つ目の完了が通知されない: {other:?}"),
    }
    match client.poll_datagram_event() {
        Some(DatagramEvent::Failed { error }) => {
            assert_eq!(
                error.code,
                sys::DWNX_ERR_INVALID_ARGUMENT,
                "2 つ目はサイズ超過として通知されること"
            );
        }
        other => panic!("2 つ目の失敗が通知されない: {other:?}"),
    }
    assert!(
        client.poll_datagram_event().is_none(),
        "通知は積んだ数だけであること"
    );

    assert_eq!(
        collect_datagrams(&drain_events(&mut server)),
        vec![b"first".to_vec()],
        "成功した DATAGRAM だけがピアへ届くこと"
    );
}

#[test]
fn datagram_is_delivered_while_stream_is_blocked_by_flow_control() {
    // フロー制御でストリームがブロックされている間も、DATAGRAM はフロー制御
    // されないためピアへ届くこと (RFC 9221 Section 5.1)。dwnx-rs はブロック中の
    // ストリームがあると Record を確定できず、DATAGRAM が Accepted のまま
    // 送信されないことがあった。
    let server_params = TransportParameters {
        initial_max_stream_data_bidi_remote: 8,
        initial_max_data: 8,
        max_datagram_frame_size: 4096,
        ..test_config()
    };
    let client_params = datagram_config(4096);
    let mut client = Connection::client(&client_params).expect("クライアントを作成できる");
    let mut server = Connection::server(&server_params).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    // クライアントが 64 バイトを書くが、ピアのクレジットは 8 バイトしかない
    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(
        client.write(id, &[0x11u8; 64], false),
        "書き込みを積めること"
    );
    pump(&mut client, &mut server, NOW);

    // サーバーがクレジットを返さないため、8 バイトだけ届いて残りはブロックされる
    let events = drain_events(&mut server);
    let (mut stream_data, _fin) = collect_stream_data(&events, id);
    assert_eq!(stream_data.len(), 8, "クレジットの分だけデータが届くこと");

    // ブロックされたストリームを残したまま DATAGRAM を送る
    assert!(
        client.send_datagram(b"datagram while blocked"),
        "DATAGRAM を送信待ちに積めること"
    );
    pump(&mut client, &mut server, NOW);

    // クレジットを返さなくても DATAGRAM が届くこと
    let events = drain_events(&mut server);
    assert_eq!(
        collect_datagrams(&events),
        vec![b"datagram while blocked".to_vec()],
        "ストリームがブロックされていても DATAGRAM が届くこと"
    );
    let mut accepted = 0;
    while let Some(event) = client.poll_datagram_event() {
        match event {
            DatagramEvent::Accepted => accepted += 1,
            DatagramEvent::Failed { error } => panic!("DATAGRAM の送信に失敗した: {error}"),
        }
    }
    assert_eq!(accepted, 1, "DATAGRAM の完了が通知されること");

    // クレジットを返すと保留していたストリームデータが届くこと
    server
        .extend_max_stream_offset(id, 64)
        .expect("ストリームの受信ウィンドウを広げられる");
    server.extend_max_offset(64);
    pump(&mut client, &mut server, NOW);
    let events = drain_events(&mut server);
    let (remaining, _fin) = collect_stream_data(&events, id);
    stream_data.extend_from_slice(&remaining);
    assert_eq!(
        stream_data.len(),
        64,
        "クレジットを返すと保留していた残りが届くこと"
    );
    assert!(
        stream_data.iter().all(|byte| *byte == 0x11),
        "受け取るデータがすべて一致すること"
    );
}

#[test]
fn blocked_stream_does_not_starve_other_streams() {
    // 先頭のストリームがフロー制御でブロックされても、後続のストリームの
    // 送信が飢餓にならないこと。同じストリーム内の送信順序は保たれる。
    let server_params = TransportParameters {
        initial_max_stream_data_bidi_remote: 8,
        initial_max_data: 64 * 1024,
        max_datagram_frame_size: 4096,
        ..test_config()
    };
    let client_params = datagram_config(4096);
    let mut client = Connection::client(&client_params).expect("クライアントを作成できる");
    let mut server = Connection::server(&server_params).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);

    // 2 本のストリームに、それぞれストリームのクレジットを超えるデータを積む
    let first = client.open_bidi().expect("1 本目のストリームを開ける");
    let second = client.open_bidi().expect("2 本目のストリームを開ける");
    assert!(
        client.write(first, &[0x11u8; 64], false),
        "1 本目の書き込みを積めること"
    );
    assert!(
        client.write(second, &[0x22u8; 64], false),
        "2 本目の書き込みを積めること"
    );
    pump(&mut client, &mut server, NOW);

    // どちらのストリームもクレジットの 8 バイトずつが届くこと。
    // 1 本目がブロックされた状態で 2 本目が送られないと、2 本目は 0 バイトになる。
    let events = drain_events(&mut server);
    let (first_data, _) = collect_stream_data(&events, first);
    let (second_data, _) = collect_stream_data(&events, second);
    assert_eq!(
        first_data,
        vec![0x11u8; 8],
        "1 本目がクレジットの分だけ届くこと"
    );
    assert_eq!(
        second_data,
        vec![0x22u8; 8],
        "1 本目がブロックされても 2 本目が飢餓にならないこと"
    );
}

#[test]
fn stream_data_and_datagram_share_one_record() {
    // ストリームデータと DATAGRAM を同じ Record に収められること。
    // dwnx は 1 つの Record に複数のフレームを並べられる (1 回の
    // dwnx_conn_read で両方のイベントが出ることで確認する)。
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(4096)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut server);

    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(
        client.write(id, b"in same record", false),
        "ストリームデータを積めること"
    );
    assert!(
        client.send_datagram(b"same record datagram"),
        "DATAGRAM を送信待ちに積めること"
    );

    // 1 回の poll_transmit で両方を含む Record が書き出されること
    let mut record = Vec::new();
    assert!(
        client.poll_transmit(&mut record, NOW).expect("送信できる"),
        "Record が書き出されること"
    );

    // 1 回の read で両方のイベントが通知されること
    server.read(&record, NOW).expect("Record を読み取れる");
    let events = drain_events(&mut server);
    assert!(
        events.contains(&Event::StreamOpened { id }),
        "同じ Record からストリームの open が通知されること"
    );
    let (data, _fin) = collect_stream_data(&events, id);
    assert_eq!(
        data, b"in same record",
        "同じ Record のストリームデータが届くこと"
    );
    assert_eq!(
        collect_datagrams(&events),
        vec![b"same record datagram".to_vec()],
        "同じ Record の DATAGRAM が届くこと"
    );

    // 同じ Record に載る場合の順序を確認する。dwnx-rs はストリームの送信を
    // 先に試し、DWNX_ERR_WRITE_MORE を受けて同じ Record に DATAGRAM を続けて
    // 書くため、受信側でもストリームデータの後に DATAGRAM が通知される。
    let mut order = Vec::new();
    for event in &events {
        match event {
            Event::StreamData { id: event_id, .. } if *event_id == id => order.push("stream"),
            Event::Datagram { .. } => order.push("datagram"),
            _ => {}
        }
    }
    assert_eq!(
        order,
        ["stream", "datagram"],
        "同じ Record ではストリームデータの後に DATAGRAM が並ぶこと"
    );
}

#[test]
fn datagrams_across_multiple_records_notify_each_completion() {
    // 1 つの Record に収まらない大きさの DATAGRAM を複数積むと、Record を
    // 跨いで送信される。積んだ数だけ完了が通知され、データも欠けないこと。
    let mut client = Connection::client(&datagram_config(65535)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(65535)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    // 4000 バイトの DATAGRAM は 1 つの Record (16382 バイト) に 1 つしか
    // 収まらないため、10 個を積むと 10 個の Record に分かれる
    let payloads: Vec<Vec<u8>> = (0..10).map(|index| vec![index as u8; 4000]).collect();
    for payload in &payloads {
        assert!(
            client.send_datagram(payload),
            "DATAGRAM を送信待ちに積めること"
        );
    }

    // Record を 1 つずつ書き出して受信側へ渡す。送信が止まらない場合に
    // テストが終わらないよう上限を設ける
    let mut records = 0;
    loop {
        let mut out = Vec::new();
        if !client.poll_transmit(&mut out, NOW).expect("送信できる") {
            break;
        }
        records += 1;
        assert!(
            records <= payloads.len() + 1,
            "積んだ数に見合う回数で送信が終わること"
        );
        server.read(&out, NOW).expect("Record を読み取れる");
    }
    assert!(records > 1, "複数の Record に跨って送信されること");

    // 積んだ数だけ Accepted が通知されること
    let mut accepted = 0;
    while let Some(event) = client.poll_datagram_event() {
        match event {
            DatagramEvent::Accepted => accepted += 1,
            DatagramEvent::Failed { error } => panic!("DATAGRAM の送信に失敗した: {error}"),
        }
    }
    assert_eq!(
        accepted,
        payloads.len(),
        "複数の Record に跨っても積んだ数だけ完了が通知されること"
    );

    // 受信側でも欠けずに積んだ順で届くこと
    let datagrams = collect_datagrams(&drain_events(&mut server));
    assert_eq!(
        datagrams, payloads,
        "複数の Record に跨る DATAGRAM が積んだ順に届くこと"
    );
}

#[test]
fn send_datagram_after_close_is_rejected() {
    // CONNECTION_CLOSE を生成した後は、DATAGRAM を積まずに false を返すこと。
    // close 直後 (CONNECTION_CLOSE が未送信) と送信後の両方で確認する。
    let mut client = Connection::client(&datagram_config(4096)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(4096)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    client.close(0, b"bye").expect("接続を閉じられる");
    assert!(
        !client.send_datagram(b"before close record"),
        "close 直後の send_datagram が false を返すこと"
    );

    // CONNECTION_CLOSE を書き出して閉じた後も受け付けないこと
    let mut record = Vec::new();
    assert!(
        client.poll_transmit(&mut record, NOW).expect("送信できる"),
        "CONNECTION_CLOSE の Record が書き出されること"
    );
    assert!(client.is_closed(), "CONNECTION_CLOSE の送信で閉じること");
    assert!(
        !client.send_datagram(b"after close record"),
        "close 後の send_datagram が false を返すこと"
    );
    assert!(
        client.poll_datagram_event().is_none(),
        "積んでいない DATAGRAM の失敗イベントは通知されないこと"
    );

    // ピアは CONNECTION_CLOSE を読めること
    server
        .read(&record, NOW)
        .expect("CONNECTION_CLOSE を読み取れる");
}

#[test]
fn pending_datagram_count_limit_rejects_new_submissions() {
    // 送信待ちの DATAGRAM が件数の上限に達したら、それ以上は積まずに false を
    // 返すこと。上限まで積んだ DATAGRAM は送信でき、空きができれば再び積めること。
    let mut client = Connection::client(&datagram_config(65535)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(65535)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    let payloads: Vec<Vec<u8>> = (0..MAX_PENDING_DATAGRAMS)
        .map(|index| vec![index as u8])
        .collect();
    for payload in &payloads {
        assert!(
            client.send_datagram(payload),
            "上限までは DATAGRAM を積めること"
        );
    }
    assert!(
        !client.send_datagram(b"overflow"),
        "件数の上限を超える DATAGRAM は積まずに false を返すこと"
    );

    // 上限まで積んだ DATAGRAM はすべて送信できること
    pump(&mut client, &mut server, NOW);
    let mut accepted = 0;
    while let Some(event) = client.poll_datagram_event() {
        match event {
            DatagramEvent::Accepted => accepted += 1,
            DatagramEvent::Failed { error } => panic!("DATAGRAM の送信に失敗した: {error}"),
        }
    }
    assert_eq!(
        accepted, MAX_PENDING_DATAGRAMS,
        "上限まで積んだ DATAGRAM の完了がすべて通知されること"
    );
    assert_eq!(
        collect_datagrams(&drain_events(&mut server)),
        payloads,
        "上限まで積んだ DATAGRAM が積んだ順に届くこと"
    );

    // 送信待ちが無くなれば再び積めること
    assert!(
        client.send_datagram(b"after drain"),
        "送信待ちが空になったら再び積めること"
    );
    pump(&mut client, &mut server, NOW);
    assert_eq!(
        collect_datagrams(&drain_events(&mut server)),
        vec![b"after drain".to_vec()],
        "上限の後でも DATAGRAM を送信できること"
    );
}

#[test]
fn pending_datagram_byte_limit_rejects_new_submissions() {
    // 件数の上限に達する前に、ペイロードの合計バイト数の上限で積めなくなること。
    // 1 つの Record に収まる最大の DATAGRAM (16377 バイト) を使い、件数では
    // なくバイト数で上限に達する状況を作る。
    let mut client = Connection::client(&datagram_config(65535)).expect("クライアントを作成できる");
    let mut server = Connection::server(&datagram_config(65535)).expect("サーバーを作成できる");
    pump(&mut client, &mut server, NOW);
    drain_events(&mut client);
    drain_events(&mut server);

    let payload = vec![0x5au8; 16377];
    // バイト数の上限だけを考慮したときに積める個数を求める
    let mut expected = 0;
    let mut bytes = 0;
    while expected < MAX_PENDING_DATAGRAMS && bytes + payload.len() <= MAX_PENDING_DATAGRAM_BYTES {
        bytes += payload.len();
        expected += 1;
    }
    assert!(
        expected < MAX_PENDING_DATAGRAMS,
        "件数の上限より先にバイト数の上限へ達すること"
    );

    for _ in 0..expected {
        assert!(
            client.send_datagram(&payload),
            "バイト数の上限までは DATAGRAM を積めること"
        );
    }
    assert!(
        !client.send_datagram(&payload),
        "バイト数の上限を超える DATAGRAM は積まずに false を返すこと"
    );

    // 上限まで積んだ DATAGRAM はすべて送信できること
    pump(&mut client, &mut server, NOW);
    let mut accepted = 0;
    while let Some(event) = client.poll_datagram_event() {
        match event {
            DatagramEvent::Accepted => accepted += 1,
            DatagramEvent::Failed { error } => panic!("DATAGRAM の送信に失敗した: {error}"),
        }
    }
    assert_eq!(
        accepted, expected,
        "バイト数の上限まで積んだ DATAGRAM の完了がすべて通知されること"
    );
    let datagrams = collect_datagrams(&drain_events(&mut server));
    assert_eq!(
        datagrams.len(),
        expected,
        "バイト数の上限まで積んだ DATAGRAM がすべて届くこと"
    );
    assert!(
        datagrams.iter().all(|datagram| datagram == &payload),
        "届いた DATAGRAM のデータが一致すること"
    );

    // 送信待ちが無くなれば再び積めること
    assert!(
        client.send_datagram(b"after drain"),
        "送信待ちが空になったら再び積めること"
    );
    pump(&mut client, &mut server, NOW);
    assert_eq!(
        collect_datagrams(&drain_events(&mut server)),
        vec![b"after drain".to_vec()],
        "上限の後でも DATAGRAM を送信できること"
    );
}
