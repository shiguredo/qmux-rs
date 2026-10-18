//! QMux (shiguredo_qmux) と dwnx の Sans I/O 相互接続テスト
//!
//! 2 つの実装をメモリ上のバイト列で接続し、双方向にストリームデータが
//! 伝わることを検証する。TLS は使わず、QMux のワイヤ表現だけを突き合わせる。

use dwnx_rs::{
    Connection as DwnxConnection, DatagramEvent as DwnxDatagramEvent, Event as DwnxEvent,
    TransportParameters,
};
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::{Duration, Timestamp};
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection as SqConnection;
use shiguredo_qmux::event::Event as SqEvent;
use shiguredo_qmux::stream::ReadResult;

/// テストで使う時刻 (マイクロ秒)
const NOW_MICROS: u64 = 1_000_000;

/// dwnx.h の `DWNX_ERR_INVALID_STATE` と同じ値
///
/// interop クレートは dwnx-sys を直接依存しないため、dwnx.h の値をここに持つ。
/// エラーは `dwnx_strerror` のメッセージではなくコードで比較する。
const DWNX_ERR_INVALID_STATE: i32 = -204;
/// dwnx.h の `DWNX_ERR_INVALID_ARGUMENT` と同じ値
const DWNX_ERR_INVALID_ARGUMENT: i32 = -201;

/// dwnx のイベントから受信した DATAGRAM のデータを取り出す
fn collect_dwnx_datagrams(events: &mut Vec<DwnxEvent>) -> Vec<Vec<u8>> {
    let mut datagrams = Vec::new();
    events.retain(|event| match event {
        DwnxEvent::Datagram { data } => {
            datagrams.push(data.clone());
            false
        }
        _ => true,
    });
    datagrams
}

/// shiguredo_qmux 側の設定
fn sq_config() -> Config {
    Config {
        max_idle_timeout: Duration::from_secs(30),
        initial_max_data: 64 * 1024,
        initial_max_stream_data_bidi_local: 16 * 1024,
        initial_max_stream_data_bidi_remote: 16 * 1024,
        initial_max_stream_data_uni: 16 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        // DATAGRAM 拡張を有効にする (RFC 9221 Section 3)
        max_datagram_frame_size: Some(65535),
        ..Config::default()
    }
}

/// dwnx 側の設定 (max_idle_timeout はナノ秒)
fn dwnx_config() -> TransportParameters {
    TransportParameters {
        initial_max_stream_data_bidi_local: 16 * 1024,
        initial_max_stream_data_bidi_remote: 16 * 1024,
        initial_max_stream_data_uni: 16 * 1024,
        initial_max_data: 64 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        max_idle_timeout: 30_000_000_000,
        // DATAGRAM 拡張を有効にする (RFC 9221 Section 3)
        max_datagram_frame_size: 65535,
        ..TransportParameters::default()
    }
}

/// 双方の送信 Record がなくなるまでバイト列を往復させる
fn pump(sq: &mut SqConnection, dwnx: &mut DwnxConnection) {
    let now = Timestamp::from_micros(NOW_MICROS);
    for _ in 0..64 {
        let mut moved = false;

        loop {
            let mut buf = Vec::new();
            let more = sq.poll_transmit(&mut buf, now).expect("送信できる");
            if !more {
                break;
            }
            dwnx.read(&buf, NOW_MICROS).expect("dwnx が読める");
            moved = true;
        }

        loop {
            let mut out = Vec::new();
            let more = dwnx
                .poll_transmit(&mut out, NOW_MICROS)
                .expect("送信できる");
            if !more {
                break;
            }
            sq.receive(&out, now);
            moved = true;
        }

        if !moved {
            return;
        }
    }
}

#[test]
fn shiguredo_client_sends_stream_to_dwnx_server() {
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");

    // ハンドシェイク (QX_TRANSPORT_PARAMETERS の交換)
    pump(&mut client, &mut server);
    assert!(
        client.peer_transport_parameters().is_some(),
        "クライアントがピアのトランスポートパラメータを受信すること"
    );

    // クライアントが双方向ストリームを開いてデータを送る
    let id = client.open_bidi().expect("ストリームを開ける");
    let written = client.write(id, b"hello", true).expect("書き込める");
    assert_eq!(written, 5, "全データを受け付けること");

    pump(&mut client, &mut server);

    // dwnx サーバー側でストリームの open とデータ受信を観測する
    let mut opened = false;
    let mut data = Vec::new();
    let mut fin = false;
    while let Some(event) = server.poll_event() {
        match event {
            DwnxEvent::StreamOpened { id: stream_id } => {
                assert_eq!(
                    stream_id,
                    id.as_u64() as i64,
                    "同じストリーム ID であること"
                );
                opened = true;
            }
            DwnxEvent::StreamData {
                data: chunk,
                fin: f,
                ..
            } => {
                data.extend_from_slice(&chunk);
                fin |= f;
            }
            _ => {}
        }
    }
    assert!(opened, "dwnx 側でストリームが開くこと");
    assert_eq!(data, b"hello", "データが一致すること");
    assert!(fin, "FIN が伝わること");
}

#[test]
fn dwnx_client_sends_stream_to_shiguredo_server() {
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);

    let id = client.open_bidi().expect("ストリームを開ける");
    client.write(id, b"world", true);

    pump(&mut server, &mut client);

    // shiguredo_qmux サーバー側でストリームの open とデータ受信を観測する
    let stream_id = id as u64;
    let mut opened = false;
    let mut data = Vec::new();
    let mut fin = false;
    while let Some(event) = server.poll_event() {
        match event {
            SqEvent::StreamOpened { id } => {
                assert_eq!(id.as_u64(), stream_id, "同じストリーム ID であること");
                opened = true;
            }
            SqEvent::StreamReadable { id } => {
                assert_eq!(id.as_u64(), stream_id);
                let mut buf = [0u8; 64];
                match server.read(id, &mut buf).expect("読み取れる") {
                    ReadResult::Data { len, fin: f } => {
                        data.extend_from_slice(&buf[..len]);
                        fin |= f;
                    }
                    ReadResult::Reset { .. } => panic!("リセットは起きない"),
                }
            }
            _ => {}
        }
    }
    assert!(opened, "shiguredo_qmux 側でストリームが開くこと");
    assert_eq!(data, b"world", "データが一致すること");
    assert!(fin, "FIN が伝わること");
}

#[test]
fn shiguredo_client_sends_unidirectional_stream_to_dwnx_server() {
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");

    pump(&mut client, &mut server);

    // クライアントが単方向ストリームを開いてデータを送る
    let id = client.open_uni().expect("単方向ストリームを開ける");
    let written = client.write(id, b"uni", true).expect("書き込める");
    assert_eq!(written, 3, "全データを受け付けること");

    pump(&mut client, &mut server);

    let mut opened = false;
    let mut data = Vec::new();
    let mut fin = false;
    while let Some(event) = server.poll_event() {
        match event {
            DwnxEvent::StreamOpened { id: stream_id } => {
                assert_eq!(
                    stream_id,
                    id.as_u64() as i64,
                    "同じストリーム ID であること"
                );
                opened = true;
            }
            DwnxEvent::StreamData {
                data: chunk,
                fin: f,
                ..
            } => {
                data.extend_from_slice(&chunk);
                fin |= f;
            }
            _ => {}
        }
    }
    assert!(opened, "dwnx 側で単方向ストリームが開くこと");
    assert_eq!(data, b"uni", "データが一致すること");
    assert!(fin, "FIN が伝わること");
}

#[test]
fn dwnx_client_sends_unidirectional_stream_to_shiguredo_server() {
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);

    let id = client.open_uni().expect("単方向ストリームを開ける");
    client.write(id, b"uni-back", true);

    pump(&mut server, &mut client);

    let stream_id = id as u64;
    let mut opened = false;
    let mut data = Vec::new();
    let mut fin = false;
    while let Some(event) = server.poll_event() {
        match event {
            SqEvent::StreamOpened { id } => {
                assert_eq!(id.as_u64(), stream_id, "同じストリーム ID であること");
                opened = true;
            }
            SqEvent::StreamReadable { id } => {
                assert_eq!(id.as_u64(), stream_id);
                let mut buf = [0u8; 64];
                match server.read(id, &mut buf).expect("読み取れる") {
                    ReadResult::Data { len, fin: f } => {
                        data.extend_from_slice(&buf[..len]);
                        fin |= f;
                    }
                    ReadResult::Reset { .. } => panic!("リセットは起きない"),
                }
            }
            _ => {}
        }
    }
    assert!(opened, "shiguredo_qmux 側で単方向ストリームが開くこと");
    assert_eq!(data, b"uni-back", "データが一致すること");
    assert!(fin, "FIN が伝わること");
}

#[test]
fn shiguredo_reset_stream_propagates_to_dwnx_server() {
    /// RESET_STREAM で伝えるアプリケーションエラーコード
    const ERROR_CODE: u64 = 0x1234;

    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");

    pump(&mut client, &mut server);

    // 一部のデータを送ってから送信側をリセットする
    let id = client.open_bidi().expect("ストリームを開ける");
    client.write(id, b"partial", false).expect("書き込める");
    pump(&mut client, &mut server);

    client.reset(id, ERROR_CODE).expect("RESET_STREAM を送れる");
    pump(&mut client, &mut server);

    let mut reset = None;
    while let Some(event) = server.poll_event() {
        if let DwnxEvent::StreamReset {
            id: stream_id,
            error_code,
            ..
        } = event
        {
            assert_eq!(
                stream_id,
                id.as_u64() as i64,
                "同じストリーム ID であること"
            );
            reset = Some(error_code);
        }
    }
    assert_eq!(
        reset,
        Some(ERROR_CODE),
        "RESET_STREAM のアプリケーションエラーコードが伝わること"
    );
}

#[test]
fn dwnx_reset_stream_propagates_to_shiguredo_server() {
    /// RESET_STREAM で伝えるアプリケーションエラーコード
    const ERROR_CODE: u64 = 0x99;

    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);

    // 一部のデータを送ってから送信側をリセットする
    let id = client.open_bidi().expect("ストリームを開ける");
    client.write(id, b"partial", false);
    client
        .shutdown_write(id, ERROR_CODE)
        .expect("RESET_STREAM を送れる");

    pump(&mut server, &mut client);

    let stream_id = id as u64;
    let mut opened = false;
    let mut reset = None;
    while let Some(event) = server.poll_event() {
        match event {
            SqEvent::StreamOpened { id } => {
                assert_eq!(id.as_u64(), stream_id, "同じストリーム ID であること");
                opened = true;
            }
            SqEvent::StreamReset { id, error_code } => {
                assert_eq!(id.as_u64(), stream_id, "同じストリーム ID であること");
                reset = Some(error_code);
            }
            _ => {}
        }
    }
    assert!(opened, "shiguredo_qmux 側でストリームが開くこと");
    assert_eq!(
        reset,
        Some(ERROR_CODE),
        "RESET_STREAM のアプリケーションエラーコードが伝わること"
    );
}

#[test]
fn shiguredo_stop_sending_propagates_to_dwnx_server() {
    /// STOP_SENDING で伝えるアプリケーションエラーコード
    const ERROR_CODE: u64 = 0x77;

    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");

    pump(&mut client, &mut server);

    // データを 1 バイト送ってストリームを作らせてから、受信の停止を要求する
    let id = client.open_bidi().expect("ストリームを開ける");
    client.write(id, b"x", false).expect("書き込める");
    pump(&mut client, &mut server);

    client
        .stop_sending(id, ERROR_CODE)
        .expect("STOP_SENDING を送れる");
    pump(&mut client, &mut server);

    let mut stop_sending = None;
    while let Some(event) = server.poll_event() {
        if let DwnxEvent::RecvStopSending {
            id: stream_id,
            error_code,
        } = event
        {
            assert_eq!(
                stream_id,
                id.as_u64() as i64,
                "同じストリーム ID であること"
            );
            stop_sending = Some(error_code);
        }
    }
    assert_eq!(
        stop_sending,
        Some(ERROR_CODE),
        "STOP_SENDING のアプリケーションエラーコードが伝わること"
    );
}

#[test]
fn dwnx_stop_sending_propagates_to_shiguredo_server() {
    /// STOP_SENDING で伝えるアプリケーションエラーコード
    const ERROR_CODE: u64 = 0x88;

    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);

    // データを 1 バイト送ってストリームを作らせてから、受信の停止を要求する
    let id = client.open_bidi().expect("ストリームを開ける");
    client.write(id, b"x", false);
    pump(&mut server, &mut client);

    client
        .shutdown_read(id, ERROR_CODE)
        .expect("STOP_SENDING を送れる");
    pump(&mut server, &mut client);

    let stream_id = id as u64;
    let mut stop_sending = None;
    while let Some(event) = server.poll_event() {
        if let SqEvent::StreamStopSending { id, error_code } = event {
            assert_eq!(id.as_u64(), stream_id, "同じストリーム ID であること");
            stop_sending = Some(error_code);
        }
    }
    assert_eq!(
        stop_sending,
        Some(ERROR_CODE),
        "STOP_SENDING のアプリケーションエラーコードが伝わること"
    );
}

#[test]
fn multiple_bidirectional_streams_are_transferred() {
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");

    pump(&mut client, &mut server);

    // 3 本の双方向ストリームを開き、それぞれ別のデータを FIN 付きで送る
    let payloads: [&[u8]; 3] = [b"first", b"second", b"third"];
    let mut ids = Vec::new();
    for payload in payloads {
        let id = client.open_bidi().expect("ストリームを開ける");
        let written = client.write(id, payload, true).expect("書き込める");
        assert_eq!(written, payload.len(), "全データを受け付けること");
        ids.push(id.as_u64() as i64);
    }

    pump(&mut client, &mut server);

    let mut opened = Vec::new();
    let mut received = std::collections::BTreeMap::new();
    while let Some(event) = server.poll_event() {
        match event {
            DwnxEvent::StreamOpened { id: stream_id } => opened.push(stream_id),
            DwnxEvent::StreamData {
                id: stream_id,
                data,
                fin,
                ..
            } => {
                assert!(fin, "ストリーム {stream_id} の FIN が伝わること");
                received.insert(stream_id, data);
            }
            _ => {}
        }
    }
    assert_eq!(opened, ids, "開いた順にストリームが開くこと");
    assert_eq!(
        received.len(),
        payloads.len(),
        "すべてのストリームが届くこと"
    );
    for (index, id) in ids.iter().enumerate() {
        assert_eq!(
            received.get(id).map(Vec::as_slice),
            Some(payloads[index]),
            "ストリーム {id} のデータが一致すること"
        );
    }
}

#[test]
fn stream_is_closed_after_fin_in_both_directions() {
    // ストリーム数の上限を 1 にして、終端したストリームのクレジットが返ることを
    // 観測できるようにする
    let server_config = Config {
        initial_max_streams_bidi: 1,
        ..sq_config()
    };
    let client_config = TransportParameters {
        initial_max_streams_bidi: 1,
        ..dwnx_config()
    };
    let mut client = DwnxConnection::client(&client_config).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, server_config).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);

    // dwnx クライアントが双方向ストリームを開き、FIN まで送る
    let id = client.open_bidi().expect("ストリームを開ける");
    client.write(id, b"bye", true);

    pump(&mut server, &mut client);

    // shiguredo_qmux サーバーがデータと FIN を読み取り、自分の FIN も送る
    let stream_id = id as u64;
    let mut stream = None;
    let mut data = Vec::new();
    let mut fin = false;
    while let Some(event) = server.poll_event() {
        if let SqEvent::StreamReadable { id } = event {
            assert_eq!(id.as_u64(), stream_id, "同じストリーム ID であること");
            let mut buf = [0u8; 64];
            match server.read(id, &mut buf).expect("読み取れる") {
                ReadResult::Data { len, fin: f } => {
                    data.extend_from_slice(&buf[..len]);
                    fin |= f;
                }
                ReadResult::Reset { .. } => panic!("リセットは起きない"),
            }
            stream = Some(id);
        }
    }
    assert_eq!(data, b"bye", "データが一致すること");
    assert!(fin, "FIN が伝わること");

    let stream = stream.expect("読み取ったストリームがある");
    let written = server.write(stream, b"", true).expect("FIN を送れる");
    assert_eq!(written, 0, "FIN のみを送ること");

    pump(&mut server, &mut client);

    // dwnx 側でストリームが閉じる
    let mut closed = false;
    while let Some(event) = client.poll_event() {
        if let DwnxEvent::StreamClosed { id: stream_id, .. } = event {
            assert_eq!(stream_id, id, "同じストリーム ID であること");
            closed = true;
        }
    }
    assert!(closed, "dwnx 側でストリームが閉じること");

    // 終端したストリームのクレジットが返り、次のストリームを開ける
    // (dwnx はクレジットが無いと DWNX_ERR_STREAM_ID_BLOCKED を返すため、開けること
    // 自体が MAX_STREAMS を受け取った証拠になる)
    pump(&mut server, &mut client);
    let next = client
        .open_bidi()
        .expect("終端したストリームのクレジットが返ること");
    assert_eq!(
        next,
        id + 4,
        "次のクライアント起動の双方向ストリームであること"
    );

    // ストリームは開いただけではピアに伝わらないため、データを送って確認する
    client.write(next, b"next", true);

    pump(&mut server, &mut client);
    let mut opened = false;
    let mut data = Vec::new();
    while let Some(event) = server.poll_event() {
        match event {
            SqEvent::StreamOpened { id } => {
                assert_eq!(id.as_u64(), next as u64, "同じストリーム ID であること");
                opened = true;
            }
            SqEvent::StreamReadable { id } => {
                let mut buf = [0u8; 64];
                if let ReadResult::Data { len, .. } = server.read(id, &mut buf).expect("読み取れる")
                {
                    data.extend_from_slice(&buf[..len]);
                }
            }
            _ => {}
        }
    }
    assert!(opened, "新しいストリームがサーバー側で開くこと");
    assert_eq!(data, b"next", "新しいストリームのデータが届くこと");
}

#[test]
fn shiguredo_client_sends_datagram_to_dwnx_server() {
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");

    // ハンドシェイクで双方が DATAGRAM 拡張を広告する
    pump(&mut client, &mut server);
    let peer = client
        .peer_transport_parameters()
        .expect("ピアのトランスポートパラメータを受信すること");
    assert_eq!(
        peer.max_datagram_frame_size,
        Some(65535),
        "dwnx が広告した max_datagram_frame_size を受信すること"
    );

    // クライアントが DATAGRAM を送る (RFC 9221)
    client
        .send_datagram(b"hello datagram")
        .expect("DATAGRAM を送れる");
    pump(&mut client, &mut server);

    // dwnx サーバー側で DATAGRAM を受信する
    let mut received = Vec::new();
    while let Some(event) = server.poll_event() {
        if let DwnxEvent::Datagram { data } = event {
            received.push(data);
        }
    }
    assert_eq!(
        received,
        vec![b"hello datagram".to_vec()],
        "shiguredo_qmux が送信した DATAGRAM が dwnx に届くこと"
    );
}

#[test]
fn dwnx_client_sends_datagram_to_shiguredo_server() {
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);
    let peer = server
        .peer_transport_parameters()
        .expect("ピアのトランスポートパラメータを受信すること");
    assert_eq!(
        peer.max_datagram_frame_size,
        Some(65535),
        "dwnx が広告した max_datagram_frame_size を受信すること"
    );

    // dwnx クライアントが DATAGRAM を送る
    assert!(
        client.send_datagram(b"world datagram"),
        "DATAGRAM を送信待ちに積めること"
    );
    pump(&mut server, &mut client);

    // dwnx 側では送信が Record に収められたことが通知される
    match client.poll_datagram_event() {
        Some(DwnxDatagramEvent::Accepted) => {}
        other => panic!("DATAGRAM の完了が通知されない: {other:?}"),
    }

    // shiguredo_qmux サーバー側で DATAGRAM を受信する
    let mut received = Vec::new();
    while let Some(event) = server.poll_event() {
        if let SqEvent::Datagram(data) = event {
            received.push(data);
        }
    }
    assert_eq!(
        received,
        vec![b"world datagram".to_vec()],
        "dwnx が送信した DATAGRAM が shiguredo_qmux に届くこと"
    );
}

#[test]
fn multiple_datagrams_and_empty_datagram_are_transferred() {
    // 空の DATAGRAM は 0 バイトの DATAGRAM フレームとして送れる
    // (RFC 9221 Section 4)
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");

    pump(&mut client, &mut server);

    let payloads: [&[u8]; 3] = [b"first", b"", b"third"];
    for payload in payloads {
        client.send_datagram(payload).expect("DATAGRAM を送れる");
    }
    pump(&mut client, &mut server);

    let mut received = Vec::new();
    while let Some(event) = server.poll_event() {
        if let DwnxEvent::Datagram { data } = event {
            received.push(data);
        }
    }
    assert_eq!(
        received,
        payloads
            .iter()
            .map(|payload| payload.to_vec())
            .collect::<Vec<_>>(),
        "複数の DATAGRAM が送信した順に届き、空の DATAGRAM も届くこと"
    );

    // 逆方向でも空の DATAGRAM が 1 つのイベントとして届くこと
    assert!(
        server.send_datagram(b""),
        "空の DATAGRAM を送信待ちに積めること"
    );
    pump(&mut client, &mut server);

    match server.poll_datagram_event() {
        Some(DwnxDatagramEvent::Accepted) => {}
        other => panic!("空の DATAGRAM の完了が通知されない: {other:?}"),
    }
    let mut received = Vec::new();
    while let Some(event) = client.poll_event() {
        if let SqEvent::Datagram(data) = event {
            received.push(data);
        }
    }
    assert_eq!(
        received,
        vec![Vec::new()],
        "dwnx が送信した空の DATAGRAM が shiguredo_qmux に届くこと"
    );
}

#[test]
fn shiguredo_datagram_to_dwnx_without_support_is_rejected() {
    // ピアが DATAGRAM 拡張を広告していない (max_datagram_frame_size が 0) 場合、
    // 送信はエラーになる (RFC 9221 Section 3)
    let server_config = TransportParameters {
        max_datagram_frame_size: 0,
        ..dwnx_config()
    };
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&server_config).expect("dwnx サーバーを作れる");

    pump(&mut client, &mut server);

    let result = client.send_datagram(b"unsupported");
    assert!(
        result.is_err(),
        "DATAGRAM 非対応のピアへの送信がエラーになること"
    );
    assert!(
        !client.is_closed(),
        "DATAGRAM の送信失敗で接続は閉じないこと"
    );
}

#[test]
fn dwnx_datagram_to_shiguredo_without_support_is_reported_as_failure() {
    // shiguredo_qmux が DATAGRAM 拡張を広告しない場合、dwnx は
    // DWNX_ERR_INVALID_STATE で送信を失敗させるが、接続は閉じない
    // (dwnx.h の dwnx_conn_writev_datagram を参照)
    let server_config = Config {
        max_datagram_frame_size: None,
        ..sq_config()
    };
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, server_config).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);

    assert!(
        client.send_datagram(b"unsupported"),
        "DATAGRAM を送信待ちに積めること"
    );
    // DATAGRAM は失敗するが、制御フレームだけの Record が出ることはある
    let now = Timestamp::from_micros(NOW_MICROS);
    let mut out = Vec::new();
    for _ in 0..16 {
        if !client
            .poll_transmit(&mut out, NOW_MICROS)
            .expect("送信できる")
        {
            break;
        }
        // 失敗した DATAGRAM は Record に含まれないため、届いた Record は
        // 読み捨てる
        server.receive(&out, now);
    }

    match client.poll_datagram_event() {
        Some(DwnxDatagramEvent::Failed { error }) => {
            assert_eq!(
                error.code, DWNX_ERR_INVALID_STATE,
                "ピア非対応は DWNX_ERR_INVALID_STATE として通知されること"
            );
        }
        other => panic!("DATAGRAM の失敗が通知されない: {other:?}"),
    }
    assert!(
        !client.is_closed(),
        "DATAGRAM の送信失敗で接続は閉じないこと"
    );
}

#[test]
fn shiguredo_datagram_larger_than_dwnx_limit_is_rejected() {
    // フレーム全体 (タイプと Length を含む) がピアの max_datagram_frame_size を
    // 超える場合は送信できない (RFC 9221 Section 3)
    let server_config = TransportParameters {
        max_datagram_frame_size: 100,
        ..dwnx_config()
    };
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&server_config).expect("dwnx サーバーを作れる");

    pump(&mut client, &mut server);

    let payload = vec![0x5au8; 200];
    let result = client.send_datagram(&payload);
    match result {
        Err(shiguredo_qmux::codec::error::Error::InvalidData(message)) => {
            assert!(
                message.contains("max_datagram_frame_size"),
                "エラーがピアの上限超過を説明すること: {message}"
            );
        }
        other => panic!("想定しない結果: {other:?}"),
    }
    assert!(
        !client.is_closed(),
        "DATAGRAM の送信失敗で接続は閉じないこと"
    );
}

#[test]
fn dwnx_datagram_larger_than_shiguredo_limit_is_reported_as_failure() {
    // フレーム全体がピアの max_datagram_frame_size を超える場合、dwnx は
    // DWNX_ERR_INVALID_ARGUMENT で送信を失敗させるが、接続は閉じない
    let server_config = Config {
        max_datagram_frame_size: Some(100),
        ..sq_config()
    };
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, server_config).expect("テスト用の接続を作成できる");

    pump(&mut server, &mut client);

    let payload = vec![0x5au8; 200];
    assert!(
        client.send_datagram(&payload),
        "DATAGRAM を送信待ちに積めること"
    );
    let now = Timestamp::from_micros(NOW_MICROS);
    let mut out = Vec::new();
    for _ in 0..16 {
        if !client
            .poll_transmit(&mut out, NOW_MICROS)
            .expect("送信できる")
        {
            break;
        }
        server.receive(&out, now);
    }

    match client.poll_datagram_event() {
        Some(DwnxDatagramEvent::Failed { error }) => {
            assert_eq!(
                error.code, DWNX_ERR_INVALID_ARGUMENT,
                "サイズ超過は DWNX_ERR_INVALID_ARGUMENT として通知されること"
            );
        }
        other => panic!("DATAGRAM の失敗が通知されない: {other:?}"),
    }
    assert!(
        !client.is_closed(),
        "DATAGRAM の送信失敗で接続は閉じないこと"
    );
}

#[test]
fn shiguredo_stream_and_datagram_share_one_record_in_dwnx() {
    // ストリームデータと DATAGRAM を同じ Record に収めた場合でも、dwnx が
    // 1 回の読み取りで両方のフレームを処理できること。
    let mut client =
        SqConnection::new(Role::Client, sq_config()).expect("テスト用の接続を作成できる");
    let mut server = DwnxConnection::server(&dwnx_config()).expect("dwnx サーバーを作れる");
    pump(&mut client, &mut server);
    while server.poll_event().is_some() {}

    let id = client.open_bidi().expect("ストリームを開ける");
    client
        .write(id, b"stream in record", false)
        .expect("書き込める");
    client
        .send_datagram(b"datagram in record")
        .expect("DATAGRAM を送れる");

    // shiguredo_qmux は送信待ちのフレームを 1 つの Record にまとめるため、
    // 1 回の poll_transmit で両方を含む Record が書き出される
    let now = Timestamp::from_micros(NOW_MICROS);
    let mut record = Vec::new();
    assert!(
        client.poll_transmit(&mut record, now).expect("送信できる"),
        "Record が書き出されること"
    );

    // 1 回の読み取りで両方のイベントが通知されること
    server
        .read(&record, NOW_MICROS)
        .expect("Record を読み取れる");
    let mut events = Vec::new();
    while let Some(event) = server.poll_event() {
        events.push(event);
    }
    assert!(
        events.contains(&DwnxEvent::StreamOpened {
            id: id.as_u64() as i64
        }),
        "同じ Record からストリームの open が通知されること"
    );
    let mut stream_data = Vec::new();
    for event in &events {
        if let DwnxEvent::StreamData { data, .. } = event {
            stream_data.extend_from_slice(data);
        }
    }
    assert_eq!(
        stream_data, b"stream in record",
        "同じ Record のストリームデータが届くこと"
    );
    assert_eq!(
        collect_dwnx_datagrams(&mut events),
        vec![b"datagram in record".to_vec()],
        "同じ Record の DATAGRAM が届くこと"
    );
}

#[test]
fn dwnx_stream_and_datagram_share_one_record_in_shiguredo() {
    // 逆方向でも STREAM と DATAGRAM が同じ Record に載った状態で相互運用できること。
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");
    pump(&mut server, &mut client);
    while client.poll_event().is_some() {}

    let id = client.open_bidi().expect("ストリームを開ける");
    assert!(
        client.write(id, b"stream in record", false),
        "書き込みを積めること"
    );
    assert!(
        client.send_datagram(b"datagram in record"),
        "DATAGRAM を送信待ちに積めること"
    );

    // dwnx-rs は同じ Record にストリームデータと DATAGRAM を詰める
    let mut record = Vec::new();
    assert!(
        client
            .poll_transmit(&mut record, NOW_MICROS)
            .expect("送信できる"),
        "Record が書き出されること"
    );

    let now = Timestamp::from_micros(NOW_MICROS);
    server.receive(&record, now);
    let mut stream_data = Vec::new();
    let mut datagrams = Vec::new();
    while let Some(event) = server.poll_event() {
        match event {
            SqEvent::StreamReadable { id: stream_id } => {
                let mut buf = [0u8; 64];
                match server
                    .read(stream_id, &mut buf)
                    .expect("ストリームを読み取れる")
                {
                    ReadResult::Data { len, .. } => {
                        stream_data.extend_from_slice(&buf[..len]);
                    }
                    ReadResult::Reset { .. } => panic!("リセットは起きない"),
                }
            }
            SqEvent::Datagram(data) => datagrams.push(data),
            _ => {}
        }
    }
    assert_eq!(
        stream_data, b"stream in record",
        "同じ Record のストリームデータが届くこと"
    );
    assert_eq!(
        datagrams,
        vec![b"datagram in record".to_vec()],
        "同じ Record の DATAGRAM が届くこと"
    );
}

#[test]
fn dwnx_datagram_split_across_receives_is_reassembled_by_shiguredo_server() {
    // dwnx が書き出した Record を 1 バイトずつ shiguredo_qmux へ渡しても、
    // DATAGRAM が欠けずに組み立てられること。
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");
    pump(&mut server, &mut client);
    while client.poll_event().is_some() {}

    let payload = b"split across receives";
    assert!(
        client.send_datagram(payload),
        "DATAGRAM を送信待ちに積めること"
    );
    let mut record = Vec::new();
    assert!(
        client
            .poll_transmit(&mut record, NOW_MICROS)
            .expect("送信できる"),
        "DATAGRAM を含む Record が書き出されること"
    );
    assert!(
        record.len() > 1,
        "複数回に分けて渡せる大きさの Record であること"
    );

    // 1 バイトずつ渡す。shiguredo_qmux の受信側で Record を跨いだ組み立てが行われる
    let now = Timestamp::from_micros(NOW_MICROS);
    for byte in &record {
        server.receive(core::slice::from_ref(byte), now);
    }

    let mut datagrams = Vec::new();
    while let Some(event) = server.poll_event() {
        if let SqEvent::Datagram(data) = event {
            datagrams.push(data);
        }
    }
    assert_eq!(
        datagrams,
        vec![payload.to_vec()],
        "分割して渡された Record から DATAGRAM が組み立てられること"
    );
}

#[test]
fn dwnx_datagram_at_record_capacity_limit_is_transferred_to_shiguredo_server() {
    // dwnx-rs の Record 容量 (16382 バイト) に収まる上限の DATAGRAM
    // (ペイロード 16377 バイト) を相互接続で送受信できること。上限を 1 バイト
    // 超えると Send 側で失敗し、接続は閉じないこと (D-02 の回帰確認)。
    let mut client = DwnxConnection::client(&dwnx_config()).expect("dwnx クライアントを作れる");
    let mut server =
        SqConnection::new(Role::Server, sq_config()).expect("テスト用の接続を作成できる");
    pump(&mut server, &mut client);
    while client.poll_event().is_some() {}

    let payload = vec![0xa5u8; 16377];
    assert!(
        client.send_datagram(&payload),
        "上限ちょうどの DATAGRAM を送信待ちに積めること"
    );
    pump(&mut server, &mut client);

    let mut datagrams = Vec::new();
    while let Some(event) = server.poll_event() {
        if let SqEvent::Datagram(data) = event {
            datagrams.push(data);
        }
    }
    assert_eq!(
        datagrams.len(),
        1,
        "上限ちょうどの DATAGRAM が 1 つ届くこと"
    );
    assert_eq!(
        datagrams[0], payload,
        "上限ちょうどの DATAGRAM のデータが一致すること"
    );

    // 送信側の完了通知をすべて取り出す
    let mut accepted = 0;
    while let Some(event) = client.poll_datagram_event() {
        match event {
            DwnxDatagramEvent::Accepted => accepted += 1,
            DwnxDatagramEvent::Failed { error } => panic!("DATAGRAM の送信に失敗した: {error}"),
        }
    }
    assert_eq!(
        accepted, 1,
        "上限ちょうどの DATAGRAM の完了が通知されること"
    );

    // 上限を 1 バイト超える DATAGRAM は失敗し、接続は閉じない
    assert!(
        client.send_datagram(&vec![0x5au8; 16378]),
        "収まらない DATAGRAM も失敗として通知するため true を返すこと"
    );
    let now = Timestamp::from_micros(NOW_MICROS);
    let mut out = Vec::new();
    for _ in 0..16 {
        if !client
            .poll_transmit(&mut out, NOW_MICROS)
            .expect("送信できる")
        {
            break;
        }
        server.receive(&out, now);
    }
    match client.poll_datagram_event() {
        Some(DwnxDatagramEvent::Failed { error }) => {
            assert_eq!(
                error.code, DWNX_ERR_INVALID_ARGUMENT,
                "Record 容量超過は DWNX_ERR_INVALID_ARGUMENT として通知されること"
            );
        }
        other => panic!("DATAGRAM の失敗が通知されない: {other:?}"),
    }
    assert!(
        !client.is_closed(),
        "Record 容量超過は接続の終了ではないこと"
    );
}
