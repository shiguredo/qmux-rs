//! QMux (shiguredo_qmux) と dwnx の Sans I/O 相互接続テスト
//!
//! 2 つの実装をメモリ上のバイト列で接続し、双方向にストリームデータが
//! 伝わることを検証する。TLS は使わず、QMux のワイヤ表現だけを突き合わせる。

use dwnx_rs::{Connection as DwnxConnection, Event as DwnxEvent, TransportParameters};
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::{Duration, Timestamp};
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection as SqConnection;
use shiguredo_qmux::event::Event as SqEvent;
use shiguredo_qmux::stream::ReadResult;

/// テストで使う時刻 (マイクロ秒)
const NOW_MICROS: u64 = 1_000_000;

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
