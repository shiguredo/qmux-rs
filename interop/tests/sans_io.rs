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
