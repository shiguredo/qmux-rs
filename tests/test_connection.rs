//! QMux 接続のエンドツーエンドの単体テスト。
//!
//! クライアントとサーバーの 2 つの [`Connection`] を Record のバイト列で接続し、
//! ハンドシェイク、ストリーム、フロー制御、DATAGRAM、QX_PING、切断、
//! プロトコル違反の検出を検証する。

use shiguredo_qmux::codec::error::{Error, TransportErrorCode};
use shiguredo_qmux::codec::frame::{self, Frame};
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::{Duration, Timestamp};
use shiguredo_qmux::codec::varint::VarInt;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection;
use shiguredo_qmux::event::{CloseReason, Event};
use shiguredo_qmux::frame::{QmuxFrame, QxPing, QxTransportParameters};
use shiguredo_qmux::record::{DEFAULT_MAX_RECORD_SIZE, RecordDecoder, encode as encode_record};
use shiguredo_qmux::stream::{ReadResult, StreamId};

/// テストで使う時刻 (マイクロ秒)
const NOW: Timestamp = Timestamp::from_micros(1_000_000);

/// テスト用の設定
///
/// ストリームと DATAGRAM を使えるだけの余裕を持たせた値にする。
fn config() -> Config {
    Config {
        max_idle_timeout: Duration::from_secs(30),
        initial_max_data: 64 * 1024,
        initial_max_stream_data_bidi_local: 16 * 1024,
        initial_max_stream_data_bidi_remote: 16 * 1024,
        initial_max_stream_data_uni: 16 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        max_record_size: DEFAULT_MAX_RECORD_SIZE,
        max_datagram_frame_size: Some(65535),
    }
}

/// クライアントとサーバーの接続の組
struct Pair {
    client: Connection,
    server: Connection,
}

impl Pair {
    /// 既定の設定で接続の組を作る
    fn new() -> Self {
        Self::with_config(config(), config())
    }

    /// 指定した設定で接続の組を作る
    fn with_config(client: Config, server: Config) -> Self {
        Self {
            client: Connection::new(Role::Client, client).expect("テスト用の接続を作成できる"),
            server: Connection::new(Role::Server, server).expect("テスト用の接続を作成できる"),
        }
    }

    /// 送信待ちの Record をすべて相手へ渡す
    ///
    /// 下位トランスポートがバイト列をそのまま運ぶ状況を再現する。
    fn transfer(&mut self) {
        self.transfer_at(NOW);
    }

    /// 指定した時刻で送信待ちの Record をすべて相手へ渡す
    fn transfer_at(&mut self, now: Timestamp) {
        for _ in 0..64 {
            let mut moved = false;
            let mut buf = Vec::new();
            while self
                .client
                .poll_transmit(&mut buf, now)
                .expect("クライアントの送信に失敗した")
            {
                self.server.receive(&buf, now);
                buf.clear();
                moved = true;
            }
            while self
                .server
                .poll_transmit(&mut buf, now)
                .expect("サーバーの送信に失敗した")
            {
                self.client.receive(&buf, now);
                buf.clear();
                moved = true;
            }
            if !moved {
                return;
            }
        }
        panic!("送信待ちの Record がなくならなかった");
    }

    /// ハンドシェイクを完了させ、その間のイベントを捨てる
    fn handshake(&mut self) {
        self.transfer();
        drain(&mut self.client);
        drain(&mut self.server);
    }
}

/// 取り出せるイベントをすべて取り出す
fn drain(conn: &mut Connection) -> Vec<Event> {
    let mut events = Vec::new();
    while let Some(event) = conn.poll_event() {
        events.push(event);
    }
    events
}

/// 接続が閉じた理由を取り出す
fn close_reason(conn: &mut Connection) -> Option<CloseReason> {
    drain(conn).into_iter().find_map(|event| match event {
        Event::ConnectionClosed(reason) => Some(reason),
        _ => None,
    })
}

/// フレームをバイト列へエンコードする
fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut buf = Vec::new();
    frame.encode(&mut buf);
    buf
}

/// QMux のフレームをバイト列へエンコードする
fn encode_qmux_frame(frame: &QmuxFrame) -> Vec<u8> {
    let mut buf = Vec::new();
    frame.encode(&mut buf);
    buf
}

/// 任意のフレーム列を 1 つの Record として相手へ直接渡す
///
/// 壊れた入力や禁止フレームに対する受信側の振る舞いを確認するために使う。
fn inject(conn: &mut Connection, frames: &[u8]) {
    let mut buf = Vec::new();
    encode_record(frames, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("テスト用の Record を作れる");
    conn.receive(&buf, NOW);
}

/// クライアントの送信待ち Record をサーバーへ渡し、渡した Record を返す
///
/// Record の境界と長さを確認するテストで使う。
fn transfer_records_to_server(pair: &mut Pair) -> Vec<Vec<u8>> {
    let mut buf = Vec::new();
    while pair
        .client
        .poll_transmit(&mut buf, NOW)
        .expect("クライアントの送信に失敗した")
    {}
    if buf.is_empty() {
        return Vec::new();
    }
    pair.server.receive(&buf, NOW);

    // テスト側で Record の境界を確認するため、上限を十分大きくしたデコーダを使う
    let mut decoder = RecordDecoder::new(1 << 24);
    decoder.extend(&buf);
    let mut records = Vec::new();
    while let Some(record) = decoder.poll_record().expect("Record を取り出せる") {
        records.push(record);
    }
    records
}

/// ストリームから読み取れるデータをすべて読み取る
///
/// 戻り値は (読み取ったデータ, FIN まで到達したか)。データがない場合は
/// 空のデータと `false` を返す。
fn read_all(conn: &mut Connection, id: StreamId) -> (Vec<u8>, bool) {
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match conn.read(id, &mut buf).expect("読み取りに失敗した") {
            ReadResult::Data { len, fin } => {
                data.extend_from_slice(&buf[..len]);
                if fin {
                    return (data, true);
                }
                if len == 0 {
                    return (data, false);
                }
            }
            ReadResult::Reset { error_code } => panic!("リセットを期待していない: {error_code}"),
        }
    }
}

/// プロトコルエラーのコードを確認する
fn assert_local_error(reason: Option<CloseReason>, expected: TransportErrorCode) {
    match reason {
        Some(CloseReason::Local { error_code, .. }) => {
            assert_eq!(error_code, expected.as_u64(), "エラーコードが一致すること");
        }
        other => panic!("ローカルの切断を期待したが {other:?} だった"),
    }
}

mod handshake {
    use super::*;

    #[test]
    fn invalid_config_is_rejected_at_creation() {
        // Section 5.2: max_record_size が既定値未満の設定は送信できないため、
        // 最初の送信を待たずに接続の作成時に拒否する
        let mut client_config = config();
        client_config.max_record_size = DEFAULT_MAX_RECORD_SIZE - 1;
        assert!(
            Connection::new(Role::Client, client_config).is_err(),
            "既定値未満の max_record_size は拒否されること"
        );
    }

    #[test]
    fn first_transmit_sends_only_transport_parameters() {
        // Section 4.2: QX_TRANSPORT_PARAMETERS は最初のフレームとして送る
        let mut client =
            Connection::new(Role::Client, config()).expect("テスト用の接続を作成できる");
        let mut buf = Vec::new();
        assert!(
            client
                .poll_transmit(&mut buf, NOW)
                .expect("送信に失敗しない"),
            "最初の送信では Record が 1 つ書き出されること"
        );

        // マジックナンバー 0x3f5153300d0a0d0a の 8 バイト可変長整数表現
        let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
        decoder.extend(&buf);
        let record = decoder
            .poll_record()
            .expect("Record を取り出せる")
            .expect("Record が完成すること");
        assert_eq!(
            &record[..8],
            &[0xff, 0x51, 0x53, 0x30, 0x0d, 0x0a, 0x0d, 0x0a],
            "先頭が QX_TRANSPORT_PARAMETERS のフレームタイプであること"
        );

        let frames =
            shiguredo_qmux::frame::decode_record(&record).expect("フレームをデコードできる");
        assert_eq!(
            frames.len(),
            1,
            "最初の Record は QX_TRANSPORT_PARAMETERS だけを含むこと"
        );
        assert!(
            matches!(frames[0], QmuxFrame::TransportParameters(_)),
            "QX_TRANSPORT_PARAMETERS であること"
        );
    }

    #[test]
    fn transport_parameters_are_exchanged() {
        let mut pair = Pair::new();
        pair.transfer();

        assert_eq!(
            drain(&mut pair.client),
            vec![Event::TransportParametersReceived],
            "クライアントがピアのトランスポートパラメータを受信すること"
        );
        assert_eq!(
            drain(&mut pair.server),
            vec![Event::TransportParametersReceived],
            "サーバーがピアのトランスポートパラメータを受信すること"
        );

        let peer = pair
            .client
            .peer_transport_parameters()
            .expect("受信後はピアのパラメータを参照できる");
        assert_eq!(peer.initial_max_data, 64 * 1024, "設定した値が届くこと");
        assert_eq!(
            peer.max_record_size, DEFAULT_MAX_RECORD_SIZE,
            "既定値が届くこと"
        );
    }

    #[test]
    fn stream_operations_require_peer_parameters() {
        // Section 4.2: ピアのトランスポートパラメータを受信するまでは、ピアの
        // フロー制御に依存する操作を行えない
        let mut client =
            Connection::new(Role::Client, config()).expect("テスト用の接続を作成できる");
        assert!(
            client.open_bidi().is_err(),
            "ハンドシェイク前はストリームを開けないこと"
        );
        assert!(
            client.send_datagram(b"x").is_err(),
            "ハンドシェイク前は DATAGRAM を送れないこと"
        );
    }

    #[test]
    fn first_frame_must_be_transport_parameters() {
        // Section 4.2: 最初のフレームが QX_TRANSPORT_PARAMETERS でない場合は
        // PROTOCOL_VIOLATION
        let mut server =
            Connection::new(Role::Server, config()).expect("テスト用の接続を作成できる");
        let padding = Frame::Padding(frame::padding::Padding { length: 1 });
        inject(&mut server, &encode_frame(&padding));

        assert!(server.is_closed(), "接続が閉じること");
        assert_local_error(
            close_reason(&mut server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }

    #[test]
    fn duplicate_transport_parameters_are_rejected() {
        // Section 4.2: QX_TRANSPORT_PARAMETERS は最初のフレームでのみ送信できる
        let mut pair = Pair::new();
        pair.handshake();

        let frame = QmuxFrame::TransportParameters(QxTransportParameters { params: Vec::new() });
        inject(&mut pair.server, &encode_qmux_frame(&frame));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }

    #[test]
    fn prohibited_transport_parameters_close_connection() {
        // Section 5.1: 禁止パラメータを受信した場合は TRANSPORT_PARAMETER_ERROR
        let mut server =
            Connection::new(Role::Server, config()).expect("テスト用の接続を作成できる");
        let mut params = Vec::new();
        VarInt::from(0x03u8).encode_to_vec(&mut params); // max_udp_payload_size (禁止)
        VarInt::from(2u8).encode_to_vec(&mut params);
        VarInt::from(1200u16).encode_to_vec(&mut params);
        let frame = QmuxFrame::TransportParameters(QxTransportParameters { params });
        inject(&mut server, &encode_qmux_frame(&frame));

        assert_local_error(
            close_reason(&mut server),
            TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
        );
    }
}

mod streams {
    use super::*;

    #[test]
    fn client_opens_bidirectional_stream() {
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        assert_eq!(
            id.as_u64(),
            0,
            "クライアント起動の双方向ストリームは 0 から始まること"
        );
        assert_eq!(
            pair.client.write(id, b"hello", true).expect("書き込める"),
            5,
            "5 バイトを受け付けること"
        );

        pair.transfer();
        let events = drain(&mut pair.server);
        assert!(
            events.contains(&Event::StreamOpened { id }),
            "ピアがストリームを開いたイベントが通知されること: {events:?}"
        );
        assert!(
            events.contains(&Event::StreamReadable { id }),
            "読み取り可能イベントが通知されること: {events:?}"
        );

        let (data, fin) = read_all(&mut pair.server, id);
        assert_eq!(data, b"hello", "データがそのまま届くこと");
        assert!(fin, "FIN まで到達すること");
    }

    #[test]
    fn server_responds_on_peer_initiated_stream() {
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"request", true).expect("書き込める");
        pair.transfer();

        let (data, fin) = read_all(&mut pair.server, id);
        assert_eq!(data, b"request", "要求が届くこと");
        assert!(fin, "FIN まで到達すること");

        // ピア起動の双方向ストリームではサーバーも送信できる
        pair.server
            .write(id, b"response", true)
            .expect("応答を書ける");
        pair.transfer();
        let (data, fin) = read_all(&mut pair.client, id);
        assert_eq!(data, b"response", "応答が届くこと");
        assert!(fin, "FIN まで到達すること");
    }

    #[test]
    fn unidirectional_stream_is_receive_only_for_peer() {
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.server.open_uni().expect("単方向ストリームを開ける");
        assert_eq!(
            id.as_u64() & 0x03,
            0x03,
            "サーバー起動の単方向ストリームの種別であること"
        );
        pair.server.write(id, b"uni", true).expect("書き込める");
        pair.transfer();

        let events = drain(&mut pair.client);
        assert!(
            events.contains(&Event::StreamOpened { id }),
            "ストリームが開かれたこと: {events:?}"
        );
        let (data, fin) = read_all(&mut pair.client, id);
        assert_eq!(data, b"uni", "データが届くこと");
        assert!(fin, "FIN まで到達すること");

        // 受信専用ストリームへ書き込むと STREAM_STATE_ERROR
        assert!(
            pair.client.write(id, b"x", false).is_err(),
            "受信専用ストリームへは書き込めないこと"
        );
    }

    #[test]
    fn large_payload_is_split_into_records_within_the_limit() {
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        let payload: Vec<u8> = (0..40_000).map(|index| (index % 251) as u8).collect();

        // フロー制御で一度に書き込めないため、止まったら相手に読ませて
        // クレジットを戻しながら進める
        let mut offset = 0;
        let mut received = Vec::new();
        let mut records = Vec::new();
        while offset < payload.len() {
            let written = pair
                .client
                .write(id, &payload[offset..], false)
                .expect("書き込める");
            if written == 0 {
                records.extend(transfer_records_to_server(&mut pair));
                pair.transfer();
                let (chunk, _) = read_all(&mut pair.server, id);
                received.extend_from_slice(&chunk);
                pair.transfer();
                continue;
            }
            offset += written;
        }

        // 最後に FIN だけを送る
        assert_eq!(
            pair.client.write(id, &[], true).expect("FIN を送れる"),
            0,
            "FIN だけのフレームはデータを受け付けないこと"
        );
        records.extend(transfer_records_to_server(&mut pair));

        assert!(!records.is_empty(), "Record が書き出されること");
        assert!(
            records.len() > 1,
            "40KB のデータは複数の Record に分割されること"
        );
        for record in &records {
            assert!(
                record.len() <= DEFAULT_MAX_RECORD_SIZE as usize,
                "Record の Frames がピアの max_record_size 以下であること: {}",
                record.len()
            );
        }

        // 残りのデータと FIN が届くこと
        let mut finished = false;
        for _ in 0..64 {
            pair.transfer();
            let (chunk, fin) = read_all(&mut pair.server, id);
            received.extend_from_slice(&chunk);
            if fin {
                finished = true;
                break;
            }
        }
        assert!(finished, "FIN まで到達すること");
        assert_eq!(received.len(), payload.len(), "受信データ長が一致すること");
        assert_eq!(received, payload, "40KB のデータが欠けずに届くこと");
    }

    #[test]
    fn stream_data_can_be_written_in_chunks() {
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"abc", false).expect("書き込める");
        pair.client.write(id, b"def", false).expect("書き込める");
        pair.client.write(id, b"", true).expect("FIN だけを送れる");
        pair.transfer();

        let (data, fin) = read_all(&mut pair.server, id);
        assert_eq!(data, b"abcdef", "チャンクが順に届くこと");
        assert!(fin, "空の FIN で終端できること");

        // FIN 後の書き込みは STREAM_STATE_ERROR
        assert!(
            pair.client.write(id, b"x", false).is_err(),
            "終端したストリームへは書き込めないこと"
        );
    }

    #[test]
    fn stop_sending_causes_reset_stream() {
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client
            .write(id, b"partial", false)
            .expect("書き込める");
        pair.transfer();
        let (data, _) = read_all(&mut pair.server, id);
        assert_eq!(data, b"partial", "データが届くこと");

        // RFC 9000 Section 3.5: STOP_SENDING を受けた送信側は RESET_STREAM を送る
        pair.server
            .stop_sending(id, 99)
            .expect("STOP_SENDING を送れる");
        pair.transfer();

        let events = drain(&mut pair.client);
        assert!(
            events.contains(&Event::StreamStopSending { id, error_code: 99 }),
            "STOP_SENDING が通知されること: {events:?}"
        );
        let events = drain(&mut pair.server);
        assert!(
            events.contains(&Event::StreamReset { id, error_code: 99 }),
            "RESET_STREAM が届くこと: {events:?}"
        );

        match pair.server.read(id, &mut [0u8; 16]).expect("読み取れる") {
            ReadResult::Reset { error_code } => {
                assert_eq!(error_code, 99, "エラーコードが一致すること")
            }
            other => panic!("リセットを期待したが {other:?} だった"),
        }
    }

    #[test]
    fn application_reset_is_reported_to_peer() {
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"data", false).expect("書き込める");
        pair.client.reset(id, 7).expect("リセットできる");
        pair.transfer();

        let events = drain(&mut pair.server);
        assert!(
            events.contains(&Event::StreamReset { id, error_code: 7 }),
            "リセットが通知されること: {events:?}"
        );
        // 終端したストリームへの書き込みとリセットはエラー
        assert!(
            pair.client.write(id, b"x", false).is_err(),
            "リセット後の書き込みはエラーになること"
        );
        assert!(
            pair.client.reset(id, 8).is_err(),
            "二重のリセットはエラーになること"
        );
    }

    #[test]
    fn duplicate_reset_stream_is_idempotent() {
        // RFC 9000 Section 12.4: 同じフレームを再受信しても副作用が増えないこと
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"data", false).expect("書き込める");
        pair.client.reset(id, 5).expect("リセットできる");
        pair.transfer();
        let events = drain(&mut pair.server);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::StreamReset { .. }))
                .count(),
            1,
            "リセットが 1 回だけ通知されること: {events:?}"
        );

        // 同じ RESET_STREAM をもう一度送りつけてもイベントは増えない
        let reset = Frame::ResetStream(frame::reset_stream::ResetStream {
            stream_id: id.as_u64(),
            application_protocol_error_code: 5,
            final_size: 4,
        });
        inject(&mut pair.server, &encode_frame(&reset));
        let events = drain(&mut pair.server);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Event::StreamReset { .. })),
            "再受信ではリセットを通知しないこと: {events:?}"
        );
        assert!(!pair.server.is_closed(), "接続は閉じないこと");
    }

    #[test]
    fn max_stream_data_creates_peer_initiated_stream() {
        // RFC 9000 Section 3.2: ピア起動の双方向ストリームでは MAX_STREAM_DATA の
        // 受信がストリームを作成する
        let mut client_config = config();
        client_config.initial_max_stream_data_bidi_local = 0;
        let mut pair = Pair::with_config(client_config, config());
        pair.handshake();

        // シーケンス番号 1 のクライアント起動双方向ストリーム (ID = 4)
        let id = StreamId::new(4).expect("ストリーム ID を作れる");
        let max_stream_data = Frame::MaxStreamData(frame::max_stream_data::MaxStreamData {
            stream_id: 4,
            maximum_stream_data: 1000,
        });
        inject(&mut pair.server, &encode_frame(&max_stream_data));

        let events = drain(&mut pair.server);
        assert!(
            events.contains(&Event::StreamOpened { id }),
            "ストリームが作成されること: {events:?}"
        );
        assert!(
            events.contains(&Event::StreamWritable { id }),
            "クレジットが増えること: {events:?}"
        );
        assert_eq!(
            pair.server.write(id, b"credit", false).expect("書き込める"),
            6,
            "受信したクレジットの範囲で書き込めること"
        );
    }
}

mod flow_control {
    use super::*;

    #[test]
    fn stream_flow_control_blocks_and_recovers() {
        // サーバーの initial_max_stream_data_bidi_remote がクライアント起動の
        // 双方向ストリームの送信上限になる (RFC 9000 Section 18.2)
        let mut server_config = config();
        server_config.initial_max_stream_data_bidi_remote = 4;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        assert_eq!(
            pair.client
                .write(id, b"0123456789", false)
                .expect("書き込める"),
            4,
            "ストリームの上限までしか受け付けないこと"
        );
        assert_eq!(
            pair.client.write(id, b"456789", false).expect("書き込める"),
            0,
            "上限に達したら 0 を返すこと"
        );

        pair.transfer();
        let (data, _) = read_all(&mut pair.server, id);
        assert_eq!(data, b"0123", "上限までのデータが届くこと");

        // 読み取ると MAX_STREAM_DATA が返り、書き込めるようになる
        pair.transfer();
        let events = drain(&mut pair.client);
        assert!(
            events.contains(&Event::StreamWritable { id }),
            "書き込み可能イベントが通知されること: {events:?}"
        );
        assert_eq!(
            pair.client.write(id, b"4567", false).expect("書き込める"),
            4,
            "クレジットが増えた分だけ書き込めること"
        );

        pair.transfer();
        let (data, _) = read_all(&mut pair.server, id);
        assert_eq!(data, b"4567", "続きが届くこと");

        // さらにクレジットが戻り、残りと FIN を送れる
        pair.transfer();
        assert_eq!(
            pair.client.write(id, b"89", true).expect("書き込める"),
            2,
            "残りと FIN を送れること"
        );
        pair.transfer();
        let (data, fin) = read_all(&mut pair.server, id);
        assert_eq!(data, b"89", "最後のデータが届くこと");
        assert!(fin, "FIN まで到達すること");
    }

    #[test]
    fn connection_flow_control_limits_total_bytes() {
        // 接続レベルの上限は initial_max_data で決まる (RFC 9000 Section 4.1)
        let mut server_config = config();
        server_config.initial_max_data = 4;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        assert_eq!(
            pair.client
                .write(id, b"0123456789", false)
                .expect("書き込める"),
            4,
            "接続レベルの上限までしか受け付けないこと"
        );
        assert_eq!(
            pair.client.write(id, b"456789", false).expect("書き込める"),
            0,
            "上限に達したら 0 を返すこと"
        );

        pair.transfer();
        let (data, _) = read_all(&mut pair.server, id);
        assert_eq!(data, b"0123", "上限までのデータが届くこと");
    }

    #[test]
    fn read_returns_credit_to_connection_window() {
        // 読み取った分だけ MAX_DATA で接続レベルのクレジットが戻る
        // (RFC 9000 Section 4.1)
        let mut server_config = config();
        server_config.initial_max_data = 4;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client
            .write(id, b"0123456789", false)
            .expect("書き込める");
        pair.transfer();
        let (data, _) = read_all(&mut pair.server, id);
        assert_eq!(data, b"0123", "上限までのデータが届くこと");

        pair.transfer();
        assert_eq!(
            pair.client.write(id, b"4567", true).expect("書き込める"),
            4,
            "クレジットが戻ると続きを書けること"
        );
        pair.transfer();
        let (data, fin) = read_all(&mut pair.server, id);
        assert_eq!(data, b"4567", "続きが届くこと");
        assert!(fin, "FIN まで到達すること");
    }

    #[test]
    fn stream_limit_is_enforced_for_local_streams() {
        // RFC 9000 Section 4.6: ピアが広告した数を超えるストリームは開けない
        let mut server_config = config();
        server_config.initial_max_streams_bidi = 1;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        pair.client.open_bidi().expect("1 つ目は開ける");
        match pair.client.open_bidi() {
            Err(Error::Protocol { code, .. }) => assert_eq!(
                code,
                TransportErrorCode::STREAM_LIMIT_ERROR,
                "STREAM_LIMIT_ERROR になること"
            ),
            other => panic!("STREAM_LIMIT_ERROR を期待したが {other:?} だった"),
        }
    }

    #[test]
    fn peer_stream_limit_violation_closes_connection() {
        // RFC 9000 Section 4.6: 広告した上限を超えるストリーム ID のフレームは
        // STREAM_LIMIT_ERROR
        let mut server_config = config();
        server_config.initial_max_streams_bidi = 1;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        // シーケンス番号 1 のクライアント起動双方向ストリーム (ID = 4) を送りつける
        let stream_frame = Frame::Stream(frame::stream::Stream {
            stream_id: 4,
            offset: 0,
            fin: false,
            data: b"x".to_vec(),
        });
        inject(&mut pair.server, &encode_frame(&stream_frame));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::STREAM_LIMIT_ERROR,
        );
    }
}

mod datagrams {
    use super::*;

    #[test]
    fn datagram_is_delivered() {
        let mut pair = Pair::new();
        pair.handshake();

        pair.client.send_datagram(b"hello").expect("送信できる");
        pair.transfer();

        let events = drain(&mut pair.server);
        assert!(
            events.contains(&Event::Datagram(b"hello".to_vec())),
            "DATAGRAM が届くこと: {events:?}"
        );
    }

    #[test]
    fn excess_datagrams_are_counted_as_dropped() {
        // アプリケーションが poll_event しない状態で保持量の上限 (64) を超えると、
        // 超えた分は破棄される。破棄した個数を観測できることを検証する。
        let mut pair = Pair::new();
        pair.handshake();

        for index in 0..100u32 {
            pair.client
                .send_datagram(&index.to_be_bytes())
                .expect("送信できる");
        }
        pair.transfer();

        assert!(
            pair.server.dropped_datagrams() >= 1,
            "保持量の上限を超えた DATAGRAM の破棄数が観測できること"
        );
    }

    #[test]
    fn datagram_larger_than_peer_limit_is_rejected() {
        let mut server_config = config();
        server_config.max_datagram_frame_size = Some(8);
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        // フレーム全体 (タイプ + 長さ + データ) が上限を超えるため送れない
        let error = pair
            .client
            .send_datagram(&[0u8; 32])
            .expect_err("上限を超える DATAGRAM は送れない");
        assert!(
            matches!(error, Error::InvalidData(_)),
            "ローカルのエラーとして扱うこと: {error:?}"
        );
    }

    #[test]
    fn datagram_without_support_is_rejected() {
        // RFC 9221 Section 3: 拡張を広告していないピアへは送れない
        let mut server_config = config();
        server_config.max_datagram_frame_size = None;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        assert!(
            pair.client.send_datagram(b"x").is_err(),
            "DATAGRAM 非対応のピアへは送れないこと"
        );
    }

    #[test]
    fn receiving_datagram_without_advertising_closes_connection() {
        // RFC 9221 Section 3: 広告していない場合は PROTOCOL_VIOLATION
        let mut server_config = config();
        server_config.max_datagram_frame_size = None;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        let datagram = Frame::Datagram(frame::datagram::Datagram {
            data: b"boom".to_vec(),
        });
        inject(&mut pair.server, &encode_frame(&datagram));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }
}

mod pings {
    use super::*;

    #[test]
    fn ping_is_answered_automatically() {
        let mut pair = Pair::new();
        pair.handshake();

        let sequence = pair.client.ping().expect("QX_PING を送れる");
        pair.transfer();

        let events = drain(&mut pair.server);
        assert!(
            events.contains(&Event::Ping { sequence }),
            "要求が通知されること: {events:?}"
        );
        let events = drain(&mut pair.client);
        assert!(
            events.contains(&Event::PingAcknowledged { sequence }),
            "応答が通知されること: {events:?}"
        );
    }

    #[test]
    fn ping_sequence_must_increase() {
        // Section 4.3: 要求のシーケンス番号は厳密に増加しなければならない
        let mut server =
            Connection::new(Role::Server, config()).expect("テスト用の接続を作成できる");
        let transport_parameters =
            QmuxFrame::TransportParameters(QxTransportParameters { params: Vec::new() });
        inject(&mut server, &encode_qmux_frame(&transport_parameters));
        drain(&mut server);

        let mut frames = Vec::new();
        frames.extend_from_slice(&encode_qmux_frame(&QmuxFrame::Ping(QxPing::Request {
            sequence: VarInt::from(5u8),
        })));
        frames.extend_from_slice(&encode_qmux_frame(&QmuxFrame::Ping(QxPing::Request {
            sequence: VarInt::from(5u8),
        })));
        inject(&mut server, &frames);

        assert_local_error(
            close_reason(&mut server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }

    #[test]
    fn ping_response_for_unsent_sequence_is_rejected() {
        // Section 4.3: 送信していないシーケンス番号への応答は PROTOCOL_VIOLATION
        let mut server =
            Connection::new(Role::Server, config()).expect("テスト用の接続を作成できる");
        let transport_parameters =
            QmuxFrame::TransportParameters(QxTransportParameters { params: Vec::new() });
        inject(&mut server, &encode_qmux_frame(&transport_parameters));
        drain(&mut server);

        let response = QmuxFrame::Ping(QxPing::Response {
            sequence: VarInt::from(0u8),
        });
        inject(&mut server, &encode_qmux_frame(&response));

        assert_local_error(
            close_reason(&mut server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }
}

mod termination {
    use super::*;

    #[test]
    fn application_close_is_reported_to_peer() {
        let mut pair = Pair::new();
        pair.handshake();

        pair.client.close(0x1234, "bye");
        assert!(pair.client.is_closed(), "閉じた状態になること");
        pair.transfer();

        assert_eq!(
            close_reason(&mut pair.server),
            Some(CloseReason::Peer {
                application: true,
                error_code: 0x1234,
                reason: "bye".to_string(),
            }),
            "ピアの切断理由が届くこと"
        );
        assert!(pair.server.is_closed(), "ピアの切断で閉じること");
    }

    #[test]
    fn idle_timeout_closes_connection() {
        let mut client_config = config();
        client_config.max_idle_timeout = Duration::from_secs(10);
        let mut server_config = config();
        server_config.max_idle_timeout = Duration::from_secs(1);
        let mut pair = Pair::with_config(client_config, server_config);
        pair.handshake();

        // RFC 9000 Section 10.1: 実効値は両者が広告した値の最小値
        assert_eq!(
            pair.client.next_timeout(),
            Some(NOW.add(Duration::from_secs(1))),
            "小さい方のアイドルタイムアウトが使われること"
        );

        let later = NOW.add(Duration::from_secs(2));
        pair.client.handle_timeout(later);
        assert!(pair.client.is_closed(), "タイムアウトで閉じること");
        assert_eq!(
            close_reason(&mut pair.client),
            Some(CloseReason::IdleTimeout),
            "アイドルタイムアウトとして通知されること"
        );
    }

    #[test]
    fn record_activity_resets_idle_timer() {
        let mut pair = Pair::new();
        pair.handshake();
        let first = pair.client.next_timeout().expect("期限があること");

        // Record を送受信するとアイドルタイマーがリセットされる (Section 7.1)
        let later = NOW.add(Duration::from_secs(5));
        pair.client.ping().expect("QX_PING を送れる");
        pair.transfer_at(later);
        assert!(
            pair.client.next_timeout() > Some(first),
            "Record を送信すると期限が延びること"
        );
        assert!(
            pair.server.next_timeout() > Some(first),
            "Record を受信すると期限が延びること"
        );
    }

    #[test]
    fn data_received_after_close_is_discarded() {
        let mut pair = Pair::new();
        pair.handshake();
        pair.client.close(0, "done");
        pair.transfer();
        drain(&mut pair.server);

        // 切断後の Record は処理せずに破棄する (Section 7.2)
        inject(
            &mut pair.server,
            &encode_frame(&Frame::Padding(frame::padding::Padding { length: 1 })),
        );
        assert_eq!(
            drain(&mut pair.server),
            Vec::new(),
            "イベントが増えないこと"
        );
    }

    #[test]
    fn long_close_reason_is_truncated() {
        // 理由が長すぎても Record に収まるよう切り詰めて切断できること
        let mut pair = Pair::new();
        pair.handshake();

        let reason = "x".repeat(64 * 1024);
        pair.client.close(0, &reason);
        let records = transfer_records_to_server(&mut pair);
        assert!(!records.is_empty(), "切断の Record が書き出されること");
        for record in &records {
            assert!(
                record.len() <= DEFAULT_MAX_RECORD_SIZE as usize,
                "Record が max_record_size 以下であること: {}",
                record.len()
            );
        }
        assert!(
            matches!(
                close_reason(&mut pair.server),
                Some(CloseReason::Peer { .. })
            ),
            "ピアが切断を観測すること"
        );
    }

    #[test]
    fn close_before_first_transmit_sends_transport_parameters_first() {
        // Section 4.2: QX_TRANSPORT_PARAMETERS が最初のフレームでなければならない
        let mut client =
            Connection::new(Role::Client, config()).expect("テスト用の接続を作成できる");
        client.close(0, "immediate");

        let mut buf = Vec::new();
        assert!(
            client
                .poll_transmit(&mut buf, NOW)
                .expect("送信に失敗しない")
        );
        let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
        decoder.extend(&buf);
        let record = decoder
            .poll_record()
            .expect("Record を取り出せる")
            .expect("Record が完成すること");
        let frames =
            shiguredo_qmux::frame::decode_record(&record).expect("フレームをデコードできる");
        assert!(
            matches!(frames[0], QmuxFrame::TransportParameters(_)),
            "最初のフレームが QX_TRANSPORT_PARAMETERS であること"
        );

        // 2 つ目の Record で CONNECTION_CLOSE が送られる
        buf.clear();
        assert!(
            client
                .poll_transmit(&mut buf, NOW)
                .expect("送信に失敗しない")
        );
        decoder.extend(&buf);
        let record = decoder
            .poll_record()
            .expect("Record を取り出せる")
            .expect("Record が完成すること");
        let frames =
            shiguredo_qmux::frame::decode_record(&record).expect("フレームをデコードできる");
        assert!(
            matches!(frames[0], QmuxFrame::Quic(Frame::ConnectionClose(_))),
            "2 つ目の Record が CONNECTION_CLOSE であること"
        );
    }
}

mod protocol_violation {
    use super::*;

    #[test]
    fn prohibited_frame_closes_connection() {
        // Section 4: 禁止フレームの受信は FRAME_ENCODING_ERROR
        let mut pair = Pair::new();
        pair.handshake();

        // PING フレーム (0x01) は QMux で禁止されている (Section 4)
        let ping = [frame::frame_type::PING as u8];
        inject(&mut pair.server, &ping);

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::FRAME_ENCODING_ERROR,
        );
        // ローカルで検出したエラーは CONNECTION_CLOSE としてピアへ届く
        pair.transfer();
        match close_reason(&mut pair.client) {
            Some(CloseReason::Peer { error_code, .. }) => assert_eq!(
                error_code,
                TransportErrorCode::FRAME_ENCODING_ERROR.as_u64(),
                "ピアへ同じエラーコードが届くこと"
            ),
            other => panic!("ピアの切断を期待したが {other:?} だった"),
        }
    }

    #[test]
    fn out_of_order_stream_offset_closes_connection() {
        // Section 4.1: 直前のペイロードに連続しないオフセットは PROTOCOL_VIOLATION
        let mut pair = Pair::new();
        pair.handshake();

        let stream_frame = Frame::Stream(frame::stream::Stream {
            stream_id: 0,
            offset: 1,
            fin: false,
            data: b"x".to_vec(),
        });
        inject(&mut pair.server, &encode_frame(&stream_frame));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }

    #[test]
    fn stream_flow_control_violation_closes_connection() {
        // RFC 9000 Section 4.1: 広告した上限を超えるデータは FLOW_CONTROL_ERROR
        let mut server_config = config();
        server_config.initial_max_stream_data_bidi_remote = 4;
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        let stream_frame = Frame::Stream(frame::stream::Stream {
            stream_id: 0,
            offset: 0,
            fin: false,
            data: b"0123456789".to_vec(),
        });
        inject(&mut pair.server, &encode_frame(&stream_frame));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::FLOW_CONTROL_ERROR,
        );
    }

    #[test]
    fn truncated_frame_in_record_closes_connection() {
        // Section 3.2: Record の終端でフレームが途切れた場合は FRAME_ENCODING_ERROR
        let mut pair = Pair::new();
        pair.handshake();

        // LEN ビット付き STREAM で長さ 5 に対して 2 バイトしかない
        let mut frames = Vec::new();
        VarInt::from(0x0au8).encode_to_vec(&mut frames);
        VarInt::from(0u8).encode_to_vec(&mut frames);
        VarInt::from(5u8).encode_to_vec(&mut frames);
        frames.extend_from_slice(b"ab");
        inject(&mut pair.server, &frames);

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::FRAME_ENCODING_ERROR,
        );
    }

    #[test]
    fn empty_record_closes_connection() {
        // Section 3.2: Frames は 1 つ以上のフレームを含む
        let mut server =
            Connection::new(Role::Server, config()).expect("テスト用の接続を作成できる");
        server.receive(&[0x00], NOW);

        assert_local_error(
            close_reason(&mut server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }

    #[test]
    fn record_larger_than_advertised_max_closes_connection() {
        // Section 5.2: 広告した max_record_size を超える Record は FRAME_ENCODING_ERROR
        let mut server =
            Connection::new(Role::Server, config()).expect("テスト用の接続を作成できる");
        let mut buf = Vec::new();
        VarInt::try_from(DEFAULT_MAX_RECORD_SIZE + 1)
            .expect("可変長整数に収まる")
            .encode_to_vec(&mut buf);
        server.receive(&buf, NOW);

        assert_local_error(
            close_reason(&mut server),
            TransportErrorCode::FRAME_ENCODING_ERROR,
        );
    }
}

mod bounds {
    //! 境界条件と、ストリームの破棄・暗黙作成の検証。
    //!
    //! - 受信した DATAGRAM の上限 (RFC 9221 Section 3)
    //! - 下位番号ストリームの暗黙作成 (RFC 9000 Section 3.2)
    //! - 終端したストリームの破棄と、破棄後のフレームの扱い (RFC 9000 Section 3.2)
    //! - 送信専用ストリームへの STREAM_DATA_BLOCKED (RFC 9000 Section 19.13)
    //! - 2^60 を超える STREAMS_BLOCKED (RFC 9000 Section 19.14)
    //! - 空の FIN による読み取り可能の通知 (RFC 9000 Section 19.8)
    //! - 最終サイズを超えるデータと RESET_STREAM (RFC 9000 Section 4.5)

    use super::*;

    #[test]
    fn datagram_larger_than_advertised_size_closes_connection() {
        let mut server_config = config();
        server_config.max_datagram_frame_size = Some(8);
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        // フレーム全体は 1 (タイプ) + 2 (長さ) + 32 (データ) = 35 バイト
        let datagram = Frame::Datagram(frame::datagram::Datagram {
            data: vec![0u8; 32],
        });
        inject(&mut pair.server, &encode_frame(&datagram));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }

    #[test]
    fn datagram_with_zero_advertised_size_closes_connection() {
        // RFC 9221 Section 3: max_datagram_frame_size の 0 は非サポートを表す
        let mut server_config = config();
        server_config.max_datagram_frame_size = Some(0);
        let mut pair = Pair::with_config(config(), server_config);
        pair.handshake();

        let datagram = Frame::Datagram(frame::datagram::Datagram {
            data: b"x".to_vec(),
        });
        inject(&mut pair.server, &encode_frame(&datagram));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::PROTOCOL_VIOLATION,
        );
    }

    #[test]
    fn max_stream_data_creates_lower_numbered_streams() {
        // RFC 9000 Section 3.2: 下位番号のストリームをすべて作成しなければならない
        let mut client_config = config();
        client_config.initial_max_stream_data_bidi_local = 0;
        let mut pair = Pair::with_config(client_config, config());
        pair.handshake();

        // シーケンス番号 1 のクライアント起動双方向ストリーム (ID = 4)
        let max_stream_data = Frame::MaxStreamData(frame::max_stream_data::MaxStreamData {
            stream_id: 4,
            maximum_stream_data: 1000,
        });
        inject(&mut pair.server, &encode_frame(&max_stream_data));

        let events = drain(&mut pair.server);
        let lower = StreamId::new(0).expect("ストリーム ID を作れる");
        let upper = StreamId::new(4).expect("ストリーム ID を作れる");
        assert!(
            events.contains(&Event::StreamOpened { id: lower }),
            "下位番号のストリームも作成されること: {events:?}"
        );
        assert!(
            events.contains(&Event::StreamOpened { id: upper }),
            "対象のストリームも作成されること: {events:?}"
        );
        assert_eq!(
            pair.server
                .write(upper, b"credit", false)
                .expect("書き込める"),
            6,
            "受信したクレジットの範囲で書き込めること"
        );
    }

    #[test]
    fn send_only_stream_rejects_stream_data_blocked() {
        // RFC 9000 Section 19.13: 送信専用ストリームへの STREAM_DATA_BLOCKED は
        // STREAM_STATE_ERROR
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_uni().expect("単方向ストリームを開ける");
        let blocked = Frame::StreamDataBlocked(frame::stream_data_blocked::StreamDataBlocked {
            stream_id: id.as_u64(),
            maximum_stream_data: 0,
        });
        inject(&mut pair.client, &encode_frame(&blocked));

        assert_local_error(
            close_reason(&mut pair.client),
            TransportErrorCode::STREAM_STATE_ERROR,
        );
    }

    #[test]
    fn streams_blocked_above_2_60_closes_connection() {
        // RFC 9000 Section 19.14: 2^60 を超える上限は FRAME_ENCODING_ERROR
        let mut pair = Pair::new();
        pair.handshake();

        let blocked = Frame::StreamsBlocked(frame::streams_blocked::StreamsBlocked {
            bidirectional: true,
            maximum_streams: (1 << 60) + 1,
        });
        inject(&mut pair.server, &encode_frame(&blocked));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::FRAME_ENCODING_ERROR,
        );
    }

    #[test]
    fn empty_fin_notifies_readable() {
        // データを持たない FIN でも、アプリケーションは終端を観測できること
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        assert_eq!(
            pair.client.write(id, &[], true).expect("FIN を送れる"),
            0,
            "FIN だけのフレームはデータを受け付けないこと"
        );
        pair.transfer();

        let events = drain(&mut pair.server);
        assert!(
            events.contains(&Event::StreamReadable { id }),
            "空の FIN でも読み取り可能を通知すること: {events:?}"
        );
        match pair.server.read(id, &mut [0u8; 8]).expect("読み取れる") {
            ReadResult::Data { len, fin } => {
                assert_eq!(len, 0, "データはないこと");
                assert!(fin, "FIN まで到達すること");
            }
            other => panic!("データを期待したが {other:?} だった"),
        }
    }

    #[test]
    fn max_streams_notifies_streams_available() {
        // ピアが上限を増やしたことをアプリケーションが観測できること
        let mut pair = Pair::new();
        pair.handshake();

        let max_streams = Frame::MaxStreams(frame::max_streams::MaxStreams {
            bidirectional: true,
            maximum_streams: 16,
        });
        inject(&mut pair.client, &encode_frame(&max_streams));

        let events = drain(&mut pair.client);
        assert!(
            events.contains(&Event::StreamsAvailable {
                bidirectional: true
            }),
            "上限が増えたことを通知すること: {events:?}"
        );
    }

    #[test]
    fn stream_limit_is_recycled_after_stream_finishes() {
        // 終端したストリームの分だけ上限を返却し、ピアが次のストリームを開けること
        let mut client_config = config();
        client_config.initial_max_streams_bidi = 1;
        let mut pair = Pair::with_config(client_config, config());
        pair.handshake();

        for round in 0..4u8 {
            // サーバーが双方向ストリームを開いてデータを送る
            let id = pair.server.open_bidi().expect("ストリームを開ける");
            pair.server
                .write(id, &[round; 4], true)
                .expect("書き込める");
            pair.transfer();

            let (data, fin) = read_all(&mut pair.client, id);
            assert_eq!(data, vec![round; 4], "データが届くこと");
            assert!(fin, "FIN まで到達すること");

            // クライアントも送信側を終端するとストリームが破棄され、
            // MAX_STREAMS で上限が返却される
            pair.client.write(id, &[], true).expect("FIN を返せる");
            pair.transfer();
            drain(&mut pair.client);
            drain(&mut pair.server);
        }
    }

    #[test]
    fn frames_for_removed_stream_are_discarded() {
        // 終端して破棄したストリームへのフレームで接続を閉じないこと
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"data", true).expect("書き込める");
        pair.transfer();
        let (data, fin) = read_all(&mut pair.server, id);
        assert_eq!(data, b"data", "データが届くこと");
        assert!(fin, "FIN まで到達すること");

        // サーバーも送信側を終端すると、双方向とも終端してストリームが破棄される
        pair.server.write(id, &[], true).expect("FIN を返せる");
        assert!(!pair.server.is_closed(), "接続は閉じないこと");

        // 破棄したストリームへのフレームを直接受け取らせてもエラーにしない
        let duplicate = Frame::Stream(frame::stream::Stream {
            stream_id: id.as_u64(),
            offset: 0,
            fin: true,
            data: b"data".to_vec(),
        });
        inject(&mut pair.server, &encode_frame(&duplicate));
        assert!(
            !pair.server.is_closed(),
            "破棄したストリームへのフレームで接続を閉じないこと"
        );
    }

    #[test]
    fn read_after_stream_finishes_returns_eof() {
        // 終端して破棄したストリームへの読み取りは EOF を返すこと
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"data", true).expect("書き込める");
        pair.transfer();
        let (data, fin) = read_all(&mut pair.server, id);
        assert_eq!(data, b"data", "データが届くこと");
        assert!(fin, "FIN まで到達すること");

        pair.server.write(id, &[], true).expect("FIN を返せる");
        match pair.server.read(id, &mut [0u8; 8]).expect("読み取れる") {
            ReadResult::Data { len, fin } => {
                assert_eq!(len, 0, "データはないこと");
                assert!(fin, "終端していること");
            }
            other => panic!("データを期待したが {other:?} だった"),
        }
    }

    #[test]
    fn data_beyond_final_size_closes_connection() {
        // RFC 9000 Section 4.5: 最終サイズを超えるデータは FINAL_SIZE_ERROR
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"abc", true).expect("書き込める");
        pair.transfer();

        let stream_frame = Frame::Stream(frame::stream::Stream {
            stream_id: id.as_u64(),
            offset: 3,
            fin: false,
            data: b"x".to_vec(),
        });
        inject(&mut pair.server, &encode_frame(&stream_frame));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::FINAL_SIZE_ERROR,
        );
    }

    #[test]
    fn reset_stream_below_received_offset_closes_connection() {
        // RFC 9000 Section 4.5: 受信済みのオフセットより小さい最終サイズは
        // FINAL_SIZE_ERROR
        let mut pair = Pair::new();
        pair.handshake();

        let id = pair.client.open_bidi().expect("ストリームを開ける");
        pair.client.write(id, b"abcdef", false).expect("書き込める");
        pair.transfer();

        let reset = Frame::ResetStream(frame::reset_stream::ResetStream {
            stream_id: id.as_u64(),
            application_protocol_error_code: 1,
            final_size: 3,
        });
        inject(&mut pair.server, &encode_frame(&reset));

        assert_local_error(
            close_reason(&mut pair.server),
            TransportErrorCode::FINAL_SIZE_ERROR,
        );
    }
}
