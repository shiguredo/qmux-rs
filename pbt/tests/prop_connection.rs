//! QMux 接続のプロパティベーステスト
//!
//! draft-ietf-quic-qmux-02 のハンドシェイク (Section 4.2) と、確立後の双方向
//! ストリーム転送について、任意のペイロードと任意の分割で成立することを検証する。
//! Sans I/O のため、下位トランスポートの代わりに 2 つの [`Connection`] 間で
//! バイト列を受け渡す。

use pbt::common::sample_nonempty_bytes;
use pbt::common::test_runner;
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Timestamp;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection;
use shiguredo_qmux::event::Event;
use shiguredo_qmux::stream::ReadResult;

/// ペイロード長の上限
const MAX_PAYLOAD_LEN: usize = 8192;

/// フロー制御のクレジット (ペイロード長の上限を十分に上回る値)
const FLOW_CONTROL_CREDIT: u64 = 1 << 20;

/// ピアが同時に開けるストリーム数
const STREAM_LIMIT: u64 = 8;

/// 読み取り 1 回あたりのバッファ長
const READ_BUFFER_LEN: usize = 1024;

/// ストリームとフロー制御を使える設定を作る
///
/// 転送するペイロードがフロー制御で止まらないよう、接続レベルとストリーム
/// レベルのクレジットを上限まで与え、ストリーム数を 1 以上にする。
fn test_config() -> Config {
    Config {
        initial_max_data: FLOW_CONTROL_CREDIT,
        initial_max_stream_data_bidi_local: FLOW_CONTROL_CREDIT,
        initial_max_stream_data_bidi_remote: FLOW_CONTROL_CREDIT,
        initial_max_stream_data_uni: FLOW_CONTROL_CREDIT,
        initial_max_streams_bidi: STREAM_LIMIT,
        initial_max_streams_uni: STREAM_LIMIT,
        ..Config::default()
    }
}

/// `1..=max` の長さを生成する
///
/// 残り全部 (1 回で送り切る) と 1 バイトは一様分布では出にくいため、境界値として
/// 混ぜる。転送と書き込みの両方の分割に使う。
fn sample_len_at_most(ctx: &mut noprop::TestCaseContext, max: usize) -> usize {
    noprop::sample_with_boundaries(ctx, &[1usize, max], noprop::Ratio::one_nth(4), |ctx| {
        noprop::sample_usize_in(ctx, 1..=max)
    })
}

/// `from` が送信する Record をすべて `to` へ届ける
///
/// poll_transmit は 1 回につき 1 つの Record を追記するため、毎回バッファを
/// clear して false が返るまで繰り返す。届けるバイト列はランダムな長さの chunk に
/// 分割し、1 つの Record が複数の receive 呼び出しにまたがる経路も通す。
fn pump(
    ctx: &mut noprop::TestCaseContext,
    from: &mut Connection,
    to: &mut Connection,
    now: Timestamp,
) {
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let transmitted = from
            .poll_transmit(&mut buf, now)
            .expect("送信する Record のエンコードに失敗した");
        if !transmitted {
            break;
        }
        assert!(!buf.is_empty(), "送信した Record が空である");

        let mut offset = 0;
        while offset < buf.len() {
            let chunk_len = sample_len_at_most(ctx, buf.len() - offset);
            to.receive(&buf[offset..offset + chunk_len], now);
            offset += chunk_len;
        }
    }
}

/// アプリケーション向けイベントをすべて取り出し、接続が閉じていないことを確認する
///
/// 転送のたびに呼び出し、ConnectionClosed が通知されていないことを検証する。
fn drain_events(conn: &mut Connection, side: &str) {
    while let Some(event) = conn.poll_event() {
        assert!(
            !matches!(event, Event::ConnectionClosed(_)),
            "{side} 側の接続が閉じられた: {event:?}"
        );
    }
}

/// 確立後の双方向ストリーム転送が任意のペイロードと分割で成立する
///
/// ペイロードを 1 回で書き切る経路と複数回に分ける経路の両方が現れることを
/// カバレッジゲートで確認する。
#[test]
fn bidirectional_stream_transfer() -> noprop::TestResult {
    let single_write_seen = std::cell::Cell::new(false);
    let chunked_write_seen = std::cell::Cell::new(false);
    let mut runner = test_runner()?;
    runner.run(128, |ctx| {
        let payload = sample_nonempty_bytes(ctx, MAX_PAYLOAD_LEN);

        let now = Timestamp::from_micros(0);
        let mut client =
            Connection::new(Role::Client, test_config()).expect("テスト用の接続を作成できる");
        let mut server =
            Connection::new(Role::Server, test_config()).expect("テスト用の接続を作成できる");

        // ハンドシェイク (Section 4.2): QX_TRANSPORT_PARAMETERS を双方向で交換する。
        // 1 往復で両端がピアのトランスポートパラメータを受信して確立する。
        for _ in 0..2 {
            pump(ctx, &mut client, &mut server, now);
            pump(ctx, &mut server, &mut client, now);
            drain_events(&mut client, "client");
            drain_events(&mut server, "server");
        }
        assert!(
            client.peer_transport_parameters().is_some(),
            "client がピアのトランスポートパラメータを受信していない"
        );
        assert!(
            server.peer_transport_parameters().is_some(),
            "server がピアのトランスポートパラメータを受信していない"
        );
        assert_eq!(
            client.peer_transport_parameters(),
            Some(server.local_transport_parameters()),
            "client が受信したトランスポートパラメータが server の送信内容と一致しない"
        );
        assert_eq!(
            server.peer_transport_parameters(),
            Some(client.local_transport_parameters()),
            "server が受信したトランスポートパラメータが client の送信内容と一致しない"
        );

        // クライアントが双方向ストリームを開き、ランダムな分割で書き込む。
        // 残り全部を 1 回で書き込む経路と、複数回に分ける経路の両方を通す。
        // 書き込みのたびに転送するため、複数の Record にまたがる経路も通る。
        // ピアが広告した上限とクレジットの照会 API がトランスポートパラメータと
        // 一致する (RFC 9000 Section 18.2)
        let peer = server.local_transport_parameters();
        assert_eq!(
            client.peer_max_streams_bidi(),
            peer.initial_max_streams_bidi,
            "ピアの双方向ストリーム上限を正しく照会できない"
        );
        assert_eq!(
            client.peer_max_streams_uni(),
            peer.initial_max_streams_uni,
            "ピアの単方向ストリーム上限を正しく照会できない"
        );
        assert_eq!(
            client.send_credit(),
            peer.initial_max_data,
            "接続レベルの送信クレジットを正しく照会できない"
        );

        let id = client
            .open_bidi()
            .expect("ピアのトランスポートパラメータを受信済みなのでストリームを開ける");
        assert_eq!(
            client
                .stream_send_credit(id)
                .expect("送信側が未終端なので残量を取得できる"),
            peer.initial_max_stream_data_bidi_remote,
            "ストリーム単位の送信クレジットを正しく照会できない"
        );
        let mut written = 0;
        let mut write_calls = 0;
        while written < payload.len() {
            let chunk_len = sample_len_at_most(ctx, payload.len() - written);
            // FIN は渡したデータをすべて受け付けた場合にのみ送信されるため、
            // ペイロードの最後に届く書き込みでのみ true を渡す
            let end = written + chunk_len;
            let accepted = client
                .write(id, &payload[written..end], end == payload.len())
                .expect("開いたばかりの双方向ストリームなので書き込める");
            assert!(
                accepted > 0,
                "フロー制御のクレジットが十分なのに書き込みが受け付けられなかった"
            );
            written += accepted;
            write_calls += 1;

            pump(ctx, &mut client, &mut server, now);
            drain_events(&mut client, "client");
            drain_events(&mut server, "server");
        }
        if write_calls == 1 {
            single_write_seen.set(true);
        } else {
            chunked_write_seen.set(true);
        }

        // サーバーが読み取り、ペイロードと FIN を確認する
        let mut received = Vec::new();
        let mut fin = false;
        for _ in 0..(MAX_PAYLOAD_LEN + 8) {
            let mut buf = [0u8; READ_BUFFER_LEN];
            match server
                .read(id, &mut buf)
                .expect("ピア起動の双方向ストリームなので読み取れる")
            {
                ReadResult::Data { len, fin: read_fin } => {
                    received.extend_from_slice(&buf[..len]);
                    if read_fin {
                        fin = true;
                        break;
                    }
                }
                ReadResult::Reset { error_code } => {
                    panic!("ピアがストリームをリセットした: error_code={error_code}")
                }
            }

            // 読み取りで発生した送信待ちフレーム (MAX_STREAM_DATA / MAX_DATA) を
            // 相手へ届け、残りのデータが届いていない場合は次の転送に進む
            pump(ctx, &mut client, &mut server, now);
            pump(ctx, &mut server, &mut client, now);
            drain_events(&mut client, "client");
            drain_events(&mut server, "server");
        }

        assert!(
            fin,
            "FIN まで読み取れなかった: received={} payload={}",
            received.len(),
            payload.len()
        );
        assert_eq!(
            received, payload,
            "受信したバイト列が送信したペイロードと一致しない"
        );
        Ok(())
    })?;

    // 書き込みの分割の両方の分岐が観測されたことを確認する
    assert!(
        single_write_seen.get(),
        "ペイロードを 1 回で書き切るケースが 1 度も観測されなかった\n{runner}"
    );
    assert!(
        chunked_write_seen.get(),
        "ペイロードを複数回に分けて書き込むケースが 1 度も観測されなかった\n{runner}"
    );
    Ok(())
}
