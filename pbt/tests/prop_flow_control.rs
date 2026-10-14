//! QMux のフロー制御のプロパティベーステスト
//!
//! 接続レベルとストリームレベルのクレジットを小さくしたときに、送信側が
//! 一部だけを受け付けて残りを再送する経路を通っても、データが欠けずに
//! FIN まで届くことを検証する (RFC 9000 Section 4.1)。
//!
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
const MAX_PAYLOAD_LEN: usize = 1024;

/// クレジットの上限 (クレジットが小さいほど分割送信が起きやすい)
const MAX_CREDIT: u64 = 32;

/// 読み取り 1 回あたりのバッファ長
const READ_BUFFER_LEN: usize = 256;

/// 無進展による無限ループを避けるための繰り返し回数の上限
const MAX_STEPS: usize = 10_000;

/// ストリームとフロー制御を有効にした基準の設定
fn base_config() -> Config {
    Config {
        initial_max_data: 1 << 20,
        initial_max_stream_data_bidi_local: 1 << 20,
        initial_max_stream_data_bidi_remote: 1 << 20,
        initial_max_stream_data_uni: 1 << 20,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        ..Config::default()
    }
}

/// `1..=MAX_CREDIT` のクレジットを生成する
///
/// 最小値 1 は一様分布では出にくいため境界値として混ぜる。0 はクレジットが
/// 恒久的に 0 のままで前進できない設定なので生成しない。
fn sample_credit(ctx: &mut noprop::TestCaseContext) -> u64 {
    noprop::sample_with_boundaries(ctx, &[1u64, MAX_CREDIT], noprop::Ratio::one_nth(4), |ctx| {
        noprop::sample_u64_in(ctx, 1..=MAX_CREDIT)
    })
}

/// `1..=max` の長さを生成する
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
fn drain_events(conn: &mut Connection, side: &str) {
    while let Some(event) = conn.poll_event() {
        assert!(
            !matches!(event, Event::ConnectionClosed(_)),
            "{side} 側の接続が閉じられた: {event:?}"
        );
    }
}

/// クレジットを小さくしても、任意のペイロードが欠けずに FIN まで届く
#[test]
fn flow_control_preserves_stream_data() -> noprop::TestResult {
    let mut runner = test_runner()?;
    runner.run(64, |ctx| {
        let payload = sample_nonempty_bytes(ctx, MAX_PAYLOAD_LEN);
        let stream_credit = sample_credit(ctx);
        let conn_credit = sample_credit(ctx);

        // ピアのクレジットが送信側の制限になる (RFC 9000 Section 18.2)
        let mut server_config = base_config();
        server_config.initial_max_stream_data_bidi_remote = stream_credit;
        server_config.initial_max_data = conn_credit;

        let now = Timestamp::from_micros(0);
        let mut client =
            Connection::new(Role::Client, base_config()).expect("テスト用の接続を作成できる");
        let mut server =
            Connection::new(Role::Server, server_config).expect("テスト用の接続を作成できる");

        // ハンドシェイク (Section 4.2)
        for _ in 0..2 {
            pump(ctx, &mut client, &mut server, now);
            pump(ctx, &mut server, &mut client, now);
            drain_events(&mut client, "client");
            drain_events(&mut server, "server");
        }

        let id = client
            .open_bidi()
            .expect("ピアのトランスポートパラメータを受信済みなのでストリームを開ける");

        let mut offset = 0;
        let mut received = Vec::new();
        let mut fin_received = false;
        for _ in 0..MAX_STEPS {
            // データを書き込み、送り終えたら FIN だけを送る。FIN は渡したデータを
            // すべて受け付けた場合にのみ送られるため、残りがあるうちは false を渡す。
            if offset < payload.len() {
                let accepted = client
                    .write(id, &payload[offset..], false)
                    .expect("開いたばかりの双方向ストリームなので書き込める");
                offset += accepted;
            } else {
                client.write(id, &[], true).expect("FIN を送れる");
            }

            // 相手に届け、クレジットを返させる
            pump(ctx, &mut client, &mut server, now);
            pump(ctx, &mut server, &mut client, now);
            drain_events(&mut client, "client");
            drain_events(&mut server, "server");

            // サーバーで読めるだけ読む
            loop {
                let mut buf = [0u8; READ_BUFFER_LEN];
                match server
                    .read(id, &mut buf)
                    .expect("ピア起動の双方向ストリームなので読み取れる")
                {
                    ReadResult::Data { len, fin } => {
                        received.extend_from_slice(&buf[..len]);
                        if fin {
                            fin_received = true;
                            break;
                        }
                        // 読めるデータが尽きたら次の書き込みへ進む
                        if len == 0 {
                            break;
                        }
                    }
                    ReadResult::Reset { error_code } => {
                        panic!("ピアがストリームをリセットした: error_code={error_code}")
                    }
                }
            }

            if fin_received {
                break;
            }
        }

        // FIN を受信したことを確認するため、残りの送信フレームを届ける
        pump(ctx, &mut client, &mut server, now);
        pump(ctx, &mut server, &mut client, now);
        drain_events(&mut server, "server");

        assert!(
            fin_received,
            "FIN まで読み取れなかった: received={} payload={}",
            received.len(),
            payload.len()
        );
        assert_eq!(
            received, payload,
            "フロー制御で分割されてもデータが欠けないこと"
        );
        Ok(())
    })?;
    Ok(())
}
