#![no_main]

//! `Connection` に任意バイト列を投入する fuzz target
//!
//! QMux の受信経路 (Record のデコード、フレームのデコード、トランスポート
//! パラメータの適用、ストリーム処理、切断処理) に任意入力を流し、panic しない
//! ことを検証する。戻り値や状態遷移は検査しない (panic 耐性だけが目的)。
//!
//! 入力の先頭バイトでロールを切り替える。
//!
//! - 偶数: クライアント接続 (`Role::Client`)
//! - 奇数: サーバー接続 (`Role::Server`)
//!
//! 確立後の経路 (ストリーム・DATAGRAM) にも到達できるよう、自分の
//! QX_TRANSPORT_PARAMETERS を自分で受信させてから任意入力を流す。受信の後は
//! poll_transmit / poll_event / open_bidi / open_uni / write / read / reset /
//! stop_sending / send_datagram / ping / next_timeout / handle_timeout / close を
//! 呼び、どの経路でも panic しないことを確認する。

use libfuzzer_sys::fuzz_target;
use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::{Duration, Timestamp};
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection;
use shiguredo_qmux::stream::StreamId;

/// fuzz で使う設定
///
/// フロー制御のクレジット・ストリーム数・DATAGRAM を有効にして、確立後に
/// ストリームと DATAGRAM の経路へ到達できるようにする。
fn config() -> Config {
    Config {
        max_idle_timeout: Duration::from_millis(1000),
        initial_max_data: 65536,
        initial_max_stream_data_bidi_local: 65536,
        initial_max_stream_data_bidi_remote: 65536,
        initial_max_stream_data_uni: 65536,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        max_datagram_frame_size: Some(65536),
        ..Config::default()
    }
}

/// 送信待ちの Record とアプリケーション向けイベントをすべて取り出す
fn drain(conn: &mut Connection, now: Timestamp) {
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match conn.poll_transmit(&mut buf, now) {
            Ok(true) => {}
            // 送信待ちがなくなったか、送信できない状態になった
            Ok(false) | Err(_) => break,
        }
    }
    while conn.poll_event().is_some() {}
}

/// 接続を作り、任意入力を 1 つ流す
fn feed(role: Role, data: &[u8], now: Timestamp) {
    let mut conn = Connection::new(role, config()).expect("fuzz 用の接続を作成できる");

    // 自分のトランスポートパラメータを自分で受信させ、確立状態にしてから
    // 任意入力を流す。確立していないとストリームや DATAGRAM の経路に到達しない。
    let mut buf = Vec::new();
    if conn.poll_transmit(&mut buf, now).unwrap_or(false) {
        conn.receive(&buf, now);
    }
    drain(&mut conn, now);

    // 任意バイト列の受信
    conn.receive(data, now);
    drain(&mut conn, now);

    // ローカル起動ストリームの操作
    if let Ok(id) = conn.open_bidi() {
        let _ = conn.write(id, data, true);
        let mut read_buf = [0u8; 1024];
        let _ = conn.read(id, &mut read_buf);
        let _ = conn.stop_sending(id, 0);
        let _ = conn.reset(id, 0);
    }
    let _ = conn.open_uni();

    // 入力から作った任意のストリーム ID の操作 (未知のストリームの経路)
    if let Some(bytes) = data.get(..8) {
        let value = u64::from_be_bytes(bytes.try_into().expect("8 バイトなので変換できる"));
        if let Some(id) = StreamId::new(value) {
            let _ = conn.write(id, data, false);
            let mut read_buf = [0u8; 64];
            let _ = conn.read(id, &mut read_buf);
            let _ = conn.stop_sending(id, 0);
            let _ = conn.reset(id, 0);
        }
    }

    // DATAGRAM と QX_PING
    let _ = conn.send_datagram(data);
    let _ = conn.ping();

    // 送信待ちの Record をすべて書き出す
    drain(&mut conn, now);

    // アイドルタイムアウトの処理
    if let Some(deadline) = conn.next_timeout() {
        conn.handle_timeout(deadline);
    }

    // 切断と、CONNECTION_CLOSE を書き出す経路。
    // error_code 0 は可変長整数の範囲内なので close は成功する
    conn.close(0, "fuzz")
        .expect("error_code 0 は可変長整数の範囲内である");
    drain(&mut conn, now);
}

fuzz_target!(|data: &[u8]| {
    // 先頭バイトをロールの切り替えに使う
    let Some((&role, payload)) = data.split_first() else {
        return;
    };
    let now = Timestamp::from_micros(1_000_000);
    match role & 0x01 {
        0 => feed(Role::Client, payload, now),
        _ => feed(Role::Server, payload, now),
    }
});
