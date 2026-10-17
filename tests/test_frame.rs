//! QMux フレームのエンコード/デコードの単体テスト。
//!
//! QMux で禁止されたフレーム、利用が許可されていないフレーム、フレームタイプの
//! 非最短エンコード、Record の終端で途切れたフレームといったエラー経路を検証する。
//! 正常系のラウンドトリップは PBT (`pbt/tests/prop_frame.rs`) が担う。

use shiguredo_qmux::codec::error::{Error, TransportErrorCode};
use shiguredo_qmux::codec::frame::{self, Frame};
use shiguredo_qmux::codec::varint::VarInt;
use shiguredo_qmux::frame::{QmuxFrame, decode, decode_record, frame_type};

/// プロトコルエラーとそのコードを確認する
fn assert_protocol_error(error: Error, expected: TransportErrorCode) {
    match error {
        Error::Protocol { code, reason } => {
            assert_eq!(code, expected, "エラーコードが一致すること: {reason}");
        }
        other => panic!("プロトコルエラーを期待したが {other:?} だった"),
    }
}

/// フレームタイプだけを持つフレームを組み立てる
///
/// 禁止フレームの判定はフレームタイプだけで決まるため、本体は空にする。
fn frame_with_type(frame_type: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    VarInt::try_from(frame_type)
        .expect("テスト用のフレームタイプは可変長整数に収まる")
        .encode_to_vec(&mut buf);
    buf
}

#[test]
fn transport_parameters_length_exceeding_record_is_rejected() {
    // Length が Record の残りを超える場合は FRAME_ENCODING_ERROR (Section 3.2)
    let mut buf = Vec::new();
    VarInt::try_from(frame_type::QX_TRANSPORT_PARAMETERS)
        .expect("フレームタイプは可変長整数に収まる")
        .encode_to_vec(&mut buf);
    VarInt::from(5u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(&[0x00, 0x01]);

    assert_protocol_error(
        decode(&buf).expect_err("長さが足りないフレームは拒否される"),
        TransportErrorCode::FRAME_ENCODING_ERROR,
    );
}

#[test]
fn stream_frame_without_length_consumes_rest() {
    // LEN ビットなしの STREAM は Record の残りすべてをデータとして使う (Section 3.2)
    let mut buf = Vec::new();
    VarInt::from(frame::frame_type::STREAM_START as u8).encode_to_vec(&mut buf);
    VarInt::from(4u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(b"data");

    let (decoded, size) = decode(&buf).expect("デコードに失敗しない");
    assert_eq!(size, buf.len(), "Record の残りすべてを消費すること");
    let QmuxFrame::Quic(Frame::Stream(stream)) = decoded else {
        panic!("STREAM フレームを期待した");
    };
    assert_eq!(stream.stream_id, 4, "ストリーム ID が一致すること");
    assert_eq!(stream.data, b"data", "データが一致すること");
    assert!(!stream.fin, "FIN は付いていないこと");
}

#[test]
fn datagram_without_length_consumes_rest() {
    let mut buf = Vec::new();
    VarInt::from(frame::frame_type::DATAGRAM as u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(b"payload");

    let (decoded, size) = decode(&buf).expect("デコードに失敗しない");
    assert_eq!(size, buf.len(), "Record の残りすべてを消費すること");
    let QmuxFrame::Quic(Frame::Datagram(datagram)) = decoded else {
        panic!("DATAGRAM フレームを期待した");
    };
    assert_eq!(datagram.data, b"payload", "データが一致すること");
}

#[test]
fn prohibited_frames_are_rejected() {
    // Section 4: 禁止フレームの受信は FRAME_ENCODING_ERROR
    let prohibited = [
        frame::frame_type::PING,
        frame::frame_type::ACK,
        frame::frame_type::ACK_ECN,
        frame::frame_type::CRYPTO,
        frame::frame_type::NEW_TOKEN,
        frame::frame_type::NEW_CONNECTION_ID,
        frame::frame_type::RETIRE_CONNECTION_ID,
        frame::frame_type::PATH_CHALLENGE,
        frame::frame_type::PATH_RESPONSE,
        frame::frame_type::HANDSHAKE_DONE,
    ];
    for frame_type in prohibited {
        let buf = frame_with_type(frame_type);
        let error = decode(&buf).expect_err("禁止フレームは拒否される");
        assert_protocol_error(error, TransportErrorCode::FRAME_ENCODING_ERROR);
    }
}

#[test]
fn unknown_and_unsupported_frames_are_rejected() {
    // Section 4 と RFC 9000 Section 12.4: 未知のフレームタイプは FRAME_ENCODING_ERROR。
    // RESET_STREAM_AT は本実装がネゴシエートしない拡張のフレーム (値は暫定)。
    for ft in [frame::frame_type::RESET_STREAM_AT, 0x3e75, 0x3f51, 0x1234] {
        let buf = frame_with_type(ft);
        let error = decode(&buf).expect_err("未知のフレームタイプは拒否される");
        assert_protocol_error(error, TransportErrorCode::FRAME_ENCODING_ERROR);
    }
}

#[test]
fn non_minimal_frame_type_is_rejected() {
    // RFC 9000 Section 12.4: フレームタイプは最短の可変長整数でなければならない。
    // 0x40 0x00 は値 0 (PADDING) の 2 バイト表現。
    let error = decode(&[0x40, 0x00]).expect_err("非最短のフレームタイプは拒否される");
    assert_protocol_error(error, TransportErrorCode::PROTOCOL_VIOLATION);
}

#[test]
fn decode_record_rejects_truncated_frame() {
    // Record の終端がフレーム境界に一致しない場合は FRAME_ENCODING_ERROR (Section 3.2)
    let mut buf = Vec::new();
    // LEN ビット付き STREAM で、長さ 5 に対して 2 バイトしかない
    VarInt::from(0x0au8).encode_to_vec(&mut buf);
    VarInt::from(0u8).encode_to_vec(&mut buf);
    VarInt::from(5u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(b"ab");

    let error = decode_record(&buf).expect_err("途切れたフレームは拒否される");
    assert_protocol_error(error, TransportErrorCode::FRAME_ENCODING_ERROR);
}

#[test]
fn decode_record_rejects_empty_input() {
    // RFC 9000 Section 12.4: フレームを含まないペイロードは PROTOCOL_VIOLATION
    let error = decode_record(&[]).expect_err("空の Record は拒否される");
    assert_protocol_error(error, TransportErrorCode::PROTOCOL_VIOLATION);
}

#[test]
fn prohibited_and_unknown_classification() {
    // 判定関数が Section 4 の禁止リストと一致すること
    assert!(shiguredo_qmux::frame::is_prohibited(
        frame::frame_type::PING
    ));
    assert!(shiguredo_qmux::frame::is_prohibited(
        frame::frame_type::HANDSHAKE_DONE
    ));
    assert!(!shiguredo_qmux::frame::is_prohibited(
        frame::frame_type::STREAM_START
    ));
    assert!(shiguredo_qmux::frame::is_allowed(
        frame::frame_type::STREAM_END
    ));
    assert!(shiguredo_qmux::frame::is_allowed(
        frame::frame_type::DATAGRAM_LEN
    ));
    assert!(!shiguredo_qmux::frame::is_allowed(
        frame::frame_type::CRYPTO
    ));
    assert!(!shiguredo_qmux::frame::is_allowed(
        frame::frame_type::RESET_STREAM_AT
    ));
}
