//! QMux フレームのエンコード/デコードの単体テスト。
//!
//! QMux で禁止されたフレーム、利用が許可されていないフレーム、フレームタイプの
//! 非最短エンコード、Record の終端で途切れたフレームといったエラー経路を検証する。
//! 正常系のラウンドトリップは PBT (`pbt/tests/prop_frame.rs`) が担う。

use shiguredo_qmux::codec::error::{Error, TransportErrorCode};
use shiguredo_qmux::codec::frame::connection_close::ConnectionClose;
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
fn iana_registered_frame_type_values_match_specification() {
    // draft-ietf-quic-qmux-02 Section 13.1 と RFC 9221 Section 4 の IANA 登録値を
    // 数値リテラルで固定する。定数の数値が誤っていても、定数シンボル同士の比較では
    // 検出できないため、相互接続を壊す変更をここで検出する。
    assert_eq!(
        frame_type::QX_TRANSPORT_PARAMETERS,
        0x3f5153300d0a0d0a,
        "QX_TRANSPORT_PARAMETERS の登録値が一致すること (Section 13.1)"
    );
    assert_eq!(
        frame_type::QX_PING,
        0x348c67529ef8c7bd,
        "QX_PING の登録値が一致すること (Section 13.1)"
    );
    assert_eq!(
        frame_type::QX_PING_ACK,
        0x348c67529ef8c7be,
        "QX_PING_ACK の登録値が一致すること (Section 13.1)"
    );
    assert_eq!(
        frame::frame_type::DATAGRAM,
        0x30,
        "DATAGRAM の登録値が一致すること (RFC 9221 Section 4)"
    );
    assert_eq!(
        frame::frame_type::DATAGRAM_LEN,
        0x31,
        "DATAGRAM_LEN の登録値が一致すること (RFC 9221 Section 4)"
    );
}

#[test]
fn connection_close_without_frame_type_encodes_unknown_as_zero() {
    // RFC 9000 Section 19.19: 0x1c (トランスポートエラー) の Frame Type フィールドは
    // 必須で、エラーを引き起こしたフレームタイプが不明な場合は 0 を送る。
    // frame_type が None でもフィールドを省略してはならない。
    let close = ConnectionClose {
        application: false,
        error_code: 0,
        frame_type: None,
        reason_phrase: Vec::new(),
    };
    let mut buf = Vec::new();
    Frame::ConnectionClose(close).encode(&mut buf);
    assert_eq!(
        buf,
        vec![0x1c, 0x00, 0x00, 0x00],
        "0x1c は未知を表す 0 の Frame Type を明示すること"
    );

    let (decoded, size) = decode(&buf).expect("エンコード直後のバイト列なのでデコードできる");
    assert_eq!(size, buf.len(), "消費バイト数がエンコード長と一致すること");
    let QmuxFrame::Quic(Frame::ConnectionClose(decoded)) = decoded else {
        panic!("CONNECTION_CLOSE フレームを期待した");
    };
    assert_eq!(
        decoded.frame_type,
        Some(0),
        "0x1c の Frame Type は 0 としてデコードされること"
    );
}

#[test]
fn application_close_omits_frame_type() {
    // RFC 9000 Section 19.19: 0x1d (アプリケーションエラー) は Frame Type フィールドを
    // 持たない。構造体に frame_type が入っていても出力してはならない。
    let close = ConnectionClose {
        application: true,
        error_code: 1,
        frame_type: Some(0x08),
        reason_phrase: Vec::new(),
    };
    let mut buf = Vec::new();
    Frame::ConnectionClose(close).encode(&mut buf);
    assert_eq!(
        buf,
        vec![0x1d, 0x01, 0x00],
        "0x1d は Frame Type を送らないこと"
    );

    let (decoded, size) = decode(&buf).expect("エンコード直後のバイト列なのでデコードできる");
    assert_eq!(size, buf.len(), "消費バイト数がエンコード長と一致すること");
    let QmuxFrame::Quic(Frame::ConnectionClose(decoded)) = decoded else {
        panic!("CONNECTION_CLOSE フレームを期待した");
    };
    assert_eq!(
        decoded.frame_type, None,
        "0x1d の Frame Type は存在しないこと"
    );
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
fn decode_returns_incomplete_for_truncated_frame() {
    // 公開 frame::decode は Record の境界を知らないため、フレーム全体が揃って
    // いなければ Error::Incomplete を返す。Record 境界では呼び出し側が
    // FRAME_ENCODING_ERROR へ変換する契約であり、decode_record は変換済みの
    // エラーを返す。
    let mut buf = Vec::new();
    // LEN ビット付き STREAM で、長さ 5 に対して 2 バイトしかない
    VarInt::from(0x0au8).encode_to_vec(&mut buf);
    VarInt::from(0u8).encode_to_vec(&mut buf);
    VarInt::from(5u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(b"ab");

    let error = decode(&buf).expect_err("途中のフレームは Incomplete になる");
    match error {
        Error::Incomplete { needed } => {
            assert_eq!(needed, 3, "揃うまでに必要なバイト数が返ること");
        }
        other => panic!("Incomplete を期待したが {other:?} だった"),
    }

    // 同じバイト列を Record としてデコードすると FRAME_ENCODING_ERROR になる
    assert_protocol_error(
        decode_record(&buf).expect_err("Record 境界で途切れたフレームは拒否される"),
        TransportErrorCode::FRAME_ENCODING_ERROR,
    );
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
