//! QMux トランスポートパラメータの単体テスト。
//!
//! 禁止パラメータの拒否、重複の検出、max_record_size の境界値といったエラー経路を
//! 検証する。正常系のラウンドトリップは PBT
//! (`pbt/tests/prop_transport_params.rs`) が担う。

use shiguredo_qmux::codec::error::{Error, TransportErrorCode};
use shiguredo_qmux::codec::varint::VarInt;
use shiguredo_qmux::record::DEFAULT_MAX_RECORD_SIZE;
use shiguredo_qmux::transport_params::param_id;
use shiguredo_qmux::transport_params::{MAX_RECORD_SIZE_ID, TransportParameters};

/// プロトコルエラーとそのコードを確認する
fn assert_protocol_error(error: Error, expected: TransportErrorCode) {
    match error {
        Error::Protocol { code, reason } => {
            assert_eq!(code, expected, "エラーコードが一致すること: {reason}");
        }
        other => panic!("プロトコルエラーを期待したが {other:?} だった"),
    }
}

/// 可変長整数の値を持つトランスポートパラメータを組み立てる
fn varint_param(id: u64, value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    VarInt::try_from(id)
        .expect("テスト用の ID は可変長整数に収まる")
        .encode_to_vec(&mut buf);
    let value = VarInt::try_from(value).expect("テスト用の値は可変長整数に収まる");
    VarInt::try_from(value.encoded_size() as u64)
        .expect("エンコード長は可変長整数に収まる")
        .encode_to_vec(&mut buf);
    value.encode_to_vec(&mut buf);
    buf
}

/// 長さ 0 の値を持つトランスポートパラメータを組み立てる
fn empty_param(id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    VarInt::try_from(id)
        .expect("テスト用の ID は可変長整数に収まる")
        .encode_to_vec(&mut buf);
    VarInt::from(0u8).encode_to_vec(&mut buf);
    buf
}

/// 任意のバイト列を値に持つトランスポートパラメータを組み立てる
fn bytes_param(id: u64, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    VarInt::try_from(id)
        .expect("テスト用の ID は可変長整数に収まる")
        .encode_to_vec(&mut buf);
    VarInt::try_from(value.len() as u64)
        .expect("テスト用の長さは可変長整数に収まる")
        .encode_to_vec(&mut buf);
    buf.extend_from_slice(value);
    buf
}

#[test]
fn defaults_when_no_parameter_is_present() {
    let params = TransportParameters::decode(&[]).expect("空のパラメータは有効");
    assert_eq!(
        params,
        TransportParameters::default(),
        "値がない場合は既定値になること"
    );
    assert_eq!(
        params.max_record_size, DEFAULT_MAX_RECORD_SIZE,
        "max_record_size の既定値は 16382 であること (Section 5.2)"
    );
}

#[test]
fn encode_omits_default_values() {
    let mut buf = Vec::new();
    TransportParameters::default()
        .encode(&mut buf)
        .expect("エンコードに失敗しない");
    assert!(buf.is_empty(), "既定値のパラメータは省略されること");
}

#[test]
fn max_record_size_below_default_is_rejected() {
    // Section 5.2: 既定値未満の値は TRANSPORT_PARAMETER_ERROR
    let buf = varint_param(MAX_RECORD_SIZE_ID, DEFAULT_MAX_RECORD_SIZE - 1);
    let error = TransportParameters::decode(&buf).expect_err("既定値未満は拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn max_record_size_with_trailing_bytes_is_rejected() {
    // 値は 1 つの可変長整数でなければならない (Section 5.2)
    let mut buf = vec![];
    VarInt::try_from(MAX_RECORD_SIZE_ID)
        .expect("ID は可変長整数に収まる")
        .encode_to_vec(&mut buf);
    // 2 バイト形式の 16383 (既定値以上) に 0x00 を足して長さ 3 にする
    VarInt::from(3u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(&[0x7f, 0xff, 0x00]);

    let error = TransportParameters::decode(&buf).expect_err("余分なバイトは拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn prohibited_parameters_are_rejected() {
    // Section 5.1: QUIC version 1 で定義されたパラメータのうち許可された 7 つ以外は禁止
    let prohibited = [
        param_id::ORIGINAL_DESTINATION_CONNECTION_ID,
        param_id::STATELESS_RESET_TOKEN,
        param_id::MAX_UDP_PAYLOAD_SIZE,
        param_id::ACK_DELAY_EXPONENT,
        param_id::MAX_ACK_DELAY,
        param_id::DISABLE_ACTIVE_MIGRATION,
        param_id::PREFERRED_ADDRESS,
        param_id::ACTIVE_CONNECTION_ID_LIMIT,
        param_id::INITIAL_SOURCE_CONNECTION_ID,
        param_id::RETRY_SOURCE_CONNECTION_ID,
    ];
    for id in prohibited {
        // 値の内容によらず、存在した時点で禁止として扱う
        let buf = empty_param(id);
        let error = TransportParameters::decode(&buf).expect_err("禁止パラメータは拒否される");
        assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
    }
}

#[test]
fn duplicate_parameters_are_rejected() {
    // RFC 9000 Section 7.4: 同じパラメータを 2 回受信した場合は
    // TRANSPORT_PARAMETER_ERROR
    let mut buf = varint_param(param_id::MAX_IDLE_TIMEOUT, 1000);
    buf.extend_from_slice(&varint_param(param_id::MAX_IDLE_TIMEOUT, 2000));
    let error = TransportParameters::decode(&buf).expect_err("重複は拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn duplicate_max_record_size_is_rejected() {
    // max_record_size は QUIC version 1 のデコーダでは未知のため、QMux 側で検出する
    let mut buf = varint_param(MAX_RECORD_SIZE_ID, 20000);
    buf.extend_from_slice(&varint_param(MAX_RECORD_SIZE_ID, 30000));
    let error = TransportParameters::decode(&buf).expect_err("重複は拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn reserved_parameters_are_ignored() {
    // RFC 9000 Section 18.1: 31 * N + 27 の形のパラメータは未知パラメータの
    // 扱いを確認するためのもので、QMux でも無視しなければならない (Section 5.1)
    let mut buf = bytes_param(27, &[0xde, 0xad, 0xbe, 0xef]);
    buf.extend_from_slice(&bytes_param(31 * 5 + 27, &[0x00]));
    let params = TransportParameters::decode(&buf).expect("予約済みパラメータは無視される");
    assert_eq!(
        params,
        TransportParameters::default(),
        "予約済みパラメータは値に影響しないこと"
    );
}

#[test]
fn unknown_parameters_are_ignored() {
    // Section 5.1: QMux で利用が定義されていないパラメータは無視する
    let buf = bytes_param(0xff, &[0x01, 0x02]);
    let params = TransportParameters::decode(&buf).expect("未知のパラメータは無視される");
    assert_eq!(params, TransportParameters::default(), "値に影響しないこと");
}

#[test]
fn stream_limit_above_2_60_is_rejected() {
    // RFC 9000 Section 4.6: 2^60 を超えるストリーム数の上限は
    // TRANSPORT_PARAMETER_ERROR
    let buf = varint_param(param_id::INITIAL_MAX_STREAMS_BIDI, (1 << 60) + 1);
    let error = TransportParameters::decode(&buf).expect_err("2^60 超は拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn truncated_parameter_value_is_rejected() {
    let mut buf = varint_param(param_id::MAX_IDLE_TIMEOUT, 1000);
    buf.pop();
    let error = TransportParameters::decode(&buf).expect_err("値が途切れたパラメータは拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn encode_rejects_value_above_varint_maximum() {
    let params = TransportParameters {
        initial_max_data: u64::MAX,
        ..TransportParameters::default()
    };
    let mut buf = Vec::new();
    let error = params
        .encode(&mut buf)
        .expect_err("可変長整数に収まらない値は拒否される");
    assert!(
        matches!(error, Error::InvalidData(_)),
        "ローカルの設定エラーとして扱うこと: {error:?}"
    );
}

#[test]
fn encode_rejects_max_record_size_below_default() {
    let params = TransportParameters {
        max_record_size: 1024,
        ..TransportParameters::default()
    };
    let mut buf = Vec::new();
    let error = params
        .encode(&mut buf)
        .expect_err("既定値未満の max_record_size は送信できない");
    assert!(
        matches!(error, Error::InvalidData(_)),
        "ローカルの設定エラーとして扱うこと: {error:?}"
    );
}

#[test]
fn encode_rejects_stream_limit_above_2_60() {
    let params = TransportParameters {
        initial_max_streams_uni: (1 << 60) + 1,
        ..TransportParameters::default()
    };
    let mut buf = Vec::new();
    let error = params
        .encode(&mut buf)
        .expect_err("2^60 を超えるストリーム数は送信できない");
    assert!(
        matches!(error, Error::InvalidData(_)),
        "ローカルの設定エラーとして扱うこと: {error:?}"
    );
}

#[test]
fn integer_parameter_with_trailing_bytes_is_rejected() {
    // RFC 9000 Section 18.2: 整数パラメータの値は 1 つの可変長整数でなければならない
    let mut buf = Vec::new();
    VarInt::try_from(param_id::INITIAL_MAX_DATA)
        .expect("ID は可変長整数に収まる")
        .encode_to_vec(&mut buf);
    // 2 バイト形式の 16383 に 0x00 を足して長さ 3 にする
    VarInt::from(3u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(&[0x7f, 0xff, 0x00]);

    let error = TransportParameters::decode(&buf).expect_err("余分なバイトは拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn integer_parameter_with_empty_value_is_rejected() {
    // 値を持たない整数パラメータは不正 (RFC 9000 Section 7.4)
    let buf = empty_param(param_id::INITIAL_MAX_STREAM_DATA_UNI);
    let error = TransportParameters::decode(&buf).expect_err("空の値は拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}

#[test]
fn max_datagram_frame_size_with_trailing_bytes_is_rejected() {
    let mut buf = Vec::new();
    VarInt::try_from(param_id::MAX_DATAGRAM_FRAME_SIZE)
        .expect("ID は可変長整数に収まる")
        .encode_to_vec(&mut buf);
    // 2 バイト形式の 16383 に 0x00 を足して長さ 3 にする
    VarInt::from(3u8).encode_to_vec(&mut buf);
    buf.extend_from_slice(&[0x7f, 0xff, 0x00]);

    let error = TransportParameters::decode(&buf).expect_err("余分なバイトは拒否される");
    assert_protocol_error(error, TransportErrorCode::TRANSPORT_PARAMETER_ERROR);
}
