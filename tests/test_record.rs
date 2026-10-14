//! QMux Record のエンコード/デコードの単体テスト。
//!
//! 増分デコード、Size の上限、空の Record の拒否など、性質テストでは扱いにくい
//! 境界値とエラー経路を検証する。正常系のラウンドトリップは PBT
//! (`pbt/tests/prop_record.rs`) が担う。

use shiguredo_qmux::codec::error::{Error, TransportErrorCode};
use shiguredo_qmux::record::{DEFAULT_MAX_RECORD_SIZE, RecordDecoder, encode};

/// プロトコルエラーとそのコードを確認する
fn assert_protocol_error(error: Error, expected: TransportErrorCode) {
    match error {
        Error::Protocol { code, reason } => {
            assert_eq!(code, expected, "エラーコードが一致すること: {reason}");
        }
        other => panic!("プロトコルエラーを期待したが {other:?} だった"),
    }
}

/// Frames フィールドの先頭に Size を付けた Record を組み立てる
///
/// Size に可変長整数の非最短形を許すため、テスト用に手で組み立てる。
fn raw_record(size_bytes: &[u8], frames: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(size_bytes);
    buf.extend_from_slice(frames);
    buf
}

#[test]
fn encode_prepends_size_varint() {
    // Size は Frames の長さで、Size 自身は含まない (Section 3.2)
    let frames = [0xaa; 10];
    let mut buf = Vec::new();
    encode(&frames, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("10 バイトの Record は作れる");

    assert_eq!(buf[0], 10, "1 バイトの Size が先頭に付くこと");
    assert_eq!(&buf[1..], &frames, "Size の後ろが Frames であること");
}

#[test]
fn encode_uses_two_byte_size_for_large_records() {
    // 63 を超える長さの Size は 2 バイトの可変長整数になる (RFC 9000 Section 16)
    let frames = [0xbb; 100];
    let mut buf = Vec::new();
    encode(&frames, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("100 バイトの Record は作れる");

    assert_eq!(
        &buf[..2],
        &[0x40, 0x64],
        "100 の 2 バイト表現が先頭に付くこと"
    );
    assert_eq!(&buf[2..], &frames, "Size の後ろが Frames であること");
}

#[test]
fn decode_waits_for_incomplete_size() {
    // 200 バイトの Size は 2 バイトの可変長整数になる。1 バイトだけでは待つ
    let frames = [0x22; 200];
    let mut buf = Vec::new();
    encode(&frames, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("Record を作れる");

    let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
    decoder.extend(&buf[..1]);
    assert!(
        decoder
            .poll_record()
            .expect("デコードに失敗しない")
            .is_none(),
        "Size が揃うまで Record を取り出さないこと"
    );

    decoder.extend(&buf[1..]);
    let record = decoder
        .poll_record()
        .expect("デコードに失敗しない")
        .expect("Record が完成する");
    assert_eq!(record, frames, "残りを渡すと Record が完成すること");
}

#[test]
fn decode_waits_for_incomplete_frames() {
    let frames = [0x33; 300];
    let mut buf = Vec::new();
    encode(&frames, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("Record を作れる");

    let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
    decoder.extend(&buf[..buf.len() - 1]);
    assert_eq!(
        decoder.buffered_len(),
        buf.len() - 1,
        "受け取った分を保持すること"
    );
    assert!(
        decoder
            .poll_record()
            .expect("デコードに失敗しない")
            .is_none(),
        "Frames が揃うまで Record を取り出さないこと"
    );

    decoder.extend(&buf[buf.len() - 1..]);
    let record = decoder
        .poll_record()
        .expect("デコードに失敗しない")
        .expect("Record が完成する");
    assert_eq!(record, frames, "Frames がそのまま取り出せること");
}

#[test]
fn decode_multiple_records_in_one_extend() {
    let first = [0x44; 10];
    let second = [0x55; 20];
    let mut buf = Vec::new();
    encode(&first, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("1 つ目の Record を作れる");
    encode(&second, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("2 つ目の Record を作れる");

    let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
    decoder.extend(&buf);
    assert_eq!(
        decoder.poll_record().expect("デコードに失敗しない"),
        Some(first.to_vec()),
        "1 つ目の Record が取り出せること"
    );
    assert_eq!(
        decoder.poll_record().expect("デコードに失敗しない"),
        Some(second.to_vec()),
        "2 つ目の Record が取り出せること"
    );
    assert!(
        decoder
            .poll_record()
            .expect("デコードに失敗しない")
            .is_none(),
        "3 つ目の Record はないこと"
    );
}

#[test]
fn decode_accepts_record_at_advertised_maximum() {
    // 広告した max_record_size と等しい Record は受け入れなければならない (Section 5.2)
    let frames = vec![0x66; DEFAULT_MAX_RECORD_SIZE as usize];
    let mut buf = Vec::new();
    encode(&frames, DEFAULT_MAX_RECORD_SIZE, &mut buf).expect("上限ちょうどの Record を作れる");

    let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
    decoder.extend(&buf);
    let record = decoder
        .poll_record()
        .expect("デコードに失敗しない")
        .expect("Record が完成する");
    assert_eq!(
        record.len(),
        DEFAULT_MAX_RECORD_SIZE as usize,
        "上限ちょうどでも受け入れること"
    );
}

#[test]
fn decode_rejects_record_larger_than_advertised_maximum() {
    // 広告した上限を超える Record は FRAME_ENCODING_ERROR (Section 5.2)
    let mut buf = Vec::new();
    encode(&[0x77; 100], 100, &mut buf).expect("上限内の Record を作れる");

    let mut decoder = RecordDecoder::new(99);
    decoder.extend(&buf);
    let error = decoder
        .poll_record()
        .expect_err("上限を超える Record は拒否される");
    assert_protocol_error(error, TransportErrorCode::FRAME_ENCODING_ERROR);
    assert_eq!(
        decoder.buffered_len(),
        0,
        "エラー後はバッファを空にすること"
    );
}

#[test]
fn decode_rejects_empty_record() {
    // Frames は 1 つ以上のフレームを含まなければならない (Section 3.2)
    let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
    decoder.extend(&[0x00]);
    let error = decoder.poll_record().expect_err("空の Record は拒否される");
    assert_protocol_error(error, TransportErrorCode::PROTOCOL_VIOLATION);
    assert_eq!(
        decoder.buffered_len(),
        0,
        "エラー後はバッファを空にすること"
    );
}

#[test]
fn decode_rejects_non_minimal_size_for_oversized_record() {
    // Size を 4 バイトでエンコードしても上限検査は同じように働く
    // (最小値は 4 バイト形式で 0x80000000、値は 200)
    let frames = [0x88; 200];
    let buf = raw_record(&[0x80, 0x00, 0x00, 0xc8], &frames);

    let mut decoder = RecordDecoder::new(100);
    decoder.extend(&buf);
    let error = decoder
        .poll_record()
        .expect_err("上限を超える Record は拒否される");
    assert_protocol_error(error, TransportErrorCode::FRAME_ENCODING_ERROR);
}

#[test]
fn decode_after_error_reports_no_record() {
    // エラー後は同じ不正入力を繰り返し報告しないようバッファを捨てる
    let mut decoder = RecordDecoder::new(DEFAULT_MAX_RECORD_SIZE);
    decoder.extend(&[0x00]);
    assert_protocol_error(
        decoder.poll_record().expect_err("空の Record は拒否される"),
        TransportErrorCode::PROTOCOL_VIOLATION,
    );
    assert!(
        decoder
            .poll_record()
            .expect("2 回目はエラーにならない")
            .is_none(),
        "エラー後のバッファは空であること"
    );
}
