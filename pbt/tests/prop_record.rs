//! QMux Record のプロパティベーステスト
//!
//! draft-ietf-quic-qmux-02 Section 3.2 の Record (Size フィールド + Frames
//! フィールド) について、encode → RecordDecoder のラウンドトリップ、任意の分割で
//! 受信した場合の同一性、およびエラー条件が「空ペイロード」と「max_record_size
//! 超過」に限られることを検証する。

use pbt::common::sample_bytes;
use pbt::common::sample_nonempty_bytes;
use pbt::common::test_runner;
use shiguredo_qmux::codec::varint;
use shiguredo_qmux::record::DEFAULT_MAX_RECORD_SIZE;
use shiguredo_qmux::record::RecordDecoder;
use shiguredo_qmux::record::encode;

/// PBT で扱うペイロード長の上限
const MAX_PAYLOAD_LEN: usize = 4096;

/// `min` 以上 `varint::MAX_VALUE` 以下の Record サイズ上限を生成する
///
/// 既定値と可変長整数の最大値は一様分布ではほぼ出ないため、境界値として混ぜる。
/// `min` より小さい境界値は、生成したペイロードを必ず収められる値だけを使うために
/// 除外する。
fn sample_max_record_size(ctx: &mut noprop::TestCaseContext, min: u64) -> u64 {
    let mut boundaries: Vec<u64> = [min, DEFAULT_MAX_RECORD_SIZE, varint::MAX_VALUE]
        .into_iter()
        .filter(|value| *value >= min)
        .collect();
    boundaries.sort_unstable();
    boundaries.dedup();
    noprop::sample_with_boundaries(ctx, &boundaries, noprop::Ratio::one_nth(4), |ctx| {
        noprop::sample_u64_in(ctx, min..=varint::MAX_VALUE)
    })
}

/// 任意の非空ペイロードが encode → RecordDecoder で元に戻る
#[test]
fn roundtrip() -> noprop::TestResult {
    let mut runner = test_runner()?;
    runner.run(256, |ctx| {
        let payload = sample_nonempty_bytes(ctx, MAX_PAYLOAD_LEN);
        let max_record_size = sample_max_record_size(ctx, payload.len() as u64);

        let mut buf = Vec::new();
        encode(&payload, max_record_size, &mut buf)
            .expect("max_record_size に収まるペイロードなのでエンコードできる");

        let mut decoder = RecordDecoder::new(max_record_size);
        assert_eq!(
            decoder.max_record_size(),
            max_record_size,
            "デコーダが保持する max_record_size が一致しない"
        );
        decoder.extend(&buf);
        assert_eq!(
            decoder
                .poll_record()
                .expect("エンコード直後のバイト列なのでデコードできる"),
            Some(payload),
            "ラウンドトリップ結果が一致しない"
        );
        assert_eq!(
            decoder.buffered_len(),
            0,
            "Record を 1 つ取り出した後にバッファが残っている"
        );
        assert_eq!(
            decoder
                .poll_record()
                .expect("バッファが空なのでエラーにならない"),
            None,
            "バッファが空なのに Record が取り出された"
        );
        Ok(())
    })?;
    Ok(())
}

/// 任意の分割で extend しても同じ Record に戻る
///
/// Record が複数の receive 呼び出しにまたがる経路を検証する。分割のたびに
/// poll_record を呼び、完成した Record だけが取り出されることも確認する。
#[test]
fn roundtrip_with_random_chunks() -> noprop::TestResult {
    let mut runner = test_runner()?;
    runner.run(256, |ctx| {
        let payload = sample_nonempty_bytes(ctx, MAX_PAYLOAD_LEN);
        let max_record_size = sample_max_record_size(ctx, payload.len() as u64);

        let mut buf = Vec::new();
        encode(&payload, max_record_size, &mut buf)
            .expect("max_record_size に収まるペイロードなのでエンコードできる");

        let mut decoder = RecordDecoder::new(max_record_size);
        let mut records = Vec::new();
        let mut offset = 0;
        while offset < buf.len() {
            let chunk_len = noprop::sample_usize_in(ctx, 1..=(buf.len() - offset));
            decoder.extend(&buf[offset..offset + chunk_len]);
            offset += chunk_len;
            while let Some(record) = decoder
                .poll_record()
                .expect("自分でエンコードしたバイト列なのでエラーにならない")
            {
                records.push(record);
            }
        }

        assert_eq!(records, vec![payload], "分割して受信した結果が一致しない");
        assert_eq!(
            decoder.buffered_len(),
            0,
            "全バイトを渡した後にバッファが残っている"
        );
        Ok(())
    })?;
    Ok(())
}

/// encode がエラーになるのは空ペイロードか max_record_size 超過のときだけ
///
/// 成否の判定だけでなく、成功した場合は Record として読み出せること、失敗した
/// 場合はバッファへ書き込まれていないことも確認する。
#[test]
fn encode_error_condition() -> noprop::TestResult {
    let ok_seen = std::cell::Cell::new(false);
    let err_seen = std::cell::Cell::new(false);
    let mut runner = test_runner()?;
    runner.run(512, |ctx| {
        // 空ペイロードと上限長は一様分布では出にくいため、境界値として混ぜる
        let payload = sample_bytes(ctx, MAX_PAYLOAD_LEN);
        // 超過の条件を作るため、ペイロード長より小さい値も生成する
        let max_record_size = noprop::sample_with_boundaries(
            ctx,
            &[0u64, DEFAULT_MAX_RECORD_SIZE, varint::MAX_VALUE],
            noprop::Ratio::one_nth(4),
            |ctx| noprop::sample_u64_in(ctx, 0..=varint::MAX_VALUE),
        );

        let mut buf = Vec::new();
        let result = encode(&payload, max_record_size, &mut buf);
        let expected_ok = !payload.is_empty() && payload.len() as u64 <= max_record_size;
        assert_eq!(
            result.is_ok(),
            expected_ok,
            "payload_len={} max_record_size={max_record_size} の成否が条件と一致しない: {result:?}",
            payload.len()
        );

        if expected_ok {
            ok_seen.set(true);
            let mut decoder = RecordDecoder::new(max_record_size);
            decoder.extend(&buf);
            assert_eq!(
                decoder
                    .poll_record()
                    .expect("エンコードに成功したバイト列なのでデコードできる"),
                Some(payload),
                "エンコードに成功した Record が元に戻らない"
            );
        } else {
            err_seen.set(true);
            assert!(
                buf.is_empty(),
                "エラー時にバッファへ書き込まれている: len={}",
                buf.len()
            );
        }
        Ok(())
    })?;

    // 成否の両方の分岐が観測されたことを確認する
    assert!(
        ok_seen.get(),
        "エンコードに成功するケースが 1 度も観測されなかった\n{runner}"
    );
    assert!(
        err_seen.get(),
        "エンコードに失敗するケースが 1 度も観測されなかった\n{runner}"
    );
    Ok(())
}

/// 必ずエラーになる条件で encode を呼んでもバッファの内容が変化しない
#[test]
fn encode_error_does_not_modify_buffer() -> noprop::TestResult {
    let mut runner = test_runner()?;
    runner.run(256, |ctx| {
        let payload = sample_bytes(ctx, MAX_PAYLOAD_LEN);
        let max_record_size = if payload.is_empty() {
            // 空ペイロードは max_record_size に関係なくエラーになる
            noprop::sample_u64_in(ctx, 0..=varint::MAX_VALUE)
        } else {
            // ペイロード長未満の上限では必ず超過エラーになる
            noprop::sample_u64_in(ctx, 0..(payload.len() as u64))
        };

        let mut buf = b"sentinel".to_vec();
        let before = buf.clone();
        encode(&payload, max_record_size, &mut buf)
            .expect_err("空ペイロードか max_record_size 超過なのでエラーになる");
        assert_eq!(
            buf, before,
            "エラー時にバッファの内容が変化している: payload_len={} max_record_size={max_record_size}",
            payload.len()
        );
        Ok(())
    })?;
    Ok(())
}
