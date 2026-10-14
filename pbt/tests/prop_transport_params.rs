//! QMux トランスポートパラメータのプロパティベーステスト
//!
//! draft-ietf-quic-qmux-02 Section 5 の [`TransportParameters`] について、
//! encode → decode のラウンドトリップが恒等写像であることを検証する。

use pbt::common::sample_option;
use pbt::common::sample_varint;
use pbt::common::test_runner;
use shiguredo_qmux::codec::varint;
use shiguredo_qmux::record::DEFAULT_MAX_RECORD_SIZE;
use shiguredo_qmux::transport_params::MAX_STREAMS_LIMIT;
use shiguredo_qmux::transport_params::TransportParameters;

/// max_record_size を生成する
///
/// Section 5.2 のとおり既定値以上、かつ可変長整数で表現できる範囲に収める。
/// 既定値とその次の値、可変長整数の最大値は一様分布ではほぼ出ないため境界値として混ぜる。
fn sample_max_record_size(ctx: &mut noprop::TestCaseContext) -> u64 {
    noprop::sample_with_boundaries(
        ctx,
        &[
            DEFAULT_MAX_RECORD_SIZE,
            DEFAULT_MAX_RECORD_SIZE + 1,
            varint::MAX_VALUE,
        ],
        noprop::Ratio::one_nth(4),
        |ctx| noprop::sample_u64_in(ctx, DEFAULT_MAX_RECORD_SIZE..=varint::MAX_VALUE),
    )
}

/// initial_max_streams_* を生成する
///
/// RFC 9000 Section 4.6 のとおり 2^60 以下に収める。0 (ストリームを開けない) と
/// 上限は一様分布ではほぼ出ないため境界値として混ぜる。
fn sample_stream_limit(ctx: &mut noprop::TestCaseContext) -> u64 {
    noprop::sample_with_boundaries(
        ctx,
        &[0u64, 1, 2, MAX_STREAMS_LIMIT],
        noprop::Ratio::one_nth(4),
        |ctx| noprop::sample_u64_in(ctx, 0..=MAX_STREAMS_LIMIT),
    )
}

/// QMux のトランスポートパラメータを生成する
///
/// - 各値は可変長整数で表現できる範囲 (varint の範囲内)
/// - max_record_size は既定値以上 (Section 5.2)
/// - initial_max_streams_* は 2^60 以下 (RFC 9000 Section 4.6)
fn sample_transport_parameters(ctx: &mut noprop::TestCaseContext) -> TransportParameters {
    TransportParameters {
        max_idle_timeout: sample_varint(ctx),
        initial_max_data: sample_varint(ctx),
        initial_max_stream_data_bidi_local: sample_varint(ctx),
        initial_max_stream_data_bidi_remote: sample_varint(ctx),
        initial_max_stream_data_uni: sample_varint(ctx),
        initial_max_streams_bidi: sample_stream_limit(ctx),
        initial_max_streams_uni: sample_stream_limit(ctx),
        max_record_size: sample_max_record_size(ctx),
        // DATAGRAM 非サポート (None) とサポート (Some) の両方を生成する
        max_datagram_frame_size: sample_option(ctx, sample_varint),
    }
}

/// encode → decode のラウンドトリップが恒等写像であることを検証する
///
/// max_datagram_frame_size が Some / None の両方の分岐を踏むことを
/// カバレッジゲートで確認する。
#[test]
fn roundtrip() -> noprop::TestResult {
    let some_seen = std::cell::Cell::new(false);
    let none_seen = std::cell::Cell::new(false);
    let mut runner = test_runner()?;
    runner.run(256, |ctx| {
        let params = sample_transport_parameters(ctx);
        let mut buf = Vec::new();
        params
            .encode(&mut buf)
            .expect("生成した値はすべて可変長整数で表現できる");
        let decoded = TransportParameters::decode(&buf)
            .expect("エンコード直後のバイト列なのでデコードできる");
        assert_eq!(decoded, params, "ラウンドトリップ結果が一致しない");

        // assert 通過後に max_datagram_frame_size の分岐を記録する
        if params.max_datagram_frame_size.is_some() {
            some_seen.set(true);
        } else {
            none_seen.set(true);
        }
        Ok(())
    })?;

    assert!(
        some_seen.get(),
        "max_datagram_frame_size が Some のケースが 1 度も観測されなかった\n{runner}"
    );
    assert!(
        none_seen.get(),
        "max_datagram_frame_size が None のケースが 1 度も観測されなかった\n{runner}"
    );
    Ok(())
}
