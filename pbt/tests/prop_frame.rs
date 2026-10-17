//! QMux フレームのプロパティベーステスト
//!
//! draft-ietf-quic-qmux-02 Section 4 の QMux 固有フレーム
//! (QX_TRANSPORT_PARAMETERS / QX_PING) と、QMux で利用が許可された QUIC version 1
//! のフレームについて、encode → decode のラウンドトリップ、Record 内の複数フレーム
//! のラウンドトリップ、decode の消費バイト数を検証する。

use pbt::common::sample_bytes;
use pbt::common::sample_varint;
use pbt::common::test_runner;
use shiguredo_qmux::codec::frame::Frame;
use shiguredo_qmux::codec::frame::connection_close::ConnectionClose;
use shiguredo_qmux::codec::frame::data_blocked::DataBlocked;
use shiguredo_qmux::codec::frame::datagram::Datagram;
use shiguredo_qmux::codec::frame::max_data::MaxData;
use shiguredo_qmux::codec::frame::max_stream_data::MaxStreamData;
use shiguredo_qmux::codec::frame::max_streams::MaxStreams;
use shiguredo_qmux::codec::frame::padding::Padding;
use shiguredo_qmux::codec::frame::reset_stream::ResetStream;
use shiguredo_qmux::codec::frame::stop_sending::StopSending;
use shiguredo_qmux::codec::frame::stream::Stream;
use shiguredo_qmux::codec::frame::stream_data_blocked::StreamDataBlocked;
use shiguredo_qmux::codec::frame::streams_blocked::StreamsBlocked;
use shiguredo_qmux::codec::varint;
use shiguredo_qmux::codec::varint::VarInt;
use shiguredo_qmux::frame::QmuxFrame;
use shiguredo_qmux::frame::QxPing;
use shiguredo_qmux::frame::QxTransportParameters;
use shiguredo_qmux::frame::decode;
use shiguredo_qmux::frame::decode_record;
use shiguredo_qmux::transport_params::MAX_STREAMS_LIMIT;

/// 生成対象のフレーム種別の数 (カバレッジゲート用)
const FRAME_KIND_COUNT: usize = 14;

/// Record 内の複数フレームのテストで生成する種別
///
/// PADDING は連続すると 1 つのフレームに融合してデコードされる (RFC 9000
/// Section 19.1) ため、複数フレームのラウンドトリップでは生成しない。
/// PADDING 自体は単体のラウンドトリップで検証する。
const RECORD_FRAME_KINDS: [usize; 13] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13];

/// PBT で扱うフレームのペイロード長の上限
const MAX_FRAME_PAYLOAD_LEN: usize = 256;

/// `0..=max` の可変長整数値を生成する
///
/// 可変長整数のエンコード長 (1 / 2 / 4 / 8 バイト) の境界を混ぜる。
fn sample_varint_at_most(ctx: &mut noprop::TestCaseContext, max: u64) -> u64 {
    let boundaries: Vec<u64> = [
        0u64,
        63,            // 1 バイト最大
        64,            // 2 バイト最小
        16_383,        // 2 バイト最大
        16_384,        // 4 バイト最小
        1_073_741_823, // 4 バイト最大
        1_073_741_824, // 8 バイト最小
        varint::MAX_VALUE,
    ]
    .into_iter()
    .filter(|value| *value <= max)
    .collect();
    noprop::sample_with_boundaries(ctx, &boundaries, noprop::Ratio::one_nth(4), |ctx| {
        noprop::sample_u64_in(ctx, 0..=max)
    })
}

/// 可変長整数として表現できる値を生成する
fn sample_varint_value(ctx: &mut noprop::TestCaseContext) -> VarInt {
    VarInt::new(sample_varint(ctx)).expect("0..=2^62-1 の値なので VarInt を作れる")
}

/// `1..=max` の長さを生成する
///
/// 1 と最大長は一様分布では出にくいため、明示的な境界値として混ぜる。
fn sample_len_at_least_one(ctx: &mut noprop::TestCaseContext, max: usize) -> usize {
    noprop::sample_with_boundaries(ctx, &[1usize, max], noprop::Ratio::one_nth(4), |ctx| {
        noprop::sample_usize_in(ctx, 1..=max)
    })
}

/// QMux で送受信するフレームを 1 つ生成する
///
/// 生成対象の全種別を等確率で選ぶ。
fn sample_qmux_frame(ctx: &mut noprop::TestCaseContext) -> QmuxFrame {
    let kind = noprop::sample_weighted_index(ctx, &[1u32; FRAME_KIND_COUNT]);
    sample_qmux_frame_of_kind(ctx, kind)
}

/// 種別番号を指定してフレームを 1 つ生成する
///
/// QX_TRANSPORT_PARAMETERS / QX_PING と、QMux で利用が許可された QUIC version 1 の
/// フレーム (STREAM / MAX_DATA / MAX_STREAM_DATA / MAX_STREAMS / DATA_BLOCKED /
/// STREAM_DATA_BLOCKED / STREAMS_BLOCKED / RESET_STREAM / STOP_SENDING / DATAGRAM /
/// PADDING / CONNECTION_CLOSE) を生成する。値はすべて可変長整数で表現できる範囲に
/// 収める。
fn sample_qmux_frame_of_kind(ctx: &mut noprop::TestCaseContext, kind: usize) -> QmuxFrame {
    match kind {
        0 => QmuxFrame::TransportParameters(QxTransportParameters {
            // デコード側はトランスポートパラメータの中身を解釈しないため任意バイト列でよい
            params: sample_bytes(ctx, MAX_FRAME_PAYLOAD_LEN),
        }),
        1 => {
            let sequence = sample_varint_value(ctx);
            QmuxFrame::Ping(if noprop::sample_bool(ctx) {
                QxPing::Request { sequence }
            } else {
                QxPing::Response { sequence }
            })
        }
        2 => {
            let data = sample_bytes(ctx, MAX_FRAME_PAYLOAD_LEN);
            // RFC 9000 Section 19.8: offset と length の和は 2^62-1 を超えられない
            let offset = sample_varint_at_most(ctx, varint::MAX_VALUE - data.len() as u64);
            QmuxFrame::Quic(Frame::Stream(Stream {
                stream_id: sample_varint(ctx),
                offset,
                fin: noprop::sample_bool(ctx),
                data,
            }))
        }
        3 => QmuxFrame::Quic(Frame::MaxData(MaxData {
            maximum_data: sample_varint(ctx),
        })),
        4 => QmuxFrame::Quic(Frame::MaxStreamData(MaxStreamData {
            stream_id: sample_varint(ctx),
            maximum_stream_data: sample_varint(ctx),
        })),
        5 => QmuxFrame::Quic(Frame::MaxStreams(MaxStreams {
            bidirectional: noprop::sample_bool(ctx),
            // RFC 9000 Section 4.6: 2^60 を超える値は送信できない
            maximum_streams: sample_varint_at_most(ctx, MAX_STREAMS_LIMIT),
        })),
        6 => QmuxFrame::Quic(Frame::DataBlocked(DataBlocked {
            maximum_data: sample_varint(ctx),
        })),
        7 => QmuxFrame::Quic(Frame::StreamDataBlocked(StreamDataBlocked {
            stream_id: sample_varint(ctx),
            maximum_stream_data: sample_varint(ctx),
        })),
        8 => QmuxFrame::Quic(Frame::StreamsBlocked(StreamsBlocked {
            bidirectional: noprop::sample_bool(ctx),
            // RFC 9000 Section 4.6: 2^60 を超える値は送信できない
            maximum_streams: sample_varint_at_most(ctx, MAX_STREAMS_LIMIT),
        })),
        9 => QmuxFrame::Quic(Frame::ResetStream(ResetStream {
            stream_id: sample_varint(ctx),
            application_protocol_error_code: sample_varint(ctx),
            final_size: sample_varint(ctx),
        })),
        10 => QmuxFrame::Quic(Frame::StopSending(StopSending {
            stream_id: sample_varint(ctx),
            application_protocol_error_code: sample_varint(ctx),
        })),
        11 => QmuxFrame::Quic(Frame::Datagram(Datagram {
            data: sample_bytes(ctx, MAX_FRAME_PAYLOAD_LEN),
        })),
        12 => QmuxFrame::Quic(Frame::Padding(Padding {
            // RFC 9000 Section 19.1: 長さ 0 の PADDING はバイト列を生まないため生成しない
            length: sample_len_at_least_one(ctx, MAX_FRAME_PAYLOAD_LEN),
        })),
        13 => {
            // RFC 9000 Section 19.19: 0x1c (トランスポートエラー) はエラーを引き起こした
            // フレームタイプを持ち、0x1d (アプリケーションエラー) は持たない。
            // encode と decode が一致する組み合わせだけを生成する。
            let application = noprop::sample_bool(ctx);
            let frame_type = if application {
                None
            } else {
                Some(sample_varint(ctx))
            };
            QmuxFrame::Quic(Frame::ConnectionClose(ConnectionClose {
                application,
                error_code: sample_varint(ctx),
                frame_type,
                reason_phrase: sample_bytes(ctx, MAX_FRAME_PAYLOAD_LEN),
            }))
        }
        other => panic!("生成対象外の種別番号が渡された: {other}"),
    }
}

/// 生成したフレームの種別番号を返す (カバレッジゲート用)
///
/// 生成対象の全種別を網羅する。新しいフレーム種別を追加したときは、
/// 種別番号を足すまでコンパイルできなくなる。
fn frame_kind(frame: &QmuxFrame) -> usize {
    match frame {
        QmuxFrame::TransportParameters(_) => 0,
        QmuxFrame::Ping(_) => 1,
        QmuxFrame::Quic(Frame::Stream(_)) => 2,
        QmuxFrame::Quic(Frame::MaxData(_)) => 3,
        QmuxFrame::Quic(Frame::MaxStreamData(_)) => 4,
        QmuxFrame::Quic(Frame::MaxStreams(_)) => 5,
        QmuxFrame::Quic(Frame::DataBlocked(_)) => 6,
        QmuxFrame::Quic(Frame::StreamDataBlocked(_)) => 7,
        QmuxFrame::Quic(Frame::StreamsBlocked(_)) => 8,
        QmuxFrame::Quic(Frame::ResetStream(_)) => 9,
        QmuxFrame::Quic(Frame::StopSending(_)) => 10,
        QmuxFrame::Quic(Frame::Datagram(_)) => 11,
        QmuxFrame::Quic(Frame::Padding(_)) => 12,
        QmuxFrame::Quic(Frame::ConnectionClose(_)) => 13,
    }
}

/// 生成した QmuxFrame が encode → decode で元に戻る
///
/// 生成対象の全種別が 1 度は現れることをカバレッジゲートで確認する。
#[test]
fn roundtrip() -> noprop::TestResult {
    let seen = std::cell::Cell::new(0u32);
    let mut runner = test_runner()?;
    runner.run(512, |ctx| {
        let frame = sample_qmux_frame(ctx);
        let mut buf = Vec::new();
        frame.encode(&mut buf);
        let (decoded, size) = decode(&buf).expect("エンコード直後のバイト列なのでデコードできる");
        assert_eq!(decoded, frame, "ラウンドトリップ結果が一致しない");
        assert_eq!(size, buf.len(), "消費バイト数がエンコード長と一致しない");

        // assert 通過後に種別を記録する
        seen.set(seen.get() | (1u32 << frame_kind(&frame)));
        Ok(())
    })?;

    let expected = (1u32 << FRAME_KIND_COUNT) - 1;
    assert_eq!(
        seen.get(),
        expected,
        "全種別のフレームが生成されなかった: seen={:#x} expected={expected:#x}\n{runner}",
        seen.get()
    );
    Ok(())
}

/// 複数のフレームを encode したバイト列が同じフレーム列に戻る
#[test]
fn decode_record_roundtrip() -> noprop::TestResult {
    let mut runner = test_runner()?;
    runner.run(256, |ctx| {
        // 1 つの Record に含めるフレーム数 (1 件と上限は一様分布では出にくいため混ぜる)
        let count =
            noprop::sample_with_boundaries(ctx, &[1usize, 8], noprop::Ratio::one_nth(4), |ctx| {
                noprop::sample_usize_in(ctx, 1..=8)
            });
        let frames: Vec<QmuxFrame> = (0..count)
            .map(|_| {
                let kind =
                    RECORD_FRAME_KINDS[noprop::sample_usize_in(ctx, 0..RECORD_FRAME_KINDS.len())];
                sample_qmux_frame_of_kind(ctx, kind)
            })
            .collect();

        let mut buf = Vec::new();
        for frame in &frames {
            frame.encode(&mut buf);
        }
        let decoded = decode_record(&buf).expect("エンコード直後のバイト列なのでデコードできる");
        assert_eq!(decoded, frames, "Record 内のフレーム列が一致しない");
        Ok(())
    })?;
    Ok(())
}

/// decode が Ok を返した場合、消費バイト数は 1 以上でバッファ長以下
#[test]
fn decode_consumes_within_buffer() -> noprop::TestResult {
    let ok_seen = std::cell::Cell::new(false);
    let mut runner = test_runner()?;
    runner.run(512, |ctx| {
        let buf = sample_bytes(ctx, 512);
        if let Ok((_, size)) = decode(&buf) {
            ok_seen.set(true);
            assert!(
                (1..=buf.len()).contains(&size),
                "消費バイト数が範囲外: size={size} buf_len={}",
                buf.len()
            );
        }
        Ok(())
    })?;
    assert!(
        ok_seen.get(),
        "decode が Ok を返すケースが 1 度も観測されなかった\n{runner}"
    );
    Ok(())
}
