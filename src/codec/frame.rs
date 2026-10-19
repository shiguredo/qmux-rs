//! QMux が利用する QUIC version 1 のフレーム (RFC 9000 Section 12.4)
//!
//! QMux で利用が許可されたフレームだけを定義する
//! (draft-ietf-quic-qmux-02 Section 4, Section 9.1)。利用が禁止されたフレームや
//! QUIC version 1 の拡張フレームはここでは扱わず、[`crate::frame`] が
//! フレームタイプで判定して拒否する。

use alloc::vec::Vec;

use crate::codec::error::Error;
use crate::codec::varint::VarInt;

/// フレームタイプ定数 (RFC 9000 Section 12.4、RFC 9221 Section 4)
///
/// QMux で利用が禁止されたフレームと、利用が許可されていない拡張フレームの
/// タイプも、判定のためにここで定義する。各フレームの形式と意味は RFC 9000
/// Section 19 および RFC 9221 Section 4 を参照すること。
pub mod frame_type {
    // RFC 9000 標準フレーム
    /// PADDING フレーム (RFC 9000 Section 19.1)
    pub const PADDING: u64 = 0x00;
    /// PING フレーム (RFC 9000 Section 19.2)
    pub const PING: u64 = 0x01;
    /// ACK フレーム (RFC 9000 Section 19.3)
    pub const ACK: u64 = 0x02;
    /// ECN 付き ACK フレーム (RFC 9000 Section 19.3)
    pub const ACK_ECN: u64 = 0x03;
    /// RESET_STREAM フレーム (RFC 9000 Section 19.4)
    pub const RESET_STREAM: u64 = 0x04;
    /// STOP_SENDING フレーム (RFC 9000 Section 19.5)
    pub const STOP_SENDING: u64 = 0x05;
    /// CRYPTO フレーム (RFC 9000 Section 19.6)
    pub const CRYPTO: u64 = 0x06;
    /// NEW_TOKEN フレーム (RFC 9000 Section 19.7)
    pub const NEW_TOKEN: u64 = 0x07;
    /// STREAM フレームのタイプ範囲の最小値 (0x08、RFC 9000 Section 19.8)
    ///
    /// STREAM フレームのタイプは 0x08 から 0x0f までの 8 種類で、OFF ビットと
    /// LEN ビットと FIN ビットの組み合わせを表す。
    pub const STREAM_START: u64 = 0x08;
    /// STREAM フレームのタイプ範囲の最大値 (0x0f、RFC 9000 Section 19.8)
    pub const STREAM_END: u64 = 0x0f;
    /// MAX_DATA フレーム (RFC 9000 Section 19.9)
    pub const MAX_DATA: u64 = 0x10;
    /// MAX_STREAM_DATA フレーム (RFC 9000 Section 19.10)
    pub const MAX_STREAM_DATA: u64 = 0x11;
    /// 双方向ストリーム数の上限を通知する MAX_STREAMS フレーム (RFC 9000 Section 19.11)
    pub const MAX_STREAMS_BIDI: u64 = 0x12;
    /// 単方向ストリーム数の上限を通知する MAX_STREAMS フレーム (RFC 9000 Section 19.11)
    pub const MAX_STREAMS_UNI: u64 = 0x13;
    /// DATA_BLOCKED フレーム (RFC 9000 Section 19.12)
    pub const DATA_BLOCKED: u64 = 0x14;
    /// STREAM_DATA_BLOCKED フレーム (RFC 9000 Section 19.13)
    pub const STREAM_DATA_BLOCKED: u64 = 0x15;
    /// 双方向ストリーム数の上限でブロックしていることを通知する STREAMS_BLOCKED フレーム
    /// (RFC 9000 Section 19.14)
    pub const STREAMS_BLOCKED_BIDI: u64 = 0x16;
    /// 単方向ストリーム数の上限でブロックしていることを通知する STREAMS_BLOCKED フレーム
    /// (RFC 9000 Section 19.14)
    pub const STREAMS_BLOCKED_UNI: u64 = 0x17;
    /// NEW_CONNECTION_ID フレーム (RFC 9000 Section 19.15)
    pub const NEW_CONNECTION_ID: u64 = 0x18;
    /// RETIRE_CONNECTION_ID フレーム (RFC 9000 Section 19.16)
    pub const RETIRE_CONNECTION_ID: u64 = 0x19;
    /// PATH_CHALLENGE フレーム (RFC 9000 Section 19.17)
    pub const PATH_CHALLENGE: u64 = 0x1a;
    /// PATH_RESPONSE フレーム (RFC 9000 Section 19.18)
    pub const PATH_RESPONSE: u64 = 0x1b;
    /// トランスポートエラーによる接続閉鎖を通知する CONNECTION_CLOSE フレーム
    /// (RFC 9000 Section 19.19)
    pub const CONNECTION_CLOSE: u64 = 0x1c;
    /// アプリケーションエラーによる接続閉鎖を通知する CONNECTION_CLOSE フレーム
    /// (RFC 9000 Section 19.19)
    pub const APPLICATION_CLOSE: u64 = 0x1d;
    /// HANDSHAKE_DONE フレーム (RFC 9000 Section 19.20)
    pub const HANDSHAKE_DONE: u64 = 0x1e;

    // DATAGRAM 拡張 (RFC 9221 Section 4)
    /// 長さフィールドを持たない DATAGRAM フレーム (RFC 9221 Section 4)
    pub const DATAGRAM: u64 = 0x30;
    /// 長さフィールドを持つ DATAGRAM フレーム (RFC 9221 Section 4)
    pub const DATAGRAM_LEN: u64 = 0x31;

    // Reliable Stream Reset 拡張 (draft-ietf-quic-reliable-stream-reset-11 Section 4)
    // QMux は本拡張をネゴシエートしないため、受信しても未知のフレームとして拒否する。
    // 値は暫定値であり、RFC 化時に変更される可能性がある。
    /// RESET_STREAM_AT フレーム (draft-ietf-quic-reliable-stream-reset-11 Section 4)
    ///
    /// QMux は本拡張をネゴシエートしないため、受信しても未知のフレームタイプとして
    /// FRAME_ENCODING_ERROR で拒否する。
    pub const RESET_STREAM_AT: u64 = 0x24;
}

pub mod connection_close;
pub mod data_blocked;
pub mod datagram;
pub mod max_data;
pub mod max_stream_data;
pub mod max_streams;
pub mod padding;
pub mod reset_stream;
pub mod stop_sending;
pub mod stream;
pub mod stream_data_blocked;
pub mod streams_blocked;

/// QMux が利用する QUIC version 1 のフレーム
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// PADDING フレーム (RFC 9000 Section 19.1)
    Padding(padding::Padding),
    /// RESET_STREAM フレーム (RFC 9000 Section 19.4)
    ResetStream(reset_stream::ResetStream),
    /// STOP_SENDING フレーム (RFC 9000 Section 19.5)
    StopSending(stop_sending::StopSending),
    /// STREAM フレーム (RFC 9000 Section 19.8)
    Stream(stream::Stream),
    /// MAX_DATA フレーム (RFC 9000 Section 19.9)
    MaxData(max_data::MaxData),
    /// MAX_STREAM_DATA フレーム (RFC 9000 Section 19.10)
    MaxStreamData(max_stream_data::MaxStreamData),
    /// MAX_STREAMS フレーム (RFC 9000 Section 19.11)
    MaxStreams(max_streams::MaxStreams),
    /// DATA_BLOCKED フレーム (RFC 9000 Section 19.12)
    DataBlocked(data_blocked::DataBlocked),
    /// STREAM_DATA_BLOCKED フレーム (RFC 9000 Section 19.13)
    StreamDataBlocked(stream_data_blocked::StreamDataBlocked),
    /// STREAMS_BLOCKED フレーム (RFC 9000 Section 19.14)
    StreamsBlocked(streams_blocked::StreamsBlocked),
    /// CONNECTION_CLOSE フレーム (RFC 9000 Section 19.19)
    ConnectionClose(connection_close::ConnectionClose),
    /// DATAGRAM フレーム (RFC 9221 Section 4)
    Datagram(datagram::Datagram),
}

impl Frame {
    /// バッファから許可されたフレームを 1 つデコードする
    ///
    /// 成功時は (フレーム, 消費バイト数) を返す。QMux で利用が禁止されたフレーム、
    /// 利用が許可されていないフレームタイプは `FRAME_ENCODING_ERROR` を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), Error> {
        if buf.is_empty() {
            return Err(Error::Incomplete { needed: 1 });
        }

        // PADDING フレームは特殊: タイプバイトが 0x00
        if buf[0] == 0x00 {
            let (frame, size) = padding::Padding::decode(buf)?;
            return Ok((Frame::Padding(frame), size));
        }

        let (frame_type, type_size) = VarInt::decode(buf)?;
        let ft = frame_type.as_u64();

        // RFC 9000 Section 12.4: Frame Type は最短エンコードを使用しなければならない (MUST)。
        // 非最短形の受信を PROTOCOL_VIOLATION として拒否するのは MAY だが、
        // デコードの正準性 (同一フレームが一意のバイト列に対応) と
        // フレーム型ガードの一貫性を保つため、本実装ではこの MAY を採用する。
        // RFC 9000 Section 16: 最短エンコードが要求されるのは Frame Type フィールドのみで、
        // payload 内の varint には適用しない。
        if type_size != frame_type.encoded_size() {
            return Err(Error::Protocol {
                code: crate::codec::error::TransportErrorCode::PROTOCOL_VIOLATION,
                reason: format!(
                    "frame type 0x{ft:x} uses non-minimal varint encoding ({type_size} bytes)"
                ),
            });
        }

        let payload = &buf[type_size..];

        match ft {
            frame_type::RESET_STREAM => {
                let (frame, size) = reset_stream::ResetStream::decode_payload(payload)?;
                Ok((Frame::ResetStream(frame), type_size + size))
            }
            frame_type::STOP_SENDING => {
                let (frame, size) = stop_sending::StopSending::decode_payload(payload)?;
                Ok((Frame::StopSending(frame), type_size + size))
            }
            frame_type::STREAM_START..=frame_type::STREAM_END => {
                let (frame, size) = stream::Stream::decode_payload(payload, ft)?;
                Ok((Frame::Stream(frame), type_size + size))
            }
            frame_type::MAX_DATA => {
                let (frame, size) = max_data::MaxData::decode_payload(payload)?;
                Ok((Frame::MaxData(frame), type_size + size))
            }
            frame_type::MAX_STREAM_DATA => {
                let (frame, size) = max_stream_data::MaxStreamData::decode_payload(payload)?;
                Ok((Frame::MaxStreamData(frame), type_size + size))
            }
            frame_type::MAX_STREAMS_BIDI | frame_type::MAX_STREAMS_UNI => {
                let (frame, size) = max_streams::MaxStreams::decode_payload(payload, ft)?;
                Ok((Frame::MaxStreams(frame), type_size + size))
            }
            frame_type::DATA_BLOCKED => {
                let (frame, size) = data_blocked::DataBlocked::decode_payload(payload)?;
                Ok((Frame::DataBlocked(frame), type_size + size))
            }
            frame_type::STREAM_DATA_BLOCKED => {
                let (frame, size) =
                    stream_data_blocked::StreamDataBlocked::decode_payload(payload)?;
                Ok((Frame::StreamDataBlocked(frame), type_size + size))
            }
            frame_type::STREAMS_BLOCKED_BIDI | frame_type::STREAMS_BLOCKED_UNI => {
                let (frame, size) = streams_blocked::StreamsBlocked::decode_payload(payload, ft)?;
                Ok((Frame::StreamsBlocked(frame), type_size + size))
            }
            frame_type::CONNECTION_CLOSE | frame_type::APPLICATION_CLOSE => {
                let (frame, size) = connection_close::ConnectionClose::decode_payload(payload, ft)?;
                Ok((Frame::ConnectionClose(frame), type_size + size))
            }
            // DATAGRAM 拡張 (RFC 9221 Section 4)
            frame_type::DATAGRAM => {
                let (frame, size) = datagram::Datagram::decode_payload_no_len(payload);
                Ok((Frame::Datagram(frame), type_size + size))
            }
            frame_type::DATAGRAM_LEN => {
                let (frame, size) = datagram::Datagram::decode_payload_with_len(payload)?;
                Ok((Frame::Datagram(frame), type_size + size))
            }
            _ => Err(Error::Protocol {
                code: crate::codec::error::TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: format!("frame type 0x{ft:x} is not allowed in QMux"),
            }),
        }
    }

    /// バッファにフレームをエンコードする
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Frame::Padding(f) => f.encode(buf),
            Frame::ResetStream(f) => f.encode(buf),
            Frame::StopSending(f) => f.encode(buf),
            Frame::Stream(f) => f.encode(buf),
            Frame::MaxData(f) => f.encode(buf),
            Frame::MaxStreamData(f) => f.encode(buf),
            Frame::MaxStreams(f) => f.encode(buf),
            Frame::DataBlocked(f) => f.encode(buf),
            Frame::StreamDataBlocked(f) => f.encode(buf),
            Frame::StreamsBlocked(f) => f.encode(buf),
            Frame::ConnectionClose(f) => f.encode(buf),
            Frame::Datagram(f) => f.encode(buf),
        }
    }
}
