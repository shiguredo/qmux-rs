//! QMux で送受信するフレーム (draft-ietf-quic-qmux-02 Section 4)
//!
//! QMux は QUIC version 1 のフレーム形式をそのまま使う (Section 4)。
//! 本モジュールは QMux 固有の QX_TRANSPORT_PARAMETERS / QX_PING と、
//! QMux で利用が許可された QUIC version 1 フレームを扱う。

use alloc::string::ToString;
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::frame::{self, Frame};
use crate::codec::varint::VarInt;

/// QMux 固有のフレームタイプ (Section 4.2, Section 4.3)
pub mod frame_type {
    /// QX_TRANSPORT_PARAMETERS フレーム (Section 4.2)
    ///
    /// 値はドラフトが IANA に provisional として登録を要求しているコードポイントで
    /// (Section 13.1)、RFC 化の際に恒久的な値へ置き換えられる可能性がある。
    ///
    /// 8 バイトの可変長整数でエンコードした先頭バイトが 0xff になるよう選ばれた
    /// マジックナンバーで、HTTP/1.1 / HTTP/2 との判別に使う。ドラフト本文はこの値の
    /// ワイヤ表現を `"\xffQMX\r\n\r\n"` と記載しているが、8 バイトの可変長整数として
    /// エンコードした実際のバイト列は ff 51 53 30 0d 0a 0d 0a になる。本実装は
    /// ドラフトのフレーム定義と IANA の表 (Section 13.1) が示すこの値に従う。
    pub const QX_TRANSPORT_PARAMETERS: u64 = 0x3f5153300d0a0d0a;
    /// QX_PING の要求 (Section 4.3)
    ///
    /// 値はドラフトの暫定コードポイントであり、RFC 化の際に変更される可能性がある。
    pub const QX_PING: u64 = 0x348c67529ef8c7bd;
    /// QX_PING の応答 (Section 4.3)
    ///
    /// 値はドラフトの暫定コードポイントであり、RFC 化の際に変更される可能性がある。
    pub const QX_PING_ACK: u64 = 0x348c67529ef8c7be;
}

/// QX_TRANSPORT_PARAMETERS フレーム (Section 4.2)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QxTransportParameters {
    /// エンコードされたトランスポートパラメータ (RFC 9000 Section 18 の形式)
    pub params: Vec<u8>,
}

/// QX_PING フレーム (Section 4.3)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QxPing {
    /// 到達性確認の要求 (タイプ 0x348c67529ef8c7bd)
    Request {
        /// ピングを識別するシーケンス番号
        sequence: VarInt,
    },
    /// 要求への応答 (タイプ 0x348c67529ef8c7be)
    Response {
        /// 要求が持っていたシーケンス番号
        sequence: VarInt,
    },
}

/// QMux で送受信するフレーム
#[derive(Debug, Clone, PartialEq)]
pub enum QmuxFrame {
    /// QX_TRANSPORT_PARAMETERS フレーム
    TransportParameters(QxTransportParameters),
    /// QX_PING フレーム
    Ping(QxPing),
    /// QMux で利用が許可された QUIC version 1 のフレーム
    Quic(Frame),
}

/// QMux で利用が禁止されている QUIC version 1 のフレームかどうか (Section 4)
///
/// 暗号ハンドシェイク用のフレーム、コネクション ID とパス移行に関するフレーム、
/// ACK を使用しない QMux では不要なフレームが禁止されている。
pub fn is_prohibited(frame_type: u64) -> bool {
    matches!(
        frame_type,
        frame::frame_type::PING
            | frame::frame_type::ACK
            | frame::frame_type::ACK_ECN
            | frame::frame_type::CRYPTO
            | frame::frame_type::NEW_TOKEN
            | frame::frame_type::NEW_CONNECTION_ID
            | frame::frame_type::RETIRE_CONNECTION_ID
            | frame::frame_type::PATH_CHALLENGE
            | frame::frame_type::PATH_RESPONSE
            | frame::frame_type::HANDSHAKE_DONE
    )
}

/// QMux で利用が許可されている QUIC version 1 のフレームかどうか (Section 4, Section 9.1)
///
/// DATAGRAM は RFC 9221 の拡張として、トランスポートパラメータでネゴシエートした
/// 場合にのみ利用できる。ネゴシエーションの判定は [`crate::connection::Connection`]
/// の責務であり、ここでは形式として利用が許可されているかだけを判定する。
pub fn is_allowed(frame_type: u64) -> bool {
    matches!(
        frame_type,
        frame::frame_type::PADDING
            | frame::frame_type::RESET_STREAM
            | frame::frame_type::STOP_SENDING
            | frame::frame_type::MAX_DATA
            | frame::frame_type::MAX_STREAM_DATA
            | frame::frame_type::MAX_STREAMS_BIDI
            | frame::frame_type::MAX_STREAMS_UNI
            | frame::frame_type::DATA_BLOCKED
            | frame::frame_type::STREAM_DATA_BLOCKED
            | frame::frame_type::STREAMS_BLOCKED_BIDI
            | frame::frame_type::STREAMS_BLOCKED_UNI
            | frame::frame_type::CONNECTION_CLOSE
            | frame::frame_type::APPLICATION_CLOSE
            | frame::frame_type::DATAGRAM
            | frame::frame_type::DATAGRAM_LEN
    ) || (frame::frame_type::STREAM_START..=frame::frame_type::STREAM_END).contains(&frame_type)
}

/// フレームを 1 つデコードする
///
/// 成功時は (フレーム, 消費バイト数) を返す。QMux で禁止されたフレーム、QMux で
/// 利用が許可されていないフレームタイプは `FRAME_ENCODING_ERROR` を返す
/// (Section 4, RFC 9000 Section 12.4)。フレームタイプは最短の可変長整数で
/// エンコードされていなければならず、非最短形は `PROTOCOL_VIOLATION` を返す
/// (RFC 9000 Section 12.4)。
///
/// # 途中データの扱い
///
/// 入力にフレームの全体が揃っていない場合は [`Error::Incomplete`] を返す。この関数は
/// Record の境界を知らないため、フレームが途中で途切れているのか、単に続きの
/// バイト列がまだ届いていないのかを区別できない。
///
/// Record の Frames フィールドをデコードする場合、Record の終端で
/// [`Error::Incomplete`] が返ったらフレームが Record 境界で途切れていることを意味する。
/// 呼び出し側はこれを `FRAME_ENCODING_ERROR` へ変換しなければならない
/// (Section 3.2)。[`decode_record`] はこの変換を行う。ストリームの途中から届いた
/// バイト列を逐次デコードする場合など、Record 境界以外で使う場合は、続きの
/// バイト列を渡して再試行すればよい。
///
/// # フレーム分類の優先順位
///
/// 最初のフレームが禁止フレームまたは未知フレームの場合、Section 4 の
/// 「禁止フレームと未知フレームは FRAME_ENCODING_ERROR」と Section 4.2 の
/// 「最初のフレームは QX_TRANSPORT_PARAMETERS」(PROTOCOL_VIOLATION) の両方が
/// 適用される。本実装はフレーム形式の判定を先に行うため、この場合は Section 4 の
/// `FRAME_ENCODING_ERROR` を優先する。どちらの場合も接続は閉じるため、相互運用上の
/// 影響はエラーコードの違いだけである。
pub fn decode(buf: &[u8]) -> Result<(QmuxFrame, usize), Error> {
    // フレームタイプは最短の可変長整数でエンコードしなければならない。
    // RFC 9000 Section 12.4 が唯一の例外として Frame Type に最短形を要求し、
    // 非最短形の受信を PROTOCOL_VIOLATION として扱うことを許している
    // (Section 16 は「Frame Type 以外は最短形でなくてよい」と述べている)。
    let (frame_type, type_size) = VarInt::decode(buf)?;
    let ft = frame_type.as_u64();
    if type_size != frame_type.encoded_size() {
        return Err(Error::Protocol {
            code: TransportErrorCode::PROTOCOL_VIOLATION,
            reason: format!("frame type 0x{ft:x} uses non-minimal varint encoding"),
        });
    }

    match ft {
        frame_type::QX_TRANSPORT_PARAMETERS => {
            let rest = &buf[type_size..];
            let (length, length_size) = VarInt::decode(rest)?;
            let len = usize::try_from(length.as_u64()).map_err(|_| Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: "QX_TRANSPORT_PARAMETERS length cannot be represented on this platform"
                    .to_string(),
            })?;
            let payload = &rest[length_size..];
            // Record の終端を越える長さを宣言するフレームは不正 (Section 3.2)
            if payload.len() < len {
                return Err(Error::Protocol {
                    code: TransportErrorCode::FRAME_ENCODING_ERROR,
                    reason: format!(
                        "QX_TRANSPORT_PARAMETERS length {len} exceeds the remaining QMux record"
                    ),
                });
            }
            Ok((
                QmuxFrame::TransportParameters(QxTransportParameters {
                    params: payload[..len].to_vec(),
                }),
                type_size + length_size + len,
            ))
        }
        frame_type::QX_PING | frame_type::QX_PING_ACK => {
            let (sequence, sequence_size) = VarInt::decode(&buf[type_size..])?;
            let ping = if ft == frame_type::QX_PING {
                QxPing::Request { sequence }
            } else {
                QxPing::Response { sequence }
            };
            Ok((QmuxFrame::Ping(ping), type_size + sequence_size))
        }
        _ => {
            if is_prohibited(ft) {
                return Err(Error::Protocol {
                    code: TransportErrorCode::FRAME_ENCODING_ERROR,
                    reason: format!("frame type 0x{ft:x} is prohibited in QMux"),
                });
            }
            // RFC 9000 Section 12.4: 未知のフレームタイプは FRAME_ENCODING_ERROR。
            // RESET_STREAM_AT は QMux で利用が許可されている拡張だが、本実装は
            // ネゴシエートしないため未知のフレームタイプとして扱う。
            if !is_allowed(ft) {
                return Err(Error::Protocol {
                    code: TransportErrorCode::FRAME_ENCODING_ERROR,
                    reason: format!("frame type 0x{ft:x} is not allowed in QMux"),
                });
            }
            let (frame, size) = Frame::decode(buf)?;
            Ok((QmuxFrame::Quic(frame), size))
        }
    }
}

/// QMux Record の Frames フィールドをフレーム列へデコードする
///
/// Record の終端がフレーム境界に一致しない場合は `FRAME_ENCODING_ERROR` を返す
/// (Section 3.2)。Frames は 1 つ以上のフレームを含まなければならない。
pub fn decode_record(frames: &[u8]) -> Result<Vec<QmuxFrame>, Error> {
    if frames.is_empty() {
        // RFC 9000 Section 12.4: フレームを含まないペイロードは PROTOCOL_VIOLATION
        // (QMux Section 3.2 も Frames が 1 つ以上のフレームを含むことを要求する)
        return Err(Error::Protocol {
            code: TransportErrorCode::PROTOCOL_VIOLATION,
            reason: "QMux record contains no frames".to_string(),
        });
    }

    let mut decoded = Vec::new();
    let mut offset = 0;
    while offset < frames.len() {
        match decode(&frames[offset..]) {
            Ok((frame, size)) => {
                // 0 バイトを消費するフレームは存在しない。デコーダの不具合で
                // 無限ループに入らないための防御。
                if size == 0 {
                    return Err(Error::Protocol {
                        code: TransportErrorCode::INTERNAL_ERROR,
                        reason: "frame decoder consumed no bytes".to_string(),
                    });
                }
                offset += size;
                decoded.push(frame);
            }
            // フレームが Record の終端で途切れている (Section 3.2)
            Err(Error::Incomplete { .. }) => {
                return Err(Error::Protocol {
                    code: TransportErrorCode::FRAME_ENCODING_ERROR,
                    reason: format!(
                        "frame at offset {offset} is truncated by the QMux record boundary"
                    ),
                });
            }
            Err(e) => return Err(e),
        }
    }
    Ok(decoded)
}

impl QmuxFrame {
    /// フレームをバッファへエンコードする
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            QmuxFrame::TransportParameters(frame) => {
                VarInt::try_from(frame_type::QX_TRANSPORT_PARAMETERS)
                    .expect("QX_TRANSPORT_PARAMETERS fits in a variable-length integer")
                    .encode_to_vec(buf);
                VarInt::try_from(frame.params.len() as u64)
                    .expect("transport parameters length fits in a variable-length integer")
                    .encode_to_vec(buf);
                buf.extend_from_slice(&frame.params);
            }
            QmuxFrame::Ping(ping) => {
                let (ft, sequence) = match ping {
                    QxPing::Request { sequence } => (frame_type::QX_PING, *sequence),
                    QxPing::Response { sequence } => (frame_type::QX_PING_ACK, *sequence),
                };
                VarInt::try_from(ft)
                    .expect("QX_PING frame type fits in a variable-length integer")
                    .encode_to_vec(buf);
                sequence.encode_to_vec(buf);
            }
            QmuxFrame::Quic(frame) => frame.encode(buf),
        }
    }
}
