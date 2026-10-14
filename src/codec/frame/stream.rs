//! STREAM フレーム (RFC 9000 Section 19.8)

use alloc::string::ToString;
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::varint::VarInt;

/// STREAM フレーム
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    /// ストリーム ID
    pub stream_id: u64,
    /// データの開始オフセット
    pub offset: u64,
    /// ストリームの終端を表す FIN ビット
    pub fin: bool,
    /// ストリームデータ
    pub data: Vec<u8>,
}

impl Stream {
    /// フレームタイプからフラグをデコードする
    ///
    /// STREAM フレームタイプ (0x08-0x0f) のビットフィールド:
    /// - bit 0 (0x01): FIN
    /// - bit 1 (0x02): LEN (長さフィールドあり)
    /// - bit 2 (0x04): OFF (オフセットフィールドあり)
    ///
    /// 成功時は (フレーム, 消費バイト数) を返す。
    pub fn decode_payload(buf: &[u8], frame_type: u64) -> Result<(Self, usize), Error> {
        let has_off = frame_type & 0x04 != 0;
        let has_len = frame_type & 0x02 != 0;
        let has_fin = frame_type & 0x01 != 0;

        let mut pos = 0;

        let (stream_id, n) = VarInt::decode(&buf[pos..])?;
        pos += n;

        let offset = if has_off {
            let (off, n) = VarInt::decode(&buf[pos..])?;
            pos += n;
            off.as_u64()
        } else {
            0
        };

        let data = if has_len {
            let (length, n) = VarInt::decode(&buf[pos..])?;
            pos += n;
            // 32 ビット環境では可変長整数の長さが usize に収まらない可能性があるため、
            // 変換できることを確認してから長さの比較に使う。
            let len = usize::try_from(length.as_u64()).map_err(|_| Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: "STREAM length cannot be represented on this platform".to_string(),
            })?;
            if buf[pos..].len() < len {
                return Err(Error::Incomplete {
                    needed: len - buf[pos..].len(),
                });
            }
            let d = buf[pos..pos + len].to_vec();
            pos += len;
            d
        } else {
            // 長さフィールドがない場合は Record の残り全て (Section 3.2)
            let d = buf[pos..].to_vec();
            pos = buf.len();
            d
        };

        // RFC 9000 Section 19.8: offset と length の和は 2^62-1 を超えてはならない。
        // 加算が溢れる場合も不正なフレームとして扱う。
        let end_offset = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: format!(
                    "STREAM offset {offset} plus length {} overflows",
                    data.len()
                ),
            })?;
        if end_offset > crate::codec::varint::MAX_VALUE {
            return Err(Error::Protocol {
                code: crate::codec::error::TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: format!("STREAM offset + length = {end_offset} exceeds 2^62-1"),
            });
        }

        Ok((
            Self {
                stream_id: stream_id.as_u64(),
                offset,
                fin: has_fin,
                data,
            },
            pos,
        ))
    }

    /// フレームタイプを含めてバッファへエンコードする
    ///
    /// 常に長さフィールドを付ける。複数フレームを 1 つの Record に詰めるためである。
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut ft = super::frame_type::STREAM_START;
        if self.fin {
            ft |= 0x01;
        }
        ft |= 0x02;
        if self.offset > 0 {
            ft |= 0x04;
        }
        VarInt::try_from(ft)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        VarInt::try_from(self.stream_id)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        if self.offset > 0 {
            VarInt::try_from(self.offset)
                .expect("value fits in a variable-length integer")
                .encode_to_vec(buf);
        }
        VarInt::try_from(self.data.len() as u64)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        buf.extend_from_slice(&self.data);
    }
}
