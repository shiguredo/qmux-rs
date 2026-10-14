//! DATAGRAM フレーム (RFC 9221 Section 4)
//!
//! フレームタイプ 0x30 (LEN ビットなし) と 0x31 (LEN ビットあり) の
//! エンコード/デコードを提供する。QMux では Record の境界がフレームの
//! payload 境界になるため、LEN ビットなしの DATAGRAM は Record の終端までを
//! payload として読み取る (draft-ietf-quic-qmux-02 Section 3.2)。

use alloc::string::ToString;
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::varint::VarInt;

/// DATAGRAM フレーム
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datagram {
    /// データグラムのペイロード
    pub data: Vec<u8>,
}

impl Datagram {
    /// LEN ビットなし (0x30) のデコード
    ///
    /// payload の残り全体をデータとして読み取り、(フレーム, 消費バイト数) を返す。
    /// 呼び出し元は Record 末尾までのスライスを渡す必要がある。
    pub fn decode_payload_no_len(buf: &[u8]) -> (Self, usize) {
        let data = buf.to_vec();
        let len = data.len();
        (Self { data }, len)
    }

    /// LEN ビットあり (0x31) のデコード
    ///
    /// Length フィールドが示す長さ分をデータとして読み取り、
    /// (フレーム, 消費バイト数) を返す。
    pub fn decode_payload_with_len(buf: &[u8]) -> Result<(Self, usize), Error> {
        let (length, n) = VarInt::decode(buf)?;
        // 32 ビット環境では可変長整数の長さが usize に収まらない可能性があるため、
        // 変換できることを確認してから長さの比較に使う。
        let length = usize::try_from(length.as_u64()).map_err(|_| Error::Protocol {
            code: TransportErrorCode::FRAME_ENCODING_ERROR,
            reason: "DATAGRAM length cannot be represented on this platform".to_string(),
        })?;
        let payload = &buf[n..];
        if payload.len() < length {
            return Err(Error::Incomplete {
                needed: length - payload.len(),
            });
        }
        let data = payload[..length].to_vec();
        Ok((Self { data }, n + length))
    }

    /// LEN ビットあり (0x31) でエンコードする
    ///
    /// 複数フレームを 1 つの Record に含められるよう、常に Length フィールドを付与する。
    pub fn encode(&self, buf: &mut Vec<u8>) {
        VarInt::from(super::frame_type::DATAGRAM_LEN as u8).encode_to_vec(buf);
        VarInt::try_from(self.data.len() as u64)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        buf.extend_from_slice(&self.data);
    }
}
