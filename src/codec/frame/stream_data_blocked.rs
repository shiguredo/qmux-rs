//! STREAM_DATA_BLOCKED フレーム (RFC 9000 Section 19.13)

use alloc::vec::Vec;

use crate::codec::error::Error;
use crate::codec::varint::VarInt;

/// STREAM_DATA_BLOCKED フレーム
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDataBlocked {
    /// ストリーム ID
    pub stream_id: u64,
    /// ブロックの原因となっているストリーム単位の上限
    pub maximum_stream_data: u64,
}

impl StreamDataBlocked {
    /// フレームタイプを除いた本体をデコードする
    ///
    /// 成功時は (フレーム, 消費バイト数) を返す。
    pub fn decode_payload(buf: &[u8]) -> Result<(Self, usize), Error> {
        let mut pos = 0;
        let (stream_id, n) = VarInt::decode(&buf[pos..])?;
        pos += n;
        let (max_data, n) = VarInt::decode(&buf[pos..])?;
        pos += n;
        Ok((
            Self {
                stream_id: stream_id.as_u64(),
                maximum_stream_data: max_data.as_u64(),
            },
            pos,
        ))
    }

    /// フレームタイプを含めてバッファへエンコードする
    pub fn encode(&self, buf: &mut Vec<u8>) {
        VarInt::from(super::frame_type::STREAM_DATA_BLOCKED as u8).encode_to_vec(buf);
        VarInt::try_from(self.stream_id)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        VarInt::try_from(self.maximum_stream_data)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
    }
}
