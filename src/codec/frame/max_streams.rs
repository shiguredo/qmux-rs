//! MAX_STREAMS フレーム (RFC 9000 Section 19.11)

use alloc::vec::Vec;

use crate::codec::error::Error;
use crate::codec::varint::VarInt;

/// MAX_STREAMS フレーム
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxStreams {
    /// 双方向ストリームの上限かどうか
    pub bidirectional: bool,
    /// 累積のストリーム数上限
    pub maximum_streams: u64,
}

impl MaxStreams {
    /// フレームタイプから方向を決めて本体をデコードする
    ///
    /// 成功時は (フレーム, 消費バイト数) を返す。
    pub fn decode_payload(buf: &[u8], frame_type: u64) -> Result<(Self, usize), Error> {
        let (val, n) = VarInt::decode(buf)?;
        Ok((
            Self {
                bidirectional: frame_type == super::frame_type::MAX_STREAMS_BIDI,
                maximum_streams: val.as_u64(),
            },
            n,
        ))
    }

    /// フレームタイプを含めてバッファへエンコードする
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let ft = if self.bidirectional {
            super::frame_type::MAX_STREAMS_BIDI
        } else {
            super::frame_type::MAX_STREAMS_UNI
        };
        VarInt::try_from(ft)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        VarInt::try_from(self.maximum_streams)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
    }
}
