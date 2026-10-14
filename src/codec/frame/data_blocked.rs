//! DATA_BLOCKED フレーム (RFC 9000 Section 19.12)

use alloc::vec::Vec;

use crate::codec::error::Error;
use crate::codec::varint::VarInt;

/// DATA_BLOCKED フレーム
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataBlocked {
    /// ブロックの原因となっている接続レベルの上限
    pub maximum_data: u64,
}

impl DataBlocked {
    /// フレームタイプを除いた本体をデコードする
    ///
    /// 成功時は (フレーム, 消費バイト数) を返す。
    pub fn decode_payload(buf: &[u8]) -> Result<(Self, usize), Error> {
        let (val, n) = VarInt::decode(buf)?;
        Ok((
            Self {
                maximum_data: val.as_u64(),
            },
            n,
        ))
    }

    /// フレームタイプを含めてバッファへエンコードする
    pub fn encode(&self, buf: &mut Vec<u8>) {
        VarInt::from(super::frame_type::DATA_BLOCKED as u8).encode_to_vec(buf);
        VarInt::try_from(self.maximum_data)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
    }
}
