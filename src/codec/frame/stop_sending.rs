//! STOP_SENDING フレーム (RFC 9000 Section 19.5)

use alloc::vec::Vec;

use crate::codec::error::Error;
use crate::codec::varint::VarInt;

/// STOP_SENDING フレーム
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopSending {
    /// ストリーム ID
    pub stream_id: u64,
    /// アプリケーションが指定したエラーコード
    pub application_protocol_error_code: u64,
}

impl StopSending {
    /// フレームタイプを除いた本体をデコードする
    ///
    /// 成功時は (フレーム, 消費バイト数) を返す。
    pub fn decode_payload(buf: &[u8]) -> Result<(Self, usize), Error> {
        let mut offset = 0;
        let (stream_id, n) = VarInt::decode(&buf[offset..])?;
        offset += n;
        let (error_code, n) = VarInt::decode(&buf[offset..])?;
        offset += n;
        Ok((
            Self {
                stream_id: stream_id.as_u64(),
                application_protocol_error_code: error_code.as_u64(),
            },
            offset,
        ))
    }

    /// フレームタイプを含めてバッファへエンコードする
    pub fn encode(&self, buf: &mut Vec<u8>) {
        VarInt::from(super::frame_type::STOP_SENDING as u8).encode_to_vec(buf);
        VarInt::try_from(self.stream_id)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        VarInt::try_from(self.application_protocol_error_code)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
    }
}
