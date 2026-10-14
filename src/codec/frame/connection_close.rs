//! CONNECTION_CLOSE フレーム (RFC 9000 Section 19.19)

use alloc::string::ToString;
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::varint::VarInt;

/// CONNECTION_CLOSE フレーム
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionClose {
    /// true: アプリケーションエラー (0x1d)、false: トランスポートエラー (0x1c)
    pub application: bool,
    /// エラーコード
    pub error_code: u64,
    /// トランスポートエラーの場合のみ: エラーを引き起こしたフレームタイプ
    pub frame_type: Option<u64>,
    /// 理由を示す UTF-8 文字列
    pub reason_phrase: Vec<u8>,
}

impl ConnectionClose {
    /// フレームタイプからアプリケーションエラーかを決めて本体をデコードする
    ///
    /// 成功時は (フレーム, 消費バイト数) を返す。
    pub fn decode_payload(buf: &[u8], ft: u64) -> Result<(Self, usize), Error> {
        let application = ft == super::frame_type::APPLICATION_CLOSE;
        let mut pos = 0;

        let (error_code, n) = VarInt::decode(&buf[pos..])?;
        pos += n;

        let frame_type = if !application {
            let (ft_val, n) = VarInt::decode(&buf[pos..])?;
            pos += n;
            Some(ft_val.as_u64())
        } else {
            None
        };

        let (reason_len, n) = VarInt::decode(&buf[pos..])?;
        pos += n;
        // 32 ビット環境では可変長整数の長さが usize に収まらない可能性があるため、
        // 変換できることを確認してから長さの比較に使う。
        let len = usize::try_from(reason_len.as_u64()).map_err(|_| Error::Protocol {
            code: TransportErrorCode::FRAME_ENCODING_ERROR,
            reason: "reason phrase length cannot be represented on this platform".to_string(),
        })?;
        if buf[pos..].len() < len {
            return Err(Error::Incomplete {
                needed: len - buf[pos..].len(),
            });
        }
        let reason_phrase = buf[pos..pos + len].to_vec();
        pos += len;

        Ok((
            Self {
                application,
                error_code: error_code.as_u64(),
                frame_type,
                reason_phrase,
            },
            pos,
        ))
    }

    /// フレームタイプを含めてバッファへエンコードする
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let ft = if self.application {
            super::frame_type::APPLICATION_CLOSE
        } else {
            super::frame_type::CONNECTION_CLOSE
        };
        VarInt::try_from(ft)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        VarInt::try_from(self.error_code)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        if let Some(frame_type) = self.frame_type {
            VarInt::try_from(frame_type)
                .expect("value fits in a variable-length integer")
                .encode_to_vec(buf);
        }
        VarInt::try_from(self.reason_phrase.len() as u64)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        buf.extend_from_slice(&self.reason_phrase);
    }
}
