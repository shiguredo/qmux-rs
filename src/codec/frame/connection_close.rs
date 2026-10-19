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
    /// トランスポートエラー (0x1c) の場合のみ: エラーを引き起こしたフレームタイプ
    ///
    /// `None` は引き起こしたフレームタイプが不明であることを表す。0x1c では
    /// フィールド自体が必須のため、[`ConnectionClose::encode`] は `None` を
    /// 未知を表す 0 として出力する (RFC 9000 Section 19.19)。アプリケーション
    /// エラー (0x1d) ではこのフィールドは使われず、値にかかわらず出力しない。
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
    ///
    /// RFC 9000 Section 19.19: Frame Type フィールドは 0x1c (トランスポートエラー)
    /// だけが持つ。0x1d (アプリケーションエラー) では出力しない。0x1c では
    /// [`ConnectionClose::frame_type`] が `None` でも必ずフィールドを出力し、未知の
    /// フレームタイプを表す 0 を書き込む (RFC 9000 Section 19.19)。
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
        // RFC 9000 Section 19.19: Frame Type フィールドは 0x1c (トランスポート
        // エラー) のみが持つ。0x1d (アプリケーションエラー) では送ってはならない。
        // フィールドの有無は self.application ではなくフレームタイプで決まるため、
        // frame_type が Some でも application が true のときは出力しない。
        // 逆に 0x1c では frame_type が None でもフィールド自体は必須であり、
        // 未知のフレームタイプを表す 0 を出力する。省略するとデコード側が
        // reason phrase の長さを Frame Type として読む不正フレームになる。
        if !self.application {
            VarInt::try_from(self.frame_type.unwrap_or(0))
                .expect("value fits in a variable-length integer")
                .encode_to_vec(buf);
        }
        VarInt::try_from(self.reason_phrase.len() as u64)
            .expect("value fits in a variable-length integer")
            .encode_to_vec(buf);
        buf.extend_from_slice(&self.reason_phrase);
    }
}
