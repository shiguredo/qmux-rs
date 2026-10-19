//! ライブラリのエラー型と QUIC トランスポートエラーコード (RFC 9000 Section 20.1)

use alloc::string::String;

/// QUIC トランスポートエラーコード (RFC 9000 Section 20.1)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransportErrorCode(u64);

impl TransportErrorCode {
    /// エラーなしで接続を閉じた (0x00)
    pub const NO_ERROR: Self = Self(0x0);
    /// 実装の内部でエラーが発生した (0x01)
    pub const INTERNAL_ERROR: Self = Self(0x1);
    /// サーバーが接続を受け付けなかった (0x02)
    pub const CONNECTION_REFUSED: Self = Self(0x2);
    /// ピアがフロー制御の上限を超えた (0x03)
    pub const FLOW_CONTROL_ERROR: Self = Self(0x3);
    /// ピアが同時に開けるストリーム数の上限を超えた (0x04)
    pub const STREAM_LIMIT_ERROR: Self = Self(0x4);
    /// ピアがストリームの状態では許されないフレームを送った (0x05)
    pub const STREAM_STATE_ERROR: Self = Self(0x5);
    /// ストリームの final size が変更された (0x06)
    pub const FINAL_SIZE_ERROR: Self = Self(0x6);
    /// 不正な形式のフレームを受信した (0x07)
    pub const FRAME_ENCODING_ERROR: Self = Self(0x7);
    /// 不正なトランスポートパラメータを受信した (0x08)
    pub const TRANSPORT_PARAMETER_ERROR: Self = Self(0x8);
    /// ピアが必要以上のコネクション ID を提供した (0x09)
    pub const CONNECTION_ID_LIMIT_ERROR: Self = Self(0x9);
    /// プロトコル違反を検出した (0x0a)
    pub const PROTOCOL_VIOLATION: Self = Self(0xa);
    /// 無効なトークンを受信した (0x0b)
    pub const INVALID_TOKEN: Self = Self(0xb);
    /// アプリケーションが接続を閉じた (0x0c)
    pub const APPLICATION_ERROR: Self = Self(0xc);
    /// CRYPTO フレーム用のバッファが上限を超えた (0x0d)
    pub const CRYPTO_BUFFER_EXCEEDED: Self = Self(0xd);
    /// TLS の鍵更新に失敗した (0x0e)
    pub const KEY_UPDATE_ERROR: Self = Self(0xe);
    /// AEAD の使用量が上限に達した (0x0f)
    pub const AEAD_LIMIT_REACHED: Self = Self(0xf);
    /// 利用可能なネットワークパスがなくなった (0x10)
    pub const NO_VIABLE_PATH: Self = Self(0x10);

    /// エラーコードの数値を返す
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for TransportErrorCode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {
            Self::NO_ERROR => write!(f, "NO_ERROR"),
            Self::INTERNAL_ERROR => write!(f, "INTERNAL_ERROR"),
            Self::CONNECTION_REFUSED => write!(f, "CONNECTION_REFUSED"),
            Self::FLOW_CONTROL_ERROR => write!(f, "FLOW_CONTROL_ERROR"),
            Self::STREAM_LIMIT_ERROR => write!(f, "STREAM_LIMIT_ERROR"),
            Self::STREAM_STATE_ERROR => write!(f, "STREAM_STATE_ERROR"),
            Self::FINAL_SIZE_ERROR => write!(f, "FINAL_SIZE_ERROR"),
            Self::FRAME_ENCODING_ERROR => write!(f, "FRAME_ENCODING_ERROR"),
            Self::TRANSPORT_PARAMETER_ERROR => write!(f, "TRANSPORT_PARAMETER_ERROR"),
            Self::CONNECTION_ID_LIMIT_ERROR => write!(f, "CONNECTION_ID_LIMIT_ERROR"),
            Self::PROTOCOL_VIOLATION => write!(f, "PROTOCOL_VIOLATION"),
            Self::INVALID_TOKEN => write!(f, "INVALID_TOKEN"),
            Self::APPLICATION_ERROR => write!(f, "APPLICATION_ERROR"),
            Self::CRYPTO_BUFFER_EXCEEDED => write!(f, "CRYPTO_BUFFER_EXCEEDED"),
            Self::KEY_UPDATE_ERROR => write!(f, "KEY_UPDATE_ERROR"),
            Self::AEAD_LIMIT_REACHED => write!(f, "AEAD_LIMIT_REACHED"),
            Self::NO_VIABLE_PATH => write!(f, "NO_VIABLE_PATH"),
            Self(code) if (0x100..=0x1ff).contains(&code) => {
                write!(f, "CRYPTO_ERROR(0x{:02x})", code & 0xff)
            }
            Self(code) => write!(f, "0x{code:x}"),
        }
    }
}

/// ライブラリのエラー型
///
/// エラーの種別ごとに、接続を閉じるべきかどうかが異なる。
/// [`Error::Protocol`] と [`Error::InvalidData`] の rustdoc を参照すること。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// プロトコルエラー
    ///
    /// この variant は使われる層によって契約が異なる。
    ///
    /// - コーデック層の受信側 API ([`crate::frame::decode`]、
    ///   [`crate::frame::decode_record`]、
    ///   [`crate::record::RecordDecoder::poll_record`]、
    ///   [`crate::transport_params::TransportParameters::decode`] など) が返すものは、
    ///   ピアの入力に起因する接続エラーを表す。呼び出し側は `code` を持つ
    ///   CONNECTION_CLOSE を送信して接続を閉じる。
    /// - [`crate::connection::Connection`] の各メソッドが返すものは、未作成の
    ///   ストリームへの操作、終端したストリームへの操作、ストリーム数の上限到達、
    ///   ピアのトランスポートパラメータ未受信のままの操作といったローカルの誤用を
    ///   表す。接続は閉じず、呼び出し側は操作をやり直すか諦める。
    ///
    /// ピアの違反を [`crate::connection::Connection::receive`] が検出した場合は、
    /// この variant を戻り値として返さない。CONNECTION_CLOSE を送信待ちに追加し、
    /// [`crate::event::Event::ConnectionClosed`] で通知する。
    ///
    /// ローカルの設定値やエンコード対象の不正は [`Error::InvalidData`] で返す。
    Protocol {
        /// 接続を閉じる原因となるトランスポートエラーコード
        code: TransportErrorCode,
        /// エラーの理由を表す人間向けの文字列
        reason: String,
    },
    /// 入力データ不足 (もっとデータが必要)
    ///
    /// デコード対象のバイト列が途中で途切れていることを表す。呼び出し側は
    /// 続きのバイト列を渡して再試行する。Record の境界でフレームが途切れている
    /// 場合、[`crate::frame::decode`] はこの variant を返し、呼び出し側が
    /// FRAME_ENCODING_ERROR へ変換する ([`crate::frame::decode_record`] は変換済みの
    /// エラーを返す)。
    Incomplete {
        /// 入力が揃うまでに必要な残りバイト数
        needed: usize,
    },
    /// ローカルの設定値またはエンコード対象が不正
    ///
    /// アプリケーションが渡した設定値 ([`crate::config::Config`] など) や、
    /// エンコード対象の値 ([`crate::record::encode`] の Frames など) の誤りを表す。
    /// ピアの入力に起因しないため、接続を閉じる必要はない。呼び出し側は値を
    /// 修正してから再試行する。
    InvalidData(String),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Protocol { code, reason } => {
                write!(f, "protocol error {code}: {reason}")
            }
            Self::Incomplete { needed } => {
                write!(f, "incomplete data: {needed} more bytes needed")
            }
            Self::InvalidData(msg) => {
                write!(f, "invalid data: {msg}")
            }
        }
    }
}

impl core::error::Error for Error {}
