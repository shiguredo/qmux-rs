//! ライブラリのエラー型と QUIC トランスポートエラーコード (RFC 9000 Section 20.1)

use alloc::string::String;

/// QUIC トランスポートエラーコード (RFC 9000 Section 20.1)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransportErrorCode(u64);

impl TransportErrorCode {
    pub const NO_ERROR: Self = Self(0x0);
    pub const INTERNAL_ERROR: Self = Self(0x1);
    pub const CONNECTION_REFUSED: Self = Self(0x2);
    pub const FLOW_CONTROL_ERROR: Self = Self(0x3);
    pub const STREAM_LIMIT_ERROR: Self = Self(0x4);
    pub const STREAM_STATE_ERROR: Self = Self(0x5);
    pub const FINAL_SIZE_ERROR: Self = Self(0x6);
    pub const FRAME_ENCODING_ERROR: Self = Self(0x7);
    pub const TRANSPORT_PARAMETER_ERROR: Self = Self(0x8);
    pub const CONNECTION_ID_LIMIT_ERROR: Self = Self(0x9);
    pub const PROTOCOL_VIOLATION: Self = Self(0xa);
    pub const INVALID_TOKEN: Self = Self(0xb);
    pub const APPLICATION_ERROR: Self = Self(0xc);
    pub const CRYPTO_BUFFER_EXCEEDED: Self = Self(0xd);
    pub const KEY_UPDATE_ERROR: Self = Self(0xe);
    pub const AEAD_LIMIT_REACHED: Self = Self(0xf);
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// プロトコルエラー (CONNECTION_CLOSE で相手に通知する)
    Protocol {
        code: TransportErrorCode,
        reason: String,
    },
    /// 入力データ不足 (もっとデータが必要)
    Incomplete { needed: usize },
    /// 不正な入力データ
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
