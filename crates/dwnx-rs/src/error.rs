//! dwnx のライブラリエラー

use dwnx_sys as sys;

/// dwnx のライブラリエラー
///
/// dwnx の API が返す負のエラーコードと `dwnx_strerror` の説明を保持する。
/// Rust 側で検出した誤りも、dwnx に渡せば `DWNX_ERR_INVALID_ARGUMENT` に
/// なるため同じ型で表す。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    /// dwnx のエラーコード
    pub code: i32,
    /// `dwnx_strerror` が返す説明
    pub message: String,
}

impl Error {
    /// dwnx のエラーコードからエラーを作る
    pub(crate) fn from_code(code: i32) -> Self {
        let message = unsafe {
            let ptr = sys::dwnx_strerror(code);
            if ptr.is_null() {
                std::borrow::Cow::Borrowed("unknown dwnx error")
            } else {
                std::ffi::CStr::from_ptr(ptr).to_string_lossy()
            }
        };
        Self {
            code,
            message: message.into_owned(),
        }
    }

    /// 引数の検証に失敗したことを表すエラーを作る
    ///
    /// dwnx は不正なトランスポートパラメータを `assert` で弾き、プロセスを
    /// abort させる (dwnx の `lib/dwnx_conn.c` の `conn_new`)。Rust 側で同じ
    /// 検査を行い、abort ではなくこのエラーを返す。
    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            code: sys::DWNX_ERR_INVALID_ARGUMENT,
            message: message.into(),
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dwnx error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}
