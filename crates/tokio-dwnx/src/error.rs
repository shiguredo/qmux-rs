//! tokio-dwnx のエラー

use core::fmt;

/// tokio-dwnx のエラー
#[derive(Debug)]
pub enum Error {
    /// dwnx のエラー
    Dwnx(dwnx_rs::Error),
    /// 下位トランスポートの I/O エラー
    Io(std::io::Error),
    /// 接続が閉じた
    ConnectionClosed,
    /// 送信待ちの DATAGRAM が上限に達した
    ///
    /// [`crate::Connection::send_datagram`] が積もうとした DATAGRAM は送信されて
    /// いない。送信待ちが減ってから送り直すこと。
    DatagramQueueFull,
    /// ピアがストリームをリセットした
    Reset(u64),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dwnx(error) => write!(f, "dwnx error: {error}"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::ConnectionClosed => write!(f, "connection is closed"),
            Self::DatagramQueueFull => write!(f, "datagram send queue is full"),
            Self::Reset(code) => write!(f, "stream was reset by the peer (error code {code})"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Dwnx(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<dwnx_rs::Error> for Error {
    fn from(error: dwnx_rs::Error) -> Self {
        Self::Dwnx(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// tokio-dwnx の結果型
pub type Result<T> = core::result::Result<T, Error>;
