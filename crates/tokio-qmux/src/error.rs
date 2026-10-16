//! tokio-qmux のエラー

use core::fmt;

use shiguredo_qmux::codec::error::Error as QmuxError;

/// tokio-qmux のエラー
#[derive(Debug)]
pub enum Error {
    /// QMux プロトコル層のエラー
    Qmux(QmuxError),
    /// 下位トランスポートの I/O エラー
    Io(std::io::Error),
    /// 接続が閉じた
    ConnectionClosed,
    /// ピアがストリームをリセットした
    Reset(u64),
    /// ピアがストリームの送信停止を要求した (STOP_SENDING)
    Stopped(u64),
    /// ドライバタスクが停止した
    DriverStopped,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Qmux(error) => write!(f, "QMux error: {error}"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::ConnectionClosed => write!(f, "connection is closed"),
            Self::Reset(code) => write!(f, "stream was reset by the peer (error code {code})"),
            Self::Stopped(code) => {
                write!(f, "the peer stopped the stream (error code {code})")
            }
            Self::DriverStopped => write!(f, "driver task has stopped"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Qmux(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<QmuxError> for Error {
    fn from(error: QmuxError) -> Self {
        Self::Qmux(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// tokio-qmux の結果型
pub type Result<T> = core::result::Result<T, Error>;
