//! tokio-qmux のエラー

use core::fmt;

use shiguredo_qmux::codec::error::Error as QmuxError;

/// tokio-qmux のエラー
///
/// 下位トランスポートの I/O エラーの詳細 (EOF、ECONNRESET、TLS の fatal alert
/// など) はこの型では保持しない。ドライバは読み取り・書き込みの I/O エラーを
/// 下位トランスポートの閉塞 (draft-ietf-quic-qmux-02 Section 7.4) として扱い、
/// [`crate::Connection::closed`] が
/// [`shiguredo_qmux::event::CloseReason::TransportClosed`] を返す。I/O エラーの
/// 診断に必要な情報は、下位トランスポートを構成する呼び出し側で記録すること。
#[derive(Debug)]
pub enum Error {
    /// QMux プロトコル層のエラー
    Qmux(QmuxError),
    /// 接続が閉じた
    ConnectionClosed,
    /// ピアがストリームをリセットした
    Reset(u64),
    /// ピアがストリームの送信停止を要求した (STOP_SENDING)
    Stopped(u64),
    /// 送信側が終端済みのストリームへ書き込もうとした
    ///
    /// FIN または RESET_STREAM を送った後のストリームへの書き込みである。
    /// ピアの STOP_SENDING で送信側が終端した場合は [`Error::Stopped`] を返す。
    StreamFinished,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Qmux(error) => write!(f, "QMux error: {error}"),
            Self::ConnectionClosed => write!(f, "connection is closed"),
            Self::Reset(code) => write!(f, "stream was reset by the peer (error code {code})"),
            Self::Stopped(code) => {
                write!(f, "the peer stopped the stream (error code {code})")
            }
            Self::StreamFinished => write!(f, "the stream has already been finished"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Qmux(error) => Some(error),
            _ => None,
        }
    }
}

impl From<QmuxError> for Error {
    fn from(error: QmuxError) -> Self {
        Self::Qmux(error)
    }
}

/// tokio-qmux の結果型
pub type Result<T> = core::result::Result<T, Error>;
