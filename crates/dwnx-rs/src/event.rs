//! dwnx 接続から通知されるイベント

use crate::config::TransportParameters;

/// dwnx 接続のイベント
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// ピアのトランスポートパラメータを受信した
    TransportParametersReceived(TransportParameters),
    /// ピアがストリームを開いた
    StreamOpened {
        /// ストリーム ID
        id: i64,
    },
    /// ストリームデータを受信した
    StreamData {
        /// ストリーム ID
        id: i64,
        /// データの開始オフセット
        offset: u64,
        /// ストリームの終端かどうか
        fin: bool,
        /// 受信したデータ
        data: Vec<u8>,
    },
    /// ストリームが閉じた
    StreamClosed {
        /// ストリーム ID
        id: i64,
        /// 受信側を閉じたアプリケーションエラーコード
        rx_error_code: Option<u64>,
        /// 送信側を閉じたアプリケーションエラーコード
        tx_error_code: Option<u64>,
    },
    /// ピアがストリームをリセットした
    StreamReset {
        /// ストリーム ID
        id: i64,
        /// 最終サイズ
        final_size: u64,
        /// アプリケーションエラーコード
        error_code: u64,
    },
    /// STOP_SENDING を受信した
    RecvStopSending {
        /// ストリーム ID
        id: i64,
        /// アプリケーションエラーコード
        error_code: u64,
    },
    /// 接続が閉じた
    ///
    /// アイドルタイムアウトの満了、または下位トランスポートの終了で発生する。
    ConnectionClosed,
}
