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
    /// ストリームの送信上限が増えた
    MaxStreamData {
        /// ストリーム ID
        id: i64,
        /// 累積の送信上限
        max_data: u64,
    },
    /// ストリーム数の上限が増えた
    MaxStreams {
        /// 双方向ストリームかどうか
        bidirectional: bool,
        /// ローカル起動のストリーム数の上限かどうか
        local: bool,
        /// 累積のストリーム数上限
        max_streams: u64,
    },
    /// ストリームデータを送信した
    StreamDataWritten {
        /// ストリーム ID
        id: i64,
        /// 開始オフセット
        offset: u64,
        /// 送信した長さ
        len: usize,
    },
    /// 接続が閉じた
    ConnectionClosed,
}
