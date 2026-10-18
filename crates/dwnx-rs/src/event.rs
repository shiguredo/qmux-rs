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
    /// DATAGRAM を受信した (RFC 9221)
    ///
    /// dwnx は DATAGRAM を部分的に届けることがあるが、dwnx-rs が全体を
    /// 組み立てられた時点でこのイベントを 1 つ積む。空の DATAGRAM も届く。
    Datagram {
        /// 受信した DATAGRAM のデータ
        data: Vec<u8>,
    },
    /// 接続が閉じた
    ///
    /// アイドルタイムアウトの満了、または下位トランスポートの終了で発生する。
    ConnectionClosed,
}
