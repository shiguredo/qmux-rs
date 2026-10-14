//! QMux 接続からアプリケーションへ通知するイベント

use alloc::string::String;
use alloc::vec::Vec;

use crate::stream::StreamId;

/// 接続が閉じた理由 (draft-ietf-quic-qmux-02 Section 7)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseReason {
    /// アイドルタイムアウトが満了した (Section 7.1)
    IdleTimeout,
    /// ローカル側から切断した
    ///
    /// アプリケーションの [`crate::connection::Connection::close`] による切断と、
    /// ピアのフレームから検出したプロトコル違反による切断の両方を含む。
    Local {
        /// アプリケーションエラー (CONNECTION_CLOSE の 0x1d) かどうか
        application: bool,
        /// 通知したエラーコード
        error_code: u64,
        /// 通知した理由
        reason: String,
    },
    /// ピアが CONNECTION_CLOSE フレームを送ってきた (Section 7.3)
    Peer {
        /// アプリケーションエラー (CONNECTION_CLOSE の 0x1d) かどうか
        application: bool,
        /// ピアが通知したエラーコード
        error_code: u64,
        /// ピアが通知した理由
        reason: String,
    },
    /// 下位トランスポートが閉じたことをアプリケーションが通知した (Section 7.4)
    TransportClosed,
}

/// QMux 接続のイベント
///
/// [`crate::connection::Connection::poll_event`] で取り出す。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// ピアのトランスポートパラメータを受信し、ストリームと DATAGRAM が使えるようになった
    TransportParametersReceived,
    /// ピアがストリームを開いた
    StreamOpened {
        /// 開かれたストリーム
        id: StreamId,
    },
    /// ストリームにデータが到着し、読み取り可能になった
    StreamReadable {
        /// 読み取り可能になったストリーム
        id: StreamId,
    },
    /// フロー制御クレジットが増え、ストリームへ書き込めるようになった
    StreamWritable {
        /// 書き込み可能になったストリーム
        id: StreamId,
    },
    /// ピアがストリーム数の上限を増やし、ストリームを開けるようになった
    StreamsAvailable {
        /// 双方向ストリームの上限が増えたかどうか (false は単方向)
        bidirectional: bool,
    },
    /// ピアがストリームをリセットした (RESET_STREAM)
    StreamReset {
        /// リセットされたストリーム
        id: StreamId,
        /// ピアが指定したアプリケーションエラーコード
        error_code: u64,
    },
    /// ピアがストリームの送信停止を要求した (STOP_SENDING)
    StreamStopSending {
        /// 対象のストリーム
        id: StreamId,
        /// ピアが指定したアプリケーションエラーコード
        error_code: u64,
    },
    /// DATAGRAM を受信した
    Datagram(Vec<u8>),
    /// QX_PING の要求を受信した
    Ping {
        /// 要求のシーケンス番号 (応答は自動的に送信される)
        sequence: u64,
    },
    /// QX_PING の応答を受信した
    PingAcknowledged {
        /// 応答のシーケンス番号
        sequence: u64,
    },
    /// 接続が閉じた
    ConnectionClosed(CloseReason),
}
