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
    /// ピアが下位トランスポートの送信側を graceful に shutdown した (Section 7.3)
    ///
    /// 下位トランスポートの読み取りで EOF を観測したときに、
    /// [`crate::connection::Connection::peer_shutdown`] が通知する。ピアが
    /// CONNECTION_CLOSE を送らずに送信側だけを閉じた場合 (アイドルタイムアウト
    /// など) に対応する。下位トランスポートの閉塞 (Section 7.4) は
    /// [`CloseReason::TransportClosed`] で通知される。
    PeerShutdown,
    /// 下位トランスポートが閉じたことをアプリケーションが通知した (Section 7.4)
    TransportClosed,
}

/// QMux 接続のイベント
///
/// [`crate::connection::Connection::poll_event`] で取り出す。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// ピアのトランスポートパラメータを受信した
    ///
    /// この時点で初めて、ピアのフロー制御に依存する操作が可能になる
    /// (draft-ietf-quic-qmux-02 Section 4.2)。ただし、すべての機能が使えることを
    /// 意味しない。ストリームはピアの初期ストリーム上限が 0 より大きい場合だけ
    /// 開け、DATAGRAM は双方の `max_datagram_frame_size` が 0 より大きい場合だけ
    /// 送受信できる。上限は [`crate::connection::Connection::peer_max_streams_bidi`]
    /// などの照会 API で確認できる。
    TransportParametersReceived,
    /// ピアがストリームを開いた
    StreamOpened {
        /// 開かれたストリーム
        id: StreamId,
    },
    /// ストリームにデータが到着し、読み取り可能になった
    ///
    /// データが届いたときに通知する。未読のデータが残っている間は再通知しない。
    /// 通知のたびに [`crate::connection::Connection::read`] を呼び、`len` が 0 の
    /// 結果 (`fin` が true の終端を含む) が返るまで読み続けることで、次のデータ
    /// 到着時に改めて通知される。
    ///
    /// [`crate::connection::Connection::stop_sending`] を呼んだ後は、以後に到着
    /// したデータを破棄するため、このイベントは通知されない
    /// (RFC 9000 Section 3.5)。
    StreamReadable {
        /// 読み取り可能になったストリーム
        id: StreamId,
    },
    /// フロー制御クレジットが増え、ストリームへ書き込めるようになった
    ///
    /// ピアが MAX_STREAM_DATA または MAX_DATA でクレジットを増やし、かつ送信側が
    /// 終端していない (RFC 9000 Section 3.1) ときに通知する。
    ///
    /// MAX_DATA による通知は、接続レベルのクレジット不足で
    /// [`crate::connection::Connection::write`] が 0 を返したストリームのうち、
    /// 実際に書き込めると見込めるものだけに限定される。増加分が小さい場合は
    /// 全ストリームへ通知せず、通知数も増加分に比例して絞る
    /// (draft-ietf-quic-qmux-02 Section 12.2 の資源消費対策)。
    ///
    /// 同じストリームへの通知が未配送のまま残っている間は積み増さない。抑止は
    /// 通知を [`crate::connection::Connection::poll_event`] で取り出したときに
    /// 解け、次のクレジット増加で改めて通知する。write を呼んだだけでは解けない。
    StreamWritable {
        /// 書き込み可能になったストリーム
        id: StreamId,
    },
    /// ピアがストリーム数の上限を増やし、ストリームを開けるようになった
    ///
    /// ピアが MAX_STREAMS で上限を増やしたときに通知する。同じ方向への通知が
    /// 未配送のまま残っている間は積み増さない。抑止は通知を
    /// [`crate::connection::Connection::poll_event`] で取り出したときに解け、
    /// 次の上限増加で改めて通知する。open を呼んだだけでは解けない。
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
    ///
    /// 応答は自動的に送信され、応答前に複数の要求を受信した場合は、受信した
    /// 最大のシーケンス番号を持つ 1 件に合算する (draft-ietf-quic-qmux-02
    /// Section 4.3 の MAY)。
    ///
    /// アプリケーション向けの通知も、未配送の間は最大 1 件に合流させ、保持する
    /// シーケンス番号は最大値だけにする (Section 12.2 の資源消費対策)。要求を
    /// 1 件ずつ観測する必要がある用途では、この合流を前提に設計すること。
    Ping {
        /// 要求のシーケンス番号
        sequence: u64,
    },
    /// QX_PING の応答を受信し、確認済みの最大シーケンス番号が更新された
    ///
    /// 応答が届くたびではなく、確認済みの最大シーケンス番号が更新されたときだけ
    /// 通知する。応答が順不同で届いた場合、より小さいシーケンス番号の応答では
    /// 通知されない (draft-ietf-quic-qmux-02 Section 4.3)。
    PingAcknowledged {
        /// 応答のシーケンス番号
        sequence: u64,
    },
    /// 接続が閉じた
    ///
    /// このイベントは 1 回だけ通知される。ただし、閉じる直前に積まれていた
    /// ストリームや DATAGRAM のイベントが、このイベントの後に取り出される
    /// ことがある。アプリケーションは `ConnectionClosed` の後で `poll_event` が
    /// `None` を返すまでイベントを読み続けること。
    ConnectionClosed(CloseReason),
}
