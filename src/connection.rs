//! QMux 接続の状態機械 (draft-ietf-quic-qmux-02)
//!
//! 下位トランスポートとのバイト列の受け渡しだけを行う Sans I/O の実装である。
//! ソケットの読み書き、TLS のハンドシェイク、時刻の管理はアプリケーションが行う。

use alloc::collections::VecDeque;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::frame::{self, Frame};
use crate::codec::role::Role;
use crate::codec::time::{Duration, Timestamp};
use crate::codec::varint::VarInt;

use crate::config::{Config, MAX_CONFIG_STREAMS};
use crate::event::{CloseReason, Event};
use crate::frame::{QmuxFrame, QxPing, QxTransportParameters};
use crate::record::{self, RecordDecoder};
use crate::stream::{PendingFrame, ReadResult, StreamId, StreamManager};
use crate::transport_params::TransportParameters;

/// アプリケーションへ未配送のまま保持する DATAGRAM の個数の上限
///
/// DATAGRAM はフロー制御されないため、ピアが連投した場合に備えて上限を設ける
/// (draft-ietf-quic-qmux-02 Section 6, Section 12.2)。上限を超えた DATAGRAM は
/// 破棄する (RFC 9221 Section 5.3 が明示的に許している)。
const MAX_QUEUED_DATAGRAMS: usize = 64;

/// アプリケーションへ未配送のまま保持する DATAGRAM の合計バイト数の上限
const MAX_QUEUED_DATAGRAM_BYTES: usize = 1 << 20;

/// フロー制御でブロックしている間、DATA_BLOCKED / STREAM_DATA_BLOCKED を
/// 再送する間隔
///
/// RFC 9000 Section 4.1 は、フロー制御で止まっている送信側が ack-eliciting な
/// パケットを送っていない場合、STREAM_DATA_BLOCKED または DATA_BLOCKED を
/// 周期的に送ることを SHOULD とする。QMux には ACK も PTO もなく、アイドル
/// タイマーは Record の送受信でのみリセットされる (draft-ietf-quic-qmux-02
/// Section 7.1) ため、ブロック中に何も送らないと max_idle_timeout で接続が
/// 切られ得る。
///
/// 1 秒は一般的な実効アイドルタイムアウト (秒単位) より十分短く、かつ無駄な
/// 送信が過剰にならない値として選んだ。RFC 9000 Section 10.1 は PTO に基づき
/// 再送間隔を動的に決めるが、QMux には RTT 推定がないため固定値とする。
/// 実効アイドルタイムアウトが無効 (0) の場合は接続が切れる心配がないため
/// 再送しない。
const BLOCKED_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// QMux 接続のトラフィック統計
///
/// draft-ietf-quic-qmux-02 Section 12.2 は、小さな Record の連投、PADDING、
/// 微小なフロー制御更新、QX_PING の連投といった資源消費パターンを監視する
/// ことを SHOULD とする。この構造体はその判断材料となる累計値を公開する。
/// しきい値の設定や接続エラー化はアプリケーションのポリシーに委ねる。
///
/// 値は [`crate::connection::Connection::new`] からの累計で、飽和加算する。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TrafficStats {
    /// 完全に受信して処理した QMux Record の数
    pub records_received: u64,
    /// 受信してデコードしたフレームの数 (フレームの処理に失敗したものも数える)
    pub frames_received: u64,
    /// 受信した PADDING フレームのバイト数 (フレームタイプを含む)
    pub padding_bytes: u64,
    /// 受信した QX_PING の要求の数
    pub ping_requests_received: u64,
    /// 送信した QMux Record の数
    pub records_sent: u64,
}

/// 接続の状態
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// ピアのトランスポートパラメータの受信待ち
    Handshaking,
    /// 確立済み (ストリームと DATAGRAM が使える)
    Established,
    /// CONNECTION_CLOSE を送信し、ピアの切断を待っている (Section 7.2)
    Closing,
    /// 切断済み (Section 7.1, Section 7.3)
    Closed,
}

/// QMux 接続
///
/// 使い方はクレートのドキュメントを参照。
#[derive(Debug)]
pub struct Connection {
    /// 設定
    config: Config,
    /// ローカルのトランスポートパラメータ
    local_params: TransportParameters,
    /// 接続の状態
    state: State,
    /// QMux Record のデコーダ
    decoder: RecordDecoder,
    /// ストリームとフロー制御の管理
    streams: StreamManager,
    /// 接続レベルのイベント
    events: VecDeque<Event>,
    /// ピアのトランスポートパラメータ
    peer_params: Option<TransportParameters>,
    /// 最初のフレームを受信したかどうか (QX_TRANSPORT_PARAMETERS の位置の検査に使う)
    first_frame_received: bool,
    /// QX_TRANSPORT_PARAMETERS を送信済みかどうか
    transport_parameters_sent: bool,
    /// 次に送る QX_PING のシーケンス番号
    next_ping_sequence: u64,
    /// 送信した QX_PING の最大のシーケンス番号
    greatest_ping_sent: Option<u64>,
    /// 応答を受信した QX_PING の最大のシーケンス番号
    greatest_ping_acknowledged: Option<u64>,
    /// 最後に受信した QX_PING の要求のシーケンス番号
    last_ping_request: Option<u64>,
    /// まだ応答していない QX_PING の要求のうち、最大のシーケンス番号
    ///
    /// 応答前に複数の要求を受信した場合は 1 件に合算する (Section 4.3 の MAY)。
    /// 実際の応答フレームは [`Connection::poll_transmit`] で 1 件だけ積む。
    pending_ping_response: Option<u64>,
    /// 最後に Record を完全に送受信した時刻 (アイドルタイマーの起点)
    last_activity: Option<Timestamp>,
    /// アプリケーションへ未配送の DATAGRAM の個数
    queued_datagrams: usize,
    /// アプリケーションへ未配送の DATAGRAM の合計バイト数
    queued_datagram_bytes: usize,
    /// 保持量の上限を超えて破棄した受信 DATAGRAM の累計個数
    dropped_datagrams: u64,
    /// トラフィック統計 (Section 12.2)
    traffic: TrafficStats,
    /// アプリケーションへまだ配送していない QX_PING の要求のシーケンス番号
    ///
    /// 要求ごとにイベントを積むとピアの連投でイベントキューが無制限に伸びる
    /// (Section 12.2)。未配送の Ping イベントは 1 件に合流させ、最大の
    /// シーケンス番号だけを保持する。
    pending_ping_event: Option<u64>,
    /// 直近に観測した時刻
    ///
    /// `write` は時刻を受け取らないため、ブロック再送の起点に使う。
    last_observed: Option<Timestamp>,
    /// フロー制御のブロック通知を次に再送する時刻 (RFC 9000 Section 4.1)
    blocked_retry_at: Option<Timestamp>,
}

impl Connection {
    /// 接続を作成する
    ///
    /// この時点では何も送信しない。最初の [`Connection::poll_transmit`] で
    /// QX_TRANSPORT_PARAMETERS フレームが送信される。
    ///
    /// 送信できない設定 (max_record_size が既定値未満、可変長整数で表現できない値
    /// など) の場合は [`crate::codec::error::Error::InvalidData`] を返す。
    /// ストリーム数の初期値が [`MAX_CONFIG_STREAMS`] を超える場合も同様に拒否する。
    pub fn new(role: Role, config: Config) -> Result<Self, Error> {
        let local_params = config.transport_parameters();

        // ピアは 1 つの MAX_STREAMS フレームで上限までストリームを開けるため、
        // 自エンドポイントが広告する初期値に実用上限を設ける
        // (draft-ietf-quic-qmux-02 Section 12.1, Section 12.2 の資源枯渇対策)。
        // ピアから受信した値には上限を課さない (RFC 9000 Section 4.6 は 2^60 まで
        // 許容する)。
        if local_params.initial_max_streams_bidi > MAX_CONFIG_STREAMS {
            return Err(Error::InvalidData(format!(
                "initial_max_streams_bidi {} exceeds the maximum {MAX_CONFIG_STREAMS}",
                local_params.initial_max_streams_bidi
            )));
        }
        if local_params.initial_max_streams_uni > MAX_CONFIG_STREAMS {
            return Err(Error::InvalidData(format!(
                "initial_max_streams_uni {} exceeds the maximum {MAX_CONFIG_STREAMS}",
                local_params.initial_max_streams_uni
            )));
        }

        // 誤設定を最初の送信まで遅延させないため、ここで一度エンコードして検証する。
        // ここを通れば poll_transmit での QX_TRANSPORT_PARAMETERS のエンコードは
        // 失敗しない。
        let mut probe = Vec::new();
        local_params.encode(&mut probe)?;
        let decoder = RecordDecoder::new(local_params.max_record_size);
        let streams = StreamManager::new(role, &local_params);
        Ok(Self {
            config,
            local_params,
            state: State::Handshaking,
            decoder,
            streams,
            events: VecDeque::new(),
            peer_params: None,
            first_frame_received: false,
            transport_parameters_sent: false,
            next_ping_sequence: 0,
            greatest_ping_sent: None,
            greatest_ping_acknowledged: None,
            last_ping_request: None,
            pending_ping_response: None,
            last_activity: None,
            queued_datagrams: 0,
            queued_datagram_bytes: 0,
            dropped_datagrams: 0,
            traffic: TrafficStats::default(),
            pending_ping_event: None,
            last_observed: None,
            blocked_retry_at: None,
        })
    }

    /// ローカルのトランスポートパラメータを返す
    pub fn local_transport_parameters(&self) -> &TransportParameters {
        &self.local_params
    }

    /// ピアのトランスポートパラメータを返す
    ///
    /// 受信前は `None` を返す。ストリームと DATAGRAM は受信後に使える。
    pub fn peer_transport_parameters(&self) -> Option<&TransportParameters> {
        self.peer_params.as_ref()
    }

    /// 保持量の上限を超えて破棄した受信 DATAGRAM の累計個数を返す
    ///
    /// QMux Section 6 はアプリケーションへ速やかに配送できない DATAGRAM の破棄を
    /// 許す。この値は破棄が起きたことをアプリケーションが観測するために使う。
    pub fn dropped_datagrams(&self) -> u64 {
        self.dropped_datagrams
    }

    /// トラフィック統計を返す (Section 12.2)
    ///
    /// ピアの入力が資源消費を狙ったパターン (小さな Record の連投、PADDING、
    /// 微小なフロー制御更新、QX_PING の連投) かどうかをアプリケーションが
    /// 判断するために使う。しきい値の設定と接続エラー化はアプリケーションの
    /// ポリシーに委ねる (仕様は監視を SHOULD とし、エラー化を MAY とする)。
    pub fn traffic_stats(&self) -> TrafficStats {
        self.traffic
    }

    /// ピアが広告した双方向ストリーム数の上限を返す
    ///
    /// この値はピアが MAX_STREAMS で増やした分も反映する。あと何本開けるかは
    /// この値から起動済みのストリーム数を引いた数になるが、起動済みの数は
    /// 公開していないため、実際に開けるかは [`Connection::open_bidi`] の結果で
    /// 判断する。ピアのトランスポートパラメータを受信するまでは 0 を返す。
    pub fn peer_max_streams_bidi(&self) -> u64 {
        self.streams.peer_max_streams_bidi()
    }

    /// ピアが広告した単方向ストリーム数の上限を返す
    ///
    /// 更新の扱いは [`Connection::peer_max_streams_bidi`] と同じ。
    pub fn peer_max_streams_uni(&self) -> u64 {
        self.streams.peer_max_streams_uni()
    }

    /// 接続レベルで書き込める残りのバイト数を返す
    ///
    /// ピアが広告した `initial_max_data` と MAX_DATA の累計から、送信済みの
    /// ストリームデータを引いた値である。ストリーム単位のクレジットは含まない
    /// (それは [`Connection::stream_send_credit`] で取得する)。実際に書き込める
    /// 量は両者の最小値になる。
    pub fn send_credit(&self) -> u64 {
        self.streams.send_credit()
    }

    /// ストリーム単位で書き込める残りのバイト数を返す
    ///
    /// 接続レベルのクレジットは含まない。実際に書き込める量は
    /// [`Connection::send_credit`] との最小値になる。
    ///
    /// 未作成のストリーム、終端して破棄したストリーム、受信専用ストリーム、
    /// 送信側を終端済みのストリームの場合はエラーを返す
    /// (未作成は [`Error::InvalidData`]、それ以外は `STREAM_STATE_ERROR` の
    /// [`Error::Protocol`])。
    pub fn stream_send_credit(&self, id: StreamId) -> Result<u64, Error> {
        self.streams.stream_send_credit(id)
    }

    /// 接続が閉じたかどうか
    ///
    /// `true` の間はアプリケーションの操作がエラーになり、受信データは破棄される。
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closing | State::Closed)
    }

    /// 下位トランスポートから受信したバイト列を処理する
    ///
    /// プロトコル違反を検出した場合は、エラーを返さずに CONNECTION_CLOSE を
    /// 送信待ちに追加し、[`Event::ConnectionClosed`] を通知する。呼び出し側は
    /// [`Connection::poll_transmit`] でその Record を送信し、下位トランスポートの
    /// 送信側を閉じること。
    ///
    /// [`Connection::is_closed`] が `true` の間は、渡されたデータを処理せずに破棄する
    /// (Section 7.2)。
    pub fn receive(&mut self, data: &[u8], now: Timestamp) {
        self.last_observed = Some(now);
        if self.is_closed() {
            return;
        }

        self.decoder.extend(data);
        loop {
            let record = match self.decoder.poll_record() {
                Ok(Some(record)) => record,
                Ok(None) => break,
                Err(error) => {
                    self.close_with_error(&error);
                    break;
                }
            };

            // Record を完全に受信したのでアイドルタイマーをリセットする (Section 7.1)
            self.touch(now);
            self.traffic.records_received = self.traffic.records_received.saturating_add(1);
            if let Err(error) = self.process_record(&record) {
                self.close_with_error(&error);
                break;
            }
            if self.is_closed() {
                break;
            }
        }
        // ピアからの MAX_DATA / MAX_STREAM_DATA でブロックが解けた可能性がある
        self.update_blocked_retry();
    }

    /// 送信する Record を 1 つ `buf` へ追記する
    ///
    /// 書き出した場合は `true` を返す。呼び出し側は `false` が返るまで繰り返し、
    /// 書き出したバイト列を下位トランスポートへ渡すこと。
    ///
    /// `buf` はクリアせず、既存の内容の末尾へ追記する。呼び出し側は呼び出しごとに
    /// `buf` をクリアするか、Record ごとに新しいバッファへ書き出すこと。前回の
    /// 内容を残したまま渡すと、古い Record を毎回送り直すことになる。
    ///
    /// 最初の呼び出しでは QX_TRANSPORT_PARAMETERS フレームだけを含む Record を
    /// 書き出す (Section 4.2)。
    ///
    /// 未応答の QX_PING の要求がある場合は、この呼び出しで応答フレームを 1 件
    /// 積む (Section 4.3)。
    ///
    /// この関数が `false` を返した後も、フロー制御のブロック通知の周期再送などで
    /// 新たな送信待ちフレームが積まれることがある (RFC 9000 Section 4.1)。
    pub fn poll_transmit(&mut self, buf: &mut Vec<u8>, now: Timestamp) -> Result<bool, Error> {
        self.last_observed = Some(now);
        if self.state == State::Closed {
            return Ok(false);
        }

        // Section 4.3: 応答前に複数の QX_PING の要求を受信していても、応答は
        // 受信した最大のシーケンス番号を持つ 1 件に合算する (MAY)。要求ごとに
        // 応答フレームを積むと、ピアが要求を連投するだけで送信待ちフレームが
        // 無制限に増える (Section 12.2)。切断が確定した後は応答しない。
        if !self.is_closed()
            && let Some(sequence) = self.pending_ping_response.take()
        {
            let mut encoded = Vec::new();
            QmuxFrame::Ping(QxPing::Response {
                sequence: VarInt::try_from(sequence)
                    .expect("QX_PING sequence received as a variable-length integer fits in one"),
            })
            .encode(&mut encoded);
            self.streams
                .pending_frames
                .push_back(PendingFrame::new(encoded));
        }

        // Section 4.2: QX_TRANSPORT_PARAMETERS は最初のフレームとして、下位
        // トランスポートが送信可能になり次第送る。ピアのトランスポートパラメータを
        // 待つ必要はない。
        //
        // `close` により State::Closing でも、未送信なら先にこのフレームを送る。
        // 最初のフレームであることが MUST のため、CONNECTION_CLOSE を先に送って
        // このフレームを後回しにはできない。CONNECTION_CLOSE は次の Record で送る。
        if !self.transport_parameters_sent {
            let mut payload = Vec::new();
            self.local_params.encode(&mut payload)?;
            let frame = QmuxFrame::TransportParameters(QxTransportParameters { params: payload });
            let mut encoded = Vec::new();
            frame.encode(&mut encoded);
            // ピアの max_record_size が不明なため、既定値を上限に使う (Section 5.2)
            record::encode(&encoded, record::DEFAULT_MAX_RECORD_SIZE, buf)?;
            self.transport_parameters_sent = true;
            self.traffic.records_sent = self.traffic.records_sent.saturating_add(1);
            self.touch(now);
            return Ok(true);
        }

        // 送信待ちのフレームを、ピアの max_record_size に収まる範囲で 1 つの
        // Record にまとめる (Section 3.2)
        let limit = self.streams.peer_max_record_size();
        let mut frames = Vec::new();
        let mut effects = Vec::new();
        while let Some(front) = self.streams.pending_frames.front() {
            let front_len = front.bytes.len() as u64;
            if front_len > limit {
                // 1 つのフレームが Record に収まらない場合は送信できない。
                // フレームを積む時点で長さを制限しているため、ここには到達しない。
                return Err(Error::InvalidData(format!(
                    "frame of {front_len} bytes exceeds the QMux record limit {limit}"
                )));
            }
            if frames.len() as u64 + front_len > limit {
                break;
            }
            if let Some(pending) = self.streams.pending_frames.pop_front() {
                frames.extend_from_slice(&pending.bytes);
                if let Some(effect) = pending.effect {
                    effects.push(effect);
                }
            }
        }
        if frames.is_empty() {
            return Ok(false);
        }

        record::encode(&frames, limit, buf)?;
        // Record を完全に書き出したので、広告した上限を反映する
        // (RFC 9000 Section 4.6 の「the limit it has sent」を基準にするため)。
        for effect in effects {
            self.streams.apply_sent_effect(effect);
        }
        self.traffic.records_sent = self.traffic.records_sent.saturating_add(1);
        // Record を完全に送信したのでアイドルタイマーをリセットする (Section 7.1)
        self.touch(now);
        Ok(true)
    }

    /// アプリケーションへ通知するイベントを 1 つ取り出す
    ///
    /// 接続レベルのイベント、合流した QX_PING の要求、ストリームのイベントの
    /// 順に取り出す。イベントが残っていない場合は `None` を返す。
    ///
    /// QX_PING の要求は、未配送の間は最大 1 件に合流し、最大のシーケンス番号
    /// だけが通知される (Section 12.2 の資源消費対策)。
    pub fn poll_event(&mut self) -> Option<Event> {
        if let Some(event) = self.events.pop_front() {
            if let Event::Datagram(ref data) = event {
                // 配送した分だけ保持量を減らし、新しい DATAGRAM を受け入れられるようにする
                self.queued_datagrams = self.queued_datagrams.saturating_sub(1);
                self.queued_datagram_bytes = self.queued_datagram_bytes.saturating_sub(data.len());
            }
            return Some(event);
        }
        // 未配送の QX_PING の要求は、最大のシーケンス番号を持つ 1 件に合流させる
        if let Some(sequence) = self.pending_ping_event.take() {
            return Some(Event::Ping { sequence });
        }
        self.streams.poll_event()
    }

    /// 次にタイマーを発火すべき時刻を返す
    ///
    /// 次のうち最も早い時刻を返す。
    ///
    /// - 実効アイドルタイムアウトの期限 (Section 7.1)
    /// - フロー制御でブロックしている間の DATA_BLOCKED / STREAM_DATA_BLOCKED の
    ///   再送時刻 (RFC 9000 Section 4.1)。周期再送は実効アイドルタイムアウトが
    ///   有効な場合だけ行う
    ///
    /// アイドルタイムアウトが無効、まだ Record を送受信していない、または接続が
    /// 閉じている場合は `None` を返す。返した時刻を過ぎたら
    /// [`Connection::handle_timeout`] を呼び出すこと。
    pub fn next_timeout(&self) -> Option<Timestamp> {
        if self.is_closed() {
            return None;
        }
        let idle = self.idle_timeout_deadline()?;
        match self.blocked_retry_at {
            Some(blocked) if self.streams.has_blocked_flow() => Some(core::cmp::min(idle, blocked)),
            _ => Some(idle),
        }
    }

    /// タイマーを処理する
    ///
    /// [`Connection::next_timeout`] が返した時刻を過ぎたときに呼び出す。
    ///
    /// - 実効アイドルタイムアウトが満了した場合は接続を閉じ、
    ///   [`Event::ConnectionClosed`] を [`CloseReason::IdleTimeout`] で通知する。
    ///   このとき QMux フレームは送信せず、呼び出し側が下位トランスポートの
    ///   送信側を閉じる (Section 7.2)。
    /// - フロー制御のブロック通知の再送時刻に達した場合は、
    ///   DATA_BLOCKED / STREAM_DATA_BLOCKED を送信待ちに積む
    ///   (RFC 9000 Section 4.1)。呼び出し側は [`Connection::poll_transmit`] で
    ///   送信すること。
    ///
    /// 閉じている場合と、どちらの期限にも達していない場合は何もしない。
    pub fn handle_timeout(&mut self, now: Timestamp) {
        self.last_observed = Some(now);
        if self.is_closed() {
            return;
        }
        // Section 7.1 / Section 7.2: アイドルタイムアウトを先に評価する。
        // タイムアウトではフレームを送信せずに接続を閉じる。
        if let Some(deadline) = self.idle_timeout_deadline()
            && now.has_elapsed(deadline)
        {
            self.streams.pending_frames.clear();
            self.state = State::Closed;
            self.blocked_retry_at = None;
            self.events
                .push_back(Event::ConnectionClosed(CloseReason::IdleTimeout));
            return;
        }

        // RFC 9000 Section 4.1: フロー制御で止まっている間は、ブロック通知を
        // 周期的に再送してピアに状況を知らせる。
        if let Some(deadline) = self.blocked_retry_at
            && now.has_elapsed(deadline)
            && self.streams.has_blocked_flow()
        {
            self.streams.resend_blocked_frames();
            self.blocked_retry_at = Some(now.add(BLOCKED_RETRY_INTERVAL));
        }
        self.update_blocked_retry();
    }

    /// 実効アイドルタイムアウトの期限を返す
    ///
    /// 実効値が 0 (無効) の場合と、まだ Record を送受信していない場合は `None`。
    fn idle_timeout_deadline(&self) -> Option<Timestamp> {
        let timeout = self.effective_idle_timeout_millis();
        if timeout == 0 {
            return None;
        }
        Some(self.last_activity?.add(Duration::from_millis(timeout)))
    }

    /// フロー制御のブロック通知の次回再送時刻を更新する
    ///
    /// 実効アイドルタイムアウトが無効の場合は、接続がアイドルで切れる心配が
    /// ないため再送しない。ブロックが解消したら予定を消す。
    fn update_blocked_retry(&mut self) {
        let retry_needed = !self.is_closed()
            && self.effective_idle_timeout_millis() != 0
            && self.streams.has_blocked_flow();
        if !retry_needed {
            self.blocked_retry_at = None;
            return;
        }
        if self.blocked_retry_at.is_none() {
            // write は時刻を受け取らないため、直近に観測した時刻を起点にする。
            // ピアのトランスポートパラメータ受信後 (write が可能になる前) に
            // receive か poll_transmit が必ず呼ばれている。
            self.blocked_retry_at = self
                .last_observed
                .map(|now| now.add(BLOCKED_RETRY_INTERVAL));
        }
    }

    /// 双方向ストリームを開く
    ///
    /// ピアのトランスポートパラメータを受信するまではエラーを返す (Section 4.2)。
    pub fn open_bidi(&mut self) -> Result<StreamId, Error> {
        self.ensure_open()?;
        self.streams.open_local(true)
    }

    /// 単方向ストリームを開く
    ///
    /// ピアのトランスポートパラメータを受信するまではエラーを返す (Section 4.2)。
    pub fn open_uni(&mut self) -> Result<StreamId, Error> {
        self.ensure_open()?;
        self.streams.open_local(false)
    }

    /// ストリームへデータを書き込む
    ///
    /// 戻り値は受け付けたバイト数。フロー制御により一部だけを受け付けた場合は、
    /// 呼び出し側が残りを再度書き込むこと。FIN は渡したデータをすべて受け付けた
    /// 場合にのみ送信する。
    ///
    /// フロー制御で書き込みが止まった場合は、[`Event::StreamWritable`] が
    /// 通知されるまで待つか、[`Connection::send_credit`] /
    /// [`Connection::stream_send_credit`] でクレジットを確認すること。実効
    /// アイドルタイムアウトが有効な場合は、ブロック中に
    /// DATA_BLOCKED / STREAM_DATA_BLOCKED が周期的に再送される
    /// (RFC 9000 Section 4.1)。
    pub fn write(&mut self, id: StreamId, data: &[u8], fin: bool) -> Result<usize, Error> {
        self.ensure_open()?;
        let written = self.streams.write(id, data, fin)?;
        // 書き込みの結果でフロー制御のブロック状態が変わった可能性がある
        self.update_blocked_retry();
        Ok(written)
    }

    /// ストリームからデータを読み取る
    ///
    /// 読み取った分のフロー制御クレジットがピアへ返る。
    pub fn read(&mut self, id: StreamId, buf: &mut [u8]) -> Result<ReadResult, Error> {
        self.ensure_open()?;
        self.streams.read(id, buf)
    }

    /// ストリームの送信側をリセットする (RESET_STREAM を送る)
    ///
    /// `error_code` はアプリケーションエラーコードで、可変長整数の最大値
    /// (2^62-1) 以下でなければならない。超える場合は [`Error::InvalidData`] を
    /// 返す (RFC 9000 Section 19.4)。
    pub fn reset(&mut self, id: StreamId, error_code: u64) -> Result<(), Error> {
        self.ensure_open()?;
        validate_application_error_code(error_code)?;
        self.streams.reset(id, error_code)
    }

    /// ストリームの受信側に送信停止を要求する (STOP_SENDING を送る)
    ///
    /// `error_code` はアプリケーションエラーコードで、可変長整数の最大値
    /// (2^62-1) 以下でなければならない。超える場合は [`Error::InvalidData`] を
    /// 返す (RFC 9000 Section 19.5)。
    ///
    /// 送信後は、以後に到着する STREAM フレームのデータを破棄し、フロー制御の
    /// 会計だけを行う (RFC 9000 Section 3.5)。破棄した分のクレジットは直ちに
    /// ピアへ返すため、ピアが RESET_STREAM を返すまで接続レベルのウィンドウが
    /// 埋まり続けることはない。破棄したデータについて
    /// [`Event::StreamReadable`] は通知しない。
    ///
    /// アプリケーションがすべてのデータを読み取った後 ("Data Recvd" にあたる
    /// [`crate::stream::RecvState::DataRead`]) でも送信する。RFC 9000 Section 3.3 は
    /// この状態での STOP_SENDING を MAY としつつ「little value」と述べているが、
    /// 本実装では読み取りを止めたことをピアへ明示するため抑止しない。
    pub fn stop_sending(&mut self, id: StreamId, error_code: u64) -> Result<(), Error> {
        self.ensure_open()?;
        validate_application_error_code(error_code)?;
        self.streams.stop_sending(id, error_code)
    }

    /// DATAGRAM を送信する (RFC 9221)
    ///
    /// ピアが DATAGRAM 拡張を広告していない場合と、フレームがピアの
    /// `max_datagram_frame_size` または `max_record_size` を超える場合はエラーを返す。
    pub fn send_datagram(&mut self, data: &[u8]) -> Result<(), Error> {
        self.ensure_open()?;
        let Some(peer) = self.peer_params.as_ref() else {
            return Err(Error::InvalidData(
                "peer transport parameters have not been received yet".to_string(),
            ));
        };
        // RFC 9221 Section 3: ピアが DATAGRAM 拡張を広告していない場合
        // (パラメータなし、または値 0) は送信できない
        let Some(peer_max) = peer.max_datagram_frame_size.filter(|max| *max > 0) else {
            return Err(Error::InvalidData(
                "the peer does not support DATAGRAM frames".to_string(),
            ));
        };

        // フレーム全体の大きさを先に計算する。上限を超えるデータを複製しないための
        // 順序である (DATAGRAM フレームは常に Length フィールドを付けて送る)。
        let payload_len = data.len() as u64;
        let length_size = VarInt::try_from(payload_len)
            .map_err(|_| {
                Error::InvalidData(
                    "datagram payload length exceeds the maximum variable-length integer"
                        .to_string(),
                )
            })?
            .encoded_size() as u64;
        let size = 1 + length_size + payload_len;

        // フレーム全体がピアの max_datagram_frame_size に収まること (RFC 9221 Section 3)
        if size > peer_max {
            return Err(Error::InvalidData(format!(
                "datagram frame of {size} bytes exceeds the peer max_datagram_frame_size {peer_max}"
            )));
        }
        // フレーム全体がピアの max_record_size にも収まること (Section 9.1)
        let limit = self.streams.peer_max_record_size();
        if size > limit {
            return Err(Error::InvalidData(format!(
                "datagram frame of {size} bytes exceeds the peer max_record_size {limit}"
            )));
        }

        let datagram = frame::datagram::Datagram {
            data: data.to_vec(),
        };

        let mut encoded = Vec::new();
        Frame::Datagram(datagram).encode(&mut encoded);
        self.streams
            .pending_frames
            .push_back(PendingFrame::new(encoded));
        Ok(())
    }

    /// QX_PING を送信し、シーケンス番号を返す
    ///
    /// シーケンス番号は 0 から始まり、呼び出しごとに厳密に増加する
    /// (Section 4.3)。
    ///
    /// ピアの応答が届くと [`Event::PingAcknowledged`] が通知される。ただし通知は
    /// 「確認済みの最大シーケンス番号が更新されたとき」だけであり、応答が順不同で
    /// 届いた場合、より小さいシーケンス番号の応答では通知されない。個々の ping と
    /// イベントを 1 対 1 に対応させないこと。
    pub fn ping(&mut self) -> Result<u64, Error> {
        self.ensure_open()?;

        // Section 4.3: 要求のシーケンス番号は厳密に増加させる
        let sequence = VarInt::try_from(self.next_ping_sequence).map_err(|_| {
            Error::InvalidData("QX_PING sequence number exceeds the maximum value".to_string())
        })?;
        self.next_ping_sequence += 1;
        self.greatest_ping_sent = Some(sequence.as_u64());

        let mut encoded = Vec::new();
        QmuxFrame::Ping(QxPing::Request { sequence }).encode(&mut encoded);
        self.streams
            .pending_frames
            .push_back(PendingFrame::new(encoded));
        Ok(sequence.as_u64())
    }

    /// 接続を閉じる
    ///
    /// CONNECTION_CLOSE フレーム (アプリケーションエラー) を送信待ちに追加する。
    /// 呼び出し側は [`Connection::poll_transmit`] で送信した後、下位トランスポートの
    /// 送信側を閉じること (Section 7.2)。
    ///
    /// `error_code` は可変長整数の最大値 (2^62-1) 以下でなければならない。超える
    /// 場合はフレームをエンコードできないため [`Error::InvalidData`] を返す
    /// (RFC 9000 Section 19.19)。すでに閉じている場合は何もせず `Ok(())` を返す
    /// (2 回目以降の呼び出しは無視する)。
    pub fn close(&mut self, error_code: u64, reason: &str) -> Result<(), Error> {
        validate_application_error_code(error_code)?;
        if self.is_closed() {
            return Ok(());
        }
        self.queue_connection_close(true, error_code, reason);
        self.state = State::Closing;
        self.events
            .push_back(Event::ConnectionClosed(CloseReason::Local {
                application: true,
                error_code,
                reason: reason.to_string(),
            }));
        self.update_blocked_retry();
        Ok(())
    }

    /// アイドルタイムアウトのリセット
    fn touch(&mut self, now: Timestamp) {
        self.last_activity = Some(now);
    }

    /// 実効アイドルタイムアウト (ミリ秒) を返す
    ///
    /// RFC 9000 Section 10.1 と Section 18.2: 両端の値のうち 0 でない値の最小値に
    /// なる。両方が 0 のときだけ無効になる。
    fn effective_idle_timeout_millis(&self) -> u64 {
        let local = self.config.max_idle_timeout.as_millis();
        match self.peer_params {
            None => local,
            Some(ref peer) => match (local, peer.max_idle_timeout) {
                (0, peer) => peer,
                (local, 0) => local,
                (local, peer) => core::cmp::min(local, peer),
            },
        }
    }

    /// 閉じた接続ではアプリケーションの操作を受け付けない
    fn ensure_open(&self) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::InvalidData("connection is closed".to_string()));
        }
        Ok(())
    }

    /// Record の Frames フィールドを処理する
    ///
    /// フレームごとの消費バイト数を保持するため、[`crate::frame::decode`] を
    /// 直接呼び出す (DATAGRAM の受信サイズの検査に必要)。
    fn process_record(&mut self, frames: &[u8]) -> Result<(), Error> {
        let mut offset = 0;
        while offset < frames.len() {
            if self.is_closed() {
                // 切断が確定した後のフレームは処理しない (Section 7.2)
                break;
            }

            let (frame, size) = match crate::frame::decode(&frames[offset..]) {
                Ok(decoded) => decoded,
                // Record の終端で途切れたフレームは FRAME_ENCODING_ERROR (Section 3.2)
                Err(Error::Incomplete { .. }) => {
                    return Err(Error::Protocol {
                        code: TransportErrorCode::FRAME_ENCODING_ERROR,
                        reason: format!(
                            "frame at offset {offset} is truncated by the QMux record boundary"
                        ),
                    });
                }
                Err(error) => return Err(error),
            };
            // 0 バイトを消費するフレームは存在しない。デコーダの不具合で
            // 無限ループに入らないための防御。
            if size == 0 {
                return Err(Error::Protocol {
                    code: TransportErrorCode::INTERNAL_ERROR,
                    reason: "frame decoder consumed no bytes".to_string(),
                });
            }
            offset += size;
            self.traffic.frames_received = self.traffic.frames_received.saturating_add(1);

            if !self.first_frame_received {
                self.first_frame_received = true;
                // Section 4.2: 最初のフレームは QX_TRANSPORT_PARAMETERS でなければ
                // ならない
                if !matches!(frame, QmuxFrame::TransportParameters(_)) {
                    return Err(Error::Protocol {
                        code: TransportErrorCode::PROTOCOL_VIOLATION,
                        reason: "the first frame is not QX_TRANSPORT_PARAMETERS".to_string(),
                    });
                }
            }
            self.process_frame(frame, size)?;
        }
        Ok(())
    }

    /// フレームを 1 つ処理する
    fn process_frame(&mut self, frame: QmuxFrame, size: usize) -> Result<(), Error> {
        match frame {
            QmuxFrame::TransportParameters(frame) => self.on_transport_parameters(&frame),
            QmuxFrame::Ping(ping) => self.on_ping(ping),
            QmuxFrame::Quic(frame) => self.on_quic_frame(frame, size),
        }
    }

    /// QX_TRANSPORT_PARAMETERS フレームを処理する (Section 4.2)
    fn on_transport_parameters(&mut self, frame: &QxTransportParameters) -> Result<(), Error> {
        if self.peer_params.is_some() {
            // Section 4.2: QX_TRANSPORT_PARAMETERS は最初のフレームでのみ送信できる
            return Err(Error::Protocol {
                code: TransportErrorCode::PROTOCOL_VIOLATION,
                reason: "QX_TRANSPORT_PARAMETERS was received more than once".to_string(),
            });
        }

        let params = TransportParameters::decode(&frame.params)?;
        self.streams.apply_peer_transport_parameters(&params);
        self.peer_params = Some(params);
        if self.state == State::Handshaking {
            self.state = State::Established;
        }
        self.events.push_back(Event::TransportParametersReceived);
        Ok(())
    }

    /// QX_PING フレームを処理する (Section 4.3)
    fn on_ping(&mut self, ping: QxPing) -> Result<(), Error> {
        match ping {
            QxPing::Request { sequence } => {
                let sequence = sequence.as_u64();
                // Section 4.3: 要求のシーケンス番号は直前の値より厳密に大きいこと
                if let Some(previous) = self.last_ping_request
                    && sequence <= previous
                {
                    return Err(Error::Protocol {
                        code: TransportErrorCode::PROTOCOL_VIOLATION,
                        reason: format!(
                            "QX_PING sequence {sequence} is not greater than the previous sequence {previous}"
                        ),
                    });
                }
                self.last_ping_request = Some(sequence);

                // Section 4.3: 応答前に複数の要求を受信した場合、受信した最大の
                // シーケンス番号を持つ 1 件だけを応答してよい (MAY)。ここでは最大の
                // シーケンス番号だけを保持し、実際の応答フレームは poll_transmit で
                // 1 件だけ積む。要求ごとに積むと、ピアが要求を連投するだけで送信待ち
                // フレームが無制限に増える (Section 12.2)。
                self.pending_ping_response = Some(match self.pending_ping_response {
                    Some(previous) => core::cmp::max(previous, sequence),
                    None => sequence,
                });
                // Section 12.2: 要求ごとにイベントを積むと、ピアが要求を連投する
                // だけでイベントキューが無制限に伸びる。未配送の間は 1 件に合流
                // させ、最大のシーケンス番号だけを保持する。
                self.pending_ping_event = Some(match self.pending_ping_event {
                    Some(previous) => core::cmp::max(previous, sequence),
                    None => sequence,
                });
                self.traffic.ping_requests_received =
                    self.traffic.ping_requests_received.saturating_add(1);
            }
            QxPing::Response { sequence } => {
                let sequence = sequence.as_u64();
                // Section 4.3: 送信した最大のシーケンス番号を超える応答は
                // PROTOCOL_VIOLATION。送っていないシーケンス番号の検出は MAY だが、
                // 送信済みの最大値との比較で検出できる範囲を検査する。
                match self.greatest_ping_sent {
                    Some(greatest) if sequence <= greatest => {}
                    _ => {
                        return Err(Error::Protocol {
                            code: TransportErrorCode::PROTOCOL_VIOLATION,
                            reason: format!("QX_PING response sequence {sequence} was never sent"),
                        });
                    }
                }

                // 応答の順序は保証されないため、新しい応答だけを通知する
                if self
                    .greatest_ping_acknowledged
                    .is_none_or(|last| sequence > last)
                {
                    self.greatest_ping_acknowledged = Some(sequence);
                    self.events.push_back(Event::PingAcknowledged { sequence });
                }
            }
        }
        Ok(())
    }

    /// QMux で利用が許可された QUIC version 1 のフレームを処理する (Section 4)
    ///
    /// `size` はフレーム全体のバイト数 (フレームタイプを含む)。
    fn on_quic_frame(&mut self, frame: Frame, size: usize) -> Result<(), Error> {
        match frame {
            // PADDING は読み飛ばす (RFC 9000 Section 19.1)。Section 12.2 の監視の
            // ため、読み飛ばしたバイト数を数える
            Frame::Padding(_) => {
                self.traffic.padding_bytes = self.traffic.padding_bytes.saturating_add(size as u64);
                Ok(())
            }
            Frame::Stream(f) => self.streams.on_stream_frame(&f),
            Frame::ResetStream(f) => self.streams.on_reset_stream(&f),
            Frame::StopSending(f) => self.streams.on_stop_sending(&f),
            Frame::MaxData(f) => {
                self.streams.on_max_data(f.maximum_data);
                Ok(())
            }
            Frame::MaxStreamData(f) => self.streams.on_max_stream_data(&f),
            Frame::MaxStreams(f) => self.streams.on_max_streams(&f),
            // ブロック通知は診断のための情報であり、受信側で行う処理はない
            // (RFC 9000 Section 19.12)
            Frame::DataBlocked(_) => Ok(()),
            // STREAM_DATA_BLOCKED は送信専用ストリームへの受信が
            // STREAM_STATE_ERROR (RFC 9000 Section 19.13)
            Frame::StreamDataBlocked(f) => self.streams.on_stream_data_blocked(&f),
            // STREAMS_BLOCKED は 2^60 を超える値が不正 (RFC 9000 Section 19.14)
            Frame::StreamsBlocked(f) => self.streams.on_streams_blocked(&f),
            Frame::ConnectionClose(f) => {
                self.on_connection_close(&f);
                Ok(())
            }
            Frame::Datagram(f) => self.on_datagram(&f, size),
        }
    }

    /// DATAGRAM フレームを処理する (RFC 9221)
    ///
    /// `size` は受信したフレーム全体のバイト数 (フレームタイプと Length を含む)。
    fn on_datagram(
        &mut self,
        datagram: &frame::datagram::Datagram,
        size: usize,
    ) -> Result<(), Error> {
        // RFC 9221 Section 3: max_datagram_frame_size の既定値 0 は非サポートを
        // 意味する。広告していない (None または 0) 場合は PROTOCOL_VIOLATION
        let Some(local_max) = self
            .local_params
            .max_datagram_frame_size
            .filter(|max| *max > 0)
        else {
            return Err(Error::Protocol {
                code: TransportErrorCode::PROTOCOL_VIOLATION,
                reason: "DATAGRAM frame was received without advertising support".to_string(),
            });
        };
        // RFC 9221 Section 3: 広告した値より大きい DATAGRAM フレームの受信も
        // PROTOCOL_VIOLATION
        if size as u64 > local_max {
            return Err(Error::Protocol {
                code: TransportErrorCode::PROTOCOL_VIOLATION,
                reason: format!(
                    "DATAGRAM frame of {size} bytes exceeds the advertised max_datagram_frame_size {local_max}"
                ),
            });
        }

        // QMux Section 6 / RFC 9221 Section 5.3: アプリケーションへ速やかに
        // 配送できない DATAGRAM は破棄してよい。保持量に上限を設け、上限を
        // 超える分は捨てる。
        if self.queued_datagrams >= MAX_QUEUED_DATAGRAMS
            || self.queued_datagram_bytes + datagram.data.len() > MAX_QUEUED_DATAGRAM_BYTES
        {
            // 破棄したことをアプリケーションが観測できるように回数を数える
            self.dropped_datagrams = self.dropped_datagrams.saturating_add(1);
            return Ok(());
        }

        // QMux では DATAGRAM は順序どおりに信頼性をもって配送される (Section 1.1)
        self.queued_datagrams += 1;
        self.queued_datagram_bytes += datagram.data.len();
        self.events
            .push_back(Event::Datagram(datagram.data.clone()));
        Ok(())
    }

    /// CONNECTION_CLOSE フレームを処理する (Section 7.3)
    fn on_connection_close(&mut self, close: &frame::connection_close::ConnectionClose) {
        self.state = State::Closed;
        // 送信側を閉じるため、送信待ちのフレームを破棄する
        self.streams.pending_frames.clear();
        self.events
            .push_back(Event::ConnectionClosed(CloseReason::Peer {
                application: close.application,
                error_code: close.error_code,
                reason: String::from_utf8_lossy(&close.reason_phrase).into_owned(),
            }));
    }

    /// ピアが下位トランスポートの送信側を shutdown したことを通知する (Section 7.3)
    ///
    /// TLS の close_notify や TCP の FIN など、下位トランスポートの読み取りで
    /// EOF を観測したときに呼び出す。Section 7.3 は、ピアの送信側の shutdown を
    /// 観測した受信側が自側の送信側を graceful に shutdown すること (SHOULD) を
    /// 定める。このメソッドは QMux の接続を閉じるだけで、送信側の shutdown は
    /// 呼び出し側が行う (応答フレームを送らない選択は Section 7.3 の許容範囲)。
    ///
    /// 送信待ちのフレームを破棄し、[`Event::ConnectionClosed`] を
    /// [`CloseReason::PeerShutdown`] で 1 回だけ通知する。以降は
    /// [`Connection::is_closed`] が `true` を返し、[`Connection::receive`] に渡された
    /// データは破棄され、アプリケーションの操作はエラーになる。2 回目以降の
    /// 呼び出しは何もしない (通知は重複しない)。
    ///
    /// 下位トランスポートが利用不能になった場合 (TCP のリセットや TLS の fatal
    /// alert) は、Section 7.4 の [`Connection::transport_closed`] を使う。両者は
    /// [`CloseReason`] で区別される。
    pub fn peer_shutdown(&mut self) {
        if self.is_closed() {
            return;
        }
        self.streams.pending_frames.clear();
        self.state = State::Closed;
        self.events
            .push_back(Event::ConnectionClosed(CloseReason::PeerShutdown));
    }

    /// 下位トランスポートが閉じたことを通知する (Section 7.4)
    ///
    /// TCP のリセットや TLS の fatal alert など、下位トランスポートが利用できなく
    /// なったときに呼び出す。QMux の接続は即座に終了する。
    ///
    /// 送信待ちのフレームを破棄し、[`Event::ConnectionClosed`] を
    /// [`CloseReason::TransportClosed`] で 1 回だけ通知する。以降は
    /// [`Connection::is_closed`] が `true` を返し、[`Connection::receive`] に渡された
    /// データは破棄され、アプリケーションの操作はエラーになる。2 回目以降の
    /// 呼び出しは何もしない (通知は重複しない)。
    pub fn transport_closed(&mut self) {
        if self.is_closed() {
            return;
        }
        self.streams.pending_frames.clear();
        self.state = State::Closed;
        self.events
            .push_back(Event::ConnectionClosed(CloseReason::TransportClosed));
    }

    /// プロトコル違反を検出した接続を閉じる
    fn close_with_error(&mut self, error: &Error) {
        let (code, reason) = match error {
            Error::Protocol { code, reason } => (*code, reason.clone()),
            // Record の境界で途切れたフレームはフレーム符号化エラーとして通知する
            // (Section 3.2)
            Error::Incomplete { .. } => (
                TransportErrorCode::FRAME_ENCODING_ERROR,
                "truncated frame inside a QMux record".to_string(),
            ),
            // 解析結果の矛盾など、ここに到達するのは実装の不具合
            Error::InvalidData(reason) => (TransportErrorCode::INTERNAL_ERROR, reason.clone()),
        };

        self.queue_connection_close(false, code.as_u64(), &reason);
        self.state = State::Closing;
        self.events
            .push_back(Event::ConnectionClosed(CloseReason::Local {
                application: false,
                error_code: code.as_u64(),
                reason,
            }));
    }

    /// CONNECTION_CLOSE フレームを送信待ちの先頭に積む (Section 7.2)
    ///
    /// 切断を伝える Record を優先するため、未送信のフレームは破棄する。
    /// 理由はピアが必ず受け入れられる長さ (既定の max_record_size に収まる長さ) に
    /// 切り詰める。切り詰めないと、長い理由を渡された場合に Record へ収められず
    /// 切断自体が送信できなくなる。
    fn queue_connection_close(&mut self, application: bool, error_code: u64, reason: &str) {
        // フレームタイプ (1 バイト) + Error Code (最大 8 バイト) +
        // Frame Type (最大 8 バイト) + Reason Phrase Length (最大 8 バイト) を
        // 見込んだ余裕を残す
        let max_reason_len = record::DEFAULT_MAX_RECORD_SIZE as usize - 32;
        let reason_phrase = truncate_reason(reason, max_reason_len);

        let close = frame::connection_close::ConnectionClose {
            application,
            error_code,
            // 0x1c ではエラーを引き起こしたフレームタイプを通知する。原因の
            // フレームタイプを特定していない場合は 0 (unknown) を使う
            // (RFC 9000 Section 19.19)
            frame_type: if application { None } else { Some(0) },
            reason_phrase,
        };
        let mut encoded = Vec::new();
        QmuxFrame::Quic(Frame::ConnectionClose(close)).encode(&mut encoded);
        self.streams.pending_frames.clear();
        self.streams
            .pending_frames
            .push_front(PendingFrame::new(encoded));
    }
}

/// アプリケーションエラーコードが可変長整数で表現できることを検査する
///
/// RESET_STREAM / STOP_SENDING / CONNECTION_CLOSE のエラーコードは可変長整数で
/// 送るため、2^62-1 を超える値はフレームをエンコードできない (RFC 9000
/// Section 16)。公開 API の入口で検査し、フレームエンコード時の panic ではなく
/// [`Error::InvalidData`] として返す。
fn validate_application_error_code(error_code: u64) -> Result<(), Error> {
    VarInt::try_from(error_code).map(|_| ()).map_err(|_| {
        Error::InvalidData(format!(
            "application error code {error_code} exceeds the maximum variable-length integer {}",
            crate::codec::varint::MAX_VALUE
        ))
    })
}

/// 理由の文字列を、UTF-8 の文字境界を壊さずに指定した長さ以下へ切り詰める
///
/// RFC 9000 Section 19.19 は Reason Phrase を UTF-8 の文字列とすることを
/// 推奨しているため、バイト列の途中で切らない。
fn truncate_reason(reason: &str, max_len: usize) -> Vec<u8> {
    let bytes = reason.as_bytes();
    if bytes.len() <= max_len {
        return bytes.to_vec();
    }
    let mut end = max_len;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    bytes[..end].to_vec()
}
