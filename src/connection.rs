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

use crate::config::Config;
use crate::event::{CloseReason, Event};
use crate::frame::{QmuxFrame, QxPing, QxTransportParameters};
use crate::record::{self, RecordDecoder};
use crate::stream::{ReadResult, StreamId, StreamManager};
use crate::transport_params::TransportParameters;

/// アプリケーションへ未配送のまま保持する DATAGRAM の個数の上限
///
/// DATAGRAM はフロー制御されないため、ピアが連投した場合に備えて上限を設ける
/// (draft-ietf-quic-qmux-02 Section 6, Section 12.2)。上限を超えた DATAGRAM は
/// 破棄する (RFC 9221 Section 5.3 が明示的に許している)。
const MAX_QUEUED_DATAGRAMS: usize = 64;

/// アプリケーションへ未配送のまま保持する DATAGRAM の合計バイト数の上限
const MAX_QUEUED_DATAGRAM_BYTES: usize = 1 << 20;

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
    /// 接続のロール
    role: Role,
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
    /// 最後に Record を完全に送受信した時刻 (アイドルタイマーの起点)
    last_activity: Option<Timestamp>,
    /// アプリケーションへ未配送の DATAGRAM の個数
    queued_datagrams: usize,
    /// アプリケーションへ未配送の DATAGRAM の合計バイト数
    queued_datagram_bytes: usize,
    /// 保持量の上限を超えて破棄した受信 DATAGRAM の累計個数
    dropped_datagrams: u64,
}

impl Connection {
    /// 接続を作成する
    ///
    /// この時点では何も送信しない。最初の [`Connection::poll_transmit`] で
    /// QX_TRANSPORT_PARAMETERS フレームが送信される。
    ///
    /// 送信できない設定 (max_record_size が既定値未満、可変長整数で表現できない値
    /// など) の場合は [`crate::codec::error::Error::InvalidData`] を返す。
    pub fn new(role: Role, config: Config) -> Result<Self, Error> {
        let local_params = config.transport_parameters();
        // 誤設定を最初の送信まで遅延させないため、ここで一度エンコードして検証する。
        // ここを通れば poll_transmit での QX_TRANSPORT_PARAMETERS のエンコードは
        // 失敗しない。
        let mut probe = Vec::new();
        local_params.encode(&mut probe)?;
        let decoder = RecordDecoder::new(local_params.max_record_size);
        let streams = StreamManager::new(role, &local_params);
        Ok(Self {
            role,
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
            last_activity: None,
            queued_datagrams: 0,
            queued_datagram_bytes: 0,
            dropped_datagrams: 0,
        })
    }

    /// 接続のロールを返す
    pub fn role(&self) -> Role {
        self.role
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
            if let Err(error) = self.process_record(&record) {
                self.close_with_error(&error);
                break;
            }
            if self.is_closed() {
                break;
            }
        }
    }

    /// 送信する Record を 1 つ `buf` へ書き出す
    ///
    /// 書き出した場合は `true` を返す。呼び出し側は `false` が返るまで繰り返し、
    /// 書き出したバイト列を下位トランスポートへ渡すこと。
    ///
    /// 最初の呼び出しでは QX_TRANSPORT_PARAMETERS フレームだけを含む Record を
    /// 書き出す (Section 4.2)。
    pub fn poll_transmit(&mut self, buf: &mut Vec<u8>, now: Timestamp) -> Result<bool, Error> {
        if self.state == State::Closed {
            return Ok(false);
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
            self.touch(now);
            return Ok(true);
        }

        // 送信待ちのフレームを、ピアの max_record_size に収まる範囲で 1 つの
        // Record にまとめる (Section 3.2)
        let limit = self.streams.peer_max_record_size();
        let mut frames = Vec::new();
        while let Some(front) = self.streams.pending_frames.front() {
            let front_len = front.len() as u64;
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
            frames.extend_from_slice(front);
            self.streams.pending_frames.pop_front();
        }
        if frames.is_empty() {
            return Ok(false);
        }

        record::encode(&frames, limit, buf)?;
        // Record を完全に送信したのでアイドルタイマーをリセットする (Section 7.1)
        self.touch(now);
        Ok(true)
    }

    /// アプリケーションへ通知するイベントを 1 つ取り出す
    pub fn poll_event(&mut self) -> Option<Event> {
        if let Some(event) = self.events.pop_front() {
            if let Event::Datagram(ref data) = event {
                // 配送した分だけ保持量を減らし、新しい DATAGRAM を受け入れられるようにする
                self.queued_datagrams = self.queued_datagrams.saturating_sub(1);
                self.queued_datagram_bytes = self.queued_datagram_bytes.saturating_sub(data.len());
            }
            return Some(event);
        }
        self.streams.poll_event()
    }

    /// 次にタイマーを発火すべき時刻を返す
    ///
    /// アイドルタイムアウトが無効、またはまだ Record を送受信していない場合は
    /// `None` を返す。
    pub fn next_timeout(&self) -> Option<Timestamp> {
        if self.is_closed() {
            return None;
        }
        let timeout = self.effective_idle_timeout_millis();
        if timeout == 0 {
            return None;
        }
        Some(self.last_activity?.add(Duration::from_millis(timeout)))
    }

    /// タイマーを処理する
    ///
    /// [`Connection::next_timeout`] が返した時刻を過ぎたときに呼び出す。アイドル
    /// タイムアウトが満了した場合は接続を閉じ、[`Event::ConnectionClosed`] を通知する。
    /// このとき QMux フレームは送信せず、呼び出し側が下位トランスポートの送信側を
    /// 閉じる (Section 7.2)。
    pub fn handle_timeout(&mut self, now: Timestamp) {
        if self.is_closed() {
            return;
        }
        let Some(deadline) = self.next_timeout() else {
            return;
        };
        if !now.has_elapsed(deadline) {
            return;
        }

        // Section 7.1 / Section 7.2: アイドルタイムアウトではフレームを送信せずに
        // 接続を閉じる。
        self.streams.pending_frames.clear();
        self.state = State::Closed;
        self.events
            .push_back(Event::ConnectionClosed(CloseReason::IdleTimeout));
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
    pub fn write(&mut self, id: StreamId, data: &[u8], fin: bool) -> Result<usize, Error> {
        self.ensure_open()?;
        self.streams.write(id, data, fin)
    }

    /// ストリームからデータを読み取る
    ///
    /// 読み取った分のフロー制御クレジットがピアへ返る。
    pub fn read(&mut self, id: StreamId, buf: &mut [u8]) -> Result<ReadResult, Error> {
        self.ensure_open()?;
        self.streams.read(id, buf)
    }

    /// ストリームの送信側をリセットする (RESET_STREAM を送る)
    pub fn reset(&mut self, id: StreamId, error_code: u64) -> Result<(), Error> {
        self.ensure_open()?;
        self.streams.reset(id, error_code)
    }

    /// ストリームの受信側に送信停止を要求する (STOP_SENDING を送る)
    pub fn stop_sending(&mut self, id: StreamId, error_code: u64) -> Result<(), Error> {
        self.ensure_open()?;
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
        self.streams.pending_frames.push_back(encoded);
        Ok(())
    }

    /// QX_PING を送信し、シーケンス番号を返す
    ///
    /// ピアが応答すると [`Event::PingAcknowledged`] が通知される。
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
        self.streams.pending_frames.push_back(encoded);
        Ok(sequence.as_u64())
    }

    /// 接続を閉じる
    ///
    /// CONNECTION_CLOSE フレーム (アプリケーションエラー) を送信待ちに追加する。
    /// 呼び出し側は [`Connection::poll_transmit`] で送信した後、下位トランスポートの
    /// 送信側を閉じること (Section 7.2)。
    pub fn close(&mut self, error_code: u64, reason: &str) {
        if self.is_closed() {
            return;
        }
        self.queue_connection_close(true, error_code, reason);
        self.state = State::Closing;
        self.events
            .push_back(Event::ConnectionClosed(CloseReason::Local {
                application: true,
                error_code,
                reason: reason.to_string(),
            }));
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

                // 受け取ったシーケンス番号をそのまま返す
                let mut encoded = Vec::new();
                QmuxFrame::Ping(QxPing::Response {
                    sequence: VarInt::try_from(sequence).expect(
                        "QX_PING sequence received as a variable-length integer fits in one",
                    ),
                })
                .encode(&mut encoded);
                self.streams.pending_frames.push_back(encoded);
                self.events.push_back(Event::Ping { sequence });
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
            // PADDING は読み飛ばす (RFC 9000 Section 19.1)
            Frame::Padding(_) => Ok(()),
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

    /// 下位トランスポートが閉じたことを通知する (Section 7.4)
    ///
    /// TCP のリセットや TLS の fatal alert など、下位トランスポートが利用できなく
    /// なったときに呼び出す。QMux の接続は即座に終了する。
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
        self.streams.pending_frames.push_front(encoded);
    }
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
