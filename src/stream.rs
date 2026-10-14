//! QMux ストリームの状態とフロー制御 (RFC 9000 Section 2 から Section 4)
//!
//! QMux は QUIC version 1 のストリーム機構をそのまま使う。ただし STREAM フレームは
//! 各ストリームで順序どおりに届き (draft-ietf-quic-qmux-02 Section 4.1)、下位
//! トランスポートへ渡した時点で ACK されたものとみなされる (同 Section 4)。
//! そのため再送や並べ替えのためのバッファを持たず、送信側の状態も
//! RFC 9000 Section 3.1 の中間状態 (Send / Data Sent) を省略する。

use alloc::collections::{BTreeMap, VecDeque};
use alloc::string::ToString;
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::frame;
use crate::codec::frame::Frame;
use crate::codec::role::Role;
use crate::codec::varint::VarInt;

use crate::event::Event;
use crate::transport_params::{MAX_STREAMS_LIMIT, TransportParameters};

/// ストリームの種別 (RFC 9000 Section 2.1)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    /// クライアント起動の双方向ストリーム (0x00)
    ClientBidi,
    /// サーバー起動の双方向ストリーム (0x01)
    ServerBidi,
    /// クライアント起動の単方向ストリーム (0x02)
    ClientUni,
    /// サーバー起動の単方向ストリーム (0x03)
    ServerUni,
}

impl StreamType {
    /// ストリーム ID から種別を求める
    pub fn from_id(id: StreamId) -> Self {
        match id.as_u64() & 0x03 {
            0x00 => Self::ClientBidi,
            0x01 => Self::ServerBidi,
            0x02 => Self::ClientUni,
            _ => Self::ServerUni,
        }
    }

    /// 双方向ストリームかどうか
    pub fn is_bidirectional(self) -> bool {
        matches!(self, Self::ClientBidi | Self::ServerBidi)
    }

    /// クライアントが起動したストリームかどうか
    pub fn is_client_initiated(self) -> bool {
        matches!(self, Self::ClientBidi | Self::ClientUni)
    }

    /// ロールと方向からローカル起動のストリーム種別を求める
    pub fn local(role: Role, bidirectional: bool) -> Self {
        match (role, bidirectional) {
            (Role::Client, true) => Self::ClientBidi,
            (Role::Client, false) => Self::ClientUni,
            (Role::Server, true) => Self::ServerBidi,
            (Role::Server, false) => Self::ServerUni,
        }
    }
}

/// ストリーム ID (RFC 9000 Section 2.1)
///
/// 下位 2 ビットがストリームの種別を表す 62 ビットの整数。可変長整数として
/// エンコードされるため、値は 2^62-1 を超えられない。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(u64);

impl StreamId {
    /// 値からストリーム ID を作る
    ///
    /// 可変長整数で表現できない値 (2^62-1 を超える値) の場合は None を返す。
    pub const fn new(value: u64) -> Option<Self> {
        if value > crate::codec::varint::MAX_VALUE {
            None
        } else {
            Some(Self(value))
        }
    }

    /// 種別とシーケンス番号からストリーム ID を作る
    ///
    /// 可変長整数で表現できない値になる場合は None を返す。
    pub const fn from_parts(stream_type: StreamType, sequence: u64) -> Option<Self> {
        // 4 * sequence + 種別が 2^62-1 を超える場合は表現できない。
        // シーケンス番号はピアの入力由来になり得るため、乗算と加算の
        // オーバーフローを checked_* で検査する。
        let Some(value) = sequence.checked_mul(4) else {
            return None;
        };
        let Some(value) = value.checked_add(stream_type as u64) else {
            return None;
        };
        if value > crate::codec::varint::MAX_VALUE {
            None
        } else {
            Some(Self(value))
        }
    }

    /// 値を返す
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// ストリームの種別を返す
    pub fn stream_type(self) -> StreamType {
        StreamType::from_id(self)
    }

    /// シーケンス番号 (同じ種別の中で何番目に開かれたか) を返す
    pub const fn sequence(self) -> u64 {
        self.0 >> 2
    }

    /// ローカル起動のストリームかどうか
    pub fn is_local(self, role: Role) -> bool {
        self.stream_type().is_client_initiated() == matches!(role, Role::Client)
    }
}

/// 送信側の状態 (RFC 9000 Section 3.1 を QMux 用に簡略化)
///
/// QMux ではデータを下位トランスポートへ渡した時点で ACK されたものとみなすため、
/// Send / Data Sent / Reset Sent にあたる中間状態を持たない。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendState {
    /// データを送信できる (Ready / Send / Data Sent をまとめた状態)
    Ready,
    /// すべてのデータを送信済み (Data Recvd、終端)
    DataRecvd,
    /// RESET_STREAM を送信済み (Reset Recvd、終端)
    ResetRecvd,
}

impl SendState {
    /// 終端状態かどうか
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::DataRecvd | Self::ResetRecvd)
    }
}

/// 受信側の状態 (RFC 9000 Section 3.2 を QMux 用に簡略化)
///
/// QMux では STREAM フレームが順序どおりに届くため、全データの受信と最終サイズの
/// 確定は同時に起きる。アプリケーションが読み取るまでの間を `SizeKnown` として扱う。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvState {
    /// データを受信中 (Recv)
    Recv,
    /// FIN を受信して最終サイズが確定し、アプリケーションの読み取りを待っている (Size Known)
    SizeKnown,
    /// アプリケーションが全データを読み取った (Data Read、終端)
    DataRead,
    /// RESET_STREAM を受信した (Reset Recvd)
    ResetRecvd,
    /// アプリケーションがリセットを観測した (Reset Read、終端)
    ResetRead,
}

impl RecvState {
    /// 終端状態かどうか
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::DataRead | Self::ResetRead)
    }
}

/// ストリームの読み取り結果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadResult {
    /// データを読み取った
    Data {
        /// 読み取ったバイト数 (0 はデータが到着していない)
        len: usize,
        /// ストリームの終端まで読み取ったかどうか
        fin: bool,
    },
    /// ピアがストリームをリセットした
    Reset {
        /// ピアが指定したアプリケーションエラーコード
        error_code: u64,
    },
}

/// 個別ストリームの状態
#[derive(Debug)]
struct Stream {
    /// ストリーム ID
    id: StreamId,
    /// ピアが起動したストリームかどうか
    peer_initiated: bool,
    /// 送信側の状態 (受信専用ストリームでは None)
    send_state: Option<SendState>,
    /// 受信側の状態 (送信専用ストリームでは None)
    recv_state: Option<RecvState>,
    /// 次に送信するバイトのオフセット
    send_offset: u64,
    /// ピアが許可したストリーム単位の送信上限 (絶対オフセット)
    send_max_data: u64,
    /// 最後に STREAM_DATA_BLOCKED を送った上限 (重複送信の抑止)
    last_stream_data_blocked: Option<u64>,
    /// 次に受信するバイトのオフセット
    recv_offset: u64,
    /// アプリケーションが読み取ったバイト数の合計 (MAX_STREAM_DATA の前進基準)
    read_offset: u64,
    /// 自エンドポイントが許可したストリーム単位の受信上限 (絶対オフセット)
    recv_max_data: u64,
    /// 受信ウィンドウの初期値 (クレジット更新の閾値)
    recv_window: u64,
    /// 受信したがアプリケーションがまだ読み取っていないデータ
    recv_buffer: VecDeque<u8>,
    /// 最終サイズ (FIN または RESET_STREAM で確定する)
    final_size: Option<u64>,
    /// ピアが送ってきた RESET_STREAM のアプリケーションエラーコード
    reset_error_code: Option<u64>,
    /// 読み取り可能イベントを通知済みかどうか (同じ読み取り状態での重複通知の抑止)
    readable_notified: bool,
}

impl Stream {
    /// 送信側が終端かどうか
    fn send_finished(&self) -> bool {
        self.send_state.is_none_or(SendState::is_terminal)
    }

    /// 受信側が終端かどうか
    fn recv_finished(&self) -> bool {
        self.recv_state.is_none_or(RecvState::is_terminal)
    }

    /// 送受信の両方が終端かどうか
    fn is_finished(&self) -> bool {
        self.send_finished() && self.recv_finished()
    }
}

/// 接続レベルのフロー制御の状態 (RFC 9000 Section 4.1)
#[derive(Debug)]
struct FlowControl {
    /// ピアが許可した接続レベルの送信上限 (絶対バイト数)
    send_max_data: u64,
    /// 送信したストリームデータの合計
    send_total: u64,
    /// 自エンドポイントが許可した受信上限 (絶対バイト数)
    recv_max_data: u64,
    /// 受信したストリームデータの合計 (RESET_STREAM の最終サイズを含む)
    recv_total: u64,
    /// アプリケーションが消費したバイト数の合計 (MAX_DATA の前進基準)
    consumed_total: u64,
    /// 受信ウィンドウの初期値
    recv_window: u64,
    /// 最後に DATA_BLOCKED を送った上限 (重複送信の抑止)
    last_data_blocked: Option<u64>,
}

/// 接続内の全ストリームとフロー制御を管理する
///
/// [`crate::connection::Connection`] が接続レベルの処理 (Record のデコード、
/// トランスポートパラメータ、QX_PING、アイドルタイムアウト、切断) を担い、
/// ストリームに関する処理をこの構造体が担う。
#[derive(Debug)]
pub(crate) struct StreamManager {
    /// 接続のロール (ストリーム ID の起動側の判定に使う)
    role: Role,
    /// ストリーム ID からストリーム状態への対応
    streams: BTreeMap<StreamId, Stream>,
    /// ローカル起動の双方向ストリームの次のシーケンス番号
    next_local_bidi: u64,
    /// ローカル起動の単方向ストリームの次のシーケンス番号
    next_local_uni: u64,
    /// 自エンドポイントが広告した、ピア起動の双方向ストリーム数の上限
    local_max_streams_bidi: u64,
    /// 自エンドポイントが広告した、ピア起動の単方向ストリーム数の上限
    local_max_streams_uni: u64,
    /// ピアが広告した、ローカル起動の双方向ストリーム数の上限
    peer_max_streams_bidi: u64,
    /// ピアが広告した、ローカル起動の単方向ストリーム数の上限
    peer_max_streams_uni: u64,
    /// 作成済みのピア起動双方向ストリームのシーケンス番号の上限 + 1
    ///
    /// 終端して破棄したストリームも数える。この値より小さいシーケンス番号の
    /// フレームは、破棄済みのストリームへのフレームとして扱う。
    peer_bidi_created: u64,
    /// 作成済みのピア起動単方向ストリームのシーケンス番号の上限 + 1
    peer_uni_created: u64,
    /// 最後に STREAMS_BLOCKED を送った上限 (重複送信の抑止)
    last_streams_blocked_bidi: Option<u64>,
    /// 最後に STREAMS_BLOCKED を送った上限 (単方向)
    last_streams_blocked_uni: Option<u64>,
    /// ピアのトランスポートパラメータを受信済みかどうか
    peer_params_received: bool,
    /// ピアが広告した QMux Record の Size の上限
    peer_max_record_size: u64,
    /// ローカルの初期ストリームデータ上限 (ローカル起動の双方向)
    local_stream_data_bidi_local: u64,
    /// ローカルの初期ストリームデータ上限 (ピア起動の双方向)
    local_stream_data_bidi_remote: u64,
    /// ローカルの初期ストリームデータ上限 (単方向)
    local_stream_data_uni: u64,
    /// ピアの初期ストリームデータ上限 (ピア起動の双方向)
    peer_stream_data_bidi_local: u64,
    /// ピアの初期ストリームデータ上限 (ローカル起動の双方向)
    peer_stream_data_bidi_remote: u64,
    /// ピアの初期ストリームデータ上限 (単方向)
    peer_stream_data_uni: u64,
    /// 接続レベルのフロー制御
    flow: FlowControl,
    /// 送信待ちフレーム (エンコード済み)
    ///
    /// QX フレームや CONNECTION_CLOSE は [`crate::connection::Connection`] が
    /// ここへ直接追加する。
    pub(crate) pending_frames: VecDeque<Vec<u8>>,
    /// アプリケーションへ通知するイベント
    events: VecDeque<Event>,
}

impl StreamManager {
    /// ローカルのトランスポートパラメータからストリーム管理を初期化する
    pub(crate) fn new(role: Role, local: &TransportParameters) -> Self {
        Self {
            role,
            streams: BTreeMap::new(),
            next_local_bidi: 0,
            next_local_uni: 0,
            local_max_streams_bidi: local.initial_max_streams_bidi,
            local_max_streams_uni: local.initial_max_streams_uni,
            peer_max_streams_bidi: 0,
            peer_max_streams_uni: 0,
            peer_bidi_created: 0,
            peer_uni_created: 0,
            last_streams_blocked_bidi: None,
            last_streams_blocked_uni: None,
            peer_params_received: false,
            // ピアの max_record_size を受信するまでは既定値を送信の上限に使う。
            // 広告される値は既定値以上でなければならない (Section 5.2) ため、
            // 既定値以下の Record はどのピアにも送信できる。
            peer_max_record_size: crate::record::DEFAULT_MAX_RECORD_SIZE,
            local_stream_data_bidi_local: local.initial_max_stream_data_bidi_local,
            local_stream_data_bidi_remote: local.initial_max_stream_data_bidi_remote,
            local_stream_data_uni: local.initial_max_stream_data_uni,
            peer_stream_data_bidi_local: 0,
            peer_stream_data_bidi_remote: 0,
            peer_stream_data_uni: 0,
            flow: FlowControl {
                send_max_data: 0,
                send_total: 0,
                recv_max_data: local.initial_max_data,
                recv_total: 0,
                consumed_total: 0,
                recv_window: local.initial_max_data,
                last_data_blocked: None,
            },
            pending_frames: VecDeque::new(),
            events: VecDeque::new(),
        }
    }

    /// ピアのトランスポートパラメータを適用する
    ///
    /// QMux ではピアのトランスポートパラメータを受信するまで、ピアのフロー制御に
    /// 依存するフレームを送信してはならない (draft-ietf-quic-qmux-02 Section 4.2)。
    pub(crate) fn apply_peer_transport_parameters(&mut self, peer: &TransportParameters) {
        self.peer_params_received = true;
        self.peer_max_record_size = peer.max_record_size;
        self.peer_max_streams_bidi = peer.initial_max_streams_bidi;
        self.peer_max_streams_uni = peer.initial_max_streams_uni;
        self.peer_stream_data_bidi_local = peer.initial_max_stream_data_bidi_local;
        self.peer_stream_data_bidi_remote = peer.initial_max_stream_data_bidi_remote;
        self.peer_stream_data_uni = peer.initial_max_stream_data_uni;
        self.flow.send_max_data = peer.initial_max_data;
        self.flow.last_data_blocked = None;
    }

    /// ピアが広告した QMux Record の Size の上限を返す
    pub(crate) fn peer_max_record_size(&self) -> u64 {
        self.peer_max_record_size
    }

    /// アプリケーションへ通知するイベントを 1 つ取り出す
    pub(crate) fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// ローカル起動のストリームを開く
    pub(crate) fn open_local(&mut self, bidirectional: bool) -> Result<StreamId, Error> {
        if !self.peer_params_received {
            // ピアのストリーム数上限が不明なため、ストリームを開けない
            // (draft-ietf-quic-qmux-02 Section 4.2)
            return Err(Error::InvalidData(
                "peer transport parameters have not been received yet".to_string(),
            ));
        }

        let stream_type = StreamType::local(self.role, bidirectional);
        let (sequence, limit) = if bidirectional {
            (self.next_local_bidi, self.peer_max_streams_bidi)
        } else {
            (self.next_local_uni, self.peer_max_streams_uni)
        };

        if sequence >= limit {
            // ピアの上限に達しているため STREAMS_BLOCKED を送る
            // (RFC 9000 Section 4.6、上限ごとに 1 回だけ送る)
            let last = if bidirectional {
                &mut self.last_streams_blocked_bidi
            } else {
                &mut self.last_streams_blocked_uni
            };
            if *last != Some(limit) {
                *last = Some(limit);
                let streams_blocked = frame::streams_blocked::StreamsBlocked {
                    bidirectional,
                    maximum_streams: limit,
                };
                self.push_frame(&Frame::StreamsBlocked(streams_blocked));
            }
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_LIMIT_ERROR,
                reason: format!("peer allows only {limit} streams of this type"),
            });
        }

        let id = StreamId::from_parts(stream_type, sequence)
            .ok_or_else(|| Error::InvalidData("stream ID exceeds the maximum value".to_string()))?;

        // ローカル起動の双方向ストリームの送信上限は、ピアから見てピア起動
        // (remote) の双方向ストリームの上限になる (RFC 9000 Section 18.2)
        let (send_state, send_max_data, recv_state, recv_max_data, recv_window) = if bidirectional {
            (
                Some(SendState::Ready),
                self.peer_stream_data_bidi_remote,
                Some(RecvState::Recv),
                self.local_stream_data_bidi_local,
                self.local_stream_data_bidi_local,
            )
        } else {
            (
                Some(SendState::Ready),
                self.peer_stream_data_uni,
                None,
                0,
                0,
            )
        };

        self.streams.insert(
            id,
            Stream {
                id,
                peer_initiated: false,
                send_state,
                recv_state,
                send_offset: 0,
                send_max_data,
                last_stream_data_blocked: None,
                recv_offset: 0,
                read_offset: 0,
                recv_max_data,
                recv_window,
                recv_buffer: VecDeque::new(),
                final_size: None,
                reset_error_code: None,
                readable_notified: false,
            },
        );

        if bidirectional {
            self.next_local_bidi += 1;
        } else {
            self.next_local_uni += 1;
        }
        Ok(id)
    }

    /// ストリームへデータを書き込む
    ///
    /// 戻り値は受け付けたバイト数。フロー制御により一度に書き込めない場合は
    /// 一部だけを受け付け、残りは呼び出し側が再度書き込む。FIN は渡されたデータを
    /// すべて受け付けた場合にのみ送信する。
    pub(crate) fn write(&mut self, id: StreamId, data: &[u8], fin: bool) -> Result<usize, Error> {
        let conn_credit = self.flow.send_max_data - self.flow.send_total;
        let peer_max_record_size = self.peer_max_record_size;

        // 送信するフレームを決めるまでをブロックに閉じ込め、ストリームの借用を
        // 送信待ちキューへの追加より先に終わらせる
        let (len, send_fin, stream_credit, frame) = {
            let Some(stream) = self.streams.get_mut(&id) else {
                return Err(self.unknown_or_closed_stream_error(id, true));
            };
            let Some(send_state) = stream.send_state else {
                return Err(Error::Protocol {
                    code: TransportErrorCode::STREAM_STATE_ERROR,
                    reason: format!("stream {} is receive-only", id.as_u64()),
                });
            };
            // 終端状態では STREAM フレームを送信してはならない (RFC 9000 Section 3.3)
            if send_state.is_terminal() {
                return Err(Error::Protocol {
                    code: TransportErrorCode::STREAM_STATE_ERROR,
                    reason: format!("stream {} is closed for sending", id.as_u64()),
                });
            }
            if data.is_empty() && !fin {
                return Ok(0);
            }

            // 送信可能量は、ストリーム単位のクレジット、接続レベルのクレジット、
            // 1 つの STREAM フレームがピアの max_record_size に収まる長さの最小値
            let stream_credit = stream.send_max_data - stream.send_offset;
            let frame_limit = max_frame_data_len(peer_max_record_size, id, stream.send_offset);
            let limit = stream_credit.min(conn_credit).min(frame_limit);
            let len = core::cmp::min(data.len() as u64, limit) as usize;

            // FIN はすべてのデータを送信できた場合にのみ付ける
            // (RFC 9000 Section 19.8: FIN は最終サイズを示す)
            let send_fin = fin && len == data.len();
            if len == 0 && !send_fin {
                (0, false, stream_credit, None)
            } else {
                let frame = Frame::Stream(frame::stream::Stream {
                    stream_id: id.as_u64(),
                    offset: stream.send_offset,
                    fin: send_fin,
                    data: data[..len].to_vec(),
                });
                stream.send_offset += len as u64;
                if send_fin {
                    stream.send_state = Some(SendState::DataRecvd);
                }
                (len, send_fin, stream_credit, Some(frame))
            }
        };

        if len == 0 && !send_fin {
            self.note_blocked(id, stream_credit, conn_credit);
            return Ok(0);
        }
        let frame = frame.expect("a STREAM frame is created when data or FIN is sent");
        self.flow.send_total += len as u64;
        self.push_frame(&frame);
        if send_fin {
            // 送信側が終端し、受信側も終端していればストリームを破棄する
            self.finish_stream(id);
        }
        Ok(len)
    }

    /// ストリームからデータを読み取る
    ///
    /// 読み取ったバイト数に応じてフロー制御クレジット (MAX_STREAM_DATA /
    /// MAX_DATA) をピアへ返す。
    pub(crate) fn read(&mut self, id: StreamId, buf: &mut [u8]) -> Result<ReadResult, Error> {
        // ストリームの状態更新をブロックに閉じ込め、借用をクレジット更新の
        // フレーム追加より先に終わらせる
        let (result, max_stream_data, consumed, finished) = {
            let Some(stream) = self.streams.get_mut(&id) else {
                // 終端して破棄したストリームへの読み取りは EOF を返す
                // (アプリケーションは終端を先に観測している)
                if self.was_created(id) {
                    return Ok(ReadResult::Data { len: 0, fin: true });
                }
                return Err(Error::InvalidData(format!(
                    "unknown stream {}",
                    id.as_u64()
                )));
            };
            let Some(recv_state) = stream.recv_state else {
                return Err(Error::Protocol {
                    code: TransportErrorCode::STREAM_STATE_ERROR,
                    reason: format!("stream {} is send-only", id.as_u64()),
                });
            };

            match recv_state {
                // RESET_STREAM をアプリケーションが観測した (RFC 9000 Section 3.2)
                RecvState::ResetRecvd => {
                    stream.recv_state = Some(RecvState::ResetRead);
                    return Ok(ReadResult::Reset {
                        error_code: stream.reset_error_code.unwrap_or(0),
                    });
                }
                // 観測済みのリセットは繰り返し通知する (アプリケーションは冪等に扱える)
                RecvState::ResetRead => {
                    return Ok(ReadResult::Reset {
                        error_code: stream.reset_error_code.unwrap_or(0),
                    });
                }
                // すべて読み取り済み
                RecvState::DataRead => {
                    return Ok(ReadResult::Data { len: 0, fin: true });
                }
                RecvState::Recv | RecvState::SizeKnown => {}
            }

            let len = core::cmp::min(buf.len(), stream.recv_buffer.len());
            for (index, byte) in stream.recv_buffer.drain(..len).enumerate() {
                buf[index] = byte;
            }
            stream.read_offset += len as u64;

            // ストリーム単位のクレジット更新 (RFC 9000 Section 4.1)。
            // Recv 状態でのみ MAX_STREAM_DATA を送る (最終サイズが確定した後は不要)。
            let mut max_stream_data = None;
            if len > 0 && recv_state == RecvState::Recv && stream.recv_window > 0 {
                let remaining = stream.recv_max_data - stream.read_offset;
                if remaining <= stream.recv_window / 2 {
                    stream.recv_max_data = stream.read_offset + stream.recv_window;
                    max_stream_data = Some(Frame::MaxStreamData(
                        frame::max_stream_data::MaxStreamData {
                            stream_id: id.as_u64(),
                            maximum_stream_data: stream.recv_max_data,
                        },
                    ));
                }
            }

            // 読み切ったら、次のデータ到着時に改めて通知できるようにする
            if stream.recv_buffer.is_empty() {
                stream.readable_notified = false;
            }

            let finished = recv_state == RecvState::SizeKnown && stream.recv_buffer.is_empty();
            if finished {
                stream.recv_state = Some(RecvState::DataRead);
            }
            (
                ReadResult::Data { len, fin: finished },
                max_stream_data,
                len as u64,
                finished,
            )
        };

        if let Some(frame) = max_stream_data {
            self.push_frame(&frame);
        }
        // 接続レベルのクレジット更新 (RFC 9000 Section 4.1)
        if consumed > 0 {
            self.flow.consumed_total += consumed;
            self.maybe_advance_conn_window();
        }
        if finished {
            // 送受信の両方が終端したらストリームを破棄する
            self.finish_stream(id);
        }
        Ok(result)
    }

    /// ストリームの送信側をリセットする (RESET_STREAM を送る)
    pub(crate) fn reset(&mut self, id: StreamId, error_code: u64) -> Result<(), Error> {
        let Some(stream) = self.streams.get_mut(&id) else {
            return Err(self.unknown_or_closed_stream_error(id, true));
        };
        let Some(send_state) = stream.send_state else {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is receive-only", id.as_u64()),
            });
        };
        // 終端状態では RESET_STREAM を送信してはならない (RFC 9000 Section 3.3)
        if send_state.is_terminal() {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is closed for sending", id.as_u64()),
            });
        }

        let final_size = stream.send_offset;
        stream.send_state = Some(SendState::ResetRecvd);
        let reset = frame::reset_stream::ResetStream {
            stream_id: id.as_u64(),
            application_protocol_error_code: error_code,
            final_size,
        };
        self.push_frame(&Frame::ResetStream(reset));
        // 送信側が終端し、受信側も終端していればストリームを破棄する
        self.finish_stream(id);
        Ok(())
    }

    /// ストリームの受信側に STOP_SENDING を送る
    pub(crate) fn stop_sending(&mut self, id: StreamId, error_code: u64) -> Result<(), Error> {
        let Some(stream) = self.streams.get(&id) else {
            return Err(self.unknown_or_closed_stream_error(id, false));
        };
        let Some(recv_state) = stream.recv_state else {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is send-only", id.as_u64()),
            });
        };
        // すでにリセットを受け取ったストリームに STOP_SENDING を送る意味はない
        // (RFC 9000 Section 3.3)
        if matches!(recv_state, RecvState::ResetRecvd | RecvState::ResetRead) {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} was already reset by the peer", id.as_u64()),
            });
        }

        let stop_sending = frame::stop_sending::StopSending {
            stream_id: id.as_u64(),
            application_protocol_error_code: error_code,
        };
        self.push_frame(&Frame::StopSending(stop_sending));
        Ok(())
    }

    /// STREAM フレームを受信する (RFC 9000 Section 19.8)
    pub(crate) fn on_stream_frame(&mut self, f: &frame::stream::Stream) -> Result<(), Error> {
        let id = StreamId::new(f.stream_id)
            .expect("stream ID decoded from a variable-length integer is at most 2^62-1");
        if !self.ensure_stream_for_frame(id)? {
            // 終端して破棄したストリームへのフレームは処理しない
            // (RFC 9000 Section 3.2: 全データを受信済みのストリームへの
            //  STREAM フレームは破棄してよい)
            return Ok(());
        }

        let stream = self
            .streams
            .get_mut(&id)
            .expect("stream was created or already existed");
        let Some(recv_state) = stream.recv_state else {
            // 送信専用ストリームへの STREAM フレームは STREAM_STATE_ERROR
            // (RFC 9000 Section 19.8)
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is send-only", id.as_u64()),
            });
        };

        // QMux Section 4.1: 直前の STREAM フレームのペイロードに連続しない
        // ペイロードを持つ STREAM フレームを受信した場合は PROTOCOL_VIOLATION
        if f.offset != stream.recv_offset {
            return Err(Error::Protocol {
                code: TransportErrorCode::PROTOCOL_VIOLATION,
                reason: format!(
                    "STREAM frame offset {} does not continue stream {} at {}",
                    f.offset,
                    id.as_u64(),
                    stream.recv_offset
                ),
            });
        }

        let end = f
            .offset
            .checked_add(f.data.len() as u64)
            .ok_or_else(|| Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: format!("stream {} offset overflows", id.as_u64()),
            })?;

        // 最終サイズが確定している場合は、それを超えるデータを不正とする
        // (RFC 9000 Section 4.5)
        if let Some(final_size) = stream.final_size
            && end > final_size
        {
            return Err(Error::Protocol {
                code: TransportErrorCode::FINAL_SIZE_ERROR,
                reason: format!(
                    "stream {} received data up to {end} beyond the final size {final_size}",
                    id.as_u64()
                ),
            });
        }

        // フロー制御 (RFC 9000 Section 4.1)
        if end > stream.recv_max_data {
            return Err(Error::Protocol {
                code: TransportErrorCode::FLOW_CONTROL_ERROR,
                reason: format!(
                    "stream {} received data up to {end} beyond the stream limit {}",
                    id.as_u64(),
                    stream.recv_max_data
                ),
            });
        }
        if self.flow.recv_total + f.data.len() as u64 > self.flow.recv_max_data {
            return Err(Error::Protocol {
                code: TransportErrorCode::FLOW_CONTROL_ERROR,
                reason: format!(
                    "connection received data beyond the connection limit {}",
                    self.flow.recv_max_data
                ),
            });
        }

        // RESET_STREAM を受信済みのストリームではデータを破棄するが、
        // フロー制御の消費は数える (RFC 9000 Section 3.2)
        let discard = matches!(recv_state, RecvState::ResetRecvd | RecvState::ResetRead);
        self.flow.recv_total += f.data.len() as u64;
        stream.recv_offset = end;
        if discard {
            return Ok(());
        }

        if !f.data.is_empty() {
            stream.recv_buffer.extend(f.data.iter().copied());
        }
        if f.fin {
            stream.final_size = Some(end);
            if recv_state == RecvState::Recv {
                stream.recv_state = Some(RecvState::SizeKnown);
            }
        }
        // データと FIN のどちらも、アプリケーションにとっては読み取りの契機になる。
        // 空の FIN だけのフレームでも通知しないと、アプリケーションが終端を
        // 観測できない (RFC 9000 Section 19.8: FIN は最終サイズを示す)。
        if (!f.data.is_empty() || f.fin) && !stream.readable_notified {
            stream.readable_notified = true;
            self.events.push_back(Event::StreamReadable { id });
        }
        Ok(())
    }

    /// RESET_STREAM フレームを受信する (RFC 9000 Section 19.4)
    pub(crate) fn on_reset_stream(
        &mut self,
        f: &frame::reset_stream::ResetStream,
    ) -> Result<(), Error> {
        let id = StreamId::new(f.stream_id)
            .expect("stream ID decoded from a variable-length integer is at most 2^62-1");
        if !self.ensure_stream_for_frame(id)? {
            // 終端して破棄したストリームへのフレームは処理しない
            // (RFC 9000 Section 3.2)
            return Ok(());
        }

        let stream = self
            .streams
            .get_mut(&id)
            .expect("stream was created or already existed");
        let Some(recv_state) = stream.recv_state else {
            // 送信専用ストリームへの RESET_STREAM は STREAM_STATE_ERROR
            // (RFC 9000 Section 19.4)
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is send-only", id.as_u64()),
            });
        };

        // 最終サイズが確定済みの場合は変更を許さない (RFC 9000 Section 4.5)
        if let Some(final_size) = stream.final_size
            && final_size != f.final_size
        {
            return Err(Error::Protocol {
                code: TransportErrorCode::FINAL_SIZE_ERROR,
                reason: format!(
                    "stream {} final size changed from {final_size} to {}",
                    id.as_u64(),
                    f.final_size
                ),
            });
        }
        if f.final_size < stream.recv_offset {
            return Err(Error::Protocol {
                code: TransportErrorCode::FINAL_SIZE_ERROR,
                reason: format!(
                    "stream {} final size {} is below the received offset {}",
                    id.as_u64(),
                    f.final_size,
                    stream.recv_offset
                ),
            });
        }

        // 最終サイズはフロー制御のクレジットを消費する (RFC 9000 Section 4.5)
        if f.final_size > stream.recv_max_data {
            return Err(Error::Protocol {
                code: TransportErrorCode::FLOW_CONTROL_ERROR,
                reason: format!(
                    "stream {} final size {} exceeds the stream limit {}",
                    id.as_u64(),
                    f.final_size,
                    stream.recv_max_data
                ),
            });
        }
        let additional = f.final_size - stream.recv_offset;
        if self.flow.recv_total + additional > self.flow.recv_max_data {
            return Err(Error::Protocol {
                code: TransportErrorCode::FLOW_CONTROL_ERROR,
                reason: format!(
                    "connection received data beyond the connection limit {}",
                    self.flow.recv_max_data
                ),
            });
        }

        // アプリケーションがすべて読み取り済みのストリームではリセットを通知しない
        // (RFC 9000 Section 3.2: 全データを受信済みならリセットを抑制してよい)
        if recv_state == RecvState::DataRead {
            return Ok(());
        }
        // 同じ RESET_STREAM を再受信しても状態とイベントは変えない
        // (RFC 9000 Section 12.4: フレームは冪等でなければならない)
        if matches!(recv_state, RecvState::ResetRecvd | RecvState::ResetRead) {
            return Ok(());
        }

        self.flow.recv_total += additional;
        stream.recv_offset = f.final_size;
        stream.final_size = Some(f.final_size);
        stream.reset_error_code = Some(f.application_protocol_error_code);
        // 破棄するデータとリセットで確定した分はアプリケーションが消費しないため、
        // 接続レベルのクレジットとして返却する。返却しないとウィンドウが
        // 解放されず、ピアが接続レベルのフロー制御で恒久的にブロックされる。
        self.flow.consumed_total += additional + stream.recv_buffer.len() as u64;
        stream.recv_buffer.clear();
        stream.recv_state = Some(RecvState::ResetRecvd);
        self.maybe_advance_conn_window();
        self.events.push_back(Event::StreamReset {
            id,
            error_code: f.application_protocol_error_code,
        });
        Ok(())
    }

    /// STOP_SENDING フレームを受信する (RFC 9000 Section 19.5)
    pub(crate) fn on_stop_sending(
        &mut self,
        f: &frame::stop_sending::StopSending,
    ) -> Result<(), Error> {
        let id = StreamId::new(f.stream_id)
            .expect("stream ID decoded from a variable-length integer is at most 2^62-1");
        if !self.ensure_stream_for_frame(id)? {
            // 終端して破棄したストリームへのフレームは処理しない
            // (RFC 9000 Section 3.2)
            return Ok(());
        }

        let stream = self
            .streams
            .get_mut(&id)
            .expect("stream was created or already existed");
        let Some(send_state) = stream.send_state else {
            // 受信専用ストリームへの STOP_SENDING は STREAM_STATE_ERROR
            // (RFC 9000 Section 19.5)
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is receive-only", id.as_u64()),
            });
        };

        // Ready 状態のストリームでは RESET_STREAM を送って送信側を終了させる
        // (RFC 9000 Section 3.5)。すでに送信を終えたストリームでは何もしない。
        if send_state == SendState::Ready {
            let final_size = stream.send_offset;
            stream.send_state = Some(SendState::ResetRecvd);
            let reset = frame::reset_stream::ResetStream {
                stream_id: id.as_u64(),
                // STOP_SENDING のエラーコードをコピーする (RFC 9000 Section 3.5)
                application_protocol_error_code: f.application_protocol_error_code,
                final_size,
            };
            self.push_frame(&Frame::ResetStream(reset));
            // 送信側が終端し、受信側も終端していればストリームを破棄する
            self.finish_stream(id);
        }

        self.events.push_back(Event::StreamStopSending {
            id,
            error_code: f.application_protocol_error_code,
        });
        Ok(())
    }

    /// MAX_DATA フレームを受信する (RFC 9000 Section 19.9)
    pub(crate) fn on_max_data(&mut self, maximum_data: u64) {
        // 上限を増やさないフレームは無視する (RFC 9000 Section 4.1)
        if maximum_data <= self.flow.send_max_data {
            return;
        }
        let previous_limit = self.flow.send_max_data;
        self.flow.send_max_data = maximum_data;
        self.flow.last_data_blocked = None;

        // 接続レベルのクレジットで止まっていたストリームをアプリケーションへ通知する。
        // send_offset >= previous_limit は「直前の接続上限に達していた」ことの近似で
        // あり、複数ストリームでクレジットを分け合っている場合、send_offset が上限
        // 未満でも接続クレジットで止まっているストリームを取りこぼしうる。通知は
        // 助言であり、アプリが write を再試行すれば進むため、ここでは近似的に絞る。
        let writable: Vec<StreamId> = self
            .streams
            .values()
            .filter(|s| s.send_state == Some(SendState::Ready) && s.send_offset >= previous_limit)
            .map(|s| s.id)
            .collect();
        for id in writable {
            self.events.push_back(Event::StreamWritable { id });
        }
    }

    /// MAX_STREAM_DATA フレームを受信する (RFC 9000 Section 19.10)
    pub(crate) fn on_max_stream_data(
        &mut self,
        f: &frame::max_stream_data::MaxStreamData,
    ) -> Result<(), Error> {
        let id = StreamId::new(f.stream_id)
            .expect("stream ID decoded from a variable-length integer is at most 2^62-1");
        // ピア起動の双方向ストリームでは MAX_STREAM_DATA の受信がストリームを
        // 作成する (RFC 9000 Section 3.2)。ローカル起動の未作成ストリームへの
        // MAX_STREAM_DATA は STREAM_STATE_ERROR (RFC 9000 Section 19.10)。
        if !self.ensure_stream_for_frame(id)? {
            // 終端して破棄したストリームへのフレームは処理しない
            // (RFC 9000 Section 3.2)
            return Ok(());
        }
        let stream = self
            .streams
            .get_mut(&id)
            .expect("stream was created or already existed");
        if stream.send_state.is_none() {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is receive-only", id.as_u64()),
            });
        }

        // 上限を増やさないフレームは無視する (RFC 9000 Section 4.1)
        if f.maximum_stream_data <= stream.send_max_data {
            return Ok(());
        }
        stream.send_max_data = f.maximum_stream_data;
        stream.last_stream_data_blocked = None;
        self.events.push_back(Event::StreamWritable { id });
        Ok(())
    }

    /// MAX_STREAMS フレームを受信する (RFC 9000 Section 19.11)
    pub(crate) fn on_max_streams(
        &mut self,
        f: &frame::max_streams::MaxStreams,
    ) -> Result<(), Error> {
        // RFC 9000 Section 4.6: 2^60 を超える上限は FRAME_ENCODING_ERROR
        if f.maximum_streams > MAX_STREAMS_LIMIT {
            return Err(Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: format!("maximum_streams {} exceeds 2^60", f.maximum_streams),
            });
        }

        let (limit, last) = if f.bidirectional {
            (
                &mut self.peer_max_streams_bidi,
                &mut self.last_streams_blocked_bidi,
            )
        } else {
            (
                &mut self.peer_max_streams_uni,
                &mut self.last_streams_blocked_uni,
            )
        };
        // 上限を増やさないフレームは無視する (RFC 9000 Section 4.6)
        if f.maximum_streams <= *limit {
            return Ok(());
        }
        *limit = f.maximum_streams;
        *last = None;
        // ストリームを開けるようになったことをアプリケーションへ通知する
        self.events.push_back(Event::StreamsAvailable {
            bidirectional: f.bidirectional,
        });
        Ok(())
    }

    /// STREAM_DATA_BLOCKED フレームを受信する (RFC 9000 Section 19.13)
    pub(crate) fn on_stream_data_blocked(
        &mut self,
        f: &frame::stream_data_blocked::StreamDataBlocked,
    ) -> Result<(), Error> {
        let id = StreamId::new(f.stream_id)
            .expect("stream ID decoded from a variable-length integer is at most 2^62-1");
        if !self.ensure_stream_for_frame(id)? {
            // 終端して破棄したストリームへのフレームは処理しない
            // (RFC 9000 Section 3.2)
            return Ok(());
        }
        let stream = self
            .streams
            .get(&id)
            .expect("stream was created or already existed");
        // 送信専用ストリームへの STREAM_DATA_BLOCKED は STREAM_STATE_ERROR
        // (RFC 9000 Section 19.13)
        if stream.recv_state.is_none() {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("stream {} is send-only", id.as_u64()),
            });
        }
        // ブロック通知は診断のための情報であり、受信側で行う処理はない
        Ok(())
    }

    /// STREAMS_BLOCKED フレームを受信する (RFC 9000 Section 19.14)
    pub(crate) fn on_streams_blocked(
        &mut self,
        f: &frame::streams_blocked::StreamsBlocked,
    ) -> Result<(), Error> {
        // RFC 9000 Section 19.14: 2^60 を超える上限は STREAM_LIMIT_ERROR または
        // FRAME_ENCODING_ERROR。MAX_STREAMS の受信時の扱いに揃える。
        if f.maximum_streams > MAX_STREAMS_LIMIT {
            return Err(Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: format!("maximum_streams {} exceeds 2^60", f.maximum_streams),
            });
        }
        // ブロック通知は診断のための情報であり、受信側で行う処理はない
        Ok(())
    }

    /// フレームが参照するストリームを用意する
    ///
    /// 戻り値が `false` の場合は、すでに終端して破棄したストリームへのフレームで
    /// あり、呼び出し側はそのフレームを破棄する (RFC 9000 Section 3.2)。
    /// ピア起動のストリームであれば作成し、ローカル起動のストリームが未作成の
    /// 場合は STREAM_STATE_ERROR を返す (RFC 9000 Section 19.8)。
    fn ensure_stream_for_frame(&mut self, id: StreamId) -> Result<bool, Error> {
        if self.streams.contains_key(&id) {
            return Ok(true);
        }
        if self.was_created(id) {
            // 作成済みで、終端して破棄したストリームへのフレーム
            return Ok(false);
        }
        if id.is_local(self.role) {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!("locally initiated stream {} does not exist", id.as_u64()),
            });
        }
        self.create_peer_streams(id)?;
        Ok(true)
    }

    /// ストリームが作成済みかどうか (終端して破棄したものを含む)
    ///
    /// ピア起動のストリームはシーケンス番号が連続して作成されるため、作成済みの
    /// 番号の上限と比較すれば判定できる。ローカル起動のストリームはアプリケーションが
    /// 開いた数と比較する。
    fn was_created(&self, id: StreamId) -> bool {
        let bidirectional = id.stream_type().is_bidirectional();
        let sequence = id.sequence();
        let created = if id.is_local(self.role) {
            if bidirectional {
                self.next_local_bidi
            } else {
                self.next_local_uni
            }
        } else if bidirectional {
            self.peer_bidi_created
        } else {
            self.peer_uni_created
        };
        sequence < created
    }

    /// 終端して破棄したストリームと未作成のストリームを区別してエラーを返す
    fn unknown_or_closed_stream_error(&self, id: StreamId, sending: bool) -> Error {
        if self.was_created(id) {
            // 終端して破棄したストリーム
            return Error::Protocol {
                code: TransportErrorCode::STREAM_STATE_ERROR,
                reason: format!(
                    "stream {} is closed for {}",
                    id.as_u64(),
                    if sending { "sending" } else { "receiving" }
                ),
            };
        }
        Error::InvalidData(format!("unknown stream {}", id.as_u64()))
    }

    /// ピア起動のストリームを、指定した ID までの未作成分も含めて作成する
    ///
    /// RFC 9000 Section 3.2: ストリームを作成する前に、同じ種別の下位番号の
    /// ストリームをすべて作成しなければならない。作成順序を両端で一致させるため、
    /// スキップされた番号のストリームも作成して通知する。
    fn create_peer_streams(&mut self, id: StreamId) -> Result<(), Error> {
        let stream_type = id.stream_type();
        let bidirectional = stream_type.is_bidirectional();
        let sequence = id.sequence();

        // 自エンドポイントが広告した上限を超えるストリームを開こうとした場合は
        // STREAM_LIMIT_ERROR (RFC 9000 Section 4.6)
        let limit = if bidirectional {
            self.local_max_streams_bidi
        } else {
            self.local_max_streams_uni
        };
        if sequence >= limit {
            return Err(Error::Protocol {
                code: TransportErrorCode::STREAM_LIMIT_ERROR,
                reason: format!(
                    "peer opened stream {} beyond the advertised limit {limit}",
                    id.as_u64()
                ),
            });
        }

        let created = if bidirectional {
            self.peer_bidi_created
        } else {
            self.peer_uni_created
        };

        for sequence in created..=sequence {
            let stream_id = StreamId::from_parts(stream_type, sequence).ok_or_else(|| {
                Error::InvalidData("stream ID exceeds the maximum value".to_string())
            })?;
            self.insert_peer_stream(stream_id);
            self.events.push_back(Event::StreamOpened { id: stream_id });
        }

        if bidirectional {
            self.peer_bidi_created = sequence + 1;
        } else {
            self.peer_uni_created = sequence + 1;
        }
        Ok(())
    }

    /// ピア起動のストリームを 1 つ作成する
    fn insert_peer_stream(&mut self, id: StreamId) {
        let bidirectional = id.stream_type().is_bidirectional();

        // ピア起動の双方向ストリームでは、送信上限はピアから見てローカル起動
        // (local) の双方向ストリームの上限、受信上限は自エンドポイントから見て
        // ピア起動 (remote) の双方向ストリームの上限になる (RFC 9000 Section 18.2)
        let (send_state, send_max_data, recv_state, recv_max_data, recv_window) = if bidirectional {
            (
                Some(SendState::Ready),
                self.peer_stream_data_bidi_local,
                Some(RecvState::Recv),
                self.local_stream_data_bidi_remote,
                self.local_stream_data_bidi_remote,
            )
        } else {
            (
                None,
                0,
                Some(RecvState::Recv),
                self.local_stream_data_uni,
                self.local_stream_data_uni,
            )
        };

        self.streams.insert(
            id,
            Stream {
                id,
                peer_initiated: true,
                send_state,
                recv_state,
                send_offset: 0,
                send_max_data,
                last_stream_data_blocked: None,
                recv_offset: 0,
                read_offset: 0,
                recv_max_data,
                recv_window,
                recv_buffer: VecDeque::new(),
                final_size: None,
                reset_error_code: None,
                readable_notified: false,
            },
        );
    }

    /// 終端したストリームを破棄し、ピア起動であれば MAX_STREAMS で上限を返却する
    ///
    /// 終端したストリームを保持し続けると、ピアがストリームを開いては閉じることを
    /// 繰り返すだけでメモリを消費させられる。RFC 9000 Section 4.6 は、閉じた
    /// ストリームの分だけ上限を増やす実装を許しているため、上限を 1 増やすたびに
    /// MAX_STREAMS を送り、同時に開けるストリーム数は初期値のまま保つ。
    /// 破棄したストリームへのフレームは [`StreamManager::was_created`] で判別して
    /// 破棄する。
    fn finish_stream(&mut self, id: StreamId) {
        let Some(stream) = self.streams.get(&id) else {
            return;
        };
        if !stream.is_finished() {
            return;
        }
        let peer_initiated = stream.peer_initiated;
        self.streams.remove(&id);

        if !peer_initiated {
            return;
        }
        let maximum_streams = if id.stream_type().is_bidirectional() {
            // RFC 9000 Section 4.6: 2^60 を超える上限は送信できないため飽和させる
            self.local_max_streams_bidi = (self.local_max_streams_bidi + 1).min(MAX_STREAMS_LIMIT);
            frame::max_streams::MaxStreams {
                bidirectional: true,
                maximum_streams: self.local_max_streams_bidi,
            }
        } else {
            // RFC 9000 Section 4.6: 2^60 を超える上限は送信できないため飽和させる
            self.local_max_streams_uni = (self.local_max_streams_uni + 1).min(MAX_STREAMS_LIMIT);
            frame::max_streams::MaxStreams {
                bidirectional: false,
                maximum_streams: self.local_max_streams_uni,
            }
        };
        self.push_frame(&Frame::MaxStreams(maximum_streams));
    }

    /// 送信がブロックされたことをピアへ通知する (RFC 9000 Section 4.1)
    fn note_blocked(&mut self, id: StreamId, stream_credit: u64, conn_credit: u64) {
        if stream_credit == 0 {
            // 同じ上限で繰り返し通知しないよう、直前の上限と比較する
            let maximum_stream_data = self.streams.get_mut(&id).and_then(|stream| {
                if stream.last_stream_data_blocked == Some(stream.send_max_data) {
                    None
                } else {
                    stream.last_stream_data_blocked = Some(stream.send_max_data);
                    Some(stream.send_max_data)
                }
            });
            if let Some(maximum_stream_data) = maximum_stream_data {
                let blocked = frame::stream_data_blocked::StreamDataBlocked {
                    stream_id: id.as_u64(),
                    maximum_stream_data,
                };
                self.push_frame(&Frame::StreamDataBlocked(blocked));
            }
        }
        if conn_credit == 0 && self.flow.last_data_blocked != Some(self.flow.send_max_data) {
            self.flow.last_data_blocked = Some(self.flow.send_max_data);
            let blocked = frame::data_blocked::DataBlocked {
                maximum_data: self.flow.send_max_data,
            };
            self.push_frame(&Frame::DataBlocked(blocked));
        }
    }

    /// 接続レベルの受信ウィンドウを消費済みバイト数に応じて前進させる
    ///
    /// 未読のデータがウィンドウの半分以下になったら、消費済みバイト数を基準に
    /// クレジットを追加する。消費済み基準にすることで、アプリケーションが
    /// 読み取らない限りウィンドウが広がらず、受信バッファが積み上がらない。
    fn maybe_advance_conn_window(&mut self) {
        if self.flow.recv_window == 0 {
            return;
        }
        let remaining = self.flow.recv_max_data - self.flow.consumed_total;
        if remaining <= self.flow.recv_window / 2 {
            self.flow.recv_max_data = self.flow.consumed_total + self.flow.recv_window;
            let max_data = frame::max_data::MaxData {
                maximum_data: self.flow.recv_max_data,
            };
            self.push_frame(&Frame::MaxData(max_data));
        }
    }

    /// フレームをエンコードして送信待ちに追加する
    fn push_frame(&mut self, frame: &Frame) {
        let mut encoded = Vec::new();
        frame.encode(&mut encoded);
        self.pending_frames.push_back(encoded);
    }
}

/// 1 つの STREAM フレームに載せられる最大データ長を返す
///
/// フレーム全体がピアの max_record_size に収まる必要がある。Length フィールドの
/// 可変長整数は最大 8 バイトなので、安全側に 8 バイトを確保する。
fn max_frame_data_len(peer_max_record_size: u64, id: StreamId, offset: u64) -> u64 {
    let id_size = varint_size(id.as_u64());
    let offset_size = if offset > 0 { varint_size(offset) } else { 0 };
    // フレームタイプ (1 バイト) + Stream ID + Offset + Length (最大 8 バイト)
    let overhead = 1 + id_size + offset_size + 8;
    peer_max_record_size.saturating_sub(overhead as u64)
}

/// 可変長整数としてエンコードしたときのバイト数を返す
fn varint_size(value: u64) -> usize {
    VarInt::try_from(value)
        .expect("value fits in a variable-length integer")
        .encoded_size()
}
