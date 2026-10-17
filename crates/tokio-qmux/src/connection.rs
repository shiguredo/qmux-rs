//! QMux 接続の非同期ハンドルとドライバ
//!
//! 下位トランスポートの読み書きと時刻の供給をバックグラウンドのドライバタスクが
//! 行い、アプリケーションはストリームと DATAGRAM のハンドル越しに操作する。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Timestamp;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection as QmuxConnection;
use shiguredo_qmux::event::{CloseReason, Event};
use shiguredo_qmux::stream::{ReadResult, StreamId};
use shiguredo_qmux::transport_params::TransportParameters;

use crate::error::{Error, Result};
use crate::stream::{RecvStream, SendEvent, SendStream, StopSendingObserved, StreamEvent};

/// アプリケーションからドライバへのコマンド
#[derive(Debug)]
pub(crate) enum Command {
    /// 双方向ストリームを開く
    OpenBidirectional(oneshot::Sender<Result<(SendStream, RecvStream)>>),
    /// 単方向ストリームを開く
    OpenUnidirectional(oneshot::Sender<Result<SendStream>>),
    /// ストリームへデータを書き込む
    ///
    /// データをすべて受け付け、FIN を指定した場合は FIN の送信まで完了した時点で
    /// `ack` へ結果を返す。フロー制御で送れない場合はドライバが保留し、送れるように
    /// なってから返す。
    Write {
        id: StreamId,
        data: Vec<u8>,
        fin: bool,
        ack: oneshot::Sender<Result<()>>,
    },
    /// ストリームの送信側をリセットする
    Reset {
        id: StreamId,
        error_code: u64,
        ack: oneshot::Sender<Result<()>>,
    },
    /// ストリームの受信側に送信停止を要求する
    StopSending {
        id: StreamId,
        error_code: u64,
        ack: oneshot::Sender<Result<()>>,
    },
    /// DATAGRAM を送信する
    Datagram {
        data: Vec<u8>,
        ack: oneshot::Sender<Result<()>>,
    },
    /// QX_PING を送信する
    Ping(oneshot::Sender<Result<u64>>),
    /// ストリームの読み取り容量が空いたことをドライバへ通知する
    PollReadable { id: StreamId },
    /// アプリケーションがストリームの受信側を破棄した
    ///
    /// [`RecvStream`] の drop から送られる。ピアへ送信停止 (STOP_SENDING) を要求し、
    /// 以後このストリームを読まないようにする (RFC 9000 Section 3.5)。
    DropRecvStream { id: StreamId },
    /// 保留している配送の再試行を促す
    ///
    /// `accept_bidirectional` / `accept_unidirectional` / `recv_stop_sending` /
    /// `recv_ping_ack` が 1 件受け取った時点で送られる。これらのメソッドは受信
    /// チャネルを待つだけでドライバを起こさないため、受け渡し待ちのキュー
    /// (`pending_accept` / `pending_stop_sending` / `pending_ping_acks`) に残った分を
    /// 配送するきっかけが要る。
    Wake,
}

impl Command {
    /// ピアのトランスポートパラメータの受信を待つ必要があるかどうか
    ///
    /// QMux はピアのトランスポートパラメータを受信するまで、ピアのフロー制御に
    /// 依存するフレームを送信してはならない (draft-ietf-quic-qmux-02 Section 4.2)。
    fn requires_establishment(&self) -> bool {
        matches!(
            self,
            Self::OpenBidirectional(_) | Self::OpenUnidirectional(_) | Self::Datagram { .. }
        )
    }
}

/// コマンド送信チャネルの容量
const COMMAND_CAPACITY: usize = 64;
/// 接続を閉じる要求チャネルの容量
///
/// 容量 1 にして、未処理の close 要求が既にある場合の送信失敗を無害化する。
const CLOSE_CAPACITY: usize = 1;
/// ピア起動ストリームの受け渡しチャネルの容量
const ACCEPT_CAPACITY: usize = 16;
/// 受信 DATAGRAM の保持数の上限
const DATAGRAM_CAPACITY: usize = 64;
/// 接続単位で観測する STOP_SENDING の保持数の上限
///
/// あふれた分はドライバが保留し、空きができ次第配送する。
const STOP_SENDING_CAPACITY: usize = 16;
/// ストリームごとのイベントチャネルの容量
///
/// 受信側と送信側でそれぞれ 1 本のチャネルを使う。
const STREAM_EVENT_CAPACITY: usize = 16;
/// 下位トランスポートから一度に読み取るバイト数の上限
const READ_BUFFER_SIZE: usize = 64 * 1024;
/// 自側の close の後でピアの shutdown を待つ上限の既定値 (3 秒)
///
/// ピアが送信側を shutdown しない限りドライバが終了しない状態を避けるため、待ち時間
/// には必ず上限を設ける。QUIC version 1 の closing 状態が PTO の 3 倍を目安に終了する
/// のと同じオーダーであり (RFC 9000 Section 10.2.1)、TCP 上の FIN の往復には十分な
/// 長さである。[`Connection::with_linger`] で変更できる。
pub const DEFAULT_LINGER: Duration = Duration::from_secs(3);

/// QMux 接続
///
/// 下位トランスポートを所有するドライバタスクを起動する。[`Connection`] を
/// ドロップすると close 要求チャネルの送信側が閉じ、ドライバは下位トランスポートを
/// 閉じて終了する (linger は待たない)。ストリームのハンドルが残っていても、ドライバ
/// 自身がコマンド送信側を保持するため、コマンドチャネルは閉じない。
#[derive(Debug)]
pub struct Connection {
    /// ドライバへのコマンド送信
    commands: mpsc::Sender<Command>,
    /// 接続を閉じる要求 (容量 1 の専用チャネル)
    close_request: mpsc::Sender<(u64, String)>,
    /// ピア起動の双方向ストリームの受け取り
    ///
    /// 複数のタスクが `&self` で同時に待てるようにするため Mutex で包む。これは
    /// 共有状態の保護ではなく、`mpsc::Receiver::recv` が `&mut self` を要求する
    /// ための内部同期である。待ち手は 1 つを想定しており、競合しても順に処理される。
    accept_bidirectional: Mutex<mpsc::Receiver<(SendStream, RecvStream)>>,
    /// ピア起動の単方向ストリームの受け取り
    ///
    /// `accept_bidirectional` と同じ理由で Mutex で包む。
    accept_unidirectional: Mutex<mpsc::Receiver<RecvStream>>,
    /// 受信 DATAGRAM の受け取り
    ///
    /// `accept_bidirectional` と同じ理由で Mutex で包む。
    datagrams: Mutex<mpsc::Receiver<Vec<u8>>>,
    /// ピアが要求した送信停止の受け取り
    ///
    /// `accept_bidirectional` と同じ理由で Mutex で包む。
    stop_sending: Mutex<mpsc::Receiver<StopSendingObserved>>,
    /// QX_PING の応答 (シーケンス番号) の受け取り
    ///
    /// `accept_bidirectional` と同じ理由で Mutex で包む。
    ping_acks: Mutex<mpsc::Receiver<u64>>,
    /// ピアのトランスポートパラメータの最新値
    ///
    /// ドライバが受信時に watch で配る。`&self` でいつでも同期取得できる。
    peer_params: watch::Receiver<Option<TransportParameters>>,
    /// 破棄した受信 DATAGRAM の累計個数
    ///
    /// Sans I/O 側と tokio 側の破棄を合算した値をドライバが watch で配る。
    dropped_datagrams: watch::Receiver<u64>,
    /// 接続が閉じた理由
    closed: watch::Receiver<Option<CloseReason>>,
}

impl Connection {
    /// 下位トランスポート上に QMux 接続を確立する
    ///
    /// `io` は接続済みの双方向バイトストリーム (TCP、TLS over TCP、UNIX ソケット、
    /// テスト用の duplex など) を渡す。TLS と ALPN のネゴシエーションは呼び出し側の
    /// 責務である。tokio ランタイムのコンテキストで呼び出すこと。
    ///
    /// ピアの shutdown を待つ上限は [`DEFAULT_LINGER`] を使う。変更する場合は
    /// [`Self::with_linger`] を使う。
    ///
    /// # エラー
    ///
    /// `config` が送信できない設定 (`max_record_size` が既定値未満など) の場合は
    /// [`Error::Qmux`] を返す。`Config::default()` はフロー制御のクレジットが 0 で
    /// DATAGRAM も無効なため、ストリームや DATAGRAM を使う場合は明示的に設定する
    /// こと。
    pub fn new<T>(role: Role, io: T, config: Config) -> Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::with_linger(role, io, config, DEFAULT_LINGER)
    }

    /// ピアの shutdown を待つ上限を指定して QMux 接続を確立する
    ///
    /// [`Self::close`] を呼んだ後、ドライバは CONNECTION_CLOSE を送って下位
    /// トランスポートの送信側を graceful に shutdown し、ピアが送信側を shutdown
    /// するのを待つ (draft-ietf-quic-qmux-02 Section 7.2)。`linger` はこの待ち時間の
    /// 上限で、`close` を要求した時点から数える。
    ///
    /// 上限を過ぎるとドライバは下位トランスポートを閉じて終了し、[`Self::closed`] は
    /// [`CloseReason::Local`] を返す。ピアが shutdown しない場合だけでなく、ピアが
    /// 読み取りを止めて送信が詰まった場合にも、ドライバが残り続けないようにするためで
    /// ある。`Duration::ZERO` を指定すると、送信側の shutdown を試みた後でピアの
    /// shutdown を待たない。
    ///
    /// # エラー
    ///
    /// [`Self::new`] と同じ。
    pub fn with_linger<T>(role: Role, io: T, config: Config, linger: Duration) -> Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (close_request, close_request_rx) = mpsc::channel(CLOSE_CAPACITY);
        let (accept_bidirectional_tx, accept_bidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (accept_unidirectional_tx, accept_unidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (datagram_tx, datagram_rx) = mpsc::channel(DATAGRAM_CAPACITY);
        let (stop_sending_tx, stop_sending_rx) = mpsc::channel(STOP_SENDING_CAPACITY);
        let (ping_ack_tx, ping_ack_rx) = mpsc::channel(STOP_SENDING_CAPACITY);
        let (closed_tx, closed_rx) = watch::channel(None);
        let (peer_params_tx, peer_params_rx) = watch::channel(None);
        let (dropped_datagrams_tx, dropped_datagrams_rx) = watch::channel(0);

        // 送信できない設定はここでエラーにする。誤設定を最初の送信まで遅延させない。
        let connection = QmuxConnection::new(role, config)?;

        let (reader, writer) = tokio::io::split(io);
        let driver = Driver {
            connection,
            reader,
            writer,
            command_rx,
            command_tx: commands.clone(),
            close_request_rx,
            accept_bidirectional_tx,
            accept_unidirectional_tx,
            datagram_tx,
            stop_sending_tx,
            ping_ack_tx,
            peer_params_tx,
            dropped_datagrams_tx,
            closed_tx,
            recv_streams: BTreeMap::new(),
            send_streams: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            pending_readable: BTreeSet::new(),
            pending_commands: VecDeque::new(),
            pending_stop_sending: VecDeque::new(),
            pending_ping_acks: VecDeque::new(),
            pending_stream_events: VecDeque::new(),
            pending_accept: VecDeque::new(),
            output: Vec::new(),
            written: 0,
            datagram_drops: 0,
            established: false,
            linger,
            linger_started: None,
            write_shutdown: false,
            linger_expired: false,
            read_scratch: vec![0u8; READ_BUFFER_SIZE],
            started: Instant::now(),
            close_reason: None,
            transport_ended: false,
        };
        tokio::spawn(driver.run());

        Ok(Self {
            commands,
            close_request,
            accept_bidirectional: Mutex::new(accept_bidirectional_rx),
            accept_unidirectional: Mutex::new(accept_unidirectional_rx),
            datagrams: Mutex::new(datagram_rx),
            stop_sending: Mutex::new(stop_sending_rx),
            ping_acks: Mutex::new(ping_ack_rx),
            peer_params: peer_params_rx,
            dropped_datagrams: dropped_datagrams_rx,
            closed: closed_rx,
        })
    }

    /// 双方向ストリームを開く
    pub async fn open_bidirectional(&self) -> Result<(SendStream, RecvStream)> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::OpenBidirectional(reply))
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)?
    }

    /// 単方向ストリームを開く
    pub async fn open_unidirectional(&self) -> Result<SendStream> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::OpenUnidirectional(reply))
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)?
    }

    /// ピア起動の双方向ストリームを受け取る
    ///
    /// 接続が閉じた場合は `Ok(None)` を返す。`&self` で待てるため、[`std::sync::Arc`] で
    /// 共有した接続や、他の accept 系と同時に `select!` して待つことができる。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// まだ受け取っていないストリームは失われない。`select!` の腕に置いてよい。
    pub async fn accept_bidirectional(&self) -> Result<Option<(SendStream, RecvStream)>> {
        let accepted = self.accept_bidirectional.lock().await.recv().await;
        if accepted.is_some() {
            self.wake_driver();
        }
        Ok(accepted)
    }

    /// ピア起動の単方向ストリームを受け取る
    ///
    /// 接続が閉じた場合は `Ok(None)` を返す。[`Self::accept_bidirectional`] と同様に
    /// `&self` で待てる。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。まだ受け取っていないストリームは、
    /// 待っている間に future を破棄しても失われない。
    pub async fn accept_unidirectional(&self) -> Result<Option<RecvStream>> {
        let accepted = self.accept_unidirectional.lock().await.recv().await;
        if accepted.is_some() {
            self.wake_driver();
        }
        Ok(accepted)
    }

    /// 保留している配送の再試行をドライバへ促す
    ///
    /// コマンドチャネルが満杯の場合は通知を諦める。満杯とは未処理のコマンドが
    /// 溜まっている状態であり、それらを 1 つずつ処理する各ループで配送の再試行も
    /// 行われるためである。
    fn wake_driver(&self) {
        let _ = self.commands.try_send(Command::Wake);
    }

    /// DATAGRAM を送信する
    ///
    /// ピアが DATAGRAM 拡張を広告していない場合や、フレームがピアの
    /// `max_datagram_frame_size` / `max_record_size` を超える場合は
    /// [`Error::Qmux`] を返す (RFC 9221 Section 3、draft-ietf-quic-qmux-02
    /// Section 9.1)。DATAGRAM は信頼的に配送されるが、アプリケーションが速やかに
    /// 受け取れない場合は受信側で破棄されうる。
    pub async fn send_datagram(&self, data: &[u8]) -> Result<()> {
        let (ack, receive) = oneshot::channel();
        self.commands
            .send(Command::Datagram {
                data: data.to_vec(),
                ack,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)?
    }

    /// DATAGRAM を受信する
    ///
    /// 接続が閉じた場合は `Ok(None)` を返す。[`Self::accept_bidirectional`] と
    /// 同様に `&self` で待てる。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// まだ受け取っていない DATAGRAM は失われない。
    pub async fn recv_datagram(&self) -> Result<Option<Vec<u8>>> {
        Ok(self.datagrams.lock().await.recv().await)
    }

    /// 到着済みの DATAGRAM をまとめて受け取る
    ///
    /// 1 件以上届くまで待ち、その時点で受け取れる分をすべてまとめて返す。返る
    /// `Vec` は空にならない。接続が閉じた場合は `Ok(None)` を返す。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。最初の DATAGRAM を待っている間に future を
    /// 破棄しても、まだ受け取っていない DATAGRAM は失われない。2 件目以降の取り出しは
    /// await を挟まずに行うため、途中で破棄されることはない。
    pub async fn recv_datagrams(&self) -> Result<Option<Vec<Vec<u8>>>> {
        let mut datagrams = self.datagrams.lock().await;
        let Some(first) = datagrams.recv().await else {
            return Ok(None);
        };
        let mut batch = vec![first];
        while let Ok(datagram) = datagrams.try_recv() {
            batch.push(datagram);
        }
        Ok(Some(batch))
    }

    /// ピアが要求した送信停止を受け取る (STOP_SENDING)
    ///
    /// 対象のストリーム ID とピアが指定したアプリケーションエラーコードを返す
    /// (RFC 9000 Section 19.5)。接続が閉じた場合は `Ok(None)` を返す。
    /// [`Self::accept_bidirectional`] と同様に `&self` で待てるため、送信ストリーム
    /// を所有するタスクとは別のタスクで待つことができる。
    ///
    /// [`SendStream::stopped`] が返すのと同じ通知を接続単位で受け取る入り口である。
    /// ストリーム単位か接続単位かは、アプリケーションのタスク構成に応じて選ぶ。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// まだ受け取っていない通知は失われない。
    pub async fn recv_stop_sending(&self) -> Result<Option<StopSendingObserved>> {
        let observed = self.stop_sending.lock().await.recv().await;
        if observed.is_some() {
            self.wake_driver();
        }
        Ok(observed)
    }

    /// ピアのトランスポートパラメータを返す
    ///
    /// ピアから受信する前は `None` を返す。DATAGRAM を送れるかどうかは
    /// `max_datagram_frame_size`、フロー制御の上限は `initial_max_data` などで確認
    /// できる (RFC 9221 Section 3、RFC 9000 Section 18.2)。
    pub fn peer_transport_parameters(&self) -> Option<TransportParameters> {
        self.peer_params.borrow().clone()
    }

    /// 破棄した受信 DATAGRAM の累計個数を返す
    ///
    /// Sans I/O 側のキュー上限と tokio 側のチャネル上限を合算した値である。
    /// QMux Section 6 は速やかに配送できない DATAGRAM の破棄を許すため、この値で
    /// 取りこぼしを検知できる。
    pub fn dropped_datagrams(&self) -> u64 {
        *self.dropped_datagrams.borrow()
    }

    /// QX_PING を送信し、シーケンス番号を返す
    ///
    /// 応答の到着は [`Self::recv_ping_ack`] で待つ。QMux Section 4.3 は、応答前に
    /// 複数の要求を受けた場合に最大のシーケンス番号 1 件だけを返すことを許している
    /// ため、要求と応答は 1 対 1 にならないことがある。
    pub async fn ping(&self) -> Result<u64> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Ping(reply))
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)?
    }

    /// ピアが返した QX_PING の応答 (シーケンス番号) を受け取る
    ///
    /// [`Self::ping`] が返したシーケンス番号以下で、まだ受け取っていない応答のうち
    /// 最も新しいものを返す (`Event::PingAcknowledged` に対応する)。接続が閉じた
    /// 場合は `Ok(None)` を返す。[`Self::accept_bidirectional`] と同様に `&self` で
    /// 待てる。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// まだ受け取っていない応答は失われない。
    pub async fn recv_ping_ack(&self) -> Result<Option<u64>> {
        let sequence = self.ping_acks.lock().await.recv().await;
        if sequence.is_some() {
            self.wake_driver();
        }
        Ok(sequence)
    }

    /// 接続を閉じる
    ///
    /// CONNECTION_CLOSE フレームが送信される。容量 1 の専用チャネルへ送るため、
    /// コマンドチャネルの輻輳で要求が失われることはない。すでに未処理の close 要求が
    /// ある場合の送信失敗は、先行する要求が処理されるため無視してよい。
    ///
    /// ドライバは CONNECTION_CLOSE を送った後、下位トランスポートの送信側を
    /// graceful に shutdown し、ピアが送信側を shutdown するのを待つ
    /// (draft-ietf-quic-qmux-02 Section 7.2)。待ち時間の上限は [`Self::new`] では
    /// [`DEFAULT_LINGER`]、[`Self::with_linger`] では指定した値である。上限を過ぎると
    /// ドライバは下位トランスポートを閉じて終了し、[`Self::closed`] が
    /// [`CloseReason::Local`] を返す。
    pub fn close(&self, error_code: u64, reason: &str) {
        let _ = self
            .close_request
            .try_send((error_code, reason.to_string()));
    }

    /// 接続が閉じたかどうか
    pub fn is_closed(&self) -> bool {
        self.closed.borrow().is_some()
    }

    /// 接続が閉じるまで待ち、理由を返す
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// その間に閉じた理由を取り逃がすことはない (内部の watch に保持される)。
    pub async fn closed(&self) -> CloseReason {
        let mut closed = self.closed.clone();
        loop {
            if let Some(reason) = closed.borrow().clone() {
                return reason;
            }
            if closed.changed().await.is_err() {
                // ドライバが理由を通知せずに終了した
                return CloseReason::TransportClosed;
            }
        }
    }
}

/// バックグラウンドで接続を駆動する
///
/// `T` は下位トランスポートの型である。読み書きの半分をそのまま保持し、動的
/// ディスパッチとヒープ確保を避ける。
struct Driver<T> {
    /// Sans I/O の QMux 接続
    connection: QmuxConnection,
    /// 下位トランスポートの読み取り側
    reader: tokio::io::ReadHalf<T>,
    /// 下位トランスポートの書き込み側
    writer: tokio::io::WriteHalf<T>,
    /// アプリケーションからのコマンド受信
    command_rx: mpsc::Receiver<Command>,
    /// 接続を閉じる要求の受信 (容量 1 の専用チャネル)
    close_request_rx: mpsc::Receiver<(u64, String)>,
    /// ストリームハンドルへ渡すコマンド送信の複製
    command_tx: mpsc::Sender<Command>,
    /// ピア起動の双方向ストリームの受け渡し
    accept_bidirectional_tx: mpsc::Sender<(SendStream, RecvStream)>,
    /// ピア起動の単方向ストリームの受け渡し
    accept_unidirectional_tx: mpsc::Sender<RecvStream>,
    /// 受信 DATAGRAM の受け渡し
    datagram_tx: mpsc::Sender<Vec<u8>>,
    /// 接続単位の STOP_SENDING 通知の受け渡し
    stop_sending_tx: mpsc::Sender<StopSendingObserved>,
    /// QX_PING の応答の受け渡し
    ping_ack_tx: mpsc::Sender<u64>,
    /// ピアのトランスポートパラメータの通知
    peer_params_tx: watch::Sender<Option<TransportParameters>>,
    /// 破棄した受信 DATAGRAM の累計個数の通知
    dropped_datagrams_tx: watch::Sender<u64>,
    /// 接続が閉じた理由の通知
    closed_tx: watch::Sender<Option<CloseReason>>,
    /// ストリーム ID から受信イベント送信への対応
    recv_streams: BTreeMap<StreamId, mpsc::Sender<StreamEvent>>,
    /// ストリーム ID から送信イベント送信への対応
    ///
    /// ピアの STOP_SENDING を送信側ハンドルへ通知するために使う。
    send_streams: BTreeMap<StreamId, mpsc::Sender<SendEvent>>,
    /// フロー制御で保留している送信データ (ストリームごとの FIFO)
    pending_writes: BTreeMap<StreamId, VecDeque<PendingWrite>>,
    /// 読み取り容量が空くのを待っているストリーム
    pending_readable: BTreeSet<StreamId>,
    /// 接続の確立を待っているコマンド
    pending_commands: VecDeque<Command>,
    /// 接続単位の通知チャネルが埋まっているため保留している STOP_SENDING
    pending_stop_sending: VecDeque<StopSendingObserved>,
    /// QX_PING の応答チャネルが埋まっているため保留している応答
    pending_ping_acks: VecDeque<u64>,
    /// 受信側のチャネルが埋まっているため保留しているストリームイベント
    ///
    /// ストリームごとの順序を保つため、先頭から順に配送する。
    pending_stream_events: VecDeque<(StreamId, StreamEvent)>,
    /// accept チャネルに空きが無いため保留しているピア起動ストリーム
    ///
    /// (ストリーム ID, 双方向かどうか) を保持し、空きができ次第ハンドルを登録する。
    /// 登録するまで Sans I/O 側からデータを読まないため、保留分はフロー制御で
    /// 有界になる。
    pending_accept: VecDeque<(StreamId, bool)>,
    /// 送信待ちの Record
    ///
    /// 1 つの Record は書き切ってから次を取り出す。書き込みの待ちはメインループの
    /// `select!` の腕で行うため、書き込みが詰まっている間も受信・コマンド・タイマーが
    /// 進む。
    output: Vec<u8>,
    /// `output` のうち書き出し済みのバイト数
    written: usize,
    /// tokio 側のチャネルが埋まって破棄した DATAGRAM の累計個数
    datagram_drops: u64,
    /// ピアのトランスポートパラメータを受信済みかどうか
    established: bool,
    /// 送信側を shutdown した後でピアの shutdown を待つ上限
    linger: Duration,
    /// 接続が閉じた理由が確定した時刻 (確定していない場合は `None`)
    ///
    /// 期限を `Instant` の加算で持たないのは、桁あふれで panic するためである。
    /// 残り時間は `linger` から経過時間を引いて求める。
    linger_started: Option<Instant>,
    /// 下位トランスポートの送信側を shutdown 済みかどうか
    write_shutdown: bool,
    /// linger の上限を過ぎたかどうか
    linger_expired: bool,
    /// ストリーム読み取り用の作業バッファ
    read_scratch: Vec<u8>,
    /// ドライバ開始時刻
    started: Instant,
    /// 接続が閉じた理由
    close_reason: Option<CloseReason>,
    /// 下位トランスポートが閉じたかどうか
    transport_ended: bool,
}

/// select で待つイベント
enum Wake {
    Read(std::io::Result<usize>),
    Write(std::io::Result<usize>),
    Command(Option<Command>),
    Close(Option<(u64, String)>),
    Timeout,
}

/// フロー制御で保留しているストリームデータ
#[derive(Debug)]
struct PendingWrite {
    /// まだ送り終えていないデータ
    data: Vec<u8>,
    /// FIN を送るかどうか
    fin: bool,
    /// 送信完了または失敗を通知する応答チャネル
    ack: oneshot::Sender<Result<()>>,
}

impl<T> Driver<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// ドライバのメインループ
    ///
    /// 読み取り・書き込み・コマンド・close 要求・タイマーを 1 つの `select!` で待つ。
    /// 書き込みが詰まっている間も他の腕が動くため、ピアが読み取りを止めても受信・
    /// コマンド・アイドルタイムアウトが進む。
    async fn run(mut self) {
        let mut read_buf = vec![0u8; READ_BUFFER_SIZE];

        loop {
            self.process_events();
            self.update_datagram_drops();
            self.retry_accept();
            self.retry_stream_events();
            self.retry_stop_sending();
            self.retry_ping_acks();
            if self.close_reason.is_none() {
                self.retry_readable();
                self.retry_writes();
            }
            // 接続が閉じた場合は、そこから送信側の shutdown と、ピアの shutdown を
            // 待つ上限を数える
            if self.close_reason.is_some() {
                self.start_linger();
            }
            // 送信する Record を用意する。書きかけの Record があるうちは次の
            // Record を取り出さない (Record を途中で切らないため)
            if self.written >= self.output.len() {
                self.output.clear();
                self.written = 0;
                let now = self.now();
                match self.connection.poll_transmit(&mut self.output, now) {
                    Ok(true) => {}
                    Ok(false) => self.output.clear(),
                    Err(_) => {
                        // 送信フレームを組み立てられない。接続を作り直すしかない
                        self.connection.transport_closed();
                        self.transport_ended = true;
                        self.process_events();
                        self.update_datagram_drops();
                        self.retry_stop_sending();
                    }
                }
            }
            // Section 7.2 / 7.3: 接続が閉じたら下位トランスポートの送信側を
            // graceful に shutdown する。書きかけの Record があるうちは行わない。
            // ピアが読み取っていなければ shutdown も完了しないため、shutdown の待ちも
            // linger の上限で打ち切る。下位トランスポートの閉塞 (Section 7.4) では
            // shutdown せずに即座に終了する。
            if self.close_reason.is_some()
                && !self.write_shutdown
                && !matches!(self.close_reason, Some(CloseReason::TransportClosed))
                && self.written >= self.output.len()
            {
                self.write_shutdown = true;
                if !self.shutdown_write_side().await {
                    // shutdown できない下位トランスポートは使えないものとして扱う
                    self.transport_ended = true;
                }
            }
            // 終了判定。自側から閉じた場合は、送信側を shutdown した後でピアの
            // shutdown を待つ (draft-ietf-quic-qmux-02 Section 7.2)。ピア起因の切断や
            // アイドルタイムアウトでは、自側の shutdown が済み次第終了する
            // (Section 7.1 / 7.3)。いずれの場合も linger の上限を過ぎたら終了する。
            let should_break = self.linger_expired
                || match &self.close_reason {
                    Some(CloseReason::TransportClosed) => true,
                    Some(CloseReason::Local { .. }) => {
                        (self.write_shutdown && self.transport_ended)
                            || self.linger_remaining().is_some_and(|r| r.is_zero())
                    }
                    Some(_) => {
                        self.write_shutdown || self.linger_remaining().is_some_and(|r| r.is_zero())
                    }
                    None => false,
                };
            if should_break {
                break;
            }

            let sleep_duration = self.next_sleep_duration();
            let wake = {
                let reader = &mut self.reader;
                let writer = &mut self.writer;
                let output = &self.output;
                let written = self.written;
                let command_rx = &mut self.command_rx;
                let close_request_rx = &mut self.close_request_rx;
                // 偏りのある選択はしない。読み取りが連続する間もコマンド・close 要求・
                // タイマーが飢餓にならないようにするためである。
                tokio::select! {
                    result = reader.read(read_buf.as_mut_slice()) => Wake::Read(result),
                    result = async {
                        // 書き出す Record が無いときは永久に待つ。0 バイトの書き込みを
                        // 繰り返してビジーループにならないようにするためである。
                        if written < output.len() {
                            writer.write(&output[written..]).await
                        } else {
                            core::future::pending().await
                        }
                    } => Wake::Write(result),
                    command = command_rx.recv() => Wake::Command(command),
                    request = close_request_rx.recv() => Wake::Close(request),
                    _ = sleep_for(sleep_duration) => Wake::Timeout,
                }
            };

            match wake {
                Wake::Read(Ok(0)) | Wake::Read(Err(_)) => {
                    // 下位トランスポートが閉じた (Section 7.4)。I/O エラーの詳細は
                    // 保持せず、切断理由は CloseReason::TransportClosed に丸める。
                    self.connection.transport_closed();
                    self.transport_ended = true;
                }
                Wake::Read(Ok(len)) => {
                    self.connection.receive(&read_buf[..len], self.now());
                }
                Wake::Write(Ok(0)) => {
                    // 書き出しの途中で 0 バイトだった場合は下位トランスポートが閉じた
                    if self.written < self.output.len() {
                        self.connection.transport_closed();
                        self.transport_ended = true;
                        self.process_events();
                    }
                }
                Wake::Write(Ok(len)) => {
                    self.written += len;
                }
                Wake::Write(Err(_)) => {
                    self.connection.transport_closed();
                    self.transport_ended = true;
                }
                Wake::Command(Some(command)) => self.handle_command(command),
                Wake::Command(None) => {
                    // ドライバ自身が command_tx を保持しているため、通常ここには
                    // 到達しない。到達した場合はアプリケーション側のハンドルが
                    // すべて消えている。
                    self.connection.transport_closed();
                    self.transport_ended = true;
                }
                Wake::Close(Some((error_code, reason))) => {
                    self.connection.close(error_code, &reason);
                }
                Wake::Close(None) => {
                    // Connection がドロップされた (close 要求チャネルの送信側が消えた)
                    self.connection.transport_closed();
                    self.transport_ended = true;
                }
                Wake::Timeout => {
                    self.connection.handle_timeout(self.now());
                }
            }
        }

        self.finish();
    }

    /// 現在時刻を QMux のタイムスタンプへ変換する
    fn now(&self) -> Timestamp {
        Timestamp::from_micros(self.started.elapsed().as_micros() as u64)
    }

    /// 次のタイマーまでのスリープ時間を返す
    ///
    /// アイドルタイムアウトと linger の上限のうち、早い方までの時間を返す。
    fn next_sleep_duration(&self) -> Option<Duration> {
        let idle = self.connection.next_timeout().map(|deadline| {
            let now = self.now().as_micros();
            let then = deadline.as_micros();
            if then <= now {
                Duration::ZERO
            } else {
                Duration::from_micros(then - now)
            }
        });
        match (idle, self.linger_remaining()) {
            (Some(idle), Some(linger)) => Some(idle.min(linger)),
            (Some(idle), None) => Some(idle),
            (None, Some(linger)) => Some(linger),
            (None, None) => None,
        }
    }

    /// 接続が閉じた理由が確定した時点から、送信側を shutdown してピアの shutdown を
    /// 待つ上限を数え始める
    fn start_linger(&mut self) {
        if self.linger_started.is_none() {
            self.linger_started = Some(Instant::now());
        }
    }

    /// linger の残り時間を返す
    ///
    /// 接続が閉じていない場合は `None` を返す。
    fn linger_remaining(&self) -> Option<Duration> {
        self.linger_started
            .map(|started| self.linger.saturating_sub(started.elapsed()))
    }

    /// 下位トランスポートの送信側を graceful に shutdown する (Section 7.2)
    ///
    /// ピアが読み取っていない場合は shutdown も完了しないため、linger の残り時間で
    /// 打ち切る。戻り値は shutdown が完了したかどうか。
    async fn shutdown_write_side(&mut self) -> bool {
        let remaining = self.linger_remaining().unwrap_or(Duration::ZERO);
        let writer = &mut self.writer;
        tokio::select! {
            biased;
            result = writer.shutdown() => result.is_ok(),
            _ = tokio::time::sleep(remaining) => false,
        }
    }

    /// 破棄した DATAGRAM の累計個数をアプリケーションへ通知する
    ///
    /// Sans I/O 側のキュー上限による破棄と tokio 側のチャネル上限による破棄を
    /// 合算する。値が変わったときだけ送る。
    fn update_datagram_drops(&self) {
        let total = self
            .datagram_drops
            .saturating_add(self.connection.dropped_datagrams());
        if *self.dropped_datagrams_tx.borrow() != total {
            let _ = self.dropped_datagrams_tx.send(total);
        }
    }

    /// 接続のイベントを取り出して処理する
    fn process_events(&mut self) {
        while let Some(event) = self.connection.poll_event() {
            match event {
                Event::TransportParametersReceived => {
                    self.established = true;
                    // アプリケーションがピアのフロー制御の値と DATAGRAM 対応を確認
                    // できるよう、受信したパラメータの最新値を配る
                    let _ = self
                        .peer_params_tx
                        .send(self.connection.peer_transport_parameters().cloned());
                    self.flush_pending_commands();
                }
                Event::StreamOpened { id } => self.on_stream_opened(id),
                Event::StreamReadable { id } => {
                    self.pending_readable.insert(id);
                }
                Event::StreamWritable { .. } | Event::StreamsAvailable { .. } => {
                    // 次のループで保留中の書き込みを再試行する
                }
                Event::StreamReset { id, error_code } => {
                    // リセットは失ってはならない制御信号である。チャネルが満杯の
                    // 場合は配送待ちキューへ積む。
                    if !self.send_event(id, StreamEvent::Reset(error_code)) {
                        // 受信側がドロップ済みである。以後このストリームは読まない
                        self.drop_recv_stream(id);
                    }
                    self.pending_readable.remove(&id);
                }
                Event::StreamStopSending { id, error_code } => {
                    // ピアの STOP_SENDING を送信側ハンドルへ通知し、送信待ちの
                    // データを破棄する
                    self.on_stop_sending(id, error_code);
                }
                Event::Datagram(data) => {
                    // 保持量に上限があるため、あふれた DATAGRAM は破棄する
                    // (draft-ietf-quic-qmux-02 Section 6)。破棄した個数は
                    // アプリケーションが観測できるように数えておく。
                    if self.datagram_tx.try_send(data).is_err() {
                        self.datagram_drops = self.datagram_drops.saturating_add(1);
                    }
                }
                Event::Ping { .. } => {
                    // QX_PING の応答は Sans I/O 側が自動で返す。アプリケーションへ
                    // 通知する必要はないため、ここでは捨てる。
                }
                Event::PingAcknowledged { sequence } => {
                    // 応答は取りこぼさないよう、配送待ちキューへ積んでから配る
                    self.pending_ping_acks.push_back(sequence);
                }
                Event::ConnectionClosed(reason) => {
                    // 自側の close の理由を、ピアの shutdown を待つ間も保持する
                    if self.close_reason.is_none() {
                        self.close_reason = Some(reason);
                    }
                }
            }
        }
    }

    /// ピア起動のストリームをアプリケーションへ渡す
    ///
    /// 受け渡しチャネルが満杯の場合は保留キューへ積む。ここでストリームを捨てると
    /// ピアの MAX_STREAMS のクレジットが返らず、ピアが新しいストリームを開けなく
    /// なるためである。
    fn on_stream_opened(&mut self, id: StreamId) {
        self.pending_accept
            .push_back((id, id.stream_type().is_bidirectional()));
    }

    /// 保留しているピア起動ストリームを、空きのある accept チャネルへ配送する
    ///
    /// ハンドルを登録するまで Sans I/O 側からデータを読まないため、アプリケーションが
    /// accept しない間の受信量はフロー制御で抑えられる。
    fn retry_accept(&mut self) {
        while let Some((id, bidirectional)) = self.pending_accept.pop_front() {
            if bidirectional {
                if self.accept_bidirectional_tx.capacity() == 0 {
                    self.pending_accept.push_front((id, true));
                    break;
                }
                let send = self.register_send(id);
                let recv = self.register_recv(id);
                if self.accept_bidirectional_tx.try_send((send, recv)).is_err() {
                    // アプリケーションが accept をやめた (受信側がドロップされた)
                    self.send_streams.remove(&id);
                    self.recv_streams.remove(&id);
                }
            } else {
                if self.accept_unidirectional_tx.capacity() == 0 {
                    self.pending_accept.push_front((id, false));
                    break;
                }
                let recv = self.register_recv(id);
                if self.accept_unidirectional_tx.try_send(recv).is_err() {
                    self.recv_streams.remove(&id);
                }
            }
        }
    }

    /// 送信イベントチャネルを登録してハンドルを返す
    fn register_send(&mut self, id: StreamId) -> SendStream {
        let (tx, rx) = mpsc::channel(STREAM_EVENT_CAPACITY);
        self.send_streams.insert(id, tx);
        SendStream::new(id, self.command_tx.clone(), rx)
    }

    /// 受信イベントチャネルを登録してハンドルを返す
    fn register_recv(&mut self, id: StreamId) -> RecvStream {
        let (tx, rx) = mpsc::channel(STREAM_EVENT_CAPACITY);
        self.recv_streams.insert(id, tx);
        RecvStream::new(id, rx, self.command_tx.clone())
    }

    /// ピアの STOP_SENDING を送信側ハンドルと接続単位の通知先へ配送する
    ///
    /// 送信側はピアが送信停止を要求した時点で終端する (RFC 9000 Section 3.5)。
    /// 送信待ちのデータを破棄し、送信側ハンドルへエラーコードを配送する。
    fn on_stop_sending(&mut self, id: StreamId, error_code: u64) {
        if let Some(tx) = self.send_streams.remove(&id) {
            let _ = tx.try_send(error_code);
        }
        if let Some(queue) = self.pending_writes.remove(&id) {
            for pending in queue {
                let _ = pending.ack.send(Err(Error::Stopped(error_code)));
            }
        }
        // 接続単位で待つ利用側にも通知する。チャネルが埋まっていても失わないよう、
        // ここでは保留キューに入れるだけにして実際の配送は retry_stop_sending が行う。
        self.pending_stop_sending
            .push_back(StopSendingObserved { id, error_code });
    }

    /// 接続単位の STOP_SENDING 通知チャネルへ保留分を配送する
    ///
    /// STOP_SENDING はストリームごとに一度だけ発生する制御信号であり、DATAGRAM と
    /// 違って破棄してはならないため、チャネルが埋まっている間はドライバが保留する。
    fn retry_stop_sending(&mut self) {
        while let Some(event) = self.pending_stop_sending.pop_front() {
            match self.stop_sending_tx.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.pending_stop_sending.push_front(event);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Connection がドロップされ、通知先がなくなった
                    self.pending_stop_sending.clear();
                    break;
                }
            }
        }
    }

    /// QX_PING の応答チャネルへ保留分を配送する
    ///
    /// 応答は接続の到達性の観測に使うため、取りこぼさないよう保留する。
    fn retry_ping_acks(&mut self) {
        while let Some(sequence) = self.pending_ping_acks.pop_front() {
            match self.ping_ack_tx.try_send(sequence) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(sequence)) => {
                    self.pending_ping_acks.push_front(sequence);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Connection がドロップされ、通知先がなくなった
                    self.pending_ping_acks.clear();
                    break;
                }
            }
        }
    }

    /// 配送待ちのストリームイベントを空きのあるチャネルへ配送する
    ///
    /// ストリームごとの順序を保つため、先頭から順に試し、満杯のものが現れた時点で
    /// 打ち切って次回に回す (順序を入れ替えないため)。
    fn retry_stream_events(&mut self) {
        while let Some((id, event)) = self.pending_stream_events.pop_front() {
            let Some(tx) = self.recv_streams.get(&id).cloned() else {
                // 受信側が無い (終端済み) ため破棄する
                continue;
            };
            match tx.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.pending_stream_events.push_front((id, event));
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // 受信側がドロップされた。以後このストリームは読まない
                    self.drop_recv_stream(id);
                }
            }
        }
    }

    /// フロー制御で保留している書き込みを再試行する
    ///
    /// 各ストリームの保留データを先頭から順に送る。`Connection::write` は一部しか
    /// 受け付けない場合があるため、残りを保持して次回に送る。送り終えたチャンクは
    /// `ack` でアプリケーションへ結果を通知する。
    fn retry_writes(&mut self) {
        let ids: Vec<StreamId> = self.pending_writes.keys().copied().collect();
        for id in ids {
            let Some(mut queue) = self.pending_writes.remove(&id) else {
                continue;
            };
            while let Some(mut pending) = queue.pop_front() {
                // 空の非 FIN チャンクは送るものがないため完了させる
                if pending.data.is_empty() && !pending.fin {
                    let _ = pending.ack.send(Ok(()));
                    continue;
                }
                match self.connection.write(id, &pending.data, pending.fin) {
                    Ok(len) => {
                        if len == pending.data.len() {
                            // 全データ (FIN 指定時は FIN も) を送り終えた。
                            // 送信側が完了したため、STOP_SENDING の通知先を破棄する
                            if pending.fin {
                                self.send_streams.remove(&id);
                            }
                            let _ = pending.ack.send(Ok(()));
                        } else if len == 0 {
                            // フロー制御で送れない。このチャンクを戻して打ち切る
                            queue.push_front(pending);
                            break;
                        } else {
                            // 一部だけ送れた。残りを次回送る
                            pending.data.drain(..len);
                            queue.push_front(pending);
                            break;
                        }
                    }
                    Err(error) => {
                        // ストリームが終端したなど、これ以上送れない。保留している
                        // 書き込みをすべて失敗させる (接続自体は生きているため、
                        // 接続断ではなくストリームの終端として返す)
                        let _ = pending.ack.send(Err(Error::Qmux(error)));
                        while let Some(rest) = queue.pop_front() {
                            let _ = rest.ack.send(Err(Error::StreamFinished));
                        }
                        break;
                    }
                }
            }
            if !queue.is_empty() {
                self.pending_writes.insert(id, queue);
            }
        }
    }

    /// 読み取り容量が空いたストリームからデータを読み取る
    fn retry_readable(&mut self) {
        let ids: Vec<StreamId> = self.pending_readable.iter().copied().collect();
        for id in ids {
            loop {
                let capacity = self.recv_streams.get(&id).map_or(0, |tx| tx.capacity());
                if capacity == 0 {
                    break;
                }
                let result = self.connection.read(id, self.read_scratch.as_mut_slice());
                match result {
                    Ok(ReadResult::Data { len, fin }) => {
                        let data = self.read_scratch[..len].to_vec();
                        if (len > 0 || fin) && !self.send_event(id, StreamEvent::Data { data, fin })
                        {
                            // アプリケーションが受信側をドロップした。ピアへ送信停止を
                            // 要求し、以後このストリームは読まない
                            self.drop_recv_stream(id);
                            break;
                        }
                        if fin {
                            self.recv_streams.remove(&id);
                            self.pending_readable.remove(&id);
                            break;
                        }
                        if len == 0 {
                            self.pending_readable.remove(&id);
                            break;
                        }
                    }
                    Ok(ReadResult::Reset { error_code }) => {
                        if !self.send_event(id, StreamEvent::Reset(error_code)) {
                            self.drop_recv_stream(id);
                        } else {
                            self.recv_streams.remove(&id);
                        }
                        self.pending_readable.remove(&id);
                        break;
                    }
                    Err(_) => {
                        // 接続が閉じた、ストリームが終端したなど。アプリケーションが
                        // 待ち続けないよう、チャネルを閉じる
                        self.recv_streams.remove(&id);
                        self.pending_readable.remove(&id);
                        break;
                    }
                }
            }
        }
    }

    /// ストリームへイベントを配送する
    ///
    /// チャネルが満杯の場合は配送待ちキューへ積む。戻り値は受信側が存在するか
    /// どうかで、`false` の場合はアプリケーションが受信側をドロップしている。
    fn send_event(&mut self, id: StreamId, event: StreamEvent) -> bool {
        let Some(tx) = self.recv_streams.get(&id).cloned() else {
            return false;
        };
        match tx.try_send(event) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(event)) => {
                self.pending_stream_events.push_back((id, event));
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// 受信側がドロップされたストリームの読み取りを止める
    ///
    /// アプリケーションがデータを受け取らないため、ピアへ送信停止 (STOP_SENDING) を
    /// 要求し、以後このストリームを読まない (RFC 9000 Section 3.5)。エラーコードは
    /// アプリケーション固有の値で、ここでは 0 (NO_ERROR) を使う。
    fn drop_recv_stream(&mut self, id: StreamId) {
        self.recv_streams.remove(&id);
        self.pending_readable.remove(&id);
        let _ = self.connection.stop_sending(id, 0);
    }

    /// アプリケーションからのコマンドを処理する
    fn handle_command(&mut self, command: Command) {
        // 接続の確立前に、ピアのフロー制御に依存するコマンドは保留する
        if !self.established && command.requires_establishment() {
            self.pending_commands.push_back(command);
            return;
        }
        self.dispatch(command);
    }

    /// 確立を待って保留していたコマンドを処理する
    fn flush_pending_commands(&mut self) {
        while let Some(command) = self.pending_commands.pop_front() {
            self.dispatch(command);
        }
    }

    /// コマンドを実際に処理する
    fn dispatch(&mut self, command: Command) {
        match command {
            Command::OpenBidirectional(reply) => {
                let result = self.connection.open_bidi().map(|id| {
                    let send = self.register_send(id);
                    let recv = self.register_recv(id);
                    (send, recv)
                });
                let _ = reply.send(result.map_err(Error::Qmux));
            }
            Command::OpenUnidirectional(reply) => {
                let result = self.connection.open_uni().map(|id| self.register_send(id));
                let _ = reply.send(result.map_err(Error::Qmux));
            }
            Command::Write { id, data, fin, ack } => {
                self.pending_writes
                    .entry(id)
                    .or_default()
                    .push_back(PendingWrite { data, fin, ack });
            }
            Command::Reset {
                id,
                error_code,
                ack,
            } => {
                // 送信待ちのデータはリセットで破棄される
                if let Some(queue) = self.pending_writes.remove(&id) {
                    for pending in queue {
                        let _ = pending.ack.send(Err(Error::StreamFinished));
                    }
                }
                // 送信側をリセットすると STOP_SENDING の通知先は不要になる
                self.send_streams.remove(&id);
                let result = self.connection.reset(id, error_code).map_err(Error::Qmux);
                let _ = ack.send(result);
            }
            Command::StopSending {
                id,
                error_code,
                ack,
            } => {
                let result = self
                    .connection
                    .stop_sending(id, error_code)
                    .map_err(Error::Qmux);
                let _ = ack.send(result);
            }
            Command::Datagram { data, ack } => {
                let result = self.connection.send_datagram(&data).map_err(Error::Qmux);
                let _ = ack.send(result);
            }
            Command::Ping(reply) => {
                let _ = reply.send(self.connection.ping().map_err(Error::Qmux));
            }
            Command::PollReadable { id } => {
                self.pending_readable.insert(id);
            }
            Command::DropRecvStream { id } => {
                // 登録が残っている場合だけ、ピアへ送信停止を要求する (終端を配送済みの
                // ストリームでは登録が外れているため何もしない)
                if self.recv_streams.contains_key(&id) {
                    self.drop_recv_stream(id);
                }
            }
            Command::Wake => {
                // 起床要求そのものに意味はない。ループ先頭の再試行で保留分が
                // 配送されるため、ここでは何もしない。
            }
        }
    }

    /// ドライバを終了し、アプリケーションへ切断を通知する
    ///
    /// `self` を消費するため、この関数を抜けると下位トランスポートも閉じられる。
    /// 保持しているチャネルは drop で閉じるため、ここで明示的に破棄しない。
    fn finish(mut self) {
        let reason = self
            .close_reason
            .take()
            .unwrap_or(CloseReason::TransportClosed);
        let _ = self.closed_tx.send(Some(reason));
    }
}

/// 指定した残り時間だけ待つ
///
/// 残り時間が無い場合 (自側の close を要求していない場合) は、期限が無いものとして
/// 永久に待つ。`Instant` ではなく残り時間で待つのは、期限の計算で桁あふれを
/// 起こさないためである。
async fn sleep_for(remaining: Option<Duration>) {
    match remaining {
        Some(remaining) => tokio::time::sleep(remaining).await,
        None => core::future::pending::<()>().await,
    }
}
