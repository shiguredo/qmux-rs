//! dwnx 接続の非同期ハンドルとドライバ
//!
//! 下位トランスポートの読み書きと時刻の供給をバックグラウンドのドライバタスクが
//! 行い、アプリケーションはストリームのハンドル越しに操作する。

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use dwnx_rs::TransportParameters;
use dwnx_rs::{
    Connection as DwnxConnection, DatagramEvent as DwnxDatagramEvent, Event as DwnxEvent,
    WriteEvent,
};

use crate::Role;
use crate::error::{Error, Result};
use crate::stream::{RecvStream, SendStream, StopSendingObserved, StreamEvent};

/// アプリケーションからドライバへのコマンド
#[derive(Debug)]
pub(crate) enum Command {
    /// 双方向ストリームを開く
    OpenBidirectional(oneshot::Sender<Result<(SendStream, RecvStream)>>),
    /// 単方向ストリームを開く
    OpenUnidirectional(oneshot::Sender<Result<SendStream>>),
    /// ストリームへデータを書き込む
    ///
    /// データをすべて受け付け、下位トランスポートへ書き出し終えた時点で `ack` へ
    /// 結果を返す。フロー制御で送れない場合はドライバが保留し、送れるようになって
    /// から返す。
    Write {
        /// ストリーム ID
        id: i64,
        /// 送信するデータ
        data: Vec<u8>,
        /// FIN を送るかどうか
        fin: bool,
        /// 送信完了または失敗を通知する応答チャネル
        ack: oneshot::Sender<Result<()>>,
    },
    /// ストリームの送信側をリセットする
    ShutdownWrite {
        /// ストリーム ID
        id: i64,
        /// アプリケーションエラーコード
        error_code: u64,
    },
    /// ストリームの受信側に送信停止を要求する
    StopSending {
        /// ストリーム ID
        id: i64,
        /// アプリケーションエラーコード
        error_code: u64,
    },
    /// DATAGRAM を送信する
    ///
    /// dwnx が Record に収め、下位トランスポートへ書き出し終えた時点で `ack` へ
    /// 結果を返す。ピアが DATAGRAM 拡張を広告していない場合、フレームがピアの
    /// `max_datagram_frame_size` を超える場合、またはローカルの Record 容量に
    /// 収まらない場合は `ack` にエラーを返す。
    Datagram {
        /// 送信するデータ
        data: Vec<u8>,
        /// 送信完了または失敗を通知する応答チャネル
        ack: oneshot::Sender<Result<()>>,
    },
    /// 受信側がデータを取り出してチャネルに空きができたことを知らせる
    ///
    /// ドライバは配送待ちのイベントを保持しているため、通知が無いと
    /// アプリケーションが読み続けていても配送が再開しない。
    PollReadable,
    /// アプリケーションが通知を 1 つ受け取ってチャネルに空きができたことを知らせる
    ///
    /// accept 系と [`crate::Connection::recv_stop_sending`] はチャネルを待つだけで
    /// ドライバを起こさないため、ドライバが保留している分を渡すには通知が要る。
    Wake,
}

impl Command {
    /// ピアのトランスポートパラメータの受信を待つ必要があるかどうか
    ///
    /// QMux はピアのトランスポートパラメータを受信するまで、ピアのフロー制御や
    /// 拡張の対応状況に依存するフレームを送信してはならない
    /// (draft-ietf-quic-qmux-02 Section 4.2)。DATAGRAM を送れるかどうかはピアの
    /// `max_datagram_frame_size` で決まるため、DATAGRAM もこの対象である
    /// (RFC 9221 Section 3)。
    fn requires_establishment(&self) -> bool {
        matches!(
            self,
            Self::OpenBidirectional(_) | Self::OpenUnidirectional(_) | Self::Datagram { .. }
        )
    }

    /// 接続が閉じた後に受け取ったコマンドへエラーを返す
    ///
    /// 応答チャネルを持つコマンドをそのまま捨てると、アプリケーションが
    /// 永久に待ち続けるため、必ず応答を返す。
    fn reject(self, error: Error) {
        match self {
            Self::OpenBidirectional(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::OpenUnidirectional(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Write { ack, .. } => {
                let _ = ack.send(Err(error));
            }
            Self::Datagram { ack, .. } => {
                let _ = ack.send(Err(error));
            }
            // 応答を待たないコマンドは捨ててよい
            Self::ShutdownWrite { .. }
            | Self::StopSending { .. }
            | Self::PollReadable
            | Self::Wake => {}
        }
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
///
/// DATAGRAM はフロー制御されないため、アプリケーションが受け取らない間に
/// 到着し続けてもメモリを使い続けないよう上限を設ける。あふれた DATAGRAM は
/// 破棄し、個数を [`Connection::dropped_datagrams`] で観測できるようにする
/// (draft-ietf-quic-qmux-02 Section 6、RFC 9221 Section 5.3)。
const DATAGRAM_CAPACITY: usize = 64;
/// ストリームごとの受信イベントチャネルの容量
///
/// あふれたイベントはドライバの配送待ちキューに残り、クレジットを返さないことで
/// dwnx の受信ウィンドウが埋まるため、キューはフロー制御により有界になる。
const RECV_CAPACITY: usize = 64;
/// 接続単位で観測する STOP_SENDING の保持数の上限
///
/// あふれた分はドライバが保留し、空きができ次第配送する。
const STOP_SENDING_CAPACITY: usize = 16;
/// 下位トランスポートから一度に読み取るバイト数の上限
const READ_BUFFER_SIZE: usize = 64 * 1024;
/// ストリーム ID の下位 2 ビットのうち単方向ストリームを表すビット
///
/// RFC 9000 Section 2.1 のとおり、ストリーム ID の下位 2 ビットが
/// 0x0 / 0x1 なら双方向、0x2 / 0x3 なら単方向である。
const UNIDIRECTIONAL_STREAM_BIT: i64 = 0x2;
/// 自側の close の後でピアの shutdown を待つ上限の既定値 (3 秒)
///
/// ピアが送信側を shutdown しない限りドライバが終了しない状態を避けるため、待ち
/// 時間には必ず上限を設ける。QUIC version 1 の closing 状態が PTO の 3 倍を目安に
/// 終了するのと同じオーダーであり (RFC 9000 Section 10.2.1)、TCP 上の FIN の往復
/// には十分な長さである。[`Connection::with_linger`] で変更できる。
pub const DEFAULT_LINGER: Duration = Duration::from_secs(3);

/// dwnx 接続
///
/// 下位トランスポートを所有するドライバタスクを起動する。[`Connection`] を
/// ドロップすると close 要求チャネルの送信側が閉じ、ドライバは下位トランスポートを
/// 閉じて終了する。ストリームのハンドルが残っていても、ドライバ自身がコマンドの
/// 送信側を保持するため、コマンドチャネルは閉じない。
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
    /// 破棄した受信 DATAGRAM の累計個数
    ///
    /// ドライバがチャネルの満杯で破棄した個数を watch で配る。
    dropped_datagrams: watch::Receiver<u64>,
    /// ピアが要求した送信停止の受け取り
    ///
    /// `accept_bidirectional` と同じ理由で Mutex で包む。
    stop_sending: Mutex<mpsc::Receiver<StopSendingObserved>>,
    /// 接続が閉じたかどうか
    closed: watch::Receiver<bool>,
}

impl Connection {
    /// 下位トランスポート上に dwnx 接続を確立する
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
    /// `config` が dwnx の要求を満たさない場合と、dwnx 接続を作成できなかった場合に
    /// [`Error::Dwnx`] を返す。`TransportParameters::default()` はフロー制御の
    /// クレジットが 0 で DATAGRAM も無効なため、ストリームや DATAGRAM を使う場合は
    /// 明示的に設定すること。
    pub fn new<T>(role: Role, io: T, config: TransportParameters) -> Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::with_linger(role, io, config, DEFAULT_LINGER)
    }

    /// ピアの shutdown を待つ上限を指定して dwnx 接続を確立する
    ///
    /// [`Self::close`] を呼んだ後、ドライバは CONNECTION_CLOSE を送って下位
    /// トランスポートの送信側を graceful に shutdown し、ピアが送信側を shutdown
    /// するのを待つ (draft-ietf-quic-qmux-02 Section 7.2)。`linger` はこの待ち時間の
    /// 上限で、`close` を要求した時点から数える。
    ///
    /// 上限を過ぎるとドライバは下位トランスポートを閉じて終了する。ピアが
    /// shutdown しない場合だけでなく、ピアが読み取りを止めて送信が詰まった場合にも、
    /// ドライバが残り続けないようにするためである。`Duration::ZERO` を指定すると、
    /// 書きかけの Record が無い限りピアの shutdown を待たない。
    ///
    /// # エラー
    ///
    /// [`Self::new`] と同じ。
    pub fn with_linger<T>(
        role: Role,
        io: T,
        config: TransportParameters,
        linger: Duration,
    ) -> Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (close_request, close_request_rx) = mpsc::channel(CLOSE_CAPACITY);
        let (accept_bidirectional_tx, accept_bidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (accept_unidirectional_tx, accept_unidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (datagram_tx, datagram_rx) = mpsc::channel(DATAGRAM_CAPACITY);
        let (stop_sending_tx, stop_sending_rx) = mpsc::channel(STOP_SENDING_CAPACITY);
        let (closed_tx, closed_rx) = watch::channel(false);
        let (dropped_datagrams_tx, dropped_datagrams_rx) = watch::channel(0);

        // 送信できない設定はここでエラーにする。誤設定を最初の送信まで遅延させない。
        let connection = match role {
            Role::Client => DwnxConnection::client(&config),
            Role::Server => DwnxConnection::server(&config),
        }?;

        let (reader, writer) = tokio::io::split(io);
        let driver = Driver {
            connection,
            reader: Box::new(reader),
            writer: Box::new(writer),
            command_rx,
            command_tx: commands.clone(),
            close_request_rx,
            accept_bidirectional_tx,
            accept_unidirectional_tx,
            datagram_tx,
            stop_sending_tx,
            dropped_datagrams_tx,
            closed_tx,
            recv_streams: BTreeMap::new(),
            pending_stream_events: VecDeque::new(),
            pending_stop_sending: VecDeque::new(),
            pending_accept: VecDeque::new(),
            pending_commands: VecDeque::new(),
            pending_acks: VecDeque::new(),
            pending_datagram_acks: VecDeque::new(),
            output: Vec::new(),
            written: 0,
            datagram_drops: 0,
            established: false,
            local_close: false,
            write_shutdown: false,
            linger,
            linger_started: None,
            linger_expired: false,
            transport_ended: false,
            started: std::time::Instant::now(),
        };
        tokio::spawn(driver.run());

        Ok(Self {
            commands,
            close_request,
            accept_bidirectional: Mutex::new(accept_bidirectional_rx),
            accept_unidirectional: Mutex::new(accept_unidirectional_rx),
            datagrams: Mutex::new(datagram_rx),
            dropped_datagrams: dropped_datagrams_rx,
            stop_sending: Mutex::new(stop_sending_rx),
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
    /// 接続が閉じた場合は `None` を返す。`&self` で待てるため、[`std::sync::Arc`] で
    /// 共有した接続や、他の accept 系と同時に `select!` して待つことができる。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// まだ受け取っていないストリームは失われない。`select!` の腕に置いてよい。
    pub async fn accept_bidirectional(&self) -> Option<(SendStream, RecvStream)> {
        let accepted = self.accept_bidirectional.lock().await.recv().await;
        // ドライバは保留しているストリームをチャネルへ移すために起床が要る
        self.wake();
        accepted
    }

    /// ピア起動の単方向ストリームを受け取る
    ///
    /// 接続が閉じた場合は `None` を返す。[`Self::accept_bidirectional`] と同様に
    /// `&self` で待てる。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。まだ受け取っていないストリームは、
    /// 待っている間に future を破棄しても失われない。
    pub async fn accept_unidirectional(&self) -> Option<RecvStream> {
        let accepted = self.accept_unidirectional.lock().await.recv().await;
        // ドライバは保留しているストリームをチャネルへ移すために起床が要る
        self.wake();
        accepted
    }

    /// ピアが要求した送信停止を受け取る (STOP_SENDING)
    ///
    /// 対象のストリーム ID とピアが指定したアプリケーションエラーコードを返す
    /// (RFC 9000 Section 19.5)。接続が閉じた場合は `None` を返す。
    /// [`Self::accept_bidirectional`] と同様に `&self` で待てるため、送信ストリーム
    /// を所有するタスクとは別のタスクで待つことができる。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// まだ受け取っていない通知は失われない。
    pub async fn recv_stop_sending(&self) -> Option<StopSendingObserved> {
        let observed = self.stop_sending.lock().await.recv().await;
        // ドライバは保留している通知をチャネルへ移すために起床が要る
        self.wake();
        observed
    }

    /// DATAGRAM を送信する
    ///
    /// dwnx が DATAGRAM を Record に収め、下位トランスポートへ書き出し終えるまで
    /// 待つ。ピアが DATAGRAM 拡張を広告していない場合、フレームがピアの
    /// `max_datagram_frame_size` を超える場合、またはフレームがローカルの Record
    /// 容量 (`DWNX_DEFAULT_MAX_RECORD_SIZE` = 16382 バイト) に収まらない場合は
    /// [`Error::Dwnx`] を返す (RFC 9221 Section 3、draft-ietf-quic-qmux-02
    /// Section 9.1)。DATAGRAM は Record として届くが、受信側が速やかに
    /// 受け取れない場合は破棄されうる。
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
    /// 同様に `&self` で待てる。保持量には上限があり、あふれた DATAGRAM は
    /// 破棄される。破棄された個数は [`Self::dropped_datagrams`] で観測できる。
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

    /// 破棄した受信 DATAGRAM の累計個数を返す
    ///
    /// 受信チャネルの上限を超えて破棄した分を数える。DATAGRAM はフロー制御
    /// されないため、アプリケーションが速やかに受け取れない場合は破棄されうる
    /// (draft-ietf-quic-qmux-02 Section 6、RFC 9221 Section 5.3)。この値で
    /// 取りこぼしを検知できる。
    pub fn dropped_datagrams(&self) -> u64 {
        *self.dropped_datagrams.borrow()
    }

    /// 接続を閉じる
    ///
    /// CONNECTION_CLOSE が送信される。容量 1 の専用チャネルへ送るため、
    /// コマンドチャネルの輻輳で要求が失われることはない。すでに未処理の close
    /// 要求がある場合の送信失敗は、先行する要求が処理されるため無視してよい。
    ///
    /// ドライバは CONNECTION_CLOSE を送った後、下位トランスポートの送信側を
    /// graceful に shutdown し、ピアが送信側を shutdown するのを待つ
    /// (draft-ietf-quic-qmux-02 Section 7.2)。待ち時間の上限は [`Self::new`] では
    /// [`DEFAULT_LINGER`]、[`Self::with_linger`] では指定した値である。上限を過ぎると
    /// ドライバは下位トランスポートを閉じて終了する。
    pub fn close(&self, error_code: u64, reason: &str) {
        let _ = self
            .close_request
            .try_send((error_code, reason.to_string()));
    }

    /// 接続が閉じたかどうか
    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// ドライバへ配送の再開を促す
    ///
    /// accept 系と [`Self::recv_stop_sending`] は受信チャネルを待つだけでドライバを
    /// 起こさないため、保留している分を渡すには通知が要る。通知は目安であり、
    /// あふれた場合もドライバが次のループで再試行するため、送信に失敗しても構わない。
    fn wake(&self) {
        let _ = self.commands.try_send(Command::Wake);
    }

    /// 接続が閉じるまで待つ
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// 閉じたことを取り逃がすことはない (内部の watch に保持される)。
    pub async fn closed(&self) {
        let mut closed = self.closed.clone();
        if *closed.borrow() {
            return;
        }
        let _ = closed.wait_for(|closed| *closed).await;
    }
}

/// バックグラウンドで接続を駆動する
struct Driver {
    /// Sans I/O の dwnx 接続
    connection: DwnxConnection,
    /// 下位トランスポートの読み取り側
    reader: Box<dyn AsyncRead + Send + Unpin>,
    /// 下位トランスポートの書き込み側
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    /// アプリケーションからのコマンド受信
    command_rx: mpsc::Receiver<Command>,
    /// ストリームハンドルへ渡すコマンド送信の複製
    command_tx: mpsc::Sender<Command>,
    /// 接続を閉じる要求の受信 (容量 1 の専用チャネル)
    close_request_rx: mpsc::Receiver<(u64, String)>,
    /// ピア起動の双方向ストリームの受け渡し
    accept_bidirectional_tx: mpsc::Sender<(SendStream, RecvStream)>,
    /// ピア起動の単方向ストリームの受け渡し
    accept_unidirectional_tx: mpsc::Sender<RecvStream>,
    /// 受信 DATAGRAM の受け渡し
    datagram_tx: mpsc::Sender<Vec<u8>>,
    /// 接続単位の STOP_SENDING 通知の受け渡し
    stop_sending_tx: mpsc::Sender<StopSendingObserved>,
    /// 破棄した受信 DATAGRAM の累計個数の通知
    dropped_datagrams_tx: watch::Sender<u64>,
    /// 接続が閉じたことの通知
    closed_tx: watch::Sender<bool>,
    /// ストリーム ID から受信イベント送信への対応
    recv_streams: BTreeMap<i64, mpsc::Sender<StreamEvent>>,
    /// 配送待ちのストリームイベント
    ///
    /// `StreamData` や `StreamClosed` は dwnx から読み出し済みのため破棄できない。
    /// アプリケーションのチャネルが埋まっている間はここに積み、順序を保ったまま
    /// 配送する。フロー制御クレジットは配送できた時点で返す。
    pending_stream_events: VecDeque<(i64, StreamEvent)>,
    /// 接続単位の通知チャネルが埋まっているため保留している STOP_SENDING
    pending_stop_sending: VecDeque<StopSendingObserved>,
    /// アプリケーションへ渡すのを待っているピア起動ストリーム
    ///
    /// accept チャネルが埋まっている間はここに積む。受信イベントのチャネルは登録
    /// 済みであるため、アプリケーションが受け取るまでのデータも欠落しない。
    pending_accept: VecDeque<PendingAccept>,
    /// 接続の確立を待っているコマンド
    pending_commands: VecDeque<Command>,
    /// 応答を返していない書き込み
    ///
    /// dwnx の送信待ちキューと同じ順序で並ぶ。完了と失敗は dwnx から
    /// [`WriteEvent`] として通知される。
    pending_acks: VecDeque<PendingAck>,
    /// 応答を返していない DATAGRAM
    ///
    /// dwnx の DATAGRAM の送信待ちキューと同じ順序で並ぶ。完了と失敗は dwnx から
    /// [`DwnxDatagramEvent`] として通知される。
    pending_datagram_acks: VecDeque<oneshot::Sender<Result<()>>>,
    /// 下位トランスポートへ書き出す Record
    ///
    /// 書き出しの途中でも他の腕を動かせるよう、ドライバが保持する。
    output: Vec<u8>,
    /// `output` のうち書き出し済みのバイト数
    written: usize,
    /// 受信チャネルが満杯で破棄した DATAGRAM の累計個数
    datagram_drops: u64,
    /// ピアのトランスポートパラメータを受信済みかどうか
    established: bool,
    /// 自側から接続を閉じたかどうか
    local_close: bool,
    /// 下位トランスポートの送信側を shutdown 済みかどうか
    write_shutdown: bool,
    /// 自側の close の後でピアの shutdown を待つ上限
    linger: Duration,
    /// 自側の close を要求した時刻 (要求していない場合は `None`)
    ///
    /// 期限を `Instant` の加算で持たないのは、桁あふれで panic するためである。
    /// 残り時間は `linger` から経過時間を引いて求める。
    linger_started: Option<Instant>,
    /// linger の上限を過ぎても Record を書き切れなかったかどうか
    linger_expired: bool,
    /// 下位トランスポートが閉じたかどうか
    transport_ended: bool,
    /// ドライバ開始時刻
    started: std::time::Instant,
}

/// アプリケーションへ渡すのを待っているピア起動ストリーム
#[derive(Debug)]
enum PendingAccept {
    /// 双方向ストリーム
    Bidirectional {
        /// ストリーム ID
        id: i64,
        /// 送信側のハンドル
        send: SendStream,
        /// 受信側のハンドル
        recv: RecvStream,
    },
    /// 単方向ストリーム
    Unidirectional {
        /// ストリーム ID
        id: i64,
        /// 受信側のハンドル
        recv: RecvStream,
    },
}

/// 応答を返していない書き込み
#[derive(Debug)]
struct PendingAck {
    /// ストリーム ID
    stream_id: i64,
    /// 送信完了または失敗を通知する応答チャネル
    ack: oneshot::Sender<Result<()>>,
}

/// select で待つイベント
enum Wake {
    Read(std::io::Result<usize>),
    Write(std::io::Result<usize>),
    Command(Option<Command>),
    Close(Option<(u64, String)>),
    Timeout,
}

impl Driver {
    /// ドライバのメインループ
    ///
    /// 読み取り・書き込み・コマンド・close 要求・タイマーを 1 つの `select!` で待つ。
    /// 書き込みが詰まっている間も他の腕が動くため、ピアが読み取りを止めても受信・
    /// コマンド・アイドルタイムアウトが進む (tokio-qmux と同じ設計)。
    async fn run(mut self) {
        let mut read_buf = vec![0u8; READ_BUFFER_SIZE];

        loop {
            self.process_events();
            self.update_datagram_drops();
            self.retry_accept();
            self.retry_stream_events();
            self.retry_stop_sending();
            // 接続が閉じた場合は、そこから linger の上限を数える。自側から閉じた
            // 場合 (Section 7.2) だけでなく、アイドルタイムアウト (Section 7.1) と
            // ピアからの CONNECTION_CLOSE (Section 7.3) でも送信側の shutdown を
            // 試みるためである。
            if self.local_close || self.connection.is_closed() {
                self.start_linger();
            }
            // 送信する Record を用意する。書きかけの Record があるうちは次の
            // Record を取り出さない (Record を途中で切らないため)。
            if self.written >= self.output.len() {
                self.output.clear();
                self.written = 0;
                let now = self.now();
                match self.connection.poll_transmit(&mut self.output, now) {
                    Ok(true) => {}
                    Ok(false) => self.output.clear(),
                    Err(_) => {
                        // dwnx.h の契約で継続できないエラーが返った
                        self.on_transport_ended();
                    }
                }
            }
            // 書き出す Record が無い場合は、送れなかった書き込みと DATAGRAM の
            // 失敗だけが通知されている。応答を返さないとアプリケーションが待ち
            // 続けるため、ここで返す。書き出しを終えた Record の応答は、書き切った
            // 時点 (書き込みの腕) で返す。
            if self.written >= self.output.len() {
                self.process_write_events();
                self.process_datagram_events();
            }
            // Section 7.2 / 7.3: 接続が閉じたら下位トランスポートの送信側を graceful に
            // shutdown する。書きかけの Record があるうちは行わない。ピアが読み取って
            // いなければ shutdown も完了しないため、shutdown の待ちは linger の上限で
            // 打ち切る。下位トランスポートの閉塞 (Section 7.4) では shutdown せずに
            // 即座に終了する。
            if (self.local_close || self.connection.is_closed())
                && !self.write_shutdown
                && !self.transport_ended
                && self.written >= self.output.len()
            {
                self.write_shutdown = true;
                if !self.shutdown_write_side().await {
                    // shutdown できない下位トランスポートは使えないものとして扱う
                    self.on_transport_ended();
                }
            }
            if self.should_break() {
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
                // 偏りのある選択はしない。書き込みが詰まっている間も受信・コマンド・
                // close 要求・タイマーが飢餓にならないようにするためである。
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
                    // 下位トランスポートが閉じた (Section 7.4)
                    self.on_transport_ended();
                }
                Wake::Read(Ok(len)) => {
                    // ピアからの CONNECTION_CLOSE は dwnx-rs が接続を閉じて
                    // 通知する。他のエラーは下位トランスポートが使えないものとして
                    // 扱う (Section 7.4)
                    if self.connection.read(&read_buf[..len], self.now()).is_err() {
                        self.on_transport_ended();
                    }
                }
                Wake::Write(Ok(0)) => {
                    // 書き出しの途中で 0 バイトだった場合は下位トランスポートが閉じた
                    if self.written < self.output.len() {
                        self.on_transport_ended();
                    }
                }
                Wake::Write(Ok(len)) => {
                    self.written += len;
                    // Record を書き切った時点で、その Record に含まれる書き込みと
                    // DATAGRAM へ応答を返す
                    if self.written >= self.output.len() {
                        self.process_write_events();
                        self.process_datagram_events();
                    }
                }
                Wake::Write(Err(_)) => {
                    self.on_transport_ended();
                }
                Wake::Command(Some(command)) => self.handle_command(command),
                Wake::Command(None) => {
                    // ドライバ自身が `command_tx` を保持しているため通常は到達しない。
                    // Connection とすべてのストリームが破棄された場合に備えた防御である。
                    self.on_transport_ended();
                }
                Wake::Close(Some((error_code, reason))) => {
                    self.request_close(error_code, reason.as_bytes());
                }
                Wake::Close(None) => {
                    // Connection がドロップされ、閉じる要求も届かなくなった
                    self.on_transport_ended();
                }
                Wake::Timeout => {
                    // linger の上限を過ぎても Record を書き切れない場合は打ち切る。
                    // ピアが読み取らない以上、書きかけの Record は届かないためである。
                    if self.written < self.output.len()
                        && self
                            .linger_remaining()
                            .is_some_and(|remaining| remaining.is_zero())
                    {
                        self.linger_expired = true;
                    }
                    if self.connection.handle_timeout(self.now()).is_err() {
                        self.on_transport_ended();
                    }
                }
            }
        }

        self.finish();
    }

    /// ドライバを終了すべきかどうか
    fn should_break(&self) -> bool {
        // 下位トランスポートの閉塞 (draft-ietf-quic-qmux-02 Section 7.4)。送信側の
        // shutdown も試みずに即座に終了する。
        if self.transport_ended {
            return true;
        }
        if self.local_close {
            // 自側から閉じた場合は、CONNECTION_CLOSE を送った後にピアの shutdown を
            // 待ってから終了する (Section 7.2)。ただし linger の上限を過ぎたら終了する。
            // ピアが shutdown しない場合や、ピアが読み取りを止めて送信が詰まった場合でも、
            // ドライバが残り続けないようにするためである。書きかけの Record があるうちは、
            // 上限を過ぎたかどうかを Timeout の腕で判定する (書き切れていればここで
            // 終了してよい)。
            return self.linger_expired
                || (self.written >= self.output.len()
                    && self
                        .linger_remaining()
                        .is_some_and(|remaining| remaining.is_zero()));
        }
        if self.connection.is_closed() {
            // ピア起因の終了 (アイドルタイムアウト Section 7.1、ピアからの
            // CONNECTION_CLOSE Section 7.3)。送信側の shutdown を 1 回試みたら終了する。
            // shutdown が完了しない場合は linger の上限で打ち切る。
            return self.write_shutdown
                || self.linger_expired
                || self
                    .linger_remaining()
                    .is_some_and(|remaining| remaining.is_zero());
        }
        false
    }

    /// 現在時刻を dwnx のタイムスタンプ (ナノ秒) へ変換する
    fn now(&self) -> u64 {
        self.started.elapsed().as_nanos() as u64
    }

    /// 次のタイマーまでのスリープ時間を返す
    ///
    /// アイドルタイムアウトと linger の上限のうち、早い方までの時間を返す。
    /// 接続が閉じた後はアイドルタイムアウトを見ない。満了済みの期限を返し続けると、
    /// 0 時間のスリープでビジーループになるためである。
    fn next_sleep_duration(&self) -> Option<Duration> {
        let idle = if self.local_close || self.connection.is_closed() {
            None
        } else {
            self.connection
                .next_timeout()
                .map(|expiry| Duration::from_nanos(expiry.saturating_sub(self.now())))
        };
        match (idle, self.linger_remaining()) {
            (Some(idle), Some(linger)) => Some(idle.min(linger)),
            (Some(idle), None) => Some(idle),
            (None, Some(linger)) => Some(linger),
            (None, None) => None,
        }
    }

    /// 自側の close の後でピアの shutdown を待つ上限を数え始める
    fn start_linger(&mut self) {
        if self.linger_started.is_none() {
            self.linger_started = Some(Instant::now());
        }
    }

    /// linger の残り時間を返す
    ///
    /// 自側の close を要求していない場合は `None` を返す。
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

    /// 下位トランスポートが使えなくなったことを記録する
    ///
    /// 送信できなくなった書き込みの応答もここで失敗させる。
    fn on_transport_ended(&mut self) {
        self.fail_pending_acks();
        self.connection.transport_closed();
        self.transport_ended = true;
    }

    /// 自側から接続を閉じる
    ///
    /// CONNECTION_CLOSE を送った後は送信待ちのデータを送らないため、応答を
    /// 返していない書き込みをここで失敗させ、アプリケーションを待たせない。
    /// CONNECTION_CLOSE の生成に失敗しても、以後は linger の上限で必ず終了する。
    fn request_close(&mut self, error_code: u64, reason: &[u8]) {
        let _ = self.connection.close(error_code, reason);
        self.fail_pending_acks();
        self.local_close = true;
        self.start_linger();
    }

    /// dwnx のイベントを取り出して処理する
    fn process_events(&mut self) {
        while let Some(event) = self.connection.poll_event() {
            match event {
                DwnxEvent::TransportParametersReceived(_) => {
                    self.established = true;
                    self.flush_pending_commands();
                }
                DwnxEvent::StreamOpened { id } => self.on_stream_opened(id),
                DwnxEvent::StreamData { id, data, fin, .. } => {
                    // クレジットはアプリケーションへ配送できた時点で返す。
                    // ここで返すと、アプリケーションが読まない限り無制限に
                    // 積まれてしまう。
                    self.pending_stream_events
                        .push_back((id, StreamEvent::Data { data, fin }));
                }
                DwnxEvent::StreamClosed { id, .. } => {
                    // ストリームが閉じたことを FIN の受信と同じ形で届ける
                    self.pending_stream_events.push_back((
                        id,
                        StreamEvent::Data {
                            data: Vec::new(),
                            fin: true,
                        },
                    ));
                }
                DwnxEvent::StreamReset { id, error_code, .. } => {
                    self.pending_stream_events
                        .push_back((id, StreamEvent::Reset(error_code)));
                }
                DwnxEvent::RecvStopSending { id, error_code } => {
                    // 接続単位で待つ利用側へ通知する。チャネルが埋まっていても
                    // 失わないよう、ここでは保留キューに入れるだけにして実際の配送は
                    // retry_stop_sending が行う。
                    self.pending_stop_sending
                        .push_back(StopSendingObserved { id, error_code });
                }
                DwnxEvent::Datagram { data } => {
                    // DATAGRAM はフロー制御されないため、保持量に上限を設ける。
                    // あふれた DATAGRAM は破棄し、個数をアプリケーションが
                    // 観測できるように数えておく (draft-ietf-quic-qmux-02 Section 6、
                    // RFC 9221 Section 5.3)。
                    if self.datagram_tx.try_send(data).is_err() {
                        self.datagram_drops = self.datagram_drops.saturating_add(1);
                    }
                }
                DwnxEvent::ConnectionClosed => {
                    // 接続が閉じたかどうかは dwnx-rs の `is_closed()` が保持して
                    // いるため、ここでは何もしない。網羅性のために腕を残す。
                }
            }
        }
    }

    /// 破棄した DATAGRAM の累計個数をアプリケーションへ通知する
    ///
    /// 値が変わったときだけ watch で配る。
    fn update_datagram_drops(&self) {
        if *self.dropped_datagrams_tx.borrow() != self.datagram_drops {
            let _ = self.dropped_datagrams_tx.send(self.datagram_drops);
        }
    }

    /// ピア起動のストリームを登録し、アプリケーションへ渡す準備をする
    fn on_stream_opened(&mut self, id: i64) {
        // RFC 9000 Section 2.1: 下位 2 ビットの 0x2 が立っていれば単方向
        if id & UNIDIRECTIONAL_STREAM_BIT == 0 {
            let send = SendStream::new(id, self.command_tx.clone());
            let recv = self.register_recv(id);
            self.pending_accept
                .push_back(PendingAccept::Bidirectional { id, send, recv });
        } else {
            let recv = self.register_recv(id);
            self.pending_accept
                .push_back(PendingAccept::Unidirectional { id, recv });
        }
    }

    /// ピア起動のストリームをアプリケーションへ渡す
    ///
    /// accept チャネルが埋まっている間は保留し、空きができ次第渡す。受信イベントの
    /// チャネルは登録済みであるため、アプリケーションが受け取るまでのデータは
    /// 欠落せず、フロー制御により有界になる。
    fn retry_accept(&mut self) {
        while let Some(pending) = self.pending_accept.pop_front() {
            match pending {
                PendingAccept::Bidirectional { id, send, recv } => {
                    if self.accept_bidirectional_tx.capacity() == 0 {
                        self.pending_accept
                            .push_front(PendingAccept::Bidirectional { id, send, recv });
                        break;
                    }
                    if self.accept_bidirectional_tx.try_send((send, recv)).is_err() {
                        // アプリケーションが accept をやめた (受信側がドロップされた)
                        self.recv_streams.remove(&id);
                    }
                }
                PendingAccept::Unidirectional { id, recv } => {
                    if self.accept_unidirectional_tx.capacity() == 0 {
                        self.pending_accept
                            .push_front(PendingAccept::Unidirectional { id, recv });
                        break;
                    }
                    if self.accept_unidirectional_tx.try_send(recv).is_err() {
                        self.recv_streams.remove(&id);
                    }
                }
            }
        }
    }

    /// 受信イベントチャネルを登録してハンドルを返す
    fn register_recv(&mut self, id: i64) -> RecvStream {
        let (tx, rx) = mpsc::channel(RECV_CAPACITY);
        self.recv_streams.insert(id, tx);
        RecvStream::new(id, rx, self.command_tx.clone())
    }

    /// 配送待ちのストリームイベントをアプリケーションへ届ける
    ///
    /// 配送できたデータの分だけフロー制御クレジットを返す。クレジットを返さない
    /// 間は dwnx の受信ウィンドウが埋まるため、このキューはフロー制御により
    /// 有界になる。ストリームごとの順序は保たれる。
    fn retry_stream_events(&mut self) {
        while let Some((id, event)) = self.pending_stream_events.pop_front() {
            let credit = match &event {
                StreamEvent::Data { data, .. } => data.len() as u64,
                StreamEvent::Reset(_) => 0,
            };
            // FIN またはリセットを配送したら、そのストリームの受信側は役目を終える
            let finished = matches!(
                &event,
                StreamEvent::Data { fin: true, .. } | StreamEvent::Reset(_)
            );
            // チャネルが閉じている場合はアプリケーションが受信側を破棄している
            let Some(tx) = self.recv_streams.get(&id).cloned() else {
                continue;
            };
            match tx.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.pending_stream_events.push_front((id, event));
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.recv_streams.remove(&id);
                    continue;
                }
            }
            if credit > 0 {
                // アプリケーションへ渡した分のフロー制御クレジットを返す
                let _ = self.connection.extend_max_stream_offset(id, credit);
                self.connection.extend_max_offset(credit);
            }
            if finished {
                self.recv_streams.remove(&id);
            }
        }
    }

    /// 接続単位の STOP_SENDING 通知チャネルへ保留分を配送する
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

    /// 書き込みの完了と失敗をアプリケーションへ返す
    ///
    /// 書き出しを終えた Record に含まれる書き込みだけを完了させる。Record を組み立て
    /// た時点で返すと、下位トランスポートへ届く前に完了したことになってしまう。
    fn process_write_events(&mut self) {
        while let Some(event) = self.connection.poll_write_event() {
            match event {
                WriteEvent::Written { stream_id } => {
                    if let Some(pending) = self.take_pending_ack(stream_id) {
                        let _ = pending.ack.send(Ok(()));
                    }
                }
                WriteEvent::Failed { stream_id, error } => {
                    if let Some(pending) = self.take_pending_ack(stream_id) {
                        let _ = pending.ack.send(Err(Error::Dwnx(error)));
                    }
                }
            }
        }
    }

    /// DATAGRAM の送信結果をアプリケーションへ返す
    ///
    /// 書き出しを終えた Record に含まれる DATAGRAM だけを完了させる
    /// ([`Self::process_write_events`] と同じ扱い)。dwnx は積んだ DATAGRAM の
    /// 数だけ [`DwnxDatagramEvent`] を返すため、応答待ちのキューとは FIFO で
    /// 対応する。
    fn process_datagram_events(&mut self) {
        while let Some(event) = self.connection.poll_datagram_event() {
            let Some(ack) = self.pending_datagram_acks.pop_front() else {
                // 応答待ちの無い結果は捨てる。ここには到達しない想定だが、
                // 到達してもアプリケーションを待たせないようにする。
                break;
            };
            match event {
                DwnxDatagramEvent::Accepted => {
                    let _ = ack.send(Ok(()));
                }
                DwnxDatagramEvent::Failed { error } => {
                    // ピア非対応やサイズ超過はこの DATAGRAM だけの失敗であり、
                    // 接続は閉じない (dwnx.h の dwnx_conn_writev_datagram を参照)
                    let _ = ack.send(Err(Error::Dwnx(error)));
                }
            }
        }
    }

    /// 指定したストリームで最も古い応答を取り出す
    ///
    /// dwnx の送信待ちキューもこのキューも FIFO であるため、同じストリーム ID の
    /// 先頭がその通知に対応する。
    fn take_pending_ack(&mut self, stream_id: i64) -> Option<PendingAck> {
        let index = self
            .pending_acks
            .iter()
            .position(|pending| pending.stream_id == stream_id)?;
        self.pending_acks.remove(index)
    }

    /// 応答を返していない書き込みと DATAGRAM をすべて失敗させる
    fn fail_pending_acks(&mut self) {
        for pending in self.pending_acks.drain(..) {
            let _ = pending.ack.send(Err(Error::ConnectionClosed));
        }
        for ack in self.pending_datagram_acks.drain(..) {
            let _ = ack.send(Err(Error::ConnectionClosed));
        }
    }

    /// アプリケーションからのコマンドを処理する
    fn handle_command(&mut self, command: Command) {
        // 自側から閉じた後は新しいストリームを開けない。応答を待っている
        // アプリケーションが残らないよう、ここでエラーを返す。
        if self.local_close {
            command.reject(Error::ConnectionClosed);
            return;
        }
        // 接続の確立前に、ピアのトランスポートパラメータの受信を要するコマンドは
        // 保留する (draft-ietf-quic-qmux-02 Section 4.2)
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
                    let send = SendStream::new(id, self.command_tx.clone());
                    let recv = self.register_recv(id);
                    (send, recv)
                });
                let _ = reply.send(result.map_err(Error::Dwnx));
            }
            Command::OpenUnidirectional(reply) => {
                let result = self
                    .connection
                    .open_uni()
                    .map(|id| SendStream::new(id, self.command_tx.clone()));
                let _ = reply.send(result.map_err(Error::Dwnx));
            }
            Command::Write { id, data, fin, ack } => {
                // dwnx は FIN を伴わない空のデータを送らないため、送るものが無い
                // 書き込みはここで完了させる (dwnx の lib/dwnx_conn.c の
                // dwnx_conn_writev_stream を参照)
                if !self.connection.write(id, &data, fin) {
                    let _ = ack.send(Ok(()));
                    return;
                }
                self.pending_acks
                    .push_back(PendingAck { stream_id: id, ack });
            }
            Command::ShutdownWrite { id, error_code } => {
                let _ = self.connection.shutdown_write(id, error_code);
            }
            Command::StopSending { id, error_code } => {
                let _ = self.connection.shutdown_read(id, error_code);
            }
            Command::Datagram { data, ack } => {
                // 接続が閉じている場合は送信されないため、ここで失敗を返す
                if !self.connection.send_datagram(&data) {
                    let _ = ack.send(Err(Error::ConnectionClosed));
                    return;
                }
                self.pending_datagram_acks.push_back(ack);
            }
            Command::PollReadable => {
                // 受信側のチャネルに空きができたため、配送待ちを再開する
                self.retry_stream_events();
            }
            Command::Wake => {
                // accept 系と STOP_SENDING の通知チャネルに空きができたため、
                // 保留している分を渡す
                self.retry_accept();
                self.retry_stop_sending();
            }
        }
    }

    /// ドライバを終了し、アプリケーションへ切断を通知する
    ///
    /// `self` を消費するため、この関数を抜けると下位トランスポートも閉じられる。
    fn finish(mut self) {
        // 応答を返していない書き込みが残っていると、アプリケーションが永久に
        // 待ち続けるため、必ず失敗させる。
        self.fail_pending_acks();
        let _ = self.closed_tx.send(true);
        self.recv_streams.clear();
        self.pending_stream_events.clear();
        self.pending_stop_sending.clear();
        self.pending_accept.clear();
        self.pending_commands.clear();
        self.pending_datagram_acks.clear();
        self.output.clear();
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
