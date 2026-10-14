//! QMux 接続の非同期ハンドルとドライバ
//!
//! 下位トランスポートの読み書きと時刻の供給をバックグラウンドのドライバタスクが
//! 行い、アプリケーションはストリームと DATAGRAM のハンドル越しに操作する。

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};

use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::time::Timestamp;
use shiguredo_qmux::config::Config;
use shiguredo_qmux::connection::Connection as QmuxConnection;
use shiguredo_qmux::event::{CloseReason, Event};
use shiguredo_qmux::stream::{ReadResult, StreamId};

use crate::error::{Error, Result};
use crate::stream::{RecvStream, SendStream, StreamEvent};

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
    StopSending { id: StreamId, error_code: u64 },
    /// DATAGRAM を送信する
    Datagram(Vec<u8>),
    /// QX_PING を送信する
    Ping(oneshot::Sender<Result<u64>>),
    /// ストリームの読み取り容量が空いたことをドライバへ通知する
    PollReadable { id: StreamId },
}

impl Command {
    /// ピアのトランスポートパラメータの受信を待つ必要があるかどうか
    ///
    /// QMux はピアのトランスポートパラメータを受信するまで、ピアのフロー制御に
    /// 依存するフレームを送信してはならない (draft-ietf-quic-qmux-02 Section 4.2)。
    fn requires_establishment(&self) -> bool {
        matches!(
            self,
            Self::OpenBidirectional(_) | Self::OpenUnidirectional(_) | Self::Datagram(_)
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
/// ストリームごとの受信イベントチャネルの容量
const RECV_CAPACITY: usize = 16;
/// 下位トランスポートから一度に読み取るバイト数の上限
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// QMux 接続
///
/// 下位トランスポートを所有するドライバタスクを起動する。[`Connection`] を
/// ドロップするとコマンドチャネルが閉じ、ドライバは下位トランスポートを閉じて
/// 終了する。
#[derive(Debug)]
pub struct Connection {
    /// ドライバへのコマンド送信
    commands: mpsc::Sender<Command>,
    /// 接続を閉じる要求 (容量 1 の専用チャネル)
    close_request: mpsc::Sender<(u64, String)>,
    /// ピア起動の双方向ストリームの受け取り
    accept_bidirectional: mpsc::Receiver<(SendStream, RecvStream)>,
    /// ピア起動の単方向ストリームの受け取り
    accept_unidirectional: mpsc::Receiver<RecvStream>,
    /// 受信 DATAGRAM の受け取り
    datagrams: mpsc::Receiver<Vec<u8>>,
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
    /// # Panics
    ///
    /// `config` が送信できない設定 (`max_record_size` が既定値未満など) の場合、
    /// 内部の Sans I/O 接続の作成に失敗し panic する。
    pub fn new<T>(role: Role, io: T, config: Config) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (close_request, close_request_rx) = mpsc::channel(CLOSE_CAPACITY);
        let (accept_bidirectional_tx, accept_bidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (accept_unidirectional_tx, accept_unidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (datagram_tx, datagram_rx) = mpsc::channel(DATAGRAM_CAPACITY);
        let (closed_tx, closed_rx) = watch::channel(None);

        let (reader, writer) = tokio::io::split(io);
        let driver = Driver {
            connection: QmuxConnection::new(role, config).expect("QMux 接続を作成できる"),
            reader: Box::new(reader),
            writer: Box::new(writer),
            command_rx,
            command_tx: commands.clone(),
            close_request_rx,
            accept_bidirectional_tx,
            accept_unidirectional_tx,
            datagram_tx,
            closed_tx,
            recv_streams: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            pending_readable: BTreeSet::new(),
            pending_commands: VecDeque::new(),
            established: false,
            read_scratch: vec![0u8; READ_BUFFER_SIZE],
            started: std::time::Instant::now(),
            close_reason: None,
            transport_ended: false,
        };
        tokio::spawn(driver.run());

        Self {
            commands,
            close_request,
            accept_bidirectional: accept_bidirectional_rx,
            accept_unidirectional: accept_unidirectional_rx,
            datagrams: datagram_rx,
            closed: closed_rx,
        }
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
    /// 接続が閉じた場合は `Ok(None)` を返す。
    pub async fn accept_bidirectional(&mut self) -> Result<Option<(SendStream, RecvStream)>> {
        Ok(self.accept_bidirectional.recv().await)
    }

    /// ピア起動の単方向ストリームを受け取る
    ///
    /// 接続が閉じた場合は `Ok(None)` を返す。
    pub async fn accept_unidirectional(&mut self) -> Result<Option<RecvStream>> {
        Ok(self.accept_unidirectional.recv().await)
    }

    /// DATAGRAM を送信する
    pub async fn send_datagram(&self, data: &[u8]) -> Result<()> {
        self.commands
            .send(Command::Datagram(data.to_vec()))
            .await
            .map_err(|_| Error::ConnectionClosed)
    }

    /// DATAGRAM を受信する
    ///
    /// 接続が閉じた場合は `Ok(None)` を返す。
    pub async fn recv_datagram(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.datagrams.recv().await)
    }

    /// QX_PING を送信し、シーケンス番号を返す
    pub async fn ping(&self) -> Result<u64> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Ping(reply))
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)?
    }

    /// 接続を閉じる
    ///
    /// CONNECTION_CLOSE フレームが送信される。容量 1 の専用チャネルへ送るため、
    /// コマンドチャネルの輻輳で要求が失われることはない。すでに未処理の close 要求が
    /// ある場合の送信失敗は、先行する要求が処理されるため無視してよい。
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
struct Driver {
    /// Sans I/O の QMux 接続
    connection: QmuxConnection,
    /// 下位トランスポートの読み取り側
    reader: Box<dyn AsyncRead + Send + Unpin>,
    /// 下位トランスポートの書き込み側
    writer: Box<dyn AsyncWrite + Send + Unpin>,
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
    /// 接続が閉じた理由の通知
    closed_tx: watch::Sender<Option<CloseReason>>,
    /// ストリーム ID から受信イベント送信への対応
    recv_streams: BTreeMap<StreamId, mpsc::Sender<StreamEvent>>,
    /// フロー制御で保留している送信データ (ストリームごとの FIFO)
    pending_writes: BTreeMap<StreamId, VecDeque<PendingWrite>>,
    /// 読み取り容量が空くのを待っているストリーム
    pending_readable: BTreeSet<StreamId>,
    /// 接続の確立を待っているコマンド
    pending_commands: VecDeque<Command>,
    /// ピアのトランスポートパラメータを受信済みかどうか
    established: bool,
    /// ストリーム読み取り用の作業バッファ
    read_scratch: Vec<u8>,
    /// ドライバ開始時刻
    started: std::time::Instant,
    /// 接続が閉じた理由
    close_reason: Option<CloseReason>,
    /// 下位トランスポートが閉じたかどうか
    transport_ended: bool,
}

/// select で待つイベント
enum Wake {
    Read(std::io::Result<usize>),
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

impl Driver {
    /// ドライバのメインループ
    async fn run(mut self) {
        let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
        let mut output = Vec::new();

        loop {
            self.process_events();
            if self.close_reason.is_none() {
                self.retry_readable();
                self.retry_writes();
            }
            if self.flush(&mut output).await.is_err() {
                self.connection.transport_closed();
                self.transport_ended = true;
                self.process_events();
            }
            // 自分から閉じた場合は、CONNECTION_CLOSE を送った後にピアの
            // shutdown を待ってから終了する (draft-ietf-quic-qmux-02 Section 7.2)。
            // ピアの切断や下位トランスポートの閉塞を観測した場合は即座に終了する。
            let should_break = match &self.close_reason {
                Some(CloseReason::Local { .. }) => self.transport_ended,
                Some(_) => true,
                None => false,
            };
            if should_break {
                break;
            }

            let sleep_duration = self.next_sleep_duration();
            let timeout = async move {
                match sleep_duration {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => core::future::pending::<()>().await,
                }
            };
            let wake = {
                let reader = &mut self.reader;
                let command_rx = &mut self.command_rx;
                let close_request_rx = &mut self.close_request_rx;
                tokio::select! {
                    biased;
                    result = reader.read(read_buf.as_mut_slice()) => Wake::Read(result),
                    command = command_rx.recv() => Wake::Command(command),
                    request = close_request_rx.recv() => Wake::Close(request),
                    _ = timeout => Wake::Timeout,
                }
            };

            match wake {
                Wake::Read(Ok(0)) | Wake::Read(Err(_)) => {
                    // 下位トランスポートが閉じた (Section 7.4)
                    self.connection.transport_closed();
                    self.transport_ended = true;
                }
                Wake::Read(Ok(len)) => {
                    self.connection.receive(&read_buf[..len], self.now());
                }
                Wake::Command(Some(command)) => self.handle_command(command),
                Wake::Command(None) => {
                    // Connection がドロップされた
                    self.connection.transport_closed();
                    self.transport_ended = true;
                }
                Wake::Close(Some((error_code, reason))) => {
                    self.connection.close(error_code, &reason);
                }
                Wake::Close(None) => {
                    // Connection がドロップされ、閉じる要求も届かなくなった
                    self.connection.transport_closed();
                    self.transport_ended = true;
                }
                Wake::Timeout => {
                    self.connection.handle_timeout(self.now());
                    if self.connection.is_closed() {
                        self.transport_ended = true;
                    }
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
    fn next_sleep_duration(&self) -> Option<std::time::Duration> {
        self.connection.next_timeout().map(|deadline| {
            let now = self.now().as_micros();
            let then = deadline.as_micros();
            if then <= now {
                std::time::Duration::ZERO
            } else {
                std::time::Duration::from_micros(then - now)
            }
        })
    }

    /// 接続のイベントを取り出して処理する
    fn process_events(&mut self) {
        while let Some(event) = self.connection.poll_event() {
            match event {
                Event::TransportParametersReceived => {
                    self.established = true;
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
                    self.send_event(id, StreamEvent::Reset(error_code));
                    self.recv_streams.remove(&id);
                    self.pending_readable.remove(&id);
                }
                Event::StreamStopSending { .. } => {
                    // STOP_SENDING を受けたストリームへの書き込みは、次の
                    // conn.write が STREAM_STATE_ERROR を返した時点で打ち切られる
                }
                Event::Datagram(data) => {
                    // 保持量に上限があるため、あふれた DATAGRAM は破棄する
                    // (draft-ietf-quic-qmux-02 Section 6)
                    let _ = self.datagram_tx.try_send(data);
                }
                Event::Ping { .. } => {}
                Event::PingAcknowledged { .. } => {}
                Event::ConnectionClosed(reason) => {
                    // 自分から閉じた場合は、ピアの shutdown を待つ間も理由を保持する
                    if self.close_reason.is_none() {
                        self.close_reason = Some(reason);
                    }
                }
            }
        }
    }

    /// ピア起動のストリームをアプリケーションへ渡す
    fn on_stream_opened(&mut self, id: StreamId) {
        if id.stream_type().is_bidirectional() {
            let send = SendStream::new(id, self.command_tx.clone());
            let recv = self.register_recv(id);
            if self.accept_bidirectional_tx.try_send((send, recv)).is_err() {
                // アプリケーションが受け取らないまま接続が閉じた場合など
                self.recv_streams.remove(&id);
            }
        } else {
            let recv = self.register_recv(id);
            if self.accept_unidirectional_tx.try_send(recv).is_err() {
                self.recv_streams.remove(&id);
            }
        }
    }

    /// 受信イベントチャネルを登録してハンドルを返す
    fn register_recv(&mut self, id: StreamId) -> RecvStream {
        let (tx, rx) = mpsc::channel(RECV_CAPACITY);
        self.recv_streams.insert(id, tx);
        RecvStream::new(id, rx, self.command_tx.clone())
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
                            // 全データ (FIN 指定時は FIN も) を送り終えた
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
                        // 書き込みをすべて失敗させる
                        let _ = pending.ack.send(Err(Error::Qmux(error)));
                        while let Some(rest) = queue.pop_front() {
                            let _ = rest.ack.send(Err(Error::ConnectionClosed));
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
                        if len > 0 || fin {
                            self.send_event(id, StreamEvent::Data { data, fin });
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
                        self.send_event(id, StreamEvent::Reset(error_code));
                        self.recv_streams.remove(&id);
                        self.pending_readable.remove(&id);
                        break;
                    }
                    Err(_) => {
                        self.pending_readable.remove(&id);
                        break;
                    }
                }
            }
        }
    }

    /// ストリームへイベントを配送する
    fn send_event(&mut self, id: StreamId, event: StreamEvent) {
        if let Some(tx) = self.recv_streams.get(&id) {
            let _ = tx.try_send(event);
        }
    }

    /// 保留している送信フレームを書き出す
    async fn flush(&mut self, output: &mut Vec<u8>) -> Result<()> {
        loop {
            output.clear();
            let more = self
                .connection
                .poll_transmit(output, self.now())
                .map_err(Error::Qmux)?;
            if !more {
                return Ok(());
            }
            self.writer.write_all(output).await.map_err(Error::Io)?;
        }
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
                    let send = SendStream::new(id, self.command_tx.clone());
                    let recv = self.register_recv(id);
                    (send, recv)
                });
                let _ = reply.send(result.map_err(Error::Qmux));
            }
            Command::OpenUnidirectional(reply) => {
                let result = self
                    .connection
                    .open_uni()
                    .map(|id| SendStream::new(id, self.command_tx.clone()));
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
                        let _ = pending.ack.send(Err(Error::ConnectionClosed));
                    }
                }
                let result = self.connection.reset(id, error_code).map_err(Error::Qmux);
                let _ = ack.send(result);
            }
            Command::StopSending { id, error_code } => {
                let _ = self.connection.stop_sending(id, error_code);
            }
            Command::Datagram(data) => {
                let _ = self.connection.send_datagram(&data);
            }
            Command::Ping(reply) => {
                let _ = reply.send(self.connection.ping().map_err(Error::Qmux));
            }
            Command::PollReadable { id } => {
                self.pending_readable.insert(id);
            }
        }
    }

    /// ドライバを終了し、アプリケーションへ切断を通知する
    fn finish(mut self) {
        let reason = self
            .close_reason
            .take()
            .unwrap_or(CloseReason::TransportClosed);
        let _ = self.closed_tx.send(Some(reason));
        self.recv_streams.clear();
        self.pending_writes.clear();
    }
}
