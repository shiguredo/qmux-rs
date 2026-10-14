//! dwnx 接続の非同期ハンドルとドライバ

use std::collections::{BTreeMap, VecDeque};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};

use dwnx_rs::{Connection as DwnxConnection, Event as DwnxEvent};

use crate::Role;
use crate::error::{Error, Result};
use crate::stream::{RecvStream, SendStream, StreamEvent};

/// アプリケーションからドライバへのコマンド
#[derive(Debug)]
pub(crate) enum Command {
    OpenBidirectional(oneshot::Sender<Result<(SendStream, RecvStream)>>),
    OpenUnidirectional(oneshot::Sender<Result<SendStream>>),
    Write { id: i64, data: Vec<u8>, fin: bool },
    ShutdownWrite { id: i64, error_code: u64 },
}

impl Command {
    /// ピアのトランスポートパラメータの受信を待つ必要があるかどうか
    fn requires_establishment(&self) -> bool {
        matches!(
            self,
            Self::OpenBidirectional(_) | Self::OpenUnidirectional(_)
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
/// ストリームごとの受信イベントチャネルの容量
const RECV_CAPACITY: usize = 64;
/// 下位トランスポートから一度に読み取るバイト数の上限
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// dwnx 接続
#[derive(Debug)]
pub struct Connection {
    commands: mpsc::Sender<Command>,
    /// 接続を閉じる要求 (容量 1 の専用チャネル)
    close_request: mpsc::Sender<(u64, String)>,
    accept_bidirectional: mpsc::Receiver<(SendStream, RecvStream)>,
    accept_unidirectional: mpsc::Receiver<RecvStream>,
    closed: watch::Receiver<bool>,
}

impl Connection {
    /// 下位トランスポート上に dwnx 接続を確立する
    ///
    /// # Panics
    ///
    /// dwnx 接続の作成に失敗した場合 panic する。
    pub fn new<T>(role: Role, io: T, config: &dwnx_rs::Config) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (close_request, close_request_rx) = mpsc::channel(CLOSE_CAPACITY);
        let (accept_bidirectional_tx, accept_bidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (accept_unidirectional_tx, accept_unidirectional_rx) = mpsc::channel(ACCEPT_CAPACITY);
        let (closed_tx, closed_rx) = watch::channel(false);

        let connection = match role {
            Role::Client => DwnxConnection::client(config),
            Role::Server => DwnxConnection::server(config),
        }
        .expect("dwnx connection is created");

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
            closed_tx,
            recv_streams: BTreeMap::new(),
            pending_commands: VecDeque::new(),
            established: false,
            local_close: false,
            transport_ended: false,
            closed: false,
            started: std::time::Instant::now(),
        };
        tokio::spawn(driver.run());

        Self {
            commands,
            close_request,
            accept_bidirectional: accept_bidirectional_rx,
            accept_unidirectional: accept_unidirectional_rx,
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
    pub async fn accept_bidirectional(&mut self) -> Result<Option<(SendStream, RecvStream)>> {
        Ok(self.accept_bidirectional.recv().await)
    }

    /// ピア起動の単方向ストリームを受け取る
    pub async fn accept_unidirectional(&mut self) -> Result<Option<RecvStream>> {
        Ok(self.accept_unidirectional.recv().await)
    }

    /// 接続を閉じる
    ///
    /// 容量 1 の専用チャネルへ送るため、コマンドチャネルの輻輳で要求が失われることは
    /// ない。すでに未処理の close 要求がある場合の送信失敗は、先行する要求が処理
    /// されるため無視してよい。
    pub fn close(&self, error_code: u64, reason: &str) {
        let _ = self
            .close_request
            .try_send((error_code, reason.to_string()));
    }

    /// 接続が閉じたかどうか
    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// 接続が閉じるまで待つ
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
    connection: DwnxConnection,
    reader: Box<dyn AsyncRead + Send + Unpin>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    /// 接続を閉じる要求の受信 (容量 1 の専用チャネル)
    close_request_rx: mpsc::Receiver<(u64, String)>,
    accept_bidirectional_tx: mpsc::Sender<(SendStream, RecvStream)>,
    accept_unidirectional_tx: mpsc::Sender<RecvStream>,
    closed_tx: watch::Sender<bool>,
    recv_streams: BTreeMap<i64, mpsc::Sender<StreamEvent>>,
    pending_commands: VecDeque<Command>,
    established: bool,
    local_close: bool,
    transport_ended: bool,
    closed: bool,
    started: std::time::Instant,
}

/// select で待つイベント
enum Wake {
    Read(std::io::Result<usize>),
    Command(Option<Command>),
    Close(Option<(u64, String)>),
    Timeout,
}

impl Driver {
    async fn run(mut self) {
        let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
        let mut output = Vec::new();

        loop {
            self.process_events();
            if self.flush(&mut output).await.is_err() {
                self.connection.transport_closed();
                self.transport_ended = true;
                self.closed = true;
            }
            if self.closed && (!self.local_close || self.transport_ended) {
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
                    self.connection.transport_closed();
                    self.transport_ended = true;
                    self.closed = true;
                }
                Wake::Read(Ok(len)) => {
                    if self.connection.read(&read_buf[..len], self.now()).is_err() {
                        self.transport_ended = true;
                        self.closed = true;
                    }
                }
                Wake::Command(Some(command)) => self.handle_command(command),
                Wake::Command(None) => {
                    self.connection.transport_closed();
                    self.transport_ended = true;
                    self.closed = true;
                }
                Wake::Close(Some((error_code, reason))) => {
                    self.connection.close(error_code, reason.as_bytes());
                    self.local_close = true;
                }
                Wake::Close(None) => {
                    // Connection がドロップされ、閉じる要求も届かなくなった
                    self.connection.transport_closed();
                    self.transport_ended = true;
                    self.closed = true;
                }
                Wake::Timeout => {
                    if self.connection.handle_timeout(self.now()).is_err() {
                        self.transport_ended = true;
                        self.closed = true;
                    }
                }
            }
        }

        let _ = self.closed_tx.send(true);
    }

    fn now(&self) -> u64 {
        self.started.elapsed().as_nanos() as u64
    }

    fn next_sleep_duration(&self) -> Option<std::time::Duration> {
        self.connection
            .next_timeout()
            .map(|expiry| expiry.saturating_sub(self.now()))
            .map(std::time::Duration::from_nanos)
    }

    fn process_events(&mut self) {
        while let Some(event) = self.connection.poll_event() {
            match event {
                DwnxEvent::TransportParametersReceived(_) => {
                    self.established = true;
                    self.flush_pending_commands();
                }
                DwnxEvent::StreamOpened { id } => self.on_stream_opened(id),
                DwnxEvent::StreamData { id, data, fin, .. } => {
                    let len = data.len() as u64;
                    self.send_event(id, StreamEvent::Data { data, fin });
                    // アプリケーションへ渡した分のフロー制御クレジットを返す
                    let _ = self.connection.extend_max_stream_offset(id, len);
                    self.connection.extend_max_offset(len);
                }
                DwnxEvent::StreamClosed { id, .. } => {
                    self.send_event(
                        id,
                        StreamEvent::Data {
                            data: Vec::new(),
                            fin: true,
                        },
                    );
                }
                DwnxEvent::StreamReset { id, error_code, .. } => {
                    self.send_event(id, StreamEvent::Reset(error_code));
                }
                _ => {}
            }
        }
    }

    fn on_stream_opened(&mut self, id: i64) {
        // ストリーム ID の下位 2 ビット: 0/1 は双方向、2/3 は単方向
        let bidirectional = id & 0x2 == 0;
        if bidirectional {
            let send = SendStream::new(id, self.command_tx.clone());
            let recv = self.register_recv(id);
            if self.accept_bidirectional_tx.try_send((send, recv)).is_err() {
                self.recv_streams.remove(&id);
            }
        } else {
            let recv = self.register_recv(id);
            if self.accept_unidirectional_tx.try_send(recv).is_err() {
                self.recv_streams.remove(&id);
            }
        }
    }

    fn register_recv(&mut self, id: i64) -> RecvStream {
        let (tx, rx) = mpsc::channel(RECV_CAPACITY);
        self.recv_streams.insert(id, tx);
        RecvStream::new(id, rx)
    }

    fn send_event(&mut self, id: i64, event: StreamEvent) {
        if let Some(tx) = self.recv_streams.get(&id) {
            let _ = tx.try_send(event);
        }
    }

    async fn flush(&mut self, output: &mut Vec<u8>) -> Result<()> {
        loop {
            output.clear();
            let more = self
                .connection
                .poll_transmit(output, self.now())
                .map_err(Error::Dwnx)?;
            if !more {
                return Ok(());
            }
            self.writer.write_all(output).await.map_err(Error::Io)?;
        }
    }

    fn handle_command(&mut self, command: Command) {
        if !self.established && command.requires_establishment() {
            self.pending_commands.push_back(command);
            return;
        }
        self.dispatch(command);
    }

    fn flush_pending_commands(&mut self) {
        while let Some(command) = self.pending_commands.pop_front() {
            self.dispatch(command);
        }
    }

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
            Command::Write { id, data, fin } => {
                self.connection.write(id, &data, fin);
            }
            Command::ShutdownWrite { id, error_code } => {
                let _ = self.connection.shutdown_write(id, error_code);
            }
        }
    }
}
