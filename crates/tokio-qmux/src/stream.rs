//! QMux ストリームの非同期ハンドル

use tokio::sync::{mpsc, oneshot};

use shiguredo_qmux::stream::StreamId;

use crate::connection::Command;
use crate::error::{Error, Result};

/// ドライバからストリームへ配送するイベント
#[derive(Debug)]
pub(crate) enum StreamEvent {
    /// データを受信した。`fin` が true の場合はこれが最後のデータ
    Data { data: Vec<u8>, fin: bool },
    /// ピアがストリームをリセットした
    Reset(u64),
}

/// ストリームの送信側
#[derive(Debug)]
pub struct SendStream {
    /// ストリーム ID
    id: StreamId,
    /// ドライバへのコマンド送信
    commands: mpsc::Sender<Command>,
    /// FIN を送信済みかどうか
    finished: bool,
}

impl SendStream {
    pub(crate) fn new(id: StreamId, commands: mpsc::Sender<Command>) -> Self {
        Self {
            id,
            commands,
            finished: false,
        }
    }

    /// ストリーム ID を返す
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// データを書き込む
    ///
    /// フロー制御による送信量の調整はドライバが行う。ドライバがデータをすべて
    /// 受け付け、保留分も送り終えるまで待つ。戻り値は受け付けたバイト数で、渡した
    /// データの長さと等しい。
    pub async fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.finished {
            return Err(Error::DriverStopped);
        }
        let (ack, receive) = oneshot::channel();
        self.commands
            .send(Command::Write {
                id: self.id,
                data: data.to_vec(),
                fin: false,
                ack,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)??;
        Ok(data.len())
    }

    /// ストリームの送信側を閉じる (FIN を送る)
    pub async fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        let (ack, receive) = oneshot::channel();
        self.commands
            .send(Command::Write {
                id: self.id,
                data: Vec::new(),
                fin: true,
                ack,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)??;
        self.finished = true;
        Ok(())
    }

    /// ストリームの送信側をリセットする (RESET_STREAM を送る)
    pub async fn reset(&mut self, error_code: u64) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        let (ack, receive) = oneshot::channel();
        self.commands
            .send(Command::Reset {
                id: self.id,
                error_code,
                ack,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        receive.await.map_err(|_| Error::ConnectionClosed)??;
        self.finished = true;
        Ok(())
    }
}

/// ストリームの受信側
#[derive(Debug)]
pub struct RecvStream {
    /// ストリーム ID
    id: StreamId,
    /// ドライバからのイベント受信
    events: mpsc::Receiver<StreamEvent>,
    /// ドライバへのコマンド送信 (フロー制御クレジットの返却に使う)
    commands: mpsc::Sender<Command>,
    /// まだアプリケーションへ渡していないデータ
    leftover: Vec<u8>,
    /// FIN を受信済みかどうか
    fin: bool,
    /// ピアが指定したリセットのエラーコード
    reset: Option<u64>,
}

impl RecvStream {
    pub(crate) fn new(
        id: StreamId,
        events: mpsc::Receiver<StreamEvent>,
        commands: mpsc::Sender<Command>,
    ) -> Self {
        Self {
            id,
            events,
            commands,
            leftover: Vec::new(),
            fin: false,
            reset: None,
        }
    }

    /// ストリーム ID を返す
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// データを読み取る
    ///
    /// ストリームの終端に達した場合は `Ok(None)` を返す。ピアがストリームを
    /// リセットした場合は [`Error::Reset`] を返す。
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        if buf.is_empty() {
            return Ok(Some(0));
        }

        loop {
            if !self.leftover.is_empty() {
                let len = core::cmp::min(buf.len(), self.leftover.len());
                buf[..len].copy_from_slice(&self.leftover[..len]);
                self.leftover.drain(..len);
                // 読み取った分だけフロー制御クレジットが返るため、ドライバに
                // 続きの読み取りを促す
                let _ = self
                    .commands
                    .try_send(Command::PollReadable { id: self.id });
                return Ok(Some(len));
            }
            if self.fin {
                return Ok(None);
            }
            if let Some(code) = self.reset {
                return Err(Error::Reset(code));
            }
            match self.events.recv().await {
                Some(StreamEvent::Data { data, fin }) => {
                    self.leftover = data;
                    self.fin = self.fin || fin;
                }
                Some(StreamEvent::Reset(code)) => {
                    self.reset = Some(code);
                }
                None => return Err(Error::ConnectionClosed),
            }
        }
    }

    /// ストリームの受信側に送信停止を要求する (STOP_SENDING を送る)
    pub async fn stop_sending(&mut self, error_code: u64) -> Result<()> {
        self.commands
            .send(Command::StopSending {
                id: self.id,
                error_code,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)
    }
}
