//! dwnx ストリームの非同期ハンドル

use tokio::sync::mpsc;

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
    id: i64,
    commands: mpsc::Sender<Command>,
    finished: bool,
}

impl SendStream {
    pub(crate) fn new(id: i64, commands: mpsc::Sender<Command>) -> Self {
        Self {
            id,
            commands,
            finished: false,
        }
    }

    /// ストリーム ID を返す
    pub fn id(&self) -> i64 {
        self.id
    }

    /// データを書き込む
    pub async fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.finished {
            return Err(Error::ConnectionClosed);
        }
        self.commands
            .send(Command::Write {
                id: self.id,
                data: data.to_vec(),
                fin: false,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        Ok(data.len())
    }

    /// ストリームの送信側を閉じる (FIN を送る)
    pub async fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.commands
            .send(Command::Write {
                id: self.id,
                data: Vec::new(),
                fin: true,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        self.finished = true;
        Ok(())
    }

    /// ストリームの送信側をリセットする
    pub async fn reset(&mut self, error_code: u64) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.commands
            .send(Command::ShutdownWrite {
                id: self.id,
                error_code,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        self.finished = true;
        Ok(())
    }
}

/// ストリームの受信側
#[derive(Debug)]
pub struct RecvStream {
    id: i64,
    events: mpsc::Receiver<StreamEvent>,
    leftover: Vec<u8>,
    fin: bool,
    reset: Option<u64>,
}

impl RecvStream {
    pub(crate) fn new(id: i64, events: mpsc::Receiver<StreamEvent>) -> Self {
        Self {
            id,
            events,
            leftover: Vec::new(),
            fin: false,
            reset: None,
        }
    }

    /// ストリーム ID を返す
    pub fn id(&self) -> i64 {
        self.id
    }

    /// データを読み取る
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        if buf.is_empty() {
            return Ok(Some(0));
        }
        loop {
            if !self.leftover.is_empty() {
                let len = core::cmp::min(buf.len(), self.leftover.len());
                buf[..len].copy_from_slice(&self.leftover[..len]);
                self.leftover.drain(..len);
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
}
