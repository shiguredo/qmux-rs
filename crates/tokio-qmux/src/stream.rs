//! QMux ストリームの非同期ハンドル

use tokio::sync::{mpsc, oneshot};

use shiguredo_qmux::stream::StreamId;

use crate::connection::Command;
use crate::error::{Error, Result};

/// ドライバからストリームの受信側へ配送するイベント
#[derive(Debug)]
pub(crate) enum StreamEvent {
    /// データを受信した。`fin` が true の場合はこれが最後のデータ
    Data { data: Vec<u8>, fin: bool },
    /// ピアがストリームをリセットした
    Reset(u64),
}

/// ドライバからストリームの送信側へ配送するイベント
#[derive(Debug, Clone, Copy)]
pub(crate) enum SendEvent {
    /// ピアが送信停止を要求した (STOP_SENDING)
    StopSending(u64),
}

/// ピアが要求した送信停止の通知 (STOP_SENDING)
///
/// [`crate::Connection::recv_stop_sending`] が返す。対象のストリーム ID と、ピアが
/// 指定したアプリケーションエラーコードを保持する。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopSendingObserved {
    /// 送信停止を要求されたストリームの ID
    pub id: StreamId,
    /// ピアが指定したアプリケーションエラーコード
    pub error_code: u64,
}

/// ストリームの送信側
#[derive(Debug)]
pub struct SendStream {
    /// ストリーム ID
    id: StreamId,
    /// ドライバへのコマンド送信
    commands: mpsc::Sender<Command>,
    /// ドライバからのイベント受信
    events: mpsc::Receiver<SendEvent>,
    /// FIN を送信済みかどうか
    finished: bool,
    /// ピアが指定した STOP_SENDING のアプリケーションエラーコード
    stop_sending: Option<u64>,
}

impl SendStream {
    pub(crate) fn new(
        id: StreamId,
        commands: mpsc::Sender<Command>,
        events: mpsc::Receiver<SendEvent>,
    ) -> Self {
        Self {
            id,
            commands,
            events,
            finished: false,
            stop_sending: None,
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
    /// データの長さと等しい。ピアが送信停止を要求している場合は
    /// [`Error::Stopped`] を返す。
    pub async fn write(&mut self, data: &[u8]) -> Result<usize> {
        self.check_stopped()?;
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
    ///
    /// ピアがすでに送信停止を要求している場合は [`Error::Stopped`] を返す
    /// ([`Self::write`] と同じ)。
    pub async fn finish(&mut self) -> Result<()> {
        self.check_stopped()?;
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
    ///
    /// ピアがすでに送信停止を要求している場合は [`Error::Stopped`] を返す
    /// ([`Self::write`] と同じ)。
    pub async fn reset(&mut self, error_code: u64) -> Result<()> {
        self.check_stopped()?;
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

    /// ピアが送信停止を要求するのを待つ (STOP_SENDING)
    ///
    /// RFC 9000 Section 19.5 の STOP_SENDING をアプリケーションへ通知する。
    /// ピアが指定したアプリケーションエラーコードを返す。2 回目以降の呼び出しは
    /// 受信済みのコードを即座に返す。
    ///
    /// FIN または RESET_STREAM を送信した後は、ドライバが送信側ハンドルを破棄する
    /// ため、以後の STOP_SENDING はここには届かず [`Error::ConnectionClosed`] を
    /// 返す。接続が閉じて通知が届かなかった場合も同じである。FIN 後もピアの
    /// STOP_SENDING を観測したい場合は [`crate::Connection::recv_stop_sending`] を
    /// 使うこと。
    ///
    /// # Cancel safety
    ///
    /// このメソッドは cancel safe である。待っている間に future を破棄しても、
    /// まだ受け取っていない通知は失われず、次の呼び出しで受け取れる。
    pub async fn stopped(&mut self) -> Result<u64> {
        // すでに通知を受け取っている場合は同じコードを返す
        if let Some(error_code) = self.stop_sending {
            return Ok(error_code);
        }
        match self.events.recv().await {
            Some(SendEvent::StopSending(error_code)) => {
                self.stop_sending = Some(error_code);
                Ok(error_code)
            }
            // ドライバが通知せずに終了した (接続が閉じた)
            None => Err(Error::ConnectionClosed),
        }
    }

    /// ピアが送信停止を要求済みなら [`Error::Stopped`] を返す
    ///
    /// [`Self::stopped`] を待たずに送信した場合でも、通知が届いていればここで
    /// 検出する。通知を消費した場合は、続く [`Self::stopped`] が同じコードを
    /// 即座に返せるように保持する。
    fn check_stopped(&mut self) -> Result<()> {
        if let Some(error_code) = self.stop_sending {
            return Err(Error::Stopped(error_code));
        }
        if let Ok(SendEvent::StopSending(error_code)) = self.events.try_recv() {
            self.stop_sending = Some(error_code);
            return Err(Error::Stopped(error_code));
        }
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
