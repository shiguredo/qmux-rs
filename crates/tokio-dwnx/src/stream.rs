//! dwnx ストリームの非同期ハンドル

use tokio::sync::{mpsc, oneshot};

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

/// ピアが要求した送信停止の通知 (STOP_SENDING)
///
/// [`crate::Connection::recv_stop_sending`] が返す。対象のストリーム ID と、
/// ピアが指定したアプリケーションエラーコードを保持する。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopSendingObserved {
    /// 送信停止を要求されたストリームの ID
    pub id: i64,
    /// ピアが指定したアプリケーションエラーコード
    pub error_code: u64,
}

/// ストリームの送信側
#[derive(Debug)]
pub struct SendStream {
    /// ストリーム ID
    id: i64,
    /// ドライバへのコマンド送信
    commands: mpsc::Sender<Command>,
    /// FIN を送信済みかどうか
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
    ///
    /// ドライバがデータをすべて受け付け、フロー制御で保留された分も含めて
    /// 下位トランスポートへ書き出し終えるまで待つ。戻り値は受け付けたバイト数で、
    /// 渡したデータの長さと等しい。ストリームが閉じている場合や接続が閉じた場合は
    /// [`Error`] を返す。
    pub async fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.finished {
            return Err(Error::ConnectionClosed);
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
    /// FIN を含む Record を下位トランスポートへ書き出し終えるまで待つ。
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
    ///
    /// 送信待ちのデータは破棄される。
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
    /// ストリーム ID
    id: i64,
    /// ドライバからのイベント受信
    events: mpsc::Receiver<StreamEvent>,
    /// ドライバへのコマンド送信
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
        id: i64,
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
    pub fn id(&self) -> i64 {
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
                // 取り出した分だけチャネルに空きができ、読み取った分のフロー制御
                // クレジットが返る。ドライバへ配送の再開を促す。
                self.poll_readable();
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
                    // イベントを 1 つ取り出してチャネルに空きができたため、
                    // 配送待ちのイベントの配送を再開させる
                    self.poll_readable();
                }
                Some(StreamEvent::Reset(code)) => {
                    self.reset = Some(code);
                    self.poll_readable();
                }
                None => return Err(Error::ConnectionClosed),
            }
        }
    }

    /// ストリームの受信側に送信停止を要求する (STOP_SENDING を送る)
    ///
    /// ピアは [`crate::Connection::recv_stop_sending`] で観測する
    /// (RFC 9000 Section 19.5)。
    pub async fn stop_sending(&mut self, error_code: u64) -> Result<()> {
        self.commands
            .send(Command::StopSending {
                id: self.id,
                error_code,
            })
            .await
            .map_err(|_| Error::ConnectionClosed)
    }

    /// ドライバへ配送の再開を促す
    ///
    /// ドライバは受信イベントのチャネルが埋まっている間、配送待ちのイベントを
    /// 保持している。チャネルに空きができたことを伝えないと、アプリケーションが
    /// 読み続けていても配送が停滞する。通知は目安であり、あふれた場合はドライバが
    /// 次のループで配送を再開するため、送信に失敗しても構わない。
    fn poll_readable(&self) {
        let _ = self.commands.try_send(Command::PollReadable);
    }
}
