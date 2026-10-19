//! dwnx 接続の安全な Sans I/O ラッパー

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::ptr;

use dwnx_sys as sys;

use crate::config::TransportParameters;
use crate::error::Error;
use crate::event::Event;

/// コールバックが通知するイベントの保管先
///
/// dwnx の `user_data` として渡すため、コールバックの中では `&Sink` しか
/// 取り出せない。可変の状態は `RefCell` で持つ。
#[derive(Debug, Default)]
struct Sink {
    /// コールバックが通知したイベント
    events: RefCell<VecDeque<Event>>,
    /// 部分配送されている DATAGRAM の組み立て状態
    datagram: RefCell<DatagramAssembly>,
}

/// 部分配送されている DATAGRAM の組み立て状態
///
/// dwnx の `recv_datagram` コールバックは、1 つの DATAGRAM を複数のチャンクに
/// 分割して通知することがある (dwnx の `lib/dwnx_conn.c` の
/// `DWNX_RECORD_READ_STATE_DATAGRAM_DATA` を参照)。チャンクは同じ DATAGRAM の
/// 先頭からの位置 `offset` と全体長 `dgramlen` を伴うため、接続ごとにこの
/// バッファへ連結して 1 つのイベントに組み立てる。
#[derive(Debug, Default)]
struct DatagramAssembly {
    /// 組み立て中のデータ
    data: Vec<u8>,
    /// 組み立て中の DATAGRAM の全体長
    ///
    /// 最初のチャンクで通知された `dgramlen` を保持し、以降のチャンクの
    /// `dgramlen` と一致することを検査する。
    dgramlen: usize,
}

thread_local! {
    /// 乱数源から乱数を得られなかったかどうか
    ///
    /// dwnx の `rand` コールバックは `user_data` を受け取らないため、失敗を
    /// 記録する場所としてスレッドローカルを使う。コールバックは
    /// [`Connection::new`] の中でのみ呼ばれる。
    static RAND_FAILED: Cell<bool> = const { Cell::new(false) };
}

/// 送信待ちのストリームデータ
#[derive(Debug)]
struct PendingWrite {
    /// ストリーム ID
    stream_id: i64,
    /// 送信するデータ
    data: Vec<u8>,
    /// 送信済みのバイト数
    sent: usize,
    /// FIN を送るかどうか
    fin: bool,
}

/// 送信待ちの DATAGRAM
#[derive(Debug)]
enum PendingDatagram {
    /// 送信するデータ
    Data(Vec<u8>),
    /// 送信できないことが確定している DATAGRAM
    ///
    /// フレームがローカルの Record 容量に収まらない場合に積む。データの代わりに
    /// 失敗の理由をキューへ積むことで、先に積んだ DATAGRAM の完了通知との順序を
    /// 保ったまま [`DatagramEvent::Failed`] を通知できる。
    Rejected(Error),
}

/// 1 つの Record に収められる DATAGRAM フレームの最大長
///
/// dwnx の Record の符号化器は、送信バッファの先頭 2 バイトを Record 長の
/// 可変長整数のために空ける (dwnx の `lib/dwnx_qre.c` の `dwnx_qre_start` と
/// `lib/dwnx_qre.h` の `DWNX_QRE_RECORDLEN_SIZE`)。そのためフレームに使えるのは
/// `dest.len() - 2` バイトである。dwnx は Record に収まらない DATAGRAM フレームの
/// `DWNX_ERR_NOBUF` を握りつぶすため (dwnx の `lib/dwnx_conn.c` の
/// `dwnx_conn_write_vmsg`)、dwnx-rs 側で事前に検査する必要がある。
const DWNX_QRE_RECORDLEN_SIZE: u64 = 2;

/// 送信待ち DATAGRAM の件数上限
///
/// [`Connection::send_datagram`] は送信待ちの DATAGRAM がこの数に達していると、
/// 新しい DATAGRAM を積まずに false を返す。DATAGRAM はフロー制御されないため
/// (RFC 9221 Section 5.1)、アプリケーションが送信完了を待たずに積み続けると
/// メモリを消費し続ける。件数の上限でそれを防ぐ。
pub const MAX_PENDING_DATAGRAMS: usize = 64;

/// 送信待ち DATAGRAM のペイロード合計バイト数の上限 (512 KiB)
///
/// 件数の上限だけでは、大きな DATAGRAM を積んだ場合にメモリ消費を抑えられない
/// ため、バイト数にも上限を設ける。フレームの符号化に必要なタイプと Length は
/// 含めず、アプリケーションが渡したペイロードの合計で数える。
///
/// 1 つの Record に収まる DATAGRAM のペイロードは最大 16377 バイトであり、
/// 件数の上限 64 件では 64 × 16377 = 1048128 バイトになる。1 MiB
/// (1048576 バイト) をバイト数の上限にすると、件数の上限に必ず先に達して
/// バイト数の上限が実効を持たないため、それを下回る 512 KiB にしている。
pub const MAX_PENDING_DATAGRAM_BYTES: usize = 512 * 1024;

/// 送信待ちの書き込みの結果
///
/// [`Connection::write`] で積んだ書き込みは [`Connection::poll_transmit`] の中で
/// dwnx へ渡される。完了と失敗は [`Connection::poll_write_event`] で取り出す。
#[derive(Debug)]
pub enum WriteEvent {
    /// 送信待ちのデータをすべて受け付けた
    Written {
        /// ストリーム ID
        stream_id: i64,
    },
    /// 書き込みを送信できなかった
    Failed {
        /// ストリーム ID
        stream_id: i64,
        /// 失敗の理由
        error: Error,
    },
}

/// 送信待ちの DATAGRAM の結果
///
/// [`Connection::send_datagram`] で積んだ DATAGRAM は
/// [`Connection::poll_transmit`] の中で dwnx へ渡される。収録と失敗は
/// [`Connection::poll_datagram_event`] で取り出す。ストリームの
/// [`WriteEvent`] と同じ設計である。
#[derive(Debug)]
pub enum DatagramEvent {
    /// dwnx が DATAGRAM を Record に収めた
    ///
    /// Record を書き出し終えたことは [`Connection::poll_transmit`] の呼び出し側が
    /// 下位トランスポートへの書き込みで確認する (ストリームの
    /// [`WriteEvent::Written`] と同じ扱い)。
    Accepted,
    /// DATAGRAM を送信できなかった
    ///
    /// ピアが DATAGRAM 拡張を広告していない場合 (`DWNX_ERR_INVALID_STATE`) と、
    /// フレームがピアの `max_datagram_frame_size` を超える場合
    /// (`DWNX_ERR_INVALID_ARGUMENT`) に発生する。どちらの場合もこの DATAGRAM
    /// だけが失敗し、接続は閉じない。それ以外のエラーでは接続全体を閉じるため、
    /// このイベントではなく [`Connection::poll_transmit`] の [`Error`] で通知する。
    Failed {
        /// 失敗の理由
        error: Error,
    },
}

/// dwnx への Record 書き込み 1 回分の結果
///
/// [`Connection::poll_transmit`] は 1 つの Record を完成させるまで
/// `dwnx_conn_write_*` を呼び続ける。その 1 回の呼び出しの結果を表す。
#[derive(Debug)]
enum WriteStep {
    /// Record が完成した (書き出したバイト数)
    Record(usize),
    /// 同じ Record に続けて書ける (`DWNX_ERR_WRITE_MORE`)
    Continue,
    /// 送るものが無い (戻り値 0)
    Idle,
}

/// dwnx 接続
///
/// dwnx の `dwnx_conn` を所有し、Sans I/O の操作を提供する。1 つの接続は
/// 単一のタスクからのみ操作すること。
pub struct Connection {
    /// dwnx の接続
    raw: *mut sys::dwnx_conn,
    /// コールバックが通知したイベントの保管先
    sink: Box<Sink>,
    /// Record を組み立てる作業バッファ
    dest: Vec<u8>,
    /// 送信待ちのストリームデータ
    pending_writes: VecDeque<PendingWrite>,
    /// 送信待ちの書き込みの結果
    write_events: VecDeque<WriteEvent>,
    /// 送信待ちの DATAGRAM
    ///
    /// 件数とペイロード合計バイト数には [`MAX_PENDING_DATAGRAMS`] と
    /// [`MAX_PENDING_DATAGRAM_BYTES`] の上限がある。
    pending_datagrams: VecDeque<PendingDatagram>,
    /// 送信待ちの DATAGRAM の結果
    datagram_events: VecDeque<DatagramEvent>,
    /// 次の Record でストリームより DATAGRAM を優先するかどうか
    ///
    /// ストリームと DATAGRAM の両方が送信待ちの間、Record を組み立てるたびに
    /// 入れ替えて、片方の完了通知が永遠に届かない飢餓状態を避ける。
    prefer_datagram: bool,
    /// 送信待ちの CONNECTION_CLOSE
    close_record: Option<Vec<u8>>,
    /// 接続が閉じたかどうか
    closed: bool,
}

// dwnx_conn はスレッド安全ではないが、このラッパーは接続を単一の所有者が
// 単一のタスクから操作することを前提とする。所有権ごと別スレッドへ移動することは
// 安全であるため Send を実装する。
unsafe impl Send for Connection {}

impl Connection {
    /// クライアントとして接続を作成する
    ///
    /// # エラー
    ///
    /// `config` が dwnx の要求を満たさない場合と (dwnx が `assert` でプロセスを
    /// abort させるため、[`TransportParameters::validate`] で事前に検査する)、
    /// dwnx が接続を作成できなかった場合に [`Error`] を返す。
    pub fn client(config: &TransportParameters) -> Result<Self, Error> {
        Self::new(true, config)
    }

    /// サーバーとして接続を作成する
    ///
    /// # エラー
    ///
    /// [`Connection::client`] と同じ。
    pub fn server(config: &TransportParameters) -> Result<Self, Error> {
        Self::new(false, config)
    }

    fn new(client: bool, config: &TransportParameters) -> Result<Self, Error> {
        // dwnx の assert でプロセスを abort させないよう、先に同じ検査を行う
        config.validate()?;

        let mut sink = Box::new(Sink::default());
        let user_data = (&mut *sink as *mut Sink).cast::<c_void>();

        let callbacks = callbacks();
        let mut settings = MaybeUninit::<sys::dwnx_settings>::uninit();
        let params = config.to_ffi();
        let mut raw: *mut sys::dwnx_conn = ptr::null_mut();

        // 前回の呼び出しの記録が残らないようにしてから接続を作る
        RAND_FAILED.with(|failed| failed.set(false));
        let rv = unsafe {
            sys::dwnx_settings_default(settings.as_mut_ptr());
            let settings = settings.assume_init();
            if client {
                sys::dwnx_conn_client_new(
                    &mut raw,
                    &callbacks,
                    &settings,
                    &params,
                    ptr::null(),
                    user_data,
                )
            } else {
                sys::dwnx_conn_server_new(
                    &mut raw,
                    &callbacks,
                    &settings,
                    &params,
                    ptr::null(),
                    user_data,
                )
            }
        };
        if rv != 0 {
            return Err(Error::from_code(rv));
        }

        // dwnx は接続の作成時に一度だけ乱数を要求する (dwnx の
        // lib/dwnx_conn.c の conn_new)。乱数源が使えない場合はゼロで埋められた
        // 種で接続が作られており、接続 ID が予測可能になるため、この接続は
        // 使わずにエラーを返す。
        if RAND_FAILED.with(|failed| failed.get()) {
            unsafe { sys::dwnx_conn_del(raw) };
            return Err(Error::invalid_argument(
                "the operating system random source is not available",
            ));
        }

        Ok(Self {
            raw,
            sink,
            dest: vec![0u8; sys::DWNX_DEFAULT_MAX_RECORD_SIZE as usize],
            pending_writes: VecDeque::new(),
            write_events: VecDeque::new(),
            pending_datagrams: VecDeque::new(),
            datagram_events: VecDeque::new(),
            prefer_datagram: false,
            close_record: None,
            closed: false,
        })
    }

    /// 受信したバイト列を処理する
    ///
    /// タイムスタンプはナノ秒単位である。
    ///
    /// ピアが CONNECTION_CLOSE を送った場合は、接続を閉じて
    /// [`Event::ConnectionClosed`] を通知し、[`Connection::is_closed`] が true に
    /// なる (アイドルタイムアウトと同じ扱い)。自側の close の後に届いたデータは
    /// 処理せずに捨てる (draft-ietf-quic-qmux-02 Section 7.2)。
    ///
    /// # エラー
    ///
    /// 上記以外で dwnx が Record を処理できなかった場合に [`Error`] を返す。
    pub fn read(&mut self, data: &[u8], now: u64) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }
        let rv = unsafe { sys::dwnx_conn_read(self.raw, data.as_ptr(), data.len(), now) };
        if rv == sys::DWNX_ERR_DRAINING {
            // ピアが CONNECTION_CLOSE を送った。以降は送信側の shutdown を待つ
            // だけなので、閉じたことを通知する。
            self.close_with_event();
            return Ok(());
        }
        if rv == sys::DWNX_ERR_CLOSING {
            // 自側の CONNECTION_CLOSE 送信後に届いたデータ。Record は dwnx が
            // 処理していないため、何もせず捨てる (Section 7.2)。
            return Ok(());
        }
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(())
    }

    /// ストリームへデータを書き込む (内部バッファへ積む)
    ///
    /// 実際の Record 生成は [`Connection::poll_transmit`] が行う。FIN を伴わない
    /// 空のデータは dwnx が何も送らないため (dwnx.h の
    /// `dwnx_conn_writev_stream` の説明を参照)、積まずに false を返す。積んだ
    /// 書き込みの完了と失敗は [`Connection::poll_write_event`] で通知する。
    pub fn write(&mut self, stream_id: i64, data: &[u8], fin: bool) -> bool {
        if data.is_empty() && !fin {
            return false;
        }
        self.pending_writes.push_back(PendingWrite {
            stream_id,
            data: data.to_vec(),
            sent: 0,
            fin,
        });
        true
    }

    /// 送信待ちの書き込みの結果を 1 つ取り出す
    ///
    /// ストリームごとの順序は [`Connection::write`] で積んだ順に保たれる。
    pub fn poll_write_event(&mut self) -> Option<WriteEvent> {
        self.write_events.pop_front()
    }

    /// DATAGRAM を送信する (内部バッファへ積む)
    ///
    /// 実際の Record 生成は [`Connection::poll_transmit`] が行う。空の DATAGRAM も
    /// 送信できる (RFC 9221 Section 4)。積んだ DATAGRAM の収録と失敗は
    /// [`Connection::poll_datagram_event`] で通知する。
    ///
    /// ピアが DATAGRAM 拡張を広告していない場合と、フレームがピアの
    /// `max_datagram_frame_size` を超える場合は、この時点ではエラーにせず
    /// [`DatagramEvent::Failed`] として通知する。dwnx が送信時に検査して
    /// `DWNX_ERR_INVALID_STATE` / `DWNX_ERR_INVALID_ARGUMENT` を返すためであり、
    /// どちらも接続は閉じない。
    ///
    /// フレーム (タイプと Length を含む) がローカルの Record 容量に収まらない
    /// 場合も [`DatagramEvent::Failed`] として通知する。この場合は dwnx が
    /// `DWNX_ERR_NOBUF` を握りつぶして何も返さないため、送信待ちに積まずに
    /// 失敗を確定させる。
    ///
    /// 戻り値は送信待ちのキューに積んだかどうかである。次の場合は積まずに false
    /// を返す。いずれもキューに積んでいないため、[`DatagramEvent`] は通知しない。
    ///
    /// - 接続が閉じている場合。[`Connection::close`] を呼んだ後も含む
    ///   (CONNECTION_CLOSE が優先されるため、積んだ DATAGRAM は送信されない)
    /// - 送信待ちの DATAGRAM が [`MAX_PENDING_DATAGRAMS`] 件に達している場合
    /// - 送信待ちの DATAGRAM のペイロード合計が [`MAX_PENDING_DATAGRAM_BYTES`]
    ///   を超える場合
    ///
    /// 上限を超えた DATAGRAM の破棄はアプリケーションの判断に委ねる。戻り値が
    /// false の場合は、送信待ちが減ってから積み直すこと。
    pub fn send_datagram(&mut self, data: &[u8]) -> bool {
        // close() は CONNECTION_CLOSE の Record を保持するだけで、送信するまで
        // closed を立てない。close 後に積んだ DATAGRAM は送信されないため、
        // close_record がある間も受け付けない。
        if self.closed || self.close_record.is_some() {
            return false;
        }

        // 送信待ちの件数とバイト数で上限を設ける。dwnx 側には送信待ちの上限が
        // 無いため、アプリケーションが完了通知を待たずに積み続けるとメモリを
        // 消費し続ける。上限に達したら積まずに false を返し、呼び出し側に
        // バックプレッシャーを伝える。
        if self.pending_datagrams.len() >= MAX_PENDING_DATAGRAMS
            || self.pending_datagram_bytes().saturating_add(data.len()) > MAX_PENDING_DATAGRAM_BYTES
        {
            return false;
        }

        // DATAGRAM フレームはタイプ (1 バイト) と Length (可変長整数)、ペイロード
        // からなる。フレーム全体が Record に収まらない場合は dwnx が
        // DWNX_ERR_NOBUF を握りつぶすため、成功も失敗も通知されず送信待ちが
        // 永久に残ってしまう。ここで事前に検査し、失敗を確定させる。
        let frame_len = datagram_frame_len(data.len());
        let capacity = (self.dest.len() as u64).saturating_sub(DWNX_QRE_RECORDLEN_SIZE);
        if frame_len > capacity {
            // 先に積んだ DATAGRAM との完了通知の順序を保つため、データの代わりに
            // 失敗をキューへ積む
            self.pending_datagrams
                .push_back(PendingDatagram::Rejected(Error::invalid_argument(
                    format!(
                        "datagram frame of {frame_len} bytes exceeds the record buffer capacity {capacity}"
                    ),
                )));
            return true;
        }

        self.pending_datagrams
            .push_back(PendingDatagram::Data(data.to_vec()));
        true
    }

    /// 送信待ち DATAGRAM のペイロード合計バイト数を返す
    ///
    /// 送信できないことが確定している [`PendingDatagram::Rejected`] はデータを
    /// 持たないため 0 バイトとして数える。件数は最大
    /// [`MAX_PENDING_DATAGRAMS`] 件であるため、毎回数え直しても十分に安い。
    fn pending_datagram_bytes(&self) -> usize {
        self.pending_datagrams
            .iter()
            .fold(0usize, |total, pending| {
                let len = match pending {
                    PendingDatagram::Data(data) => data.len(),
                    PendingDatagram::Rejected(_) => 0,
                };
                total.saturating_add(len)
            })
    }

    /// 送信待ちの DATAGRAM の結果を 1 つ取り出す
    ///
    /// [`Connection::send_datagram`] で積んだ順に取り出せる。
    pub fn poll_datagram_event(&mut self) -> Option<DatagramEvent> {
        self.datagram_events.pop_front()
    }

    /// 送信する Record を 1 つ `out` へ書き出す
    ///
    /// 書き出した場合は true を返す。
    ///
    /// # エラー
    ///
    /// dwnx が継続できないエラーを返した場合に [`Error`] を返す。dwnx.h の
    /// `dwnx_conn_writev_stream` / `dwnx_conn_writev_datagram` の契約では、
    /// それぞれが継続を許すエラー以外の負のエラーで接続を閉じることになっている。
    pub fn poll_transmit(&mut self, out: &mut Vec<u8>, now: u64) -> Result<bool, Error> {
        out.clear();

        // CONNECTION_CLOSE は他のフレームより優先して送る
        if let Some(record) = self.close_record.take() {
            out.extend_from_slice(&record);
            self.closed = true;
            return Ok(true);
        }
        if self.closed {
            return Ok(false);
        }

        loop {
            // ストリームと DATAGRAM の両方が送信待ちの間は、Record を組み立てる
            // たびに優先する側を入れ替える。dwnx はどちらも同じ Record に詰められる
            // が、片方だけを優先し続けると、アプリケーションが継続的に書き込んで
            // いる間もう片方の完了通知が永遠に届かなくなる。
            let has_stream = !self.pending_writes.is_empty();
            let has_datagram = !self.pending_datagrams.is_empty();
            let write_datagram = has_datagram && (!has_stream || self.prefer_datagram);
            if has_stream && has_datagram {
                self.prefer_datagram = !self.prefer_datagram;
            }

            let step = if write_datagram {
                self.write_datagram(now)?
            } else {
                self.write_stream(now)?
            };

            match step {
                WriteStep::Record(len) => {
                    out.extend_from_slice(&self.dest[..len]);
                    return Ok(true);
                }
                // DWNX_ERR_WRITE_MORE は同じ Record に続けて書けることを表す
                WriteStep::Continue => continue,
                WriteStep::Idle => {
                    // 選んだ側に送るものが無かった。もう片方が送信待ちなら試す。
                    // 例えばフロー制御でブロックされたストリームがある間に
                    // DATAGRAM を積んだ場合、ストリーム側の Idle で止まると
                    // 次の起床まで DATAGRAM が送信されない。
                    let other = if write_datagram {
                        self.write_stream(now)?
                    } else {
                        self.write_datagram(now)?
                    };
                    match other {
                        WriteStep::Record(len) => {
                            out.extend_from_slice(&self.dest[..len]);
                            return Ok(true);
                        }
                        WriteStep::Continue => continue,
                        // どちらも送るものが無い
                        WriteStep::Idle => return Ok(false),
                    }
                }
            }
        }
    }

    /// dwnx へストリームデータを書き込む 1 ステップ
    ///
    /// 送信待ちのストリームデータを先頭から順に試す。先頭のストリームがフロー
    /// 制御でブロックされていても、後続のストリームや制御フレームが飢餓に
    /// ならないようにするためである。同じストリームの送信順序は
    /// `pending_writes` の順序で保たれる。
    ///
    /// この Record に書けるストリームデータが無い場合は、
    /// `dwnx_conn_write_record` で開始済みの Record を確定する。dwnx.h の
    /// 説明のとおり、`DWNX_ERR_WRITE_MORE` / `DWNX_ERR_STREAM_DATA_BLOCKED` の
    /// 後はこの関数で Record を確定できる。開始済みの Record を確定せずに
    /// 戻ると、同じ Record に収めた DATAGRAM が送信されないまま残る。
    fn write_stream(&mut self, now: u64) -> Result<WriteStep, Error> {
        // この Record で進捗できなかったストリーム ID
        //
        // 同じストリームの後続の書き込みを先に送ると順序が入れ替わるため、
        // 進捗できなかったストリームは今回の Record では試さない。
        let mut blocked: Vec<i64> = Vec::new();

        let mut index = 0;
        while index < self.pending_writes.len() {
            let stream_id = self.pending_writes[index].stream_id;
            if blocked.contains(&stream_id) {
                index += 1;
                continue;
            }

            let (data_ptr, data_len, flags) = {
                let write = &self.pending_writes[index];
                let remaining = &write.data[write.sent..];
                let flags = if write.fin {
                    sys::DWNX_WRITE_STREAM_FLAG_FIN
                } else {
                    sys::DWNX_WRITE_STREAM_FLAG_NONE
                };
                (remaining.as_ptr(), remaining.len(), flags)
            };

            let dest_ptr = self.dest.as_mut_ptr();
            let dest_len = self.dest.len();
            let mut pdatalen: sys::dwnx_ssize = -1;
            let rv = unsafe {
                sys::dwnx_conn_write_stream(
                    self.raw,
                    dest_ptr,
                    dest_len,
                    &mut pdatalen,
                    flags,
                    stream_id,
                    data_ptr,
                    data_len,
                    now,
                )
            };

            if rv >= 0 {
                if rv == 0 {
                    // 送るものが無い。制御フレームの Record も無い
                    break;
                }
                self.finish_write(index, pdatalen);
                return Ok(WriteStep::Record(rv as usize));
            }

            match rv as i32 {
                sys::DWNX_ERR_WRITE_MORE => {
                    let accepted = pdatalen.max(0) as usize;
                    // STREAM フレームが 1 バイトも書けていない場合は、この
                    // ストリームを今回の Record では後回しにして次を試す
                    if accepted == 0 && data_len > 0 {
                        blocked.push(stream_id);
                        index += 1;
                        continue;
                    }
                    self.finish_write(index, pdatalen);
                    return Ok(WriteStep::Continue);
                }
                sys::DWNX_ERR_STREAM_DATA_BLOCKED => {
                    // フロー制御で送れない。後続のストリームを試す
                    blocked.push(stream_id);
                    index += 1;
                    continue;
                }
                code @ sys::DWNX_ERR_STREAM_SHUT_WR => {
                    // 送信側が閉じているため、この書き込みは送れない。dwnx は
                    // 継続してよいエラーとして扱うため、失敗を通知して次へ進む。
                    self.fail_write(index, code);
                    continue;
                }
                sys::DWNX_ERR_STREAM_NOT_FOUND => {
                    // ストリームが存在しないため、この書き込みは送れない。dwnx.h
                    // の契約では継続できないエラーであり、接続を閉じる必要がある。
                    self.fail_write(index, sys::DWNX_ERR_STREAM_NOT_FOUND);
                    return Err(Error::from_code(sys::DWNX_ERR_STREAM_NOT_FOUND));
                }
                other => return Err(Error::from_code(other)),
            }
        }

        // ストリームデータを書けない場合でも、開始済みの Record を確定するか、
        // 制御フレームだけの Record を組み立てる。
        let rv = unsafe {
            sys::dwnx_conn_write_record(self.raw, self.dest.as_mut_ptr(), self.dest.len(), now)
        };
        if rv >= 0 {
            if rv == 0 {
                Ok(WriteStep::Idle)
            } else {
                Ok(WriteStep::Record(rv as usize))
            }
        } else {
            Err(Error::from_code(rv as i32))
        }
    }

    /// dwnx へ DATAGRAM を書き込む 1 ステップ
    ///
    /// 送信待ちの DATAGRAM を 1 つ書き込む。dwnx.h の
    /// `dwnx_conn_writev_datagram` の契約に従い、戻り値と `paccepted` の
    /// 組み合わせで後続の処理を決める。
    fn write_datagram(&mut self, now: u64) -> Result<WriteStep, Error> {
        // 送信できないことが確定している DATAGRAM は、データを送らずに失敗を
        // 通知する。先に積んだ DATAGRAM の完了通知との順序はキューの順序で保つ。
        if matches!(
            self.pending_datagrams.front(),
            Some(PendingDatagram::Rejected(_))
        ) {
            if let Some(PendingDatagram::Rejected(error)) = self.pending_datagrams.pop_front() {
                self.datagram_events
                    .push_back(DatagramEvent::Failed { error });
            }
            return Ok(WriteStep::Continue);
        }

        let Some(PendingDatagram::Data(data)) = self.pending_datagrams.front() else {
            // 送信待ちの DATAGRAM が無い場合は制御フレームだけの Record を組み立てる
            return self.write_stream(now);
        };
        let (data_ptr, data_len) = (data.as_ptr(), data.len());

        let mut paccepted: core::ffi::c_int = 0;
        let rv = unsafe {
            sys::dwnx_conn_write_datagram(
                self.raw,
                self.dest.as_mut_ptr(),
                self.dest.len(),
                &mut paccepted,
                sys::DWNX_WRITE_DATAGRAM_FLAG_NONE,
                data_ptr,
                data_len,
                now,
            )
        };

        if rv >= 0 {
            if rv == 0 {
                return Ok(WriteStep::Idle);
            }
            // 戻り値が正でも accepted が 0 の場合は、他のフレームが Record を
            // 占めており DATAGRAM がまだ書かれていない。キューに残して次の
            // Record で再試行する (dwnx.h の `dwnx_conn_writev_datagram` を参照)。
            if paccepted == 0 {
                return Ok(WriteStep::Record(rv as usize));
            }
            self.pending_datagrams.pop_front();
            self.datagram_events.push_back(DatagramEvent::Accepted);
            return Ok(WriteStep::Record(rv as usize));
        }

        match rv as i32 {
            sys::DWNX_ERR_WRITE_MORE => {
                if paccepted == 0 {
                    // dwnx は DATAGRAM を符号化できた場合にだけ WRITE_MORE を
                    // 返す (dwnx の lib/dwnx_conn.c の dwnx_conn_write_vmsg)。
                    // そのためここには到達しないが、無限ループを避けるための
                    // 進捗ガードを置く。
                    return Ok(WriteStep::Idle);
                }
                self.pending_datagrams.pop_front();
                self.datagram_events.push_back(DatagramEvent::Accepted);
                Ok(WriteStep::Continue)
            }
            // ピアが DATAGRAM 拡張を広告していない、または DATAGRAM がピアの
            // max_datagram_frame_size を超える。どちらもこの DATAGRAM の失敗と
            // して通知し、接続は閉じない (dwnx.h の
            // `dwnx_conn_writev_datagram` の契約)。
            code @ (sys::DWNX_ERR_INVALID_STATE | sys::DWNX_ERR_INVALID_ARGUMENT) => {
                self.pending_datagrams.pop_front();
                self.datagram_events.push_back(DatagramEvent::Failed {
                    error: Error::from_code(code),
                });
                Ok(WriteStep::Continue)
            }
            // それ以外の負のエラーでは接続を閉じる。この DATAGRAM の失敗は
            // 通知せず (接続の終了は呼び出し側が poll_transmit のエラーで知る)、
            // キューからも取り除く。
            other => {
                self.pending_datagrams.pop_front();
                Err(Error::from_code(other))
            }
        }
    }

    /// 送信の進捗を反映し、送り終えたエントリを捨てる
    ///
    /// dwnx は渡したデータをすべて STREAM フレームに収めた場合にのみ FIN を
    /// 立てるため、すべて受け付けた時点でその書き込みは完了である。
    fn finish_write(&mut self, index: usize, pdatalen: sys::dwnx_ssize) {
        let Some(write) = self.pending_writes.get_mut(index) else {
            return;
        };
        let accepted = pdatalen.max(0) as usize;
        write.sent = (write.sent + accepted).min(write.data.len());
        if write.sent >= write.data.len() {
            let stream_id = write.stream_id;
            self.pending_writes.remove(index);
            self.write_events
                .push_back(WriteEvent::Written { stream_id });
        }
    }

    /// 送信できなかった書き込みを捨てて失敗を通知する
    fn fail_write(&mut self, index: usize, code: i32) {
        let Some(write) = self.pending_writes.remove(index) else {
            return;
        };
        self.write_events.push_back(WriteEvent::Failed {
            stream_id: write.stream_id,
            error: Error::from_code(code),
        });
    }

    /// イベントを 1 つ取り出す
    pub fn poll_event(&mut self) -> Option<Event> {
        self.sink.events.borrow_mut().pop_front()
    }

    /// 双方向ストリームを開く
    pub fn open_bidi(&mut self) -> Result<i64, Error> {
        let mut stream_id = -1i64;
        let rv =
            unsafe { sys::dwnx_conn_open_bidi_stream(self.raw, &mut stream_id, ptr::null_mut()) };
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(stream_id)
    }

    /// 単方向ストリームを開く
    pub fn open_uni(&mut self) -> Result<i64, Error> {
        let mut stream_id = -1i64;
        let rv =
            unsafe { sys::dwnx_conn_open_uni_stream(self.raw, &mut stream_id, ptr::null_mut()) };
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(stream_id)
    }

    /// ストリームの送信側をリセットする (RESET_STREAM を送る)
    pub fn shutdown_write(&mut self, stream_id: i64, error_code: u64) -> Result<(), Error> {
        let rv =
            unsafe { sys::dwnx_conn_shutdown_stream_write(self.raw, 0, stream_id, error_code) };
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(())
    }

    /// ストリームの受信側に送信停止を要求する (STOP_SENDING を送る)
    ///
    /// ピアは [`Event::RecvStopSending`] として観測する。
    pub fn shutdown_read(&mut self, stream_id: i64, error_code: u64) -> Result<(), Error> {
        let rv = unsafe { sys::dwnx_conn_shutdown_stream_read(self.raw, 0, stream_id, error_code) };
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(())
    }

    /// ストリームの受信ウィンドウを広げる
    pub fn extend_max_stream_offset(&mut self, stream_id: i64, amount: u64) -> Result<(), Error> {
        let rv = unsafe { sys::dwnx_conn_extend_max_stream_offset(self.raw, stream_id, amount) };
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(())
    }

    /// 接続レベルの受信ウィンドウを広げる
    pub fn extend_max_offset(&mut self, amount: u64) {
        unsafe { sys::dwnx_conn_extend_max_offset(self.raw, amount) };
    }

    /// 接続を閉じる (CONNECTION_CLOSE を生成する)
    ///
    /// 理由句が Record に収まらない場合は、UTF-8 の文字境界を壊さない範囲で
    /// 切り詰める。生成した Record は次の [`Connection::poll_transmit`] が
    /// 書き出す。
    ///
    /// # エラー
    ///
    /// CONNECTION_CLOSE を生成できなかった場合に [`Error`] を返す。dwnx が既に
    /// closing / draining 状態の場合は、送るものが無いため `Ok(())` を返して
    /// [`Connection::is_closed`] が true になる。
    pub fn close(&mut self, error_code: u64, reason: &[u8]) -> Result<(), Error> {
        let mut ccerr = MaybeUninit::<sys::dwnx_ccerr>::uninit();
        let mut record = vec![0u8; self.dest.len()];
        // 時刻は dwnx が知っている最新値を使う。dwnx は受け取るタイムスタンプが
        // 単調増加であることを assert で検査するため (dwnx の lib/dwnx_conn.c の
        // conn_update_timestamp)、ここで独自の時刻を作らない。
        let now = unsafe { sys::dwnx_conn_get_timestamp(self.raw) };
        // 理由句の長さは可変長整数で符号化されるため、Record に収まらない場合は
        // DWNX_ERR_NOBUF が返る。半分ずつ縮めながら UTF-8 の文字境界に合わせる。
        let mut reason_len = reason.len();
        loop {
            let rv = unsafe {
                sys::dwnx_ccerr_default(ccerr.as_mut_ptr());
                let mut ccerr = ccerr.assume_init();
                sys::dwnx_ccerr_set_application_error(
                    &mut ccerr,
                    error_code,
                    reason.as_ptr(),
                    reason_len,
                );
                sys::dwnx_conn_write_connection_close(
                    self.raw,
                    record.as_mut_ptr(),
                    record.len(),
                    &ccerr,
                    now,
                )
            };
            if rv > 0 {
                record.truncate(rv as usize);
                self.close_record = Some(record);
                return Ok(());
            }
            match rv as i32 {
                // 接続は既に closing / draining 状態で、送るべき
                // CONNECTION_CLOSE が無い。下位トランスポートを閉じるのは
                // 呼び出し側の責務である。
                0 => {
                    self.closed = true;
                    return Ok(());
                }
                sys::DWNX_ERR_NOBUF if reason_len > 0 => {
                    reason_len = floor_char_boundary(reason, reason_len / 2);
                }
                other => return Err(Error::from_code(other)),
            }
        }
    }

    /// 下位トランスポートが閉じたことを通知する
    ///
    /// 通知は 1 回だけ行う。
    pub fn transport_closed(&mut self) {
        if self.closed {
            return;
        }
        self.close_with_event();
    }

    /// 接続を閉じて [`Event::ConnectionClosed`] を通知する
    fn close_with_event(&mut self) {
        self.closed = true;
        self.sink
            .events
            .borrow_mut()
            .push_back(Event::ConnectionClosed);
    }

    /// 接続が閉じたかどうか
    ///
    /// アイドルタイムアウトの満了、下位トランスポートの終了、CONNECTION_CLOSE の
    /// 送信のいずれかで true になる。
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// 次のタイマー時刻 (ナノ秒) を返す
    ///
    /// タイマーが無い場合は `None` を返す。
    pub fn next_timeout(&self) -> Option<u64> {
        let expiry = unsafe { sys::dwnx_conn_get_expiry(self.raw) };
        if expiry == u64::MAX {
            None
        } else {
            Some(expiry)
        }
    }

    /// タイマーを処理する
    ///
    /// アイドルタイムアウトが満了した場合は、接続を閉じて
    /// [`Event::ConnectionClosed`] を通知する。
    pub fn handle_timeout(&mut self, now: u64) -> Result<(), Error> {
        let rv = unsafe { sys::dwnx_conn_handle_expiry(self.raw, now) };
        if rv == sys::DWNX_ERR_IDLE_CLOSE {
            self.close_with_event();
            return Ok(());
        }
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe { sys::dwnx_conn_del(self.raw) };
    }
}

/// UTF-8 の文字境界を壊さない位置まで長さを縮める
///
/// 縮めた位置のバイトが UTF-8 の継続バイト (0b10xxxxxx) である間だけ手前へ
/// 戻す。理由句が UTF-8 として不正なバイト列の場合も、バイト列として安全な
/// 位置で切る。
fn floor_char_boundary(bytes: &[u8], len: usize) -> usize {
    let mut len = len.min(bytes.len());
    while len > 0 && len < bytes.len() && (bytes[len] & 0xC0) == 0x80 {
        len -= 1;
    }
    len
}

/// DATAGRAM フレーム全体のバイト数を返す
///
/// フレームはタイプ (1 バイト。dwnx は常に Length 付きの
/// `DWNX_FRAME_DATAGRAM_LEN` を使う) と Length (可変長整数)、ペイロードからなる
/// (RFC 9221 Section 4、dwnx の `lib/dwnx_frame.c` の
/// `dwnx_frame_datagram_framelen`)。
fn datagram_frame_len(payload_len: usize) -> u64 {
    let payload_len = payload_len as u64;
    1u64.saturating_add(varint_len(payload_len))
        .saturating_add(payload_len)
}

/// 可変長整数を符号化したときのバイト数を返す
///
/// RFC 9000 Section 16 の可変長整数は先頭 2 ビットで長さが決まる。
fn varint_len(value: u64) -> u64 {
    if value < 64 {
        1
    } else if value < 16384 {
        2
    } else if value < 1 << 30 {
        4
    } else {
        8
    }
}

/// dwnx のコールバック一式を組み立てる
///
/// dwnx.h の説明のとおり `rand` 以外のコールバックは省略できる。使わない
/// コールバックは `None` にして、到達しない no-op を持たないようにする。
fn callbacks() -> sys::dwnx_callbacks {
    sys::dwnx_callbacks {
        rand: Some(rand),
        recv_transport_params: Some(recv_transport_params),
        recv_stream_data: Some(recv_stream_data),
        stream_open: Some(stream_open),
        stream_close: Some(stream_close),
        stream_reset: Some(stream_reset),
        // 送信側を自分から閉じたときの通知。状態は dwnx が管理しており、
        // アプリケーションへ渡す情報が無いため受け取らない。
        stream_stop_sending: None,
        recv_stop_sending: Some(recv_stop_sending),
        // MAX_STREAM_DATA / MAX_STREAMS は dwnx の送信処理が吸収するため、
        // アプリケーションへ渡す必要が無い。
        extend_max_stream_data: None,
        extend_max_local_streams_bidi: None,
        extend_max_local_streams_uni: None,
        extend_max_remote_streams_bidi: None,
        extend_max_remote_streams_uni: None,
        // 書き込みの完了は WriteEvent で通知する。
        write_stream_data_offset: None,
        // DATAGRAM の受信は Event::Datagram で通知する。
        recv_datagram: Some(recv_datagram),
    }
}

/// user_data からイベント保管先を取り出す
unsafe fn sink<'a>(user_data: *mut c_void) -> &'a Sink {
    unsafe { &*(user_data as *const Sink) }
}

/// 乱数源 (dwnx が予測不能なデータを必要としたときに呼ばれる)
///
/// dwnx は接続の作成時に一度だけ乱数を要求する (dwnx の lib/dwnx_conn.c の
/// `conn_new`)。コールバックの戻り値ではエラーを伝えられないため、乱数が
/// 得られなかった場合はゼロで埋めたうえで [`RAND_FAILED`] に記録し、
/// [`Connection::new`] が接続を使わずに [`Error`] を返す。
unsafe extern "C" fn rand(dest: *mut u8, destlen: usize) {
    if destlen == 0 {
        return;
    }
    let buf = unsafe { core::slice::from_raw_parts_mut(dest, destlen) };
    if getrandom::fill(buf).is_err() {
        buf.fill(0);
        RAND_FAILED.with(|failed| failed.set(true));
    }
}

unsafe extern "C" fn recv_transport_params(
    _conn: *mut sys::dwnx_conn,
    params: *const sys::dwnx_transport_params,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    let params = unsafe { TransportParameters::from_ffi(&*params) };
    unsafe { sink(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::TransportParametersReceived(params));
    0
}

unsafe extern "C" fn recv_stream_data(
    _conn: *mut sys::dwnx_conn,
    flags: u32,
    stream_id: i64,
    offset: u64,
    data: *const u8,
    datalen: usize,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> core::ffi::c_int {
    let data = if datalen == 0 {
        Vec::new()
    } else {
        unsafe { core::slice::from_raw_parts(data, datalen) }.to_vec()
    };
    let fin = flags & sys::DWNX_STREAM_DATA_FLAG_FIN != 0;
    unsafe { sink(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::StreamData {
            id: stream_id,
            offset,
            fin,
            data,
        });
    0
}

/// DATAGRAM の受信 (全体または部分)
///
/// dwnx は 1 つの DATAGRAM を複数のチャンクに分割して通知することがある
/// (入力バイト列がフレームの途中で区切られた場合)。チャンクを連結して全体を
/// 組み立てられた時点で [`Event::Datagram`] を 1 つ積む。
///
/// `fin` はこのチャンクが DATAGRAM の最後かどうか、`dgramlen` は DATAGRAM 全体の
/// 長さ、`offset` は DATAGRAM の先頭からのこのチャンクの位置である
/// (dwnx.h の `dwnx_recv_datagram` を参照)。
unsafe extern "C" fn recv_datagram(
    _conn: *mut sys::dwnx_conn,
    fin: core::ffi::c_int,
    dgramlen: usize,
    offset: usize,
    data: *const u8,
    datalen: usize,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    let sink = unsafe { sink(user_data) };
    let mut assembly = sink.datagram.borrow_mut();

    if offset == 0 {
        // 新しい DATAGRAM の先頭である。前回の組み立てが残っていれば捨てる
        assembly.data.clear();
        assembly.dgramlen = dgramlen;
    } else if offset != assembly.data.len() || dgramlen != assembly.dgramlen {
        // dwnx 自身がフレームの読み取り位置を管理しており、offset は
        // dgramlen - field_left として求まる (dwnx の lib/dwnx_conn.c の
        // DWNX_RECORD_READ_STATE_DATAGRAM_DATA)。そのため offset の連続性と
        // dgramlen の一致は dwnx が保証しており、ここに来るのは内部状態が
        // 壊れた場合だけである。壊れたデータをアプリケーションへ渡すより、
        // この DATAGRAM を捨てる方が安全である。コールバックの失敗
        // (DWNX_ERR_CALLBACK_FAILURE による接続の close) にしないのは、
        // フレーム自体は dwnx が検査済みであり、過剰なためである。
        assembly.data.clear();
        return 0;
    }

    if datalen > 0 {
        let chunk = unsafe { core::slice::from_raw_parts(data, datalen) };
        assembly.data.extend_from_slice(chunk);
    }

    if fin != 0 {
        // 全体長と組み立てた長さが一致した場合だけ 1 つの DATAGRAM として通知する。
        // ゼロ長 DATAGRAM は data が空で dgramlen も 0 のため、この検査で通る。
        let complete = assembly.data.len() == assembly.dgramlen;
        let data = core::mem::take(&mut assembly.data);
        if complete {
            sink.events.borrow_mut().push_back(Event::Datagram { data });
        }
    }

    0
}

unsafe extern "C" fn stream_open(
    _conn: *mut sys::dwnx_conn,
    stream_id: i64,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    unsafe { sink(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::StreamOpened { id: stream_id });
    0
}

unsafe extern "C" fn stream_close(
    _conn: *mut sys::dwnx_conn,
    flags: u32,
    stream_id: i64,
    rx_app_error_code: u64,
    tx_app_error_code: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> core::ffi::c_int {
    // dwnx.h の dwnx_stream_close の説明にあるとおり、フラグのビットで
    // エラーコードが設定されているかどうかを表す
    const RX_SET: u32 = 0x01;
    const TX_SET: u32 = 0x02;
    unsafe { sink(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::StreamClosed {
            id: stream_id,
            rx_error_code: (flags & RX_SET != 0).then_some(rx_app_error_code),
            tx_error_code: (flags & TX_SET != 0).then_some(tx_app_error_code),
        });
    0
}

unsafe extern "C" fn stream_reset(
    _conn: *mut sys::dwnx_conn,
    stream_id: i64,
    final_size: u64,
    app_error_code: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> core::ffi::c_int {
    unsafe { sink(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::StreamReset {
            id: stream_id,
            final_size,
            error_code: app_error_code,
        });
    0
}

unsafe extern "C" fn recv_stop_sending(
    _conn: *mut sys::dwnx_conn,
    stream_id: i64,
    app_error_code: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> core::ffi::c_int {
    unsafe { sink(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::RecvStopSending {
            id: stream_id,
            error_code: app_error_code,
        });
    0
}
