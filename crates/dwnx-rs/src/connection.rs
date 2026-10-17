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

    /// 送信する Record を 1 つ `out` へ書き出す
    ///
    /// 書き出した場合は true を返す。
    ///
    /// # エラー
    ///
    /// dwnx が継続できないエラーを返した場合に [`Error`] を返す。dwnx.h の
    /// `dwnx_conn_writev_stream` の契約では、`DWNX_ERR_WRITE_MORE` /
    /// `DWNX_ERR_STREAM_DATA_BLOCKED` / `DWNX_ERR_STREAM_SHUT_WR` 以外の負の
    /// エラーで接続を閉じることになっている。
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
            let front = self.pending_writes.front();
            let (stream_id, data_ptr, data_len, flags) = match front {
                Some(write) => {
                    let remaining = &write.data[write.sent..];
                    let flags = if write.fin {
                        sys::DWNX_WRITE_STREAM_FLAG_FIN
                    } else {
                        sys::DWNX_WRITE_STREAM_FLAG_NONE
                    };
                    (write.stream_id, remaining.as_ptr(), remaining.len(), flags)
                }
                None => (-1i64, ptr::null(), 0usize, sys::DWNX_WRITE_STREAM_FLAG_NONE),
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
                    return Ok(false);
                }
                if stream_id != -1 {
                    self.finish_write(stream_id, pdatalen);
                }
                out.extend_from_slice(&self.dest[..rv as usize]);
                return Ok(true);
            }

            match rv as i32 {
                sys::DWNX_ERR_WRITE_MORE => {
                    let accepted = pdatalen.max(0) as usize;
                    // STREAM フレームが 1 バイトも書けていない場合は無限ループを避ける
                    if stream_id != -1 && accepted == 0 && data_len > 0 {
                        return Ok(false);
                    }
                    if stream_id != -1 {
                        self.finish_write(stream_id, pdatalen);
                    }
                    continue;
                }
                sys::DWNX_ERR_STREAM_DATA_BLOCKED => return Ok(false),
                code @ sys::DWNX_ERR_STREAM_SHUT_WR => {
                    // 送信側が閉じているため、この書き込みは送れない。dwnx は
                    // 継続してよいエラーとして扱うため、失敗を通知して次へ進む。
                    self.fail_write(code);
                    continue;
                }
                sys::DWNX_ERR_STREAM_NOT_FOUND => {
                    // ストリームが存在しないため、この書き込みは送れない。dwnx.h の
                    // 契約では継続できないエラーであり、接続を閉じる必要がある。
                    self.fail_write(sys::DWNX_ERR_STREAM_NOT_FOUND);
                    return Err(Error::from_code(sys::DWNX_ERR_STREAM_NOT_FOUND));
                }
                other => return Err(Error::from_code(other)),
            }
        }
    }

    /// 送信の進捗を反映し、送り終えたエントリを捨てる
    ///
    /// dwnx は渡したデータをすべて STREAM フレームに収めた場合にのみ FIN を
    /// 立てるため、すべて受け付けた時点でその書き込みは完了である。
    fn finish_write(&mut self, stream_id: i64, pdatalen: sys::dwnx_ssize) {
        let Some(write) = self.pending_writes.front_mut() else {
            return;
        };
        if write.stream_id != stream_id {
            return;
        }
        let accepted = pdatalen.max(0) as usize;
        write.sent = (write.sent + accepted).min(write.data.len());
        if write.sent >= write.data.len() {
            self.pending_writes.pop_front();
            self.write_events
                .push_back(WriteEvent::Written { stream_id });
        }
    }

    /// 送信できなかった書き込みを捨てて失敗を通知する
    fn fail_write(&mut self, code: i32) {
        let Some(write) = self.pending_writes.pop_front() else {
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
