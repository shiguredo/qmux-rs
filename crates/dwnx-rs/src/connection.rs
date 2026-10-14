//! dwnx 接続の安全な Sans I/O ラッパー

use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::ptr;

use dwnx_sys as sys;

use crate::config::TransportParameters;
use crate::event::Event;

/// コールバックが通知するイベントの保管先
#[derive(Debug, Default)]
struct Sink {
    events: RefCell<VecDeque<Event>>,
}

/// dwnx のライブラリエラー
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    /// dwnx のエラーコード
    pub code: i32,
    /// `dwnx_strerror` が返す説明
    pub message: String,
}

impl Error {
    fn from_code(code: i32) -> Self {
        let message = unsafe {
            let ptr = sys::dwnx_strerror(code);
            if ptr.is_null() {
                std::borrow::Cow::Borrowed("unknown dwnx error")
            } else {
                std::ffi::CStr::from_ptr(ptr).to_string_lossy()
            }
        };
        Self {
            code,
            message: message.into_owned(),
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dwnx error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

/// 送信待ちのストリームデータ
#[derive(Debug)]
struct PendingWrite {
    stream_id: i64,
    data: Vec<u8>,
    sent: usize,
    fin: bool,
}

/// dwnx 接続
///
/// dwnx の `dwnx_conn` を所有し、Sans I/O の操作を提供する。1 つの接続は
/// 単一のタスクからのみ操作すること。
pub struct Connection {
    raw: *mut sys::dwnx_conn,
    sink: Box<Sink>,
    dest: Vec<u8>,
    pending_writes: VecDeque<PendingWrite>,
    close_record: Option<Vec<u8>>,
    closed: bool,
}

// dwnx_conn はスレッド安全ではないが、このラッパーは接続を単一の所有者が
// 単一のタスクから操作することを前提とする。所有権ごと別スレッドへ移動することは
// 安全であるため Send を実装する。
unsafe impl Send for Connection {}

impl Connection {
    /// クライアントとして接続を作成する
    pub fn client(config: &TransportParameters) -> Result<Self, Error> {
        Self::new(true, config)
    }

    /// サーバーとして接続を作成する
    pub fn server(config: &TransportParameters) -> Result<Self, Error> {
        Self::new(false, config)
    }

    fn new(client: bool, config: &TransportParameters) -> Result<Self, Error> {
        let mut sink = Box::new(Sink::default());
        let user_data = (&mut *sink as *mut Sink).cast::<c_void>();

        let callbacks = callbacks();
        let mut settings = MaybeUninit::<sys::dwnx_settings>::uninit();
        let params = config.to_ffi();
        let mut raw: *mut sys::dwnx_conn = ptr::null_mut();

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

        Ok(Self {
            raw,
            sink,
            dest: vec![0u8; sys::DWNX_DEFAULT_MAX_RECORD_SIZE as usize],
            pending_writes: VecDeque::new(),
            close_record: None,
            closed: false,
        })
    }

    /// 受信したバイト列を処理する
    ///
    /// タイムスタンプはナノ秒単位である。
    pub fn read(&mut self, data: &[u8], now: u64) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }
        let rv = unsafe { sys::dwnx_conn_read(self.raw, data.as_ptr(), data.len(), now) };
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(())
    }

    /// ストリームへデータを書き込む (内部バッファへ積む)
    ///
    /// 実際の Record 生成は [`Connection::poll_transmit`] が行う。
    pub fn write(&mut self, stream_id: i64, data: &[u8], fin: bool) {
        if data.is_empty() && !fin {
            return;
        }
        self.pending_writes.push_back(PendingWrite {
            stream_id,
            data: data.to_vec(),
            sent: 0,
            fin,
        });
    }

    /// 送信する Record を 1 つ `out` へ書き出す
    ///
    /// 書き出した場合は true を返す。
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

            let code = rv as i32;
            match code {
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
                sys::DWNX_ERR_STREAM_NOT_FOUND | sys::DWNX_ERR_STREAM_SHUT_WR => {
                    self.pending_writes.pop_front();
                    continue;
                }
                other => return Err(Error::from_code(other)),
            }
        }
    }

    /// 送信の進捗を反映し、送り終えたエントリを捨てる
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
        }
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

    /// ストリームの送信側をリセットする
    pub fn shutdown_write(&mut self, stream_id: i64, error_code: u64) -> Result<(), Error> {
        let rv =
            unsafe { sys::dwnx_conn_shutdown_stream_write(self.raw, 0, stream_id, error_code) };
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
    pub fn close(&mut self, error_code: u64, reason: &[u8]) {
        let mut ccerr = MaybeUninit::<sys::dwnx_ccerr>::uninit();
        let mut record = vec![0u8; self.dest.len()];
        let rv = unsafe {
            sys::dwnx_ccerr_default(ccerr.as_mut_ptr());
            let mut ccerr = ccerr.assume_init();
            sys::dwnx_ccerr_set_application_error(
                &mut ccerr,
                error_code,
                reason.as_ptr(),
                reason.len(),
            );
            sys::dwnx_conn_write_connection_close(
                self.raw,
                record.as_mut_ptr(),
                record.len(),
                &ccerr,
                0,
            )
        };
        if rv > 0 {
            record.truncate(rv as usize);
            self.close_record = Some(record);
        } else {
            self.closed = true;
        }
    }

    /// 下位トランスポートが閉じたことを通知する
    pub fn transport_closed(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.sink
            .events
            .borrow_mut()
            .push_back(Event::ConnectionClosed);
    }

    /// 接続が閉じたかどうか
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// 次のタイマー時刻 (ナノ秒) を返す
    pub fn next_timeout(&self) -> Option<u64> {
        let expiry = unsafe { sys::dwnx_conn_get_expiry(self.raw) };
        if expiry == u64::MAX {
            None
        } else {
            Some(expiry)
        }
    }

    /// タイマーを処理する
    pub fn handle_timeout(&mut self, now: u64) -> Result<(), Error> {
        let rv = unsafe { sys::dwnx_conn_handle_expiry(self.raw, now) };
        if rv == sys::DWNX_ERR_IDLE_CLOSE {
            self.closed = true;
            self.sink
                .events
                .borrow_mut()
                .push_back(Event::ConnectionClosed);
            return Ok(());
        }
        if rv != 0 {
            return Err(Error::from_code(rv));
        }
        Ok(())
    }

    /// ローカルのトランスポートパラメータを返す
    pub fn local_transport_parameters(&self) -> TransportParameters {
        let params = unsafe { sys::dwnx_conn_get_local_transport_params(self.raw) };
        if params.is_null() {
            return TransportParameters::default();
        }
        TransportParameters::from_ffi(unsafe { &*params })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe { sys::dwnx_conn_del(self.raw) };
    }
}

/// dwnx のコールバック一式を組み立てる
fn callbacks() -> sys::dwnx_callbacks {
    sys::dwnx_callbacks {
        rand: Some(rand),
        recv_transport_params: Some(recv_transport_params),
        recv_stream_data: Some(recv_stream_data),
        stream_open: Some(stream_open),
        stream_close: Some(stream_close),
        stream_reset: Some(stream_reset),
        stream_stop_sending: Some(stream_stop_sending),
        recv_stop_sending: Some(recv_stop_sending),
        extend_max_stream_data: Some(extend_max_stream_data),
        extend_max_local_streams_bidi: Some(extend_max_local_streams_bidi),
        extend_max_local_streams_uni: Some(extend_max_local_streams_uni),
        extend_max_remote_streams_bidi: Some(extend_max_remote_streams_bidi),
        extend_max_remote_streams_uni: Some(extend_max_remote_streams_uni),
        write_stream_data_offset: Some(write_stream_data_offset),
    }
}

/// user_data からイベント保管先を取り出す
unsafe fn sink<'a>(user_data: *mut c_void) -> &'a Sink {
    unsafe { &*(user_data as *const Sink) }
}

/// user_data から可変のイベント保管先を取り出す
unsafe fn sink_mut<'a>(user_data: *mut c_void) -> &'a mut Sink {
    unsafe { &mut *(user_data as *mut Sink) }
}

/// 乱数源 (dwnx が予測不能なデータを必要としたときに呼ばれる)
unsafe extern "C" fn rand(dest: *mut u8, destlen: usize) {
    if destlen == 0 {
        return;
    }
    let buf = unsafe { core::slice::from_raw_parts_mut(dest, destlen) };
    // 乱数が得られない場合は接続を作り直せないため、ここでは panic せずゼロで埋める。
    // getrandom は OS の乱数源が使えない場合にのみ失敗する。
    if getrandom::fill(buf).is_err() {
        buf.fill(0);
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

unsafe extern "C" fn stream_stop_sending(
    _conn: *mut sys::dwnx_conn,
    _stream_id: i64,
    _app_error_code: u64,
    _user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> core::ffi::c_int {
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

unsafe extern "C" fn extend_max_stream_data(
    _conn: *mut sys::dwnx_conn,
    stream_id: i64,
    max_data: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> core::ffi::c_int {
    unsafe { sink_mut(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::MaxStreamData {
            id: stream_id,
            max_data,
        });
    0
}

/// STREAMS の上限が増えたことを通知するための共通処理
fn push_max_streams(user_data: *mut c_void, bidirectional: bool, local: bool, max_streams: u64) {
    unsafe { sink_mut(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::MaxStreams {
            bidirectional,
            local,
            max_streams,
        });
}

unsafe extern "C" fn extend_max_local_streams_bidi(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    push_max_streams(user_data, true, true, max_streams);
    0
}

unsafe extern "C" fn extend_max_local_streams_uni(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    push_max_streams(user_data, false, true, max_streams);
    0
}

unsafe extern "C" fn extend_max_remote_streams_bidi(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    push_max_streams(user_data, true, false, max_streams);
    0
}

unsafe extern "C" fn extend_max_remote_streams_uni(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    push_max_streams(user_data, false, false, max_streams);
    0
}

unsafe extern "C" fn write_stream_data_offset(
    _conn: *mut sys::dwnx_conn,
    stream_id: i64,
    offset: u64,
    len: usize,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> core::ffi::c_int {
    unsafe { sink(user_data) }
        .events
        .borrow_mut()
        .push_back(Event::StreamDataWritten {
            id: stream_id,
            offset,
            len,
        });
    0
}
