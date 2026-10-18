//! dwnx のトランスポートパラメータ (dwnx_transport_params に対応)

use dwnx_sys as sys;

use crate::error::Error;

/// 1 ミリ秒を表す dwnx のタイムスタンプの刻み
///
/// dwnx.h の `DWNX_MILLISECONDS` と同じ値である。bindgen は型変換を含む
/// マクロを展開しないため、ここで定義する。dwnx のタイムスタンプはナノ秒単位で
/// あり (dwnx.h の `DWNX_NANOSECONDS`)、`DWNX_MICROSECONDS` はその 1000 倍、
/// `DWNX_MILLISECONDS` はさらにその 1000 倍である。
const DWNX_MILLISECONDS: u64 = 1_000_000;

/// [`TransportParameters`] で指定できるストリーム数の初期値の上限
///
/// ピアは 1 つの MAX_STREAMS フレームで上限までストリームを開けるため、広告する
/// 初期値が大きいほど、1 フレームで一括生成されるストリームとイベントの数が増える。
/// [`TransportParameters::validate`] はこの値を超える `initial_max_streams_bidi` /
/// `initial_max_streams_uni` を拒否する (draft-ietf-quic-qmux-02 Section 12.1,
/// Section 12.2 の資源枯渇対策)。`shiguredo_qmux::config::MAX_CONFIG_STREAMS` と
/// 同じ上限であり、RFC 9000 Section 4.6 が許容する 2^60 までの値をそのまま広告する
/// ことはしない。
pub const MAX_CONFIG_STREAMS: u64 = 1 << 16;

/// dwnx のトランスポートパラメータ
///
/// dwnx の `dwnx_transport_params` に対応する。`max_idle_timeout` は
/// ナノ秒単位である (dwnx のタイムスタンプはナノ秒)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportParameters {
    /// ローカル起動の双方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_bidi_local: u64,
    /// ピア起動の双方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_bidi_remote: u64,
    /// 単方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_uni: u64,
    /// 接続レベルで受信できるデータ量の初期値
    pub initial_max_data: u64,
    /// ピアが起動できる双方向ストリーム数の初期値
    pub initial_max_streams_bidi: u64,
    /// ピアが起動できる単方向ストリーム数の初期値
    pub initial_max_streams_uni: u64,
    /// アイドルタイムアウト (ナノ秒、0 は無効)
    pub max_idle_timeout: u64,
    /// ピアが送信できる QMux Record の Size の上限
    ///
    /// dwnx はアプリケーションが指定した値を既定値
    /// (`DWNX_DEFAULT_MAX_RECORD_SIZE`) に強制するため (dwnx の
    /// `lib/dwnx_conn.c` の `conn_new` にある "We do not let application increase
    /// max record size.")、この値は既定値と等しくなければならない。
    /// [`TransportParameters::validate`] は既定値以外を拒否する。
    pub max_record_size: u64,
    /// 受信を受け付ける DATAGRAM フレームの最大サイズ (RFC 9221 Section 3)
    ///
    /// 0 は DATAGRAM 拡張を使わないことを表し、その場合ピアは DATAGRAM を
    /// 送れない。DATAGRAM を受信するには 0 より大きい値を指定する。
    /// dwnx の `dwnx_transport_params` の `max_datagram_frame_size` に対応する。
    pub max_datagram_frame_size: u64,
}

impl Default for TransportParameters {
    fn default() -> Self {
        Self {
            initial_max_stream_data_bidi_local: 0,
            initial_max_stream_data_bidi_remote: 0,
            initial_max_stream_data_uni: 0,
            initial_max_data: 0,
            initial_max_streams_bidi: 0,
            initial_max_streams_uni: 0,
            max_idle_timeout: 0,
            max_record_size: u64::from(sys::DWNX_DEFAULT_MAX_RECORD_SIZE),
            max_datagram_frame_size: 0,
        }
    }
}

impl TransportParameters {
    /// dwnx に渡せる値かどうかを検証する
    ///
    /// dwnx は `dwnx_conn_client_new` / `dwnx_conn_server_new` の中で
    /// トランスポートパラメータを `assert` で検査する (dwnx の
    /// `lib/dwnx_conn.c` の `conn_new`)。`assert` は `NDEBUG` を定義していない
    /// ビルドではプロセスを abort させるため、同じ検査を Rust 側でも行う。
    /// `assert` に掛からない値でも、dwnx が黙って上書きする `max_record_size`
    /// のような設定は既定値以外を拒否する。
    ///
    /// # エラー
    ///
    /// dwnx が受け付けない設定の場合は [`Error`]
    /// (`DWNX_ERR_INVALID_ARGUMENT`) を返す。
    pub fn validate(&self) -> Result<(), Error> {
        let max_varint = sys::DWNX_MAX_VARINT;

        // ピアが 1 つの MAX_STREAMS フレームで開けるストリーム数の実用上限
        // (draft-ietf-quic-qmux-02 Section 12.1, Section 12.2 の資源枯渇対策)。
        // 受信したピアのパラメータには上限を課さない (RFC 9000 Section 4.6 は
        // 2^60 まで許容する)。
        for (name, value) in [
            ("initial_max_streams_bidi", self.initial_max_streams_bidi),
            ("initial_max_streams_uni", self.initial_max_streams_uni),
        ] {
            if value > MAX_CONFIG_STREAMS {
                return Err(Error::invalid_argument(format!(
                    "{name} must not exceed MAX_CONFIG_STREAMS ({MAX_CONFIG_STREAMS})"
                )));
            }
        }

        // 各 initial_max_* は可変長整数として符号化するため DWNX_MAX_VARINT を
        // 超えられない (dwnx の lib/dwnx_conv.c の dwnx_put_uvarint を参照)。
        // initial_max_stream_data_uni だけは conn_new の assert に無いが、局所
        // トランスポートパラメータの符号化時に同じ assert に掛かる。
        for (name, value) in [
            (
                "initial_max_stream_data_bidi_local",
                self.initial_max_stream_data_bidi_local,
            ),
            (
                "initial_max_stream_data_bidi_remote",
                self.initial_max_stream_data_bidi_remote,
            ),
            (
                "initial_max_stream_data_uni",
                self.initial_max_stream_data_uni,
            ),
            ("initial_max_data", self.initial_max_data),
            ("initial_max_streams_bidi", self.initial_max_streams_bidi),
            ("initial_max_streams_uni", self.initial_max_streams_uni),
        ] {
            if value > max_varint {
                return Err(Error::invalid_argument(format!(
                    "{name} must not exceed DWNX_MAX_VARINT ({max_varint})"
                )));
            }
        }

        // max_idle_timeout は「無効」を 0 で表すため、UINT64_MAX を値として
        // 受け付けない
        if self.max_idle_timeout == u64::MAX {
            return Err(Error::invalid_argument(
                "max_idle_timeout must not be UINT64_MAX",
            ));
        }
        // max_idle_timeout はミリ秒単位の可変長整数として送るため、ミリ秒に
        // 換算した値が DWNX_MAX_VARINT を超えられない
        if self.max_idle_timeout / DWNX_MILLISECONDS > max_varint {
            return Err(Error::invalid_argument(format!(
                "max_idle_timeout must not exceed DWNX_MAX_VARINT milliseconds ({max_varint})"
            )));
        }

        // dwnx の conn_new はアプリケーションが指定した max_record_size を
        // 既定値へ強制する (dwnx の lib/dwnx_conn.c にある "We do not let
        // application increase max record size.")。既定値以外を指定しても
        // 黙って無視されるため、設定の誤りに気づけるよう拒否する。
        let default_max_record_size = u64::from(sys::DWNX_DEFAULT_MAX_RECORD_SIZE);
        if self.max_record_size != default_max_record_size {
            return Err(Error::invalid_argument(format!(
                "max_record_size must be DWNX_DEFAULT_MAX_RECORD_SIZE ({default_max_record_size}); dwnx ignores other values"
            )));
        }

        // max_datagram_frame_size は 0 より大きい場合だけ可変長整数として
        // 符号化される (dwnx の lib/dwnx_transport_params.c を参照)。ほかの
        // initial_max_* と同じく DWNX_MAX_VARINT を超えられない。dwnx の
        // conn_new には assert が無いが、符号化の dwnx_put_uvarint が assert で
        // プロセスを abort させるため、Rust 側で先に検査する。
        if self.max_datagram_frame_size > max_varint {
            return Err(Error::invalid_argument(format!(
                "max_datagram_frame_size must not exceed DWNX_MAX_VARINT ({max_varint})"
            )));
        }

        Ok(())
    }

    /// FFI の構造体へ変換する
    pub(crate) fn to_ffi(&self) -> sys::dwnx_transport_params {
        sys::dwnx_transport_params {
            initial_max_stream_data_bidi_local: self.initial_max_stream_data_bidi_local,
            initial_max_stream_data_bidi_remote: self.initial_max_stream_data_bidi_remote,
            initial_max_stream_data_uni: self.initial_max_stream_data_uni,
            initial_max_data: self.initial_max_data,
            initial_max_streams_bidi: self.initial_max_streams_bidi,
            initial_max_streams_uni: self.initial_max_streams_uni,
            max_idle_timeout: self.max_idle_timeout,
            max_record_size: self.max_record_size,
            max_datagram_frame_size: self.max_datagram_frame_size,
        }
    }

    /// FFI の構造体から変換する
    pub(crate) fn from_ffi(params: &sys::dwnx_transport_params) -> Self {
        Self {
            initial_max_stream_data_bidi_local: params.initial_max_stream_data_bidi_local,
            initial_max_stream_data_bidi_remote: params.initial_max_stream_data_bidi_remote,
            initial_max_stream_data_uni: params.initial_max_stream_data_uni,
            initial_max_data: params.initial_max_data,
            initial_max_streams_bidi: params.initial_max_streams_bidi,
            initial_max_streams_uni: params.initial_max_streams_uni,
            max_idle_timeout: params.max_idle_timeout,
            max_record_size: params.max_record_size,
            max_datagram_frame_size: params.max_datagram_frame_size,
        }
    }
}
