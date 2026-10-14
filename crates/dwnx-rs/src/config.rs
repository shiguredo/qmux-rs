//! dwnx のトランスポートパラメータ (dwnx_transport_params に対応)

use dwnx_sys as sys;

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
    pub max_record_size: u64,
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
        }
    }
}

impl TransportParameters {
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
        }
    }
}

/// dwnx 接続の設定
pub type Config = TransportParameters;
