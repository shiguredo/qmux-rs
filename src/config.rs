//! QMux 接続の設定 (draft-ietf-quic-qmux-02 Section 5)

use crate::codec::time::Duration;

use crate::record::DEFAULT_MAX_RECORD_SIZE;
use crate::transport_params::TransportParameters;

/// [`Config`] で指定できるストリーム数の初期値の上限
///
/// ピアは 1 つの MAX_STREAMS フレームで上限までストリームを開けるため、広告する
/// 初期値が大きいほど、1 フレームで一括生成されるストリームとイベントの数が増える。
/// [`crate::connection::Connection::new`] はこの値を超える
/// [`Config::initial_max_streams_bidi`] / [`Config::initial_max_streams_uni`] を
/// [`crate::codec::error::Error::InvalidData`] として拒否する
/// (draft-ietf-quic-qmux-02 Section 12.1, Section 12.2 の資源枯渇対策)。
/// RFC 9000 Section 4.6 が許容する 2^60 までの値をそのまま広告することはしない。
pub const MAX_CONFIG_STREAMS: u64 = 1 << 16;

/// QMux 接続の設定
///
/// 値はそのまま QX_TRANSPORT_PARAMETERS フレームとしてピアへ送られる。既定値は
/// RFC 9000 と draft-ietf-quic-qmux-02 の既定値と同じで、フロー制御のクレジットは
/// 0 である。ストリームや DATAGRAM を使う場合は明示的に設定すること。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// アイドルタイムアウト (Section 7.1)。`Duration::ZERO` は無効
    pub max_idle_timeout: Duration,
    /// 接続レベルで受信できるデータ量の初期値 (RFC 9000 Section 18.2)
    pub initial_max_data: u64,
    /// ローカル起動の双方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_bidi_local: u64,
    /// ピア起動の双方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_bidi_remote: u64,
    /// 単方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_uni: u64,
    /// ピアが起動できる双方向ストリーム数の初期値
    ///
    /// [`MAX_CONFIG_STREAMS`] 以下でなければならない。
    pub initial_max_streams_bidi: u64,
    /// ピアが起動できる単方向ストリーム数の初期値
    ///
    /// [`MAX_CONFIG_STREAMS`] 以下でなければならない。
    pub initial_max_streams_uni: u64,
    /// ピアが送信できる QMux Record の Size の上限 (Section 5.2)
    ///
    /// `DEFAULT_MAX_RECORD_SIZE` (16382) 以上でなければならない。既定値未満の値を
    /// 設定した場合、[`crate::connection::Connection::new`] が
    /// [`crate::codec::error::Error::InvalidData`] を返す。
    pub max_record_size: u64,
    /// 受信を許可する DATAGRAM フレームの最大サイズ (RFC 9221 Section 3)
    ///
    /// `None` と `Some(0)` はどちらも DATAGRAM 拡張を使わないことを表す
    /// (RFC 9221 では 0 が非サポートを意味する)。
    pub max_datagram_frame_size: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_idle_timeout: Duration::ZERO,
            initial_max_data: 0,
            initial_max_stream_data_bidi_local: 0,
            initial_max_stream_data_bidi_remote: 0,
            initial_max_stream_data_uni: 0,
            initial_max_streams_bidi: 0,
            initial_max_streams_uni: 0,
            max_record_size: DEFAULT_MAX_RECORD_SIZE,
            max_datagram_frame_size: None,
        }
    }
}

impl Config {
    /// 設定をローカルのトランスポートパラメータへ変換する
    ///
    /// アイドルタイムアウトはミリ秒単位のトランスポートパラメータへ変換する。
    pub fn transport_parameters(&self) -> TransportParameters {
        TransportParameters {
            max_idle_timeout: self.max_idle_timeout.as_millis(),
            initial_max_data: self.initial_max_data,
            initial_max_stream_data_bidi_local: self.initial_max_stream_data_bidi_local,
            initial_max_stream_data_bidi_remote: self.initial_max_stream_data_bidi_remote,
            initial_max_stream_data_uni: self.initial_max_stream_data_uni,
            initial_max_streams_bidi: self.initial_max_streams_bidi,
            initial_max_streams_uni: self.initial_max_streams_uni,
            max_record_size: self.max_record_size,
            max_datagram_frame_size: self.max_datagram_frame_size,
        }
    }
}
