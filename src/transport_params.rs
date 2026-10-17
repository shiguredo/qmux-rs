//! QMux のトランスポートパラメータ (draft-ietf-quic-qmux-02 Section 5)
//!
//! QMux は QUIC version 1 と同じ形式 (RFC 9000 Section 18) でトランスポート
//! パラメータを交換するが、利用できるパラメータが制限される (Section 5.1)。
//! また QMux 固有の max_record_size パラメータ (Section 5.2) を持つ。
//!
//! デコードは許可されたパラメータだけを取り出す単一パスで行い、QUIC version 1 の
//! 汎用デコーダには依存しない。

use alloc::collections::BTreeSet;
use alloc::string::ToString;
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::varint::VarInt;
use crate::record::DEFAULT_MAX_RECORD_SIZE;

/// トランスポートパラメータ ID (RFC 9000 Section 18.2、RFC 9221 Section 3)
pub mod param_id {
    pub const ORIGINAL_DESTINATION_CONNECTION_ID: u64 = 0x00;
    pub const MAX_IDLE_TIMEOUT: u64 = 0x01;
    pub const STATELESS_RESET_TOKEN: u64 = 0x02;
    pub const MAX_UDP_PAYLOAD_SIZE: u64 = 0x03;
    pub const INITIAL_MAX_DATA: u64 = 0x04;
    pub const INITIAL_MAX_STREAM_DATA_BIDI_LOCAL: u64 = 0x05;
    pub const INITIAL_MAX_STREAM_DATA_BIDI_REMOTE: u64 = 0x06;
    pub const INITIAL_MAX_STREAM_DATA_UNI: u64 = 0x07;
    pub const INITIAL_MAX_STREAMS_BIDI: u64 = 0x08;
    pub const INITIAL_MAX_STREAMS_UNI: u64 = 0x09;
    pub const ACK_DELAY_EXPONENT: u64 = 0x0a;
    pub const MAX_ACK_DELAY: u64 = 0x0b;
    pub const DISABLE_ACTIVE_MIGRATION: u64 = 0x0c;
    pub const PREFERRED_ADDRESS: u64 = 0x0d;
    pub const ACTIVE_CONNECTION_ID_LIMIT: u64 = 0x0e;
    pub const INITIAL_SOURCE_CONNECTION_ID: u64 = 0x0f;
    pub const RETRY_SOURCE_CONNECTION_ID: u64 = 0x10;

    // DATAGRAM 拡張 (RFC 9221 Section 3)
    pub const MAX_DATAGRAM_FRAME_SIZE: u64 = 0x20;
}

/// max_record_size トランスポートパラメータの ID (Section 5.2)
///
/// 値はドラフトの暫定コードポイントであり、RFC 化の際に変更される可能性がある。
pub const MAX_RECORD_SIZE_ID: u64 = 0x0571c59429cd0845;

/// ストリーム数の上限として送受信できる最大値 (RFC 9000 Section 4.6)
///
/// これを超える値を広告すると、可変長整数で表現できないストリーム ID を
/// 許すことになるため、受信側は TRANSPORT_PARAMETER_ERROR で接続を閉じる。
pub const MAX_STREAMS_LIMIT: u64 = 1 << 60;

/// QMux のトランスポートパラメータ (Section 5)
///
/// 値が 0 のパラメータは QUIC version 1 の既定値と同じ意味を持つ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportParameters {
    /// アイドルタイムアウト (ミリ秒、0 は無効。RFC 9000 Section 18.2)
    pub max_idle_timeout: u64,
    /// 接続レベルで受信できるデータ量の初期値
    pub initial_max_data: u64,
    /// ローカル起動の双方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_bidi_local: u64,
    /// ピア起動の双方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_bidi_remote: u64,
    /// 単方向ストリームで受信できるデータ量の初期値
    pub initial_max_stream_data_uni: u64,
    /// ピアが起動できる双方向ストリーム数の初期値
    pub initial_max_streams_bidi: u64,
    /// ピアが起動できる単方向ストリーム数の初期値
    pub initial_max_streams_uni: u64,
    /// ピアが送信できる QMux Record の Size の上限 (省略時は既定値)
    pub max_record_size: u64,
    /// DATAGRAM フレームの最大サイズ (RFC 9221 Section 3)
    ///
    /// `None` と `Some(0)` はどちらも DATAGRAM の受信をサポートしないことを
    /// 意味する (0 が既定値であり、非サポートを表す)。エンコードでは `None` は
    /// パラメータを省略し、`Some(0)` は値 0 のパラメータとして送信する。
    pub max_datagram_frame_size: Option<u64>,
}

impl Default for TransportParameters {
    fn default() -> Self {
        Self {
            max_idle_timeout: 0,
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

impl TransportParameters {
    /// エンコードされたトランスポートパラメータをデコードする
    ///
    /// QMux で禁止されたパラメータ (Section 5.1)、重複したパラメータ
    /// (RFC 9000 Section 7.4)、値が不正なパラメータは TRANSPORT_PARAMETER_ERROR を
    /// 返す。QMux で利用が定義されていないパラメータは無視する (Section 5.1)。
    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let mut params = Self::default();
        let mut max_record_size = None;
        // 重複検出用の既読 ID 集合 (RFC 9000 Section 7.4)
        let mut seen = BTreeSet::new();
        let mut offset = 0;

        while offset < buf.len() {
            let (id, n) = VarInt::decode(&buf[offset..]).map_err(transport_parameter_error)?;
            offset += n;
            let (length, n) = VarInt::decode(&buf[offset..]).map_err(transport_parameter_error)?;
            offset += n;

            let len = usize::try_from(length.as_u64()).map_err(|_| Error::Protocol {
                code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                reason: "transport parameter length cannot be represented on this platform"
                    .to_string(),
            })?;
            // 32 ビット環境で offset + len がオーバーフローしないよう検査する
            let end = offset.checked_add(len).ok_or_else(|| Error::Protocol {
                code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                reason: "transport parameter length cannot be represented on this platform"
                    .to_string(),
            })?;
            let value = buf.get(offset..end).ok_or_else(|| Error::Protocol {
                code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                reason: format!("transport parameter {} is truncated", id.as_u64()),
            })?;
            offset = end;

            let id = id.as_u64();

            // RFC 9000 Section 7.4: 同じパラメータを 2 回以上受信した場合は
            // TRANSPORT_PARAMETER_ERROR として扱う。max_record_size は QUIC version 1
            // のデコーダでは未知のパラメータとして無視されるため、ここで検出する。
            if !seen.insert(id) {
                return Err(Error::Protocol {
                    code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                    reason: format!("duplicate transport parameter {id:#x}"),
                });
            }

            // Section 5.1: QUIC version 1 で定義されたパラメータのうち、許可された
            // 7 つ以外は禁止されている。
            if is_prohibited(id) {
                return Err(Error::Protocol {
                    code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                    reason: format!("transport parameter {id:#x} is prohibited in QMux"),
                });
            }

            // RFC 9000 Section 18.2: 整数のパラメータの値は 1 つの可変長整数
            // (1 / 2 / 4 / 8 バイト) でなければならない。値が空、または余分な
            // バイトを含む場合は値が不正であり TRANSPORT_PARAMETER_ERROR とする
            // (RFC 9000 Section 7.4)。
            if is_integer_parameter(id) {
                if len == 0 {
                    return Err(Error::Protocol {
                        code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                        reason: format!("transport parameter {id:#x} has an empty value"),
                    });
                }
                let (_, consumed) = VarInt::decode(value).map_err(transport_parameter_error)?;
                if consumed != len {
                    return Err(Error::Protocol {
                        code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                        reason: format!(
                            "transport parameter {id:#x} has {} trailing bytes",
                            len - consumed
                        ),
                    });
                }
            }

            match id {
                param_id::MAX_IDLE_TIMEOUT => {
                    params.max_idle_timeout = decode_integer(value)?;
                }
                param_id::INITIAL_MAX_DATA => {
                    params.initial_max_data = decode_integer(value)?;
                }
                param_id::INITIAL_MAX_STREAM_DATA_BIDI_LOCAL => {
                    params.initial_max_stream_data_bidi_local = decode_integer(value)?;
                }
                param_id::INITIAL_MAX_STREAM_DATA_BIDI_REMOTE => {
                    params.initial_max_stream_data_bidi_remote = decode_integer(value)?;
                }
                param_id::INITIAL_MAX_STREAM_DATA_UNI => {
                    params.initial_max_stream_data_uni = decode_integer(value)?;
                }
                param_id::INITIAL_MAX_STREAMS_BIDI => {
                    params.initial_max_streams_bidi = decode_integer(value)?;
                }
                param_id::INITIAL_MAX_STREAMS_UNI => {
                    params.initial_max_streams_uni = decode_integer(value)?;
                }
                param_id::MAX_DATAGRAM_FRAME_SIZE => {
                    params.max_datagram_frame_size = Some(decode_integer(value)?);
                }
                MAX_RECORD_SIZE_ID => {
                    // Section 5.2: 値は 1 つの可変長整数で、既定値以上でなければならない
                    let decoded = decode_integer(value)?;
                    if decoded < DEFAULT_MAX_RECORD_SIZE {
                        return Err(Error::Protocol {
                            code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                            reason: format!(
                                "max_record_size {decoded} is below the default {DEFAULT_MAX_RECORD_SIZE}"
                            ),
                        });
                    }
                    max_record_size = Some(decoded);
                }
                _ => {
                    // 未知のパラメータは無視する (RFC 9000 Section 7.4.2)
                }
            }
        }

        params.max_record_size = max_record_size.unwrap_or(DEFAULT_MAX_RECORD_SIZE);

        // RFC 9000 Section 4.6: ストリーム数の上限が 2^60 を超える値は不正
        if params.initial_max_streams_bidi > MAX_STREAMS_LIMIT {
            return Err(Error::Protocol {
                code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                reason: format!(
                    "initial_max_streams_bidi {} exceeds 2^60",
                    params.initial_max_streams_bidi
                ),
            });
        }
        if params.initial_max_streams_uni > MAX_STREAMS_LIMIT {
            return Err(Error::Protocol {
                code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
                reason: format!(
                    "initial_max_streams_uni {} exceeds 2^60",
                    params.initial_max_streams_uni
                ),
            });
        }

        Ok(params)
    }

    /// トランスポートパラメータをエンコードする
    ///
    /// 値が 0 のパラメータは既定値と同じため省略する (RFC 9000 Section 18.2)。
    /// ただし [`TransportParameters::max_datagram_frame_size`] は例外で、`None` は
    /// パラメータ自体を省略し、`Some(0)` は DATAGRAM の非サポートを明示するために
    /// 値 0 のパラメータとして送信する (RFC 9221 Section 3 の既定値は 0)。
    /// 可変長整数で表現できない値、2^60 を超えるストリーム数の上限はエラーを返す。
    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
        if self.max_idle_timeout != 0 {
            encode_varint_param(buf, param_id::MAX_IDLE_TIMEOUT, self.max_idle_timeout)?;
        }
        if self.initial_max_data != 0 {
            encode_varint_param(buf, param_id::INITIAL_MAX_DATA, self.initial_max_data)?;
        }
        if self.initial_max_stream_data_bidi_local != 0 {
            encode_varint_param(
                buf,
                param_id::INITIAL_MAX_STREAM_DATA_BIDI_LOCAL,
                self.initial_max_stream_data_bidi_local,
            )?;
        }
        if self.initial_max_stream_data_bidi_remote != 0 {
            encode_varint_param(
                buf,
                param_id::INITIAL_MAX_STREAM_DATA_BIDI_REMOTE,
                self.initial_max_stream_data_bidi_remote,
            )?;
        }
        if self.initial_max_stream_data_uni != 0 {
            encode_varint_param(
                buf,
                param_id::INITIAL_MAX_STREAM_DATA_UNI,
                self.initial_max_stream_data_uni,
            )?;
        }
        // RFC 9000 Section 4.6: 2^60 を超えるストリーム数の上限は送信してはならない
        check_stream_limit("initial_max_streams_bidi", self.initial_max_streams_bidi)?;
        if self.initial_max_streams_bidi != 0 {
            encode_varint_param(
                buf,
                param_id::INITIAL_MAX_STREAMS_BIDI,
                self.initial_max_streams_bidi,
            )?;
        }
        check_stream_limit("initial_max_streams_uni", self.initial_max_streams_uni)?;
        if self.initial_max_streams_uni != 0 {
            encode_varint_param(
                buf,
                param_id::INITIAL_MAX_STREAMS_UNI,
                self.initial_max_streams_uni,
            )?;
        }
        // Section 5.2: 既定値と同じ場合は省略する。既定値未満の値は送信できない
        if self.max_record_size != DEFAULT_MAX_RECORD_SIZE {
            if self.max_record_size < DEFAULT_MAX_RECORD_SIZE {
                return Err(Error::InvalidData(format!(
                    "max_record_size {} is below the default {DEFAULT_MAX_RECORD_SIZE}",
                    self.max_record_size
                )));
            }
            encode_varint_param(buf, MAX_RECORD_SIZE_ID, self.max_record_size)?;
        }
        // RFC 9221 Section 3: Some(0) は DATAGRAM の非サポートを明示するため、
        // 値 0 のパラメータとして送信する。None はパラメータ自体を省略する
        // (省略時の既定値も 0 だが、decode 側で None と Some(0) を区別できる)。
        if let Some(max_datagram_frame_size) = self.max_datagram_frame_size {
            encode_varint_param(
                buf,
                param_id::MAX_DATAGRAM_FRAME_SIZE,
                max_datagram_frame_size,
            )?;
        }
        Ok(())
    }
}

/// 整数パラメータの値をデコードする
///
/// 呼び出し前に [`is_integer_parameter`] の検査で、値が空でなく、1 つの
/// 可変長整数として過不足なく消費されることが確認されている。
fn decode_integer(value: &[u8]) -> Result<u64, Error> {
    let (v, _) = VarInt::decode(value).map_err(transport_parameter_error)?;
    Ok(v.as_u64())
}

/// 値が可変長整数 1 つでエンコードされるパラメータかどうか (RFC 9000 Section 18.2)
///
/// max_record_size (Section 5.2) と DATAGRAM 拡張の max_datagram_frame_size
/// (RFC 9221 Section 3) も値は可変長整数である。
pub fn is_integer_parameter(id: u64) -> bool {
    matches!(
        id,
        param_id::MAX_IDLE_TIMEOUT
            | param_id::INITIAL_MAX_DATA
            | param_id::INITIAL_MAX_STREAM_DATA_BIDI_LOCAL
            | param_id::INITIAL_MAX_STREAM_DATA_BIDI_REMOTE
            | param_id::INITIAL_MAX_STREAM_DATA_UNI
            | param_id::INITIAL_MAX_STREAMS_BIDI
            | param_id::INITIAL_MAX_STREAMS_UNI
            | param_id::MAX_DATAGRAM_FRAME_SIZE
            | MAX_RECORD_SIZE_ID
    )
}

/// QMux で利用が禁止されているトランスポートパラメータかどうか (Section 5.1)
///
/// QUIC version 1 (RFC 9000 Section 18.2) で定義されたパラメータのうち、許可された
/// 7 つ以外はすべて禁止されている。QUIC version 1 以外で定義されたパラメータは
/// 禁止の対象ではなく、利用が定義されていないものは無視する。
pub fn is_prohibited(id: u64) -> bool {
    matches!(
        id,
        param_id::ORIGINAL_DESTINATION_CONNECTION_ID
            | param_id::STATELESS_RESET_TOKEN
            | param_id::MAX_UDP_PAYLOAD_SIZE
            | param_id::ACK_DELAY_EXPONENT
            | param_id::MAX_ACK_DELAY
            | param_id::DISABLE_ACTIVE_MIGRATION
            | param_id::PREFERRED_ADDRESS
            | param_id::ACTIVE_CONNECTION_ID_LIMIT
            | param_id::INITIAL_SOURCE_CONNECTION_ID
            | param_id::RETRY_SOURCE_CONNECTION_ID
    )
}

/// トランスポートパラメータのデコードエラーを TRANSPORT_PARAMETER_ERROR に揃える
///
/// RFC 9000 Section 7.4: 値が不正なトランスポートパラメータを受信した場合は
/// TRANSPORT_PARAMETER_ERROR として接続を閉じる。
fn transport_parameter_error(err: Error) -> Error {
    match err {
        // すでに接続エラーとして表現されているものはそのまま伝える
        Error::Protocol { code, reason } => Error::Protocol { code, reason },
        other => Error::Protocol {
            code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
            reason: format!("invalid transport parameter: {other}"),
        },
    }
}

/// ストリーム数の上限が 2^60 以下であることを確認する (RFC 9000 Section 4.6)
fn check_stream_limit(name: &str, value: u64) -> Result<(), Error> {
    if value > MAX_STREAMS_LIMIT {
        return Err(Error::InvalidData(format!("{name} {value} exceeds 2^60")));
    }
    Ok(())
}

/// 可変長整数のトランスポートパラメータをエンコードする
fn encode_varint_param(buf: &mut Vec<u8>, id: u64, value: u64) -> Result<(), Error> {
    let id = VarInt::try_from(id).map_err(|_| {
        Error::InvalidData(format!(
            "transport parameter ID {id:#x} exceeds the maximum variable-length integer"
        ))
    })?;
    let value = VarInt::try_from(value).map_err(|_| {
        Error::InvalidData(format!(
            "transport parameter {id} value {value} exceeds the maximum variable-length integer"
        ))
    })?;

    id.encode_to_vec(buf);
    // 値の長さは可変長整数のエンコード長と一致する (RFC 9000 Section 18)
    VarInt::try_from(value.encoded_size() as u64)
        .expect("variable-length integer encoded size fits in a variable-length integer")
        .encode_to_vec(buf);
    value.encode_to_vec(buf);
    Ok(())
}
