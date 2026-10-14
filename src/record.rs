//! QMux Record のエンコードとデコード (draft-ietf-quic-qmux-02 Section 3.2)
//!
//! QMux Record は可変長整数の Size フィールドと、その長さを持つ Frames フィールドから
//! 構成される。Frames は QUIC version 1 のフレームを連結したバイト列で、Record の
//! 境界は長さフィールドを省略したフレーム (LEN ビットなしの STREAM など) の終端にも
//! なる。

use alloc::string::ToString;
use alloc::vec::Vec;

use crate::codec::error::{Error, TransportErrorCode};
use crate::codec::varint::VarInt;

/// max_record_size トランスポートパラメータの既定値 (draft-ietf-quic-qmux-02 Section 5.2)
///
/// 2 バイトの Size フィールドと 16382 バイトの Frames フィールドで 16KB の Record を
/// 作れる値で、TLS 1.3 の最大レコード長 (RFC 8446 Section 5.1) に対応する。
pub const DEFAULT_MAX_RECORD_SIZE: u64 = 16382;

/// 1 つの QMux Record をエンコードしてバッファへ追加する
///
/// `frames` には 1 つ以上のフレームを連結したバイト列を渡す。空の場合と、
/// ピアが広告した `max_record_size` を超える場合はエラーを返す。エラー時は
/// バッファの内容を変化させない。
pub fn encode(frames: &[u8], max_record_size: u64, buf: &mut Vec<u8>) -> Result<(), Error> {
    // Frames フィールドは 1 つ以上のフレームを含まなければならない (Section 3.2)
    if frames.is_empty() {
        return Err(Error::Protocol {
            code: TransportErrorCode::FRAME_ENCODING_ERROR,
            reason: "QMux record must contain at least one frame".to_string(),
        });
    }
    let size = frames.len() as u64;
    // ピアが広告した max_record_size を超える Record を送ってはならない (Section 5.2)
    if size > max_record_size {
        return Err(Error::Protocol {
            code: TransportErrorCode::FRAME_ENCODING_ERROR,
            reason: format!(
                "QMux record size {size} exceeds peer max_record_size {max_record_size}"
            ),
        });
    }
    // Size は Frames の長さで、Size フィールド自身は含まない (Section 3.2)
    let size = VarInt::try_from(size).map_err(|_| Error::Protocol {
        code: TransportErrorCode::FRAME_ENCODING_ERROR,
        reason: format!("QMux record size {size} cannot be encoded as a variable-length integer"),
    })?;
    size.encode_to_vec(buf);
    buf.extend_from_slice(frames);
    Ok(())
}

/// QMux Record の逐次デコーダ
///
/// バイトストリームから届く断片を [`RecordDecoder::extend`] で渡し、
/// [`RecordDecoder::next`] で完成した Record の Frames フィールドを取り出す。
///
/// 途中まで受信した Record を保持するバッファの大きさは、広告した
/// `max_record_size` と、1 回の [`RecordDecoder::extend`] に渡された長さの
/// 範囲に収まる。Size フィールドのデコード直後に上限を検査するため、
/// ピアが巨大な Size を宣言しても、その分を確保することはない。
#[derive(Debug)]
pub struct RecordDecoder {
    /// まだ Record として取り出していない受信バイト列
    buffer: Vec<u8>,
    /// 自エンドポイントが広告した max_record_size (ピアが送信できる Size の上限)
    max_record_size: u64,
}

impl RecordDecoder {
    /// `max_record_size` を上限として Record をデコードするデコーダを作る
    pub fn new(max_record_size: u64) -> Self {
        Self {
            buffer: Vec::new(),
            max_record_size,
        }
    }

    /// 自エンドポイントが広告した max_record_size を返す
    pub fn max_record_size(&self) -> u64 {
        self.max_record_size
    }

    /// 受信したバイト列を追加する
    ///
    /// 呼び出し側は、追加した後に [`RecordDecoder::next`] を呼び出して
    /// 完成した Record を取り出し続けること。
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// まだ Record として取り出していないバイト数を返す
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// 完成した Record を 1 つ取り出す
    ///
    /// 名前が `Iterator::next` と紛らわしいため `poll_record` としている。
    /// Record が完成していない場合は `Ok(None)` を返す。ピアの違反を検出した場合は
    /// エラーを返し、その時点でバッファを空にする (同じ不正入力を繰り返し
    /// 報告しないため)。
    pub fn poll_record(&mut self) -> Result<Option<Vec<u8>>, Error> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        // Size は可変長整数のため、全体が届くまで待つ
        let (size, header_len) = match VarInt::decode(&self.buffer) {
            Ok(decoded) => decoded,
            Err(Error::Incomplete { .. }) => return Ok(None),
            Err(e) => {
                self.buffer.clear();
                return Err(e);
            }
        };
        let size = size.as_u64();

        // Section 5.2: 広告した max_record_size を超える Record を受信した場合は
        // FRAME_ENCODING_ERROR で接続を閉じる。Size をデコードした直後に検査するため、
        // 超過分のバイトを保持しない。
        if size > self.max_record_size {
            self.buffer.clear();
            return Err(Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: format!(
                    "QMux record size {size} exceeds advertised max_record_size {}",
                    self.max_record_size
                ),
            });
        }

        // Section 3.2: Frames は 1 つ以上のフレームを含む。空の Record は
        // フレームを含まないため受信してはならない。
        if size == 0 {
            self.buffer.clear();
            return Err(Error::Protocol {
                code: TransportErrorCode::PROTOCOL_VIOLATION,
                reason: "QMux record contains no frames".to_string(),
            });
        }

        // 32 ビット環境では max_record_size が usize に収まらない可能性があるため、
        // 変換できることを確認してから長さ計算に使う。
        let size = usize::try_from(size).map_err(|_| {
            self.buffer.clear();
            Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: "QMux record size cannot be represented on this platform".to_string(),
            }
        })?;
        // 32 ビット環境で header_len + size がオーバーフローしないよう検査する
        let Some(total) = header_len.checked_add(size) else {
            self.buffer.clear();
            return Err(Error::Protocol {
                code: TransportErrorCode::FRAME_ENCODING_ERROR,
                reason: "QMux record length cannot be represented on this platform".to_string(),
            });
        };
        if self.buffer.len() < total {
            return Ok(None);
        }

        let frames = self.buffer[header_len..total].to_vec();
        self.buffer.drain(..total);
        Ok(Some(frames))
    }
}
