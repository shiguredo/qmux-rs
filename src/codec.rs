//! QMux が必要とする QUIC version 1 の基本型と codec
//!
//! QMux は QUIC version 1 のフレーム形式とトランスポートパラメータ形式を
//! そのまま再利用する (draft-ietf-quic-qmux-02 Section 4, Section 5)。
//! 本モジュールはそのうち QMux が利用する範囲だけを提供する。
//!
//! QMux で利用が禁止されているフレームの型は定義しない。禁止フレームの判定は
//! [`crate::frame`] がフレームタイプの定数を使って行う。

pub mod error;
pub mod frame;
pub mod role;
pub mod time;
pub mod varint;
