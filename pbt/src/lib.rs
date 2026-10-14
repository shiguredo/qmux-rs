//! shiguredo_qmux の Property-Based Testing 用クレート
//!
//! 公開 API に対するプロパティを noprop で検証する。テストターゲットは
//! `pbt/tests/prop_*.rs` に置き、共通のサンプラーは [`common`] から取り込む。

pub mod common;
