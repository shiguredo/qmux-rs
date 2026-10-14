//! dwnx (QMux の C 実装) の FFI バインディング
//!
//! `[package.metadata.external-dependencies]` に固定した dwnx のソースを
//! `build.rs` が clone してビルドし、`dwnx.h` の宣言をそのまま提供する。
//! 安全なラッパーは `dwnx-rs` が提供する。
//!
//! ビルドには git、C11 コンパイラ、libclang (bindgen 用) が必要である。

// bindgen が生成する bindings は C の命名規則に従い、使われない宣言も含む。
// 生成コードに限り命名・dead_code・clippy の lint を許可する。
#![allow(
    non_snake_case,
    non_upper_case_globals,
    non_camel_case_types,
    dead_code,
    clippy::all
)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
