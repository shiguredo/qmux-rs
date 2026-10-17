//! dwnx (QMux の C 実装) の FFI バインディング
//!
//! `[package.metadata.external-dependencies]` に固定した dwnx のソースを
//! `build.rs` が clone してビルドし、`dwnx.h` の宣言をそのまま提供する。
//! 安全なラッパーは `dwnx-rs` が提供する。
//!
//! ビルドには git、C11 コンパイラ、libclang (bindgen 用) が必要である。

// 生成コードは C の命名規則に従うため、型名と定数の命名の lint だけを抑止する。
// 同じ目的で抑止しうる `non_snake_case` と `dead_code` は、生成コードでは発火
// しないため指定しない (`expect` は発火しない lint を書くと
// `unfulfilled_lint_expectations` で失敗する)。clippy の lint も生成コードでは
// 発火しないことを `cargo clippy --all-targets -- -D warnings` で確認している。
#![expect(non_upper_case_globals, non_camel_case_types)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
