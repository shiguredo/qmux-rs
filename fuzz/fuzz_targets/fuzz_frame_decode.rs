#![no_main]

//! 任意バイト列を QMux フレームとしてデコードする fuzz target
//!
//! `frame::decode` と `frame::decode_record` に任意入力を流し、panic しないことを
//! 検証する。

use libfuzzer_sys::fuzz_target;
use shiguredo_qmux::frame::{decode, decode_record};

fuzz_target!(|data: &[u8]| {
    // 任意入力に対してパニックしないことを検証する
    let _ = decode(data);
    let _ = decode_record(data);
});
