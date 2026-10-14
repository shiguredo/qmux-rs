#![no_main]

//! 任意バイト列を QUIC 可変長整数としてデコードする fuzz target
//!
//! `VarInt::decode` に任意入力を流し、panic しないことを検証する。

use libfuzzer_sys::fuzz_target;
use shiguredo_qmux::codec::varint::VarInt;

fuzz_target!(|data: &[u8]| {
    // 任意入力に対してパニックしないことを検証する
    let _ = VarInt::decode(data);
});
