#![no_main]

//! 任意バイト列を QMux のトランスポートパラメータとしてデコードする fuzz target
//!
//! `TransportParameters::decode` に任意入力を流し、panic しないことを検証する。

use libfuzzer_sys::fuzz_target;
use shiguredo_qmux::transport_params::TransportParameters;

fuzz_target!(|data: &[u8]| {
    // 任意入力に対してパニックしないことを検証する
    let _ = TransportParameters::decode(data);
});
