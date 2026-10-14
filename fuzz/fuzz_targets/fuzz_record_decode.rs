#![no_main]

//! 任意バイト列を QMux Record として逐次デコードする fuzz target
//!
//! `RecordDecoder` に任意の `max_record_size` と任意バイト列を流し、panic しない
//! ことを検証する。上限検査の経路に入るよう、`max_record_size` も入力から決める。

use libfuzzer_sys::fuzz_target;
use shiguredo_qmux::record::RecordDecoder;

fuzz_target!(|data: &[u8]| {
    // 先頭 2 バイトを max_record_size の決定に使う
    let Some((&high, rest)) = data.split_first() else {
        return;
    };
    let Some((&low, payload)) = rest.split_first() else {
        return;
    };
    let max_record_size = u64::from(u16::from_be_bytes([high, low]));

    let mut decoder = RecordDecoder::new(max_record_size);
    decoder.extend(payload);
    while let Ok(Some(_)) = decoder.poll_record() {
        // 取り出せる Record をすべて取り出す
    }
});
