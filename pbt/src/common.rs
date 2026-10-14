//! pbt クレート共通の Runner 生成とサンプラー
//!
//! 複数の `prop_*.rs` から使う入力生成ヘルパーとシード管理を 1 箇所に集約する。
//! 各テストターゲットは `use pbt::common::...;` で取り込む。

use shiguredo_qmux::codec::varint;

/// PBT シードの環境変数名 (全 property テスト共通)
///
/// 文字列リテラルの散在はタイポが時刻シードへの静かなフォールバックになり、
/// 再現性喪失でしか表面化しないため、定数で 1 箇所管理する。
/// 失敗時はエラー出力の `reproduce with:` ヒントに表示されるシードをこの
/// 環境変数に設定すると同じケースを再実行できる。
pub const PBT_SEED_ENV_VAR: &str = "PBT_SEED";

/// 環境変数または時刻からシードを得て `Runner` を作る
pub fn test_runner() -> noprop::TestResult<noprop::Runner> {
    let seed = noprop::seed_from_env_or_time(PBT_SEED_ENV_VAR)?;
    Ok(noprop::Runner::new(seed))
}

/// 長さ `0..=max_len` のバイト列を生成する
///
/// 空・1 バイト・最大長は一様分布では出にくいため、明示的な境界値として混ぜる。
pub fn sample_bytes(ctx: &mut noprop::TestCaseContext, max_len: usize) -> Vec<u8> {
    let len = noprop::sample_with_boundaries(
        ctx,
        &[0usize, 1, max_len],
        noprop::Ratio::one_nth(4),
        |ctx| noprop::sample_usize_in(ctx, 0..=max_len),
    );
    noprop::sample_bytes_vec(ctx, len)
}

/// 長さ `1..=max_len` の空でないバイト列を生成する
///
/// 1 バイトと最大長は一様分布では出にくいため、明示的な境界値として混ぜる。
pub fn sample_nonempty_bytes(ctx: &mut noprop::TestCaseContext, max_len: usize) -> Vec<u8> {
    let len =
        noprop::sample_with_boundaries(ctx, &[1usize, max_len], noprop::Ratio::one_nth(4), |ctx| {
            noprop::sample_usize_in(ctx, 1..=max_len)
        });
    noprop::sample_bytes_vec(ctx, len)
}

/// 50% の確率で None を、残りは `sample` の値を Some で返す
///
/// トランスポートパラメータやフレームの Option フィールド向け。
/// Some / None の両分岐がプロパティで観測されることをカバレッジゲートで確認して使う。
pub fn sample_option<T>(
    ctx: &mut noprop::TestCaseContext,
    sample: impl FnOnce(&mut noprop::TestCaseContext) -> T,
) -> Option<T> {
    if noprop::sample_bool(ctx) {
        Some(sample(ctx))
    } else {
        None
    }
}

/// VarInt の有効範囲全体 (0..=2^62-1) を生成する
///
/// エンコード長 1 / 2 / 4 / 8 バイト (RFC 9000 Section 16) の境界値を混ぜ、
/// 一様分布ではほぼ出ない短いエンコードを確実に踏む。
pub fn sample_varint(ctx: &mut noprop::TestCaseContext) -> u64 {
    noprop::sample_with_boundaries(
        ctx,
        &[
            0u64,
            63,                // 1 バイト最大 (6 ビット値)
            64,                // 2 バイト最小
            16_383,            // 2 バイト最大 (14 ビット値)
            16_384,            // 4 バイト最小
            1_073_741_823,     // 4 バイト最大 (30 ビット値)
            1_073_741_824,     // 8 バイト最小
            varint::MAX_VALUE, // 8 バイト最大 (62 ビット値)
        ],
        noprop::Ratio::one_nth(4),
        |ctx| noprop::sample_u64_in(ctx, 0..=varint::MAX_VALUE),
    )
}
