//! タイムスタンプとデュレーション型
//!
//! Sans I/O のため std::time::Instant に依存しない。
//! アプリケーションから渡されるマイクロ秒単位の単調増加値を使用する。

/// マイクロ秒単位の単調増加タイムスタンプ
///
/// アプリケーションが提供する時刻値を保持する。
/// 具体的なエポックはアプリケーション依存。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// マイクロ秒値からタイムスタンプを生成する
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros)
    }

    /// マイクロ秒値を返す
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    /// Duration を加算する
    ///
    /// overflow 時は `u64::MAX` に飽和する。
    pub const fn add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration.0))
    }

    /// other 以降かどうかを返す
    pub const fn has_elapsed(self, other: Self) -> bool {
        self.0 >= other.0
    }
}

/// マイクロ秒単位のデュレーション
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration(u64);

impl Duration {
    /// ゼロ長のデュレーション
    pub const ZERO: Self = Self(0);

    /// ミリ秒値からデュレーションを生成する
    ///
    /// overflow 時は `u64::MAX` マイクロ秒に飽和する。
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1000))
    }

    /// 秒値からデュレーションを生成する
    ///
    /// overflow 時は `u64::MAX` マイクロ秒に飽和する。
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000))
    }

    /// マイクロ秒値を返す
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    /// ミリ秒値を返す (端数切り捨て)
    pub const fn as_millis(self) -> u64 {
        self.0 / 1000
    }
}
