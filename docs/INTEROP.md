# 相互接続テスト

`interop` クレートは、`shiguredo_qmux` (Rust) と dwnx (C) が同じ QMux ワイヤ表現を
話せることを検証します。Sans I/O と tokio を使った I/O ありの両方を対象にします。

## リポジトリ構成

| クレート | 役割 |
|---|---|
| `shiguredo_qmux` (root) | Sans I/O & no_std + alloc の QMux 実装 |
| `crates/tokio-qmux` | `shiguredo_qmux` の tokio 非同期ラッパー |
| `crates/dwnx-sys` | dwnx の C ソースをビルドし、FFI 宣言を生成する |
| `crates/dwnx-rs` | `dwnx_conn` の安全な Sans I/O ラッパー |
| `crates/tokio-dwnx` | `dwnx-rs` の tokio 非同期ラッパー |
| `interop` | 2 実装を接続する相互接続テスト |

## dwnx の固定とビルド

- dwnx の取得先は `crates/dwnx-sys/Cargo.toml` の `[package.metadata.external-dependencies]`
  に宣言する
  - `git = "https://github.com/ngtcp2/dwnx.git"`
  - `ref = "3941ae176af6aa8f878a83d082ad099101b6de2a"`
- `dwnx-sys` の `build.rs` は次を行う。git submodule は使わない
  - `git clone` して指定した ref をチェックアウトする。チェックアウト先はビルド
    プロファイルごとの target ディレクトリにキャッシュし、同じ ref では再 clone しない
  - `version.h.in` から `dwnx/version.h` を生成する (configure が生成するファイルを再現)
  - `lib/*.c` を `cc` で静的ライブラリとしてコンパイルする (`DWNX_STATICLIB` を定義)
  - `bindgen` で `dwnx.h` から FFI 宣言を生成する
- ビルドには git、C11 コンパイラ、libclang (bindgen 用) が必要である
  - macOS では `LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib` を設定する

## テスト

### Sans I/O (`interop/tests/sans_io.rs`)

`shiguredo_qmux` の `Connection` と `dwnx-rs` の `Connection` をメモリ上のバイト列で
接続します。TLS は使わず、QMux のワイヤ表現だけを突き合わせます。

- `shiguredo_client_sends_stream_to_dwnx_server`
- `dwnx_client_sends_stream_to_shiguredo_server`

### tokio + TLS (`interop/tests/tokio_tls.rs`)

TCP + TLS (rustls + aws-lc-sys) + ALPN `hqx-01` で接続し、次を検証します。

- ALPN がネゴシエートされること
- `tokio-qmux` クライアントから `tokio-dwnx` サーバーへ双方向ストリームでデータが届くこと
- サーバーが返したエコーがクライアントに届くこと

HTTP/3 を QMux 上で動かす場合の ALPN は `h3qx-01`（HTTP/3 over draft-ietf-quic-qmux-01）です
(`draft-kazuho-httpbis-http3-over-qmux-00`、将来 `h3qx`)。本テストはストリームの echo のみを
検証するため、dwnx の example と同じ `hqx-01` を使っています。

自己署名証明書は `rcgen` で生成し、クライアントはテスト専用の検証器で受け入れます。

## ALPN 識別子

ALPN 識別子は下位トランスポートの利用者が決めるものであり、`shiguredo_qmux` 自身は
ALPN を扱いません。QMux 上で動かすアプリケーションプロトコルの識別子は、QUIC 上の
同じプロトコルと区別できる値にする必要があります (draft-ietf-quic-qmux-02 Section 8.1)。

現在の運用は次のとおりです。

- HTTP/3 over QMux: `h3qx-01` (`draft-kazuho-httpbis-http3-over-qmux-00`)
  - 相互接続テストはストリームの echo のみを検証するため `hqx-01` を使う
- MOQT over QMux: `moqt-22`
  - native (QUIC) と同一の識別子で統一する。Section 8.1 は QMux マッピングに
    QUIC マッピングと別の識別子を MUST とするため、この点は MOQT 側ドラフトとの
    間で引き続き整理が必要である

## 実行

```bash
# Sans I/O の相互接続
cargo test -p interop --test sans_io

# tokio + TLS の相互接続
cargo test -p interop --test tokio_tls
```

## 既知の制約

- dwnx は DATAGRAM 拡張と RESET_STREAM_AT 拡張を実装していないため、相互接続テストの
  対象外である
- `tokio-dwnx` は受信したストリームデータに対して即座にフロー制御クレジットを返す。
  アプリケーション側のバッファリングに応じた厳密なバックプレッシャは今後の課題である
