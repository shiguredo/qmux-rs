# 相互接続テスト

`interop` クレートは、`shiguredo_qmux` (Rust) と dwnx (C) が同じ QMux ワイヤ表現を
話せることを検証します。Sans I/O と tokio を使った I/O ありの両方を対象にします。

## リポジトリ構成

リポジトリ全体のクレート構成は [README.md の「構成」](../README.md#構成) を参照してください。

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
- `shiguredo_client_sends_unidirectional_stream_to_dwnx_server`
- `dwnx_client_sends_unidirectional_stream_to_shiguredo_server`
- `shiguredo_reset_stream_propagates_to_dwnx_server`
- `dwnx_reset_stream_propagates_to_shiguredo_server`
- `shiguredo_stop_sending_propagates_to_dwnx_server`
- `dwnx_stop_sending_propagates_to_shiguredo_server`
- `multiple_bidirectional_streams_are_transferred`
- `stream_is_closed_after_fin_in_both_directions`

### tokio + TLS (`interop/tests/tokio_tls.rs`)

TCP + TLS (rustls + aws-lc-sys) + ALPN で接続し、次を検証します。

- ALPN がネゴシエートされること
- `tokio-qmux` クライアントから `tokio-dwnx` サーバーへ双方向ストリームでデータが届くこと
- サーバーが返したエコーがクライアントに届くこと

テストで使う ALPN 識別子は「ALPN 識別子」節を参照してください。証明書は `rcgen` で
テスト用の CA と `localhost` のサーバー証明書を生成し、CA を `rustls::RootCertStore` に
入れて標準の検証器で検証します。

## ALPN 識別子

ALPN 識別子は下位トランスポートの利用者が決めるものであり、`shiguredo_qmux` 自身は
ALPN を扱いません。QMux 上で動かすアプリケーションプロトコルの識別子は、QUIC 上の
同じプロトコルと区別できる値にする必要があります (draft-ietf-quic-qmux-02 Section 8.1)。

現在の運用は次のとおりです。

- HTTP/3 over QMux: `h3qx-01` (`draft-kazuho-httpbis-http3-over-qmux-00`。将来は `h3qx`)
- MOQT over QMux: `moqt-22`
  - native (QUIC) と同一の識別子で統一する。Section 8.1 は QMux マッピングに
    QUIC マッピングと別の識別子を MUST とするため、この点は MOQT 側ドラフトとの
    間で引き続き整理が必要である
- 相互接続テスト: `hqx-01`
  - ストリームの echo のみを検証するため、dwnx の example と同じ識別子を使う

## 実行

```bash
# Sans I/O の相互接続
cargo test -p interop --test sans_io

# tokio + TLS の相互接続
cargo test -p interop --test tokio_tls
```

## 既知の制約

### dwnx 側

- dwnx は DATAGRAM 拡張と RESET_STREAM_AT 拡張を実装していないため、この 2 つは
  相互接続テストの対象外である
  - DATAGRAM の送受信は `tests/test_connection.rs` が `shiguredo_qmux` だけで検証する
  - 単方向ストリーム、RESET_STREAM、STOP_SENDING、複数ストリームの同時実行、FIN 後の
    クローズは Sans I/O の相互接続テストで検証する
- `dwnx_rs::MAX_CONFIG_STREAMS` (2^16) が `TransportParameters::initial_max_streams_bidi` /
  `initial_max_streams_uni` の実用上限である (`shiguredo_qmux::config::MAX_CONFIG_STREAMS` と
  同じ値。draft-ietf-quic-qmux-02 Section 12.1, Section 12.2 の資源枯渇対策)。超過した値は
  `TransportParameters::validate` が `Err` (`DWNX_ERR_INVALID_ARGUMENT`) で拒否する。受信した
  ピアのパラメータには上限を課さない (RFC 9000 Section 4.6 は 2^60 まで許容する)

### `tokio-dwnx` 側

- DATAGRAM の送受信 API を持たない (dwnx が DATAGRAM 拡張を実装していないため)
- `SendStream::write` は、渡したデータをドライバがすべて受け付け、下位トランスポートへ
  書き出し終えるまで待つ。フロー制御で保留された分も書き出してから戻る
- 切断時は、自側の `close`、アイドルタイムアウト (draft-ietf-quic-qmux-02 Section 7.1)、
  ピアからの CONNECTION_CLOSE (Section 7.3) のいずれでも、下位トランスポートの送信側を
  graceful に shutdown してから終了する。自側の `close` ではピアの shutdown も待つ
- 待ち時間の上限は `tokio_dwnx::DEFAULT_LINGER` (3 秒) である。
  `tokio_dwnx::Connection::with_linger` で変更できる。上限を過ぎると、ドライバは
  下位トランスポートを閉じて終了する
- 下位トランスポートの閉塞 (Section 7.4) では shutdown せずに即座に終了する
- `Connection::closed` は終了理由を返さない (tokio-qmux の `Connection::closed` は
  `CloseReason` を返す)
- 受信したストリームデータのフロー制御クレジットは、アプリケーションへの配送が成功した
  時点で返す。未配送のイベントはクレジットを返さないため、配送待ちキューはフロー制御で
  有界になる。アプリケーションが受け取ってから処理するまでの量に応じたバックプレッシャは
  今後の課題である
- `RecvStream` をドロップしても STOP_SENDING を送らない。送信停止を要求する場合は
  `RecvStream::stop_sending` を明示的に呼ぶ
