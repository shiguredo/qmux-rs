# 相互接続テスト

`interop` クレートは、`shiguredo_qmux` (Rust) と dwnx (C) が同じ QMux ワイヤ表現を
話せることを検証します。Sans I/O と tokio を使った I/O ありの両方を対象にします。

## リポジトリ構成

リポジトリ全体のクレート構成は [README.md の「構成」](../README.md#構成) を参照してください。

## dwnx の固定とビルド

- dwnx の取得先は `crates/dwnx-sys/Cargo.toml` の `[package.metadata.external-dependencies]`
  に宣言する
  - `git = "https://github.com/ngtcp2/dwnx.git"`
  - `ref = "aeb481c7098a7d326047ffa52740c0f4e4fa2d48"`
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
- `shiguredo_client_sends_datagram_to_dwnx_server`
- `dwnx_client_sends_datagram_to_shiguredo_server`
- `multiple_datagrams_and_empty_datagram_are_transferred`
- `shiguredo_datagram_to_dwnx_without_support_is_rejected`
- `dwnx_datagram_to_shiguredo_without_support_is_reported_as_failure`
- `shiguredo_datagram_larger_than_dwnx_limit_is_rejected`
- `dwnx_datagram_larger_than_shiguredo_limit_is_reported_as_failure`
- `dwnx_datagram_at_record_capacity_limit_is_transferred_to_shiguredo_server`
- `dwnx_datagram_split_across_receives_is_reassembled_by_shiguredo_server`
- `shiguredo_stream_and_datagram_share_one_record_in_dwnx`
- `dwnx_stream_and_datagram_share_one_record_in_shiguredo`

### tokio + TLS (`interop/tests/tokio_tls.rs`)

TCP + TLS (rustls + aws-lc-sys) + ALPN で接続し、次を検証します。

- ALPN がネゴシエートされること
- `tokio-qmux` クライアントから `tokio-dwnx` サーバーへ双方向ストリームでデータが届くこと
- サーバーが返したエコーがクライアントに届くこと
- `tokio-qmux` と `tokio-dwnx` の間で DATAGRAM が往復すること (空の DATAGRAM を含む)
- 逆向きの役割の組み合わせ (`tokio-dwnx` クライアントから `tokio-qmux` サーバー) でも、
  双方向ストリームのエコーと DATAGRAM の往復が成立すること
  - `tokio-dwnx` の open / finish / close (linger) と `tokio-qmux` の accept の組み合わせを
    検証する

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
  - 2026-10-18 時点の暫定決定として、native (QUIC) と同一の識別子で統一する。
    MOQT over QMux の individual draft (`draft-nandakumar-moq-qmux-moqt-00` Section 3.2)
    が native MOQT と同じ ALPN を使うと定めており、MOQT 側に QMux 用の ALPN 登録が
    無いため、相互接続を優先する
  - draft-ietf-quic-qmux-02 Section 8.1 は QMux マッピングに QUIC マッピングと別の
    識別子を MUST とする (SVCB 互換のため) ため、この運用は同節から外れる。QMux 用の
    ALPN が定義された時点で見直す
  - 同一 ALPN の間は、QMux 非対応のサーバーに TCP で接続しても TLS は成功するため、
    QMux のトランスポートパラメータ待ちにタイムアウトを設けて検知する。加えて、SVCB で
    QUIC 用と QMux 用を区別できず、0-RTT のチケットも ALPN で分離されない
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

- dwnx は RESET_STREAM_AT 拡張を実装していないため、RESET_STREAM_AT は相互接続
  テストの対象外である
  - DATAGRAM 拡張 (RFC 9221) は dwnx 本体、`dwnx-rs`、`tokio-dwnx` が対応している。
    Sans I/O と tokio + TLS の相互接続テストで送受信を検証する
  - DATAGRAM の受信は、受信側の `max_datagram_frame_size` を 0 より大きくした場合だけ
    有効になる。0 は DATAGRAM 拡張の非サポートを意味する (RFC 9221 Section 3)
  - 単方向ストリーム、RESET_STREAM、STOP_SENDING、複数ストリームの同時実行、FIN 後の
    クローズは Sans I/O の相互接続テストで検証する
- `dwnx_rs::MAX_CONFIG_STREAMS` (2^16) が `TransportParameters::initial_max_streams_bidi` /
  `initial_max_streams_uni` の実用上限である (`shiguredo_qmux::config::MAX_CONFIG_STREAMS` と
  同じ値。draft-ietf-quic-qmux-02 Section 12.1, Section 12.2 の資源枯渇対策)。超過した値は
  `TransportParameters::validate` が `Err` (`DWNX_ERR_INVALID_ARGUMENT`) で拒否する。受信した
  ピアのパラメータには上限を課さない (RFC 9000 Section 4.6 は 2^60 まで許容する)
- dwnx はアプリケーションが指定した `max_record_size` を既定値
  (`DWNX_DEFAULT_MAX_RECORD_SIZE` = 16382 バイト) に強制する。そのため
  `TransportParameters::max_record_size` は既定値以外を
  `TransportParameters::validate` が拒否する
- dwnx-rs の送信バッファは `DWNX_DEFAULT_MAX_RECORD_SIZE` 固定である。DATAGRAM フレーム
  全体 (タイプ、Length、ペイロード) が Record に収まらない場合は、送信側で
  `DWNX_ERR_INVALID_ARGUMENT` として通知し、接続は閉じない
  - dwnx 自身は Record に収まらない DATAGRAM フレームの `DWNX_ERR_NOBUF` を返さないため、
    dwnx-rs が送信前にフレーム長を検査している

### `tokio-dwnx` 側

- DATAGRAM の送受信 API を持つ。`Connection::send_datagram` は DATAGRAM を Record に
  収め、下位トランスポートへ書き出し終えるまで待つ。`Connection::recv_datagram` /
  `Connection::recv_datagrams` で受信する。受信チャネルの上限を超えて破棄した個数は
  `Connection::dropped_datagrams` で観測できる。DATAGRAM を使うには双方の
  `TransportParameters::max_datagram_frame_size` を 0 より大きくする
  - 送信できる DATAGRAM は、ローカルの Record 容量 (16382 バイト) にフレーム全体が
    収まるものに限る。超える場合は `send_datagram` が `Error::Dwnx`
    (`DWNX_ERR_INVALID_ARGUMENT`) を返し、接続は閉じない
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
