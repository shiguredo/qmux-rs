# QMux

`shiguredo_qmux` は draft-ietf-quic-qmux-02 の QMux version 1 を実装した
Sans I/O のクレートです。

## QMux とは

QUIC version 1 は UDP 上でストリームとデータグラムを提供しますが、UDP が中間装置で
ブロックされる環境や、UDP の処理コストが問題になる環境では TCP + TLS に劣ります。
QMux は、TLS over TCP のような順序保証された双方向のバイトストリーム上で、QUIC の
ストリーム操作とデータグラム操作を提供する互換層です。

QMux 自体は暗号化も認証も行いません。接続の確立、秘匿、完全性、アプリケーションプロトコルの
ネゴシエーション (ALPN) は下位トランスポートの責務です。

## クレート構成

| モジュール | 役割 |
| --- | --- |
| `shiguredo_qmux::connection` | QMux の Record 層、接続状態機械、トランスポートパラメータ交換 |
| `shiguredo_qmux::stream` | ストリームの多重化、フロー制御、MAX_STREAMS |
| `shiguredo_qmux::record` | QMux Record のエンコード/デコード |
| `shiguredo_qmux::frame` | QMux 固有フレームと、利用が許可された QUIC version 1 フレームの判定 |
| `shiguredo_qmux::transport_params` | トランスポートパラメータの許可/禁止判定と `max_record_size` |
| `shiguredo_qmux::codec` | QUIC version 1 の可変長整数、フレーム、基本型 (no_std + alloc) |

リポジトリ全体のクレート構成は [README.md の「構成」](../README.md#構成) を参照してください。

相互接続テストの詳細は [INTEROP.md](./INTEROP.md) を参照してください。

`shiguredo_qmux` は下位トランスポートの I/O を行いません。アプリケーションが次のループを回します。

1. 下位トランスポートから読んだバイト列を `Connection::receive` に渡す
2. `Connection::poll_event` でイベントを取り出して処理する
3. `Connection::poll_transmit` が `false` を返すまで送信バイト列を書き出し、下位トランスポートへ書く
4. `Connection::next_timeout` の時刻に `Connection::handle_timeout` を呼ぶ

`Connection::is_closed` が `true` になったら、`poll_transmit` で残りを書き出した後に下位
トランスポートの送信側を閉じます (draft-ietf-quic-qmux-02 Section 7.2)。

## tokio ラッパーの切断

`crates/tokio-qmux` のドライバは、`tokio_qmux::Connection::close` の要求を受けると
CONNECTION_CLOSE を送り、下位トランスポートの送信側を graceful に shutdown し、
ピアが送信側を shutdown するのを待ちます (draft-ietf-quic-qmux-02 Section 7.2)。

この待ち時間には上限 (linger) があり、既定値は `tokio_qmux::DEFAULT_LINGER` (3 秒)
です。`tokio_qmux::Connection::with_linger` で変更できます。

ピアが shutdown しない場合も、ピアが読み取りを止めて送信が詰まった場合も、期限を
過ぎるとドライバは下位トランスポートを閉じて終了します。このとき
`tokio_qmux::Connection::closed` は `shiguredo_qmux::event::CloseReason::Local` を
返します。

ピアが送ってきた CONNECTION_CLOSE (Section 7.3) とアイドルタイムアウト (Section 7.1)
では、ドライバは下位トランスポートの送信側を shutdown した後、ピアの shutdown を
待たずに終了します。下位トランスポートの閉塞 (Section 7.4) では shutdown せずに
即座に終了します。

`tokio_qmux::Connection` をドロップした場合も、ドライバは linger の期限を待たずに
下位トランスポートを閉じて終了します。この上限は close の後にピアの shutdown を
待つ間の打ち切りであり、ドロップによる終了を遅らせるものではありません。

## tokio ラッパーのストリーム

`tokio_qmux::RecvStream` をドロップすると、アプリケーションがもうデータを受け取らない
ものとして、ドライバはピアへ STOP_SENDING を送ります (RFC 9000 Section 3.5)。
アプリケーションが受け取らないデータをピアに送り続けさせないためです。

`tokio_qmux::SendStream` の `write` は、FIN を送った後とピアの STOP_SENDING で送信側が
終端した後に `tokio_qmux::Error::StreamFinished` を返します。DATAGRAM を送れない場合は
`tokio_qmux::Connection::send_datagram` が `Err` を返します。

## 仕様との対応

| 仕様の節 | 実装 |
| --- | --- |
| Section 3.2 QMux Record | `record.rs`。Size の可変長整数と Frames。空の Record は PROTOCOL_VIOLATION、`max_record_size` 超過は FRAME_ENCODING_ERROR |
| Section 4 利用できるフレーム | `frame.rs`。禁止フレームと未知フレームは FRAME_ENCODING_ERROR |
| Section 4.1 STREAM の順序 | `stream.rs`。直前のペイロードに連続しないオフセットは PROTOCOL_VIOLATION |
| Section 4.2 QX_TRANSPORT_PARAMETERS | `frame.rs` / `connection.rs`。最初のフレーム以外は PROTOCOL_VIOLATION |
| Section 4.3 QX_PING | `connection.rs`。要求のシーケンス番号は厳密増加。応答は自動送信。tokio 側は `tokio_qmux::Connection::recv_ping_ack` で応答を待てる |
| Section 5.1 許可 / 禁止パラメータ | `transport_params.rs`。禁止パラメータは TRANSPORT_PARAMETER_ERROR |
| Section 5.2 max_record_size | `record.rs` / `transport_params.rs`。既定値 16382、既定値未満は TRANSPORT_PARAMETER_ERROR |
| Section 6 Forward Progress | `receive` はアプリケーションの読み取りと独立に Record を処理する |
| Section 7.1 アイドルタイムアウト | `connection.rs`。Record の送受信でリセットし、実効値は両端の 0 でない値の最小値 |
| Section 7.2 / 7.3 切断 | `connection.rs` と `crates/tokio-qmux`。CONNECTION_CLOSE の送受信と、切断後の受信データの破棄。tokio 側は切断後に下位トランスポートの送信側を shutdown し、ピアの shutdown を待つ上限 (linger) を設けて期限でドライバを終了する |
| Section 9.1 DATAGRAM 拡張 | `connection.rs`。送信は `max_datagram_frame_size` と `max_record_size` の両方で上限を検査し、受信は広告した上限の超過と非サポート時の受信を PROTOCOL_VIOLATION とする |
| Section 12.2 サービス妨害 | 受信した DATAGRAM は未配送の保持量に上限を設け、超過分を破棄する (Section 6 が明示的に許す) |

### DATAGRAM の実効サイズ

DATAGRAM フレームは常に Length フィールド付き (0x31) で送信するため、既定の
`max_record_size` (16382) のときに 1 つの Record へ収められるペイロードの最大長は
16379 バイトです (フレームタイプ 1 バイト + Length 2 バイト + ペイロード)。
Section 9.1 が例示する 16381 バイトは Length フィールドを省略した場合の値です。

送信できる DATAGRAM の大きさは、ピアが広告した `max_datagram_frame_size` と
`max_record_size` の両方で決まります (`Connection::send_datagram` がフレーム全体の
大きさを両方の上限と比較します)。DATAGRAM フレームは 1 つの Record に収める必要が
あるため、実効の上限は `max_record_size` 側になります。ピアの `max_record_size` が
既定値のときは、ピアが `max_datagram_frame_size` をそれより大きく広告しても、
ペイロードが 16379 バイトを超える DATAGRAM は送れません。

RFC 9000 側の規則は QMux の順序保証に合わせて適用します。

- 下位トランスポートへ渡した時点で ACK されたものとみなし (Section 4)、送信側の状態から
  Send / Data Sent / Reset Sent を省略する
- STREAM フレームが順序どおりに届くため、受信側で並べ替えのためのバッファを持たない
- ピアのトランスポートパラメータを受信するまで、ピアのフロー制御に依存する操作
  (ストリームを開く、データを書く、DATAGRAM を送る) を行わない (Section 4.2)
- 終端したストリームは破棄し、ピア起動のストリームであれば MAX_STREAMS で上限を
  返却する (RFC 9000 Section 4.6)。破棄したストリームへの遅延フレームは破棄する
  (RFC 9000 Section 3.2)
- ピアが下位番号を飛ばしてストリームを使った場合は、間のストリームも作成する
  (RFC 9000 Section 3.2)。作成済みかどうかはシーケンス番号の上限で判定する

### ストリーム数の初期値の上限

`Config::initial_max_streams_bidi` と `Config::initial_max_streams_uni` には
`shiguredo_qmux::config::MAX_CONFIG_STREAMS` (2^16) の上限があります。ピアは 1 つの
MAX_STREAMS フレームで広告した初期値までストリームを開けるため、初期値を大きくするほど
1 フレームで一括生成されるストリームが増えます。`Connection::new` は上限を超える値を
`shiguredo_qmux::codec::error::Error::InvalidData` として拒否します
(draft-ietf-quic-qmux-02 Section 12.1, Section 12.2 の資源枯渇対策)。ピアから受信した
パラメータには上限を課しません (RFC 9000 Section 4.6 は 2^60 まで許容します)。

## 仕様の記載に関する注意

### QX_TRANSPORT_PARAMETERS のマジックナンバー

ドラフトは QX_TRANSPORT_PARAMETERS のフレームタイプを `0x3f5153300d0a0d0a` と記載し、
そのワイヤ表現を `"\xffQMX\r\n\r\n"` としています。しかし `0x3f5153300d0a0d0a` を 8 バイトの
可変長整数としてエンコードすると `ff 51 53 30 0d 0a 0d 0a` (`"\xffQS0\r\n\r\n"`) になり、
`"\xffQMX\r\n\r\n"` に対応する値は `0x3f514d580d0a0d0a` です。

`shiguredo_qmux` は、ドラフトのフレーム定義と IANA の表に記載されている値
`0x3f5153300d0a0d0a` を採用します。これは ngtcp2/dwnx の採用値と一致します。

### 未実装

- RESET_STREAM_AT 拡張 (Section 9.2)。本実装は `reset_stream_at` を広告せず、
  RESET_STREAM_AT フレームは未知のフレームタイプとして FRAME_ENCODING_ERROR で扱う
- 0-RTT (Section 8.2) の remembered transport parameters
- ALPN (Section 8.1) によるアプリケーションプロトコルのネゴシエーション。
  下位トランスポートの責務であり、`shiguredo_qmux` は関与しない

## テスト

- 単体テスト: `tests/test_record.rs`、`test_frame.rs`、`test_transport_params.rs`、`test_stream.rs`
- 結合テスト: `tests/test_connection.rs`。2 つの `Connection` をバイト列で接続し、
  ハンドシェイク、ストリーム、フロー制御、DATAGRAM、QX_PING、切断、プロトコル違反を検証する
- PBT: `pbt/tests/prop_*.rs` (noprop)。Record / フレーム / トランスポートパラメータ /
  ストリーム (ID の表現とフロー制御) / 接続の性質を検証する
- Fuzzing: `fuzz/fuzz_targets/*.rs` (cargo-fuzz)。受信経路とデコーダの panic 耐性を検証する

## 参照

- `refs/quic/draft-ietf-quic-qmux-02.txt` (QMux)
- `refs/quic/rfc9000.txt` (QUIC version 1)
- `refs/quic/rfc9221.txt` (DATAGRAM 拡張)
- `refs/quic/rfc7301.txt` (TLS ALPN)
- `refs/quic/rfc8446.txt` (TLS 1.3)
- `refs/quic/draft-ietf-quic-reliable-stream-reset-11.txt` (RESET_STREAM_AT 拡張)
- `refs/quic/draft-kazuho-httpbis-http3-over-qmux-00.txt` (HTTP/3 over QMux。未実装。ALPN `h3qx-01` の参照用)
