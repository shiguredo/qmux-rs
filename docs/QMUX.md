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
| `shiguredo_qmux::config` | 接続設定 (`Config`) とストリーム数上限 (`MAX_CONFIG_STREAMS`) |
| `shiguredo_qmux::event` | アプリケーションへ通知するイベント (`Event`) と切断理由 (`CloseReason`) |

リポジトリ全体のクレート構成は [README.md の「構成」](../README.md#構成) を参照してください。

相互接続テストの詳細は [INTEROP.md](./INTEROP.md) を参照してください。

`shiguredo_qmux` は下位トランスポートの I/O を行いません。アプリケーションが次のループを回します。

1. 下位トランスポートから読んだバイト列を `Connection::receive` に渡す
2. `Connection::poll_event` でイベントを取り出して処理する
3. `Connection::poll_transmit` が `false` を返すまで送信バイト列を書き出し、下位トランスポートへ書く。
   `poll_transmit` は `buf` へ追記するため、呼び出しごとに `buf` をクリアするか、Record ごとに
   新しいバッファへ書き出す
4. `Connection::next_timeout` の時刻に `Connection::handle_timeout` を呼ぶ。時刻には
   アイドルタイムアウトの期限に加え、フロー制御でブロック中の `DATA_BLOCKED` /
   `STREAM_DATA_BLOCKED` の再送時刻が含まれる (RFC 9000 Section 4.1 の SHOULD)。
   `handle_timeout` はアイドルタイムアウトの切断と再送フレームの追加の両方を行う

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

ピアが送ってきた CONNECTION_CLOSE (Section 7.3) では、ドライバは下位トランスポートの
送信側を shutdown した後、ピアの shutdown を待たずに終了します。アイドルタイムアウト
(Section 7.1) の場合は、自側の close と同じく linger の上限までピアの shutdown を
待ちます。下位トランスポートの閉塞 (Section 7.4) では shutdown せずに即座に終了します。

読み取りで EOF を観測した場合 (ピアが送信側を shutdown した場合、Section 7.3) は、
`shiguredo_qmux::connection::Connection::peer_shutdown` で接続を閉じ、自側の送信側を
graceful に shutdown して終了します。このとき `tokio_qmux::Connection::closed` は
`shiguredo_qmux::event::CloseReason::PeerShutdown` を返します。読み取りエラーや
書き込みエラーは Section 7.4 の閉塞として `CloseReason::TransportClosed` を返します。

終了が確定した時点で未送信の Record がある場合、ドライバはまだ書き出していない Record
を破棄します。下位トランスポートへの書き込みが途中の Record は境界を壊さないため
完遂します。

`tokio_qmux::Connection` をドロップした場合も、ドライバは linger の期限を待たずに
下位トランスポートを閉じて終了します。この上限は close の後にピアの shutdown を
待つ間の打ち切りであり、ドロップによる終了を遅らせるものではありません。

## tokio ラッパーのストリーム

`tokio_qmux::RecvStream` をドロップすると、アプリケーションがもうデータを受け取らない
ものとして、ドライバはピアへ STOP_SENDING を送ります (RFC 9000 Section 3.5)。
アプリケーションが受け取らないデータをピアに送り続けさせないためです。ただし、
FIN または RESET_STREAM を配送済みのストリームと、ドライバのコマンドチャネルが満杯の
場合は STOP_SENDING を送らないことがあります。

`tokio_qmux::SendStream` の `write` は、FIN または RESET_STREAM を送った後に
`tokio_qmux::Error::StreamFinished` を返します。ピアの STOP_SENDING で送信側が終端した
後は `tokio_qmux::Error::Stopped(error_code)` を返します。DATAGRAM を送れない場合は
`tokio_qmux::Connection::send_datagram` が `Err` を返します。

`tokio_qmux::Connection::open_bidirectional` / `open_unidirectional` は、ピアの
ストリーム上限に達している場合、ピアが MAX_STREAMS を送るまで待ちます。待機中に
接続が閉じた場合は `tokio_qmux::Error::ConnectionClosed` を返します。

`tokio_qmux::Connection::close` は `Result<()>` を返し、アプリケーションエラーコードが
可変長整数の上限 (2^62-1) を超える場合は `tokio_qmux::Error::Qmux` を返します。

## 仕様との対応

| 仕様の節 | 実装 |
| --- | --- |
| Section 3.2 QMux Record | `record.rs`。受信した空の Record は PROTOCOL_VIOLATION、`max_record_size` 超過は FRAME_ENCODING_ERROR。送信側のローカルエラーは `Error::InvalidData` |
| Section 4 利用できるフレーム | `frame.rs`。禁止フレームと未知フレームは FRAME_ENCODING_ERROR |
| Section 4.1 STREAM の順序 | `stream.rs`。直前のペイロードに連続しないオフセットは PROTOCOL_VIOLATION |
| Section 4.2 QX_TRANSPORT_PARAMETERS | `frame.rs` / `connection.rs`。最初のフレーム以外は PROTOCOL_VIOLATION。最初のフレームが禁止フレームまたは未知フレームの場合は Section 4 の FRAME_ENCODING_ERROR を優先する (仕様内で MUST が衝突するため) |
| Section 4.3 QX_PING | `connection.rs`。要求のシーケンス番号は厳密増加。応答は自動送信。tokio 側は `tokio_qmux::Connection::recv_ping_ack` で応答を待てる |
| Section 5.1 許可 / 禁止パラメータ | `transport_params.rs`。禁止パラメータは TRANSPORT_PARAMETER_ERROR |
| Section 5.2 max_record_size | `record.rs` / `transport_params.rs`。既定値 16382、既定値未満は TRANSPORT_PARAMETER_ERROR |
| Section 6 Forward Progress | `receive` はアプリケーションの読み取りと独立に Record を処理する |
| Section 7.1 アイドルタイムアウト | `connection.rs`。実効値は両端の 0 でない値の最小値。ブロック中で実効アイドルタイムアウトが有効な場合は `DATA_BLOCKED` / `STREAM_DATA_BLOCKED` を 1 秒間隔で再送する (`next_timeout` / `handle_timeout`) |
| Section 7.2 / 7.3 切断 | `connection.rs` / `crates/tokio-qmux`。CONNECTION_CLOSE の送受信、切断後のデータ破棄、EOF の `CloseReason::PeerShutdown` 通知、tokio 側の linger 待ち (「tokio ラッパーの切断」参照) |
| Section 8.1 ALPN | 下位トランスポートの責務であり、`shiguredo_qmux` は関与しない。tokio-qmux の doc に利用者が負う MUST (未ネゴシエート時のハンドシェイク中止、QUIC マッピングと異なる識別子) を記載し、interop が ALPN 不一致の負の経路を検証する |
| Section 8.2 0-RTT | 未実装。remembered transport parameters を扱わない (詳細は「未実装」節) |
| Section 9.1 DATAGRAM 拡張 | `connection.rs`。送信は `max_datagram_frame_size` と `max_record_size` の両方で上限を検査し、受信は広告した上限の超過と非サポート時の受信を PROTOCOL_VIOLATION とする |
| Section 9.2 RESET_STREAM_AT | 未対応。`reset_stream_at` を広告せず、受信した RESET_STREAM_AT は未知のフレームタイプとして FRAME_ENCODING_ERROR で扱う (詳細は「未実装」節) |
| Section 12.2 サービス妨害 | 受信した DATAGRAM は未配送の保持量に上限を設け、超過分を破棄する (Section 6 が明示的に許す)。受信 Record 数・フレーム数・PADDING バイト数・QX_PING 要求数は `Connection::traffic_stats()` の `TrafficStats` で観測でき、怪しいトラフィックの監視に使える |

### DATAGRAM の実効サイズ

DATAGRAM フレームは常に Length フィールド付き (0x31) で送信するため、既定の
`max_record_size` (16382) のときに 1 つの Record へ収められるペイロードの最大長は
16379 バイトです (フレームタイプ 1 バイト + Length 2 バイト + ペイロード)。
Section 9.1 が例示する 16381 バイトは Length フィールドを省略した場合の値です。

送信できる DATAGRAM の大きさは、ピアが広告した `max_datagram_frame_size` と
`max_record_size` の両方で決まります (`Connection::send_datagram` がフレーム全体の
大きさを両方の上限と比較します)。実効の上限は 2 つの値の最小値です。ピアの
`max_record_size` が既定値 (16382) のときは Record 側が上限になり、ピアが
`max_datagram_frame_size` をそれより大きく広告しても、ペイロードが 16379 バイトを
超える DATAGRAM は送れません。ピアが `max_datagram_frame_size` を 16382 より小さく
広告した場合は、`max_datagram_frame_size` 側が上限になります。

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
- `MAX_DATA` / `MAX_STREAM_DATA` / `MAX_STREAMS` の広告は、フレームが実際に Record へ
  書き出された時点で有効になる。受信データの検査は送信済みの上限を基準にする
  (RFC 9000 Section 4.1 / Section 4.6 の「送信した上限」)
- STOP_SENDING を送った後に届く STREAM データは破棄し、フロー制御の会計だけ行って
  クレジットを返す (RFC 9000 Section 3.5)
- 通知は合流する。未配送の `Event::Ping` は最大 1 件 (最大シーケンス番号)、
  `StreamWritable` / `StreamsAvailable` は `poll_event` で取り出すまで再通知しない
  (draft-ietf-quic-qmux-02 Section 12.2 の資源消費対策)

### ストリーム数の初期値の上限

`Config::initial_max_streams_bidi` と `Config::initial_max_streams_uni` には
`shiguredo_qmux::config::MAX_CONFIG_STREAMS` (2^16) の上限があります。ピアは 1 つの
MAX_STREAMS フレームで広告した初期値までストリームを開けるため、初期値を大きくするほど
1 フレームで一括生成されるストリームが増えます。`Connection::new` は上限を超える値を
`shiguredo_qmux::codec::error::Error::InvalidData` として拒否します
(draft-ietf-quic-qmux-02 Section 12.1, Section 12.2 の資源枯渇対策)。ピアから受信した
パラメータには上限を課しません (RFC 9000 Section 4.6 は 2^60 まで許容します)。

この上限は、ピアが 1 つの STREAM フレーム (または MAX_STREAM_DATA) で下位番号を
飛ばしてストリームを使ったときに、間のストリームを一括生成するための資源上限でも
あります。初期値を 2^16 に設定すると、1 フレームで最大 2^16 件のストリームと
`StreamOpened` イベントが生成され得ます。既定値は 0 であり、大きな値を設定する場合は
この増幅を理解したうえで設定する必要があります。

### アイドルタイマーのリセット時点

Sans I/O のため、`Connection::poll_transmit` が Record をバッファへ書き出した時点を
「Record を完全に送信した」ものとして扱います。実際に下位トランスポートへの書き込みが
完了した時点ではありません (Section 7.1)。この差は、`poll_transmit` の戻り値をそのまま
下位トランスポートへ書き出すという利用契約で吸収します。書き込みが長時間ブロックする
下位トランスポートでは、実効のアイドルタイムアウトが仕様より早くリセットされ得ます。

### 上限とクレジットの照会

`Connection` は送信側の現在の上限を照会する API を持ちます。

- `peer_max_streams_bidi()` / `peer_max_streams_uni()`: ピアが広告した、ローカル起動
  ストリームを開ける累計上限
- `send_credit()`: 接続レベルの残り送信クレジット (バイト)
- `stream_send_credit(id)`: 指定ストリームの残り送信クレジット (バイト)

ストリームを開けるかどうかは「既に開いた数」と `peer_max_streams_*` の比較で、
書き込めるかどうかは接続レベルとストリームレベルの残りクレジットの両方で決まります。
`write` は受け付けたバイト数を返すため、残りは戻り値とこれらの照会 API から判断できます。

### Config とトランスポートパラメータの変換

`Config` の値はそのまま送られるわけではありません。`Config::transport_parameters` が
次の変換を行います。

- アイドルタイムアウト (`Duration`) はミリ秒に切り捨てて `max_idle_timeout` にする
- 値 0 のパラメータは既定値と同じため省略する
- `max_record_size` が既定値 (16382) のときは省略する
- `max_datagram_frame_size` は `None` ならパラメータを省略し、`Some(0)` なら
  DATAGRAM 非サポートを明示する値 0 のパラメータとして送る

### エラーの契約

`Connection` の公開メソッドが返す `Error::Protocol` は、ローカルな誤用
(STREAM_STATE_ERROR など) を表し、接続は閉じません。ピアの入力から検出したプロトコル
違反は `Connection::receive` の中で CONNECTION_CLOSE を送信待ちに追加し、
`Event::ConnectionClosed` で通知します。`Connection::receive` はエラーを返しません。
コーデックの受信側 API (`frame::decode` など) が返す `Error::Protocol` は、呼び出し側が
接続エラーとして扱う前提です。送信側のローカルなエンコード失敗は
`Error::InvalidData` です。

`Connection::close` / `reset` / `stop_sending` のアプリケーションエラーコードは
可変長整数の上限 (2^62-1) 以下でなければなりません。超える値は `Error::InvalidData`
として返され、panic しません。`Connection::close` は `Result<(), Error>` を返します。

## 仕様の記載に関する注意

### QX_TRANSPORT_PARAMETERS のマジックナンバー

ドラフトは QX_TRANSPORT_PARAMETERS のフレームタイプを `0x3f5153300d0a0d0a` と記載し、
そのワイヤ表現を `"\xffQMX\r\n\r\n"` としています。しかし `0x3f5153300d0a0d0a` を 8 バイトの
可変長整数としてエンコードすると `ff 51 53 30 0d 0a 0d 0a` (`"\xffQS0\r\n\r\n"`) になり、
`"\xffQMX\r\n\r\n"` に対応する値は `0x3f514d580d0a0d0a` です。

`shiguredo_qmux` は、ドラフトのフレーム定義と IANA の表に記載されている値
`0x3f5153300d0a0d0a` を採用します。これは ngtcp2/dwnx の採用値と一致します。

### 未実装

- RESET_STREAM_AT 拡張 (Section 9.2)。本実装は `reset_stream_at` トランスポート
  パラメータを広告せず、RESET_STREAM_AT フレームは未知のフレームタイプとして
  FRAME_ENCODING_ERROR で扱う。Section 9.2 は拡張の使用を許可するが必須ではない。
  `reset_stream_at` を受信した場合は、拡張を理解しない実装として未知の
  トランスポートパラメータを無視する (RFC 9000 Section 7.4.2)。拡張の仕様
  (draft-ietf-quic-reliable-stream-reset-11 Section 3) は、理解する実装に空でない値の
  TRANSPORT_PARAMETER_ERROR を要求するが、本実装は拡張を広告しないためこの要件の
  対象外である
- 0-RTT (Section 8.2) の remembered transport parameters。クライアントの early data は
  未対応であり、呼び出し側が TLS の 0-RTT を有効にしてはならない。有効にする場合は、
  サーバー側 MUST (RFC 9000 Section 7.4.1 の制約を満たすトランスポートパラメータの
  送信) を呼び出し側が保証する必要がある
- ALPN (Section 8.1) によるアプリケーションプロトコルのネゴシエーション。
  下位トランスポートの責務であり、`shiguredo_qmux` は関与しない。QMux over TLS を使う
  呼び出し側は、ALPN がネゴシエートされない場合に no_application_protocol で
  ハンドシェイクを中止する MUST を負う。詳細は `crates/tokio-qmux` のドキュメントを
  参照する。QMux 用の ALPN 識別子は QUIC 用と異なる値でなければならない
- Section 10 Version Agility は version 1 の実装に対する規範要求を持たない
  (in-band upgrade の具体的方式は未定義) ため、実装対象外である

## テスト

- 単体テスト: `tests/test_record.rs`、`test_frame.rs`、`test_transport_params.rs`、`test_stream.rs`
- 結合テスト: `tests/test_connection.rs`。2 つの `Connection` をバイト列で接続し、
  ハンドシェイク、ストリーム、フロー制御、DATAGRAM、QX_PING、切断、プロトコル違反、
  送信済み上限 (announced limits)、ブロック通知の再送、STOP_SENDING 後の破棄、
  エラーコードの境界、クレジット照会、監視カウンタ、ピア shutdown を検証する
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
