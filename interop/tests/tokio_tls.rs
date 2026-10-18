//! QMux (tokio-qmux) と dwnx (tokio-dwnx) の tokio + TLS 相互接続テスト
//!
//! TCP + TLS (rustls + aws-lc-sys) + ALPN で接続し、クライアントから
//! 双方向ストリームでデータを送り、サーバーがエコーを返すことを検証する。
//! 証明書は rcgen で生成したテスト用の CA と `localhost` のサーバー証明書を使い、
//! クライアントは CA を信頼して標準の検証器でサーバー証明書を検証する。

use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;

use dwnx_rs::TransportParameters as DwnxTransportParameters;
use shiguredo_qmux::codec::role::Role as QmuxRole;
use shiguredo_qmux::codec::time::Duration as QmuxDuration;
use shiguredo_qmux::config::Config as QmuxConfig;
use tokio_dwnx::Role as DwnxRole;
use tokio_qmux::Connection as QmuxConnection;

/// テストで使う ALPN 識別子
///
/// HTTP/3 over QMux の ALPN は `h3qx-01` (draft-kazuho-httpbis-http3-over-qmux-00)。
/// 本テストはストリームの echo のみを検証するため、dwnx の example と同じ HQ 用の
/// `hqx-01` を使う。
const ALPN: &[u8] = b"hqx-01";

/// shiguredo_qmux 側の設定
fn qmux_config() -> QmuxConfig {
    QmuxConfig {
        max_idle_timeout: QmuxDuration::from_secs(30),
        initial_max_data: 64 * 1024,
        initial_max_stream_data_bidi_local: 16 * 1024,
        initial_max_stream_data_bidi_remote: 16 * 1024,
        initial_max_stream_data_uni: 16 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        // DATAGRAM 拡張を有効にする (RFC 9221 Section 3)
        max_datagram_frame_size: Some(65535),
        ..QmuxConfig::default()
    }
}

/// dwnx 側の設定 (max_idle_timeout はナノ秒)
fn dwnx_config() -> DwnxTransportParameters {
    DwnxTransportParameters {
        initial_max_stream_data_bidi_local: 16 * 1024,
        initial_max_stream_data_bidi_remote: 16 * 1024,
        initial_max_stream_data_uni: 16 * 1024,
        initial_max_data: 64 * 1024,
        initial_max_streams_bidi: 8,
        initial_max_streams_uni: 8,
        max_idle_timeout: 30_000_000_000,
        // DATAGRAM 拡張を有効にする (RFC 9221 Section 3)
        max_datagram_frame_size: 65535,
        ..DwnxTransportParameters::default()
    }
}

/// テスト用の証明書 (CA とサーバー証明書)
struct TestCertificates {
    /// サーバー証明書のチェーン
    cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    /// サーバー証明書の秘密鍵 (PKCS#8)
    key: rustls::pki_types::PrivatePkcs8KeyDer<'static>,
    /// クライアントが信頼する CA 証明書
    ca_cert: rustls::pki_types::CertificateDer<'static>,
}

/// テスト用の CA と `localhost` のサーバー証明書を生成する
///
/// rcgen で CA (`IsCa::Ca`) を作り、その CA でサーバー証明書に署名する。クライアントは
/// CA 証明書を `rustls::RootCertStore` に入れ、標準の検証器でサーバー証明書を検証する
/// (検証を無効化する検証器は使わない)。
fn generate_certificates() -> TestCertificates {
    // CA 証明書。SAN は持たせず、証明書署名のための鍵用途だけを与える
    let mut ca_params =
        rcgen::CertificateParams::new(Vec::new()).expect("CA のパラメータを作成できる");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "qmux-rs interop test CA");
    let ca_key = rcgen::KeyPair::generate().expect("CA の鍵を生成できる");
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).expect("CA 証明書を作成できる");

    // サーバー証明書。SAN はテストで接続する `localhost` にする
    let mut server_params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("サーバーのパラメータを作成できる");
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    server_params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    // CA で署名した証明書であることを示す Authority Key Identifier を付ける
    server_params.use_authority_key_identifier_extension = true;
    let server_key = rcgen::KeyPair::generate().expect("サーバーの鍵を生成できる");
    let server_cert = server_params
        .signed_by(&server_key, &ca)
        .expect("CA でサーバー証明書に署名できる");

    TestCertificates {
        cert_chain: vec![server_cert.der().clone()],
        key: rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()),
        ca_cert: ca.der().clone(),
    }
}

/// ALPN を設定したサーバー設定を作る
fn server_config(certificates: &TestCertificates) -> Arc<rustls::ServerConfig> {
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            certificates.cert_chain.clone(),
            rustls::pki_types::PrivateKeyDer::Pkcs8(certificates.key.clone_key()),
        )
        .expect("証明書を設定できる");
    config.alpn_protocols = vec![ALPN.to_vec()];
    Arc::new(config)
}

/// CA を信頼するクライアント設定を作る
fn client_config(ca_cert: rustls::pki_types::CertificateDer<'static>) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca_cert).expect("CA 証明書を追加できる");
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![ALPN.to_vec()];
    Arc::new(config)
}

/// テストの待ち時間の上限
///
/// 実装の退行でテストがハングしないよう、すべての待ちに上限を設ける。
const GUARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// guard timeout 付きで future を待つ
async fn with_guard<T>(future: impl std::future::Future<Output = T>) -> T {
    match tokio::time::timeout(GUARD_TIMEOUT, future).await {
        Ok(value) => value,
        Err(_) => panic!("guard timeout ({GUARD_TIMEOUT:?}) を超えた"),
    }
}

/// tokio-qmux のストリームを終端まで読み取る
async fn read_all_qmux(recv: &mut tokio_qmux::RecvStream) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match recv.read(&mut buf).await.expect("読み取りに失敗した") {
            Some(0) => continue,
            Some(len) => data.extend_from_slice(&buf[..len]),
            None => return data,
        }
    }
}

/// tokio-dwnx のストリームを終端まで読み取る
async fn read_all_dwnx(recv: &mut tokio_dwnx::RecvStream) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match recv.read(&mut buf).await.expect("読み取りに失敗した") {
            Some(0) => continue,
            Some(len) => data.extend_from_slice(&buf[..len]),
            None => return data,
        }
    }
}

#[tokio::test]
async fn qmux_client_dwnx_server_over_tls() {
    // aws-lc-sys をプロバイダとして使う
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // テスト用の CA と `localhost` のサーバー証明書を生成する
    let certificates = generate_certificates();
    let tls_server_config = server_config(&certificates);
    let tls_client_config = client_config(certificates.ca_cert.clone());

    let listener = with_guard(TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("listen できる");
    let addr = listener.local_addr().expect("addr を取得できる");

    let server = tokio::spawn(async move {
        let (tcp, _) = with_guard(listener.accept()).await.expect("accept できる");
        let acceptor = TlsAcceptor::from(tls_server_config);
        let tls = with_guard(acceptor.accept(tcp))
            .await
            .expect("TLS ハンドシェイクできる");
        assert_eq!(
            tls.get_ref().1.alpn_protocol(),
            Some(ALPN),
            "ALPN がネゴシエートされること"
        );

        let connection = tokio_dwnx::Connection::new(DwnxRole::Server, tls, dwnx_config())
            .expect("QMux 接続を作成できる");
        let (mut send, mut recv) = with_guard(connection.accept_bidirectional())
            .await
            .expect("ストリームが届く");

        let data = with_guard(read_all_dwnx(&mut recv)).await;
        assert_eq!(data, b"hello from qmux", "受信データが一致すること");

        // エコーを返す
        let written = with_guard(send.write(&data)).await.expect("書き込める");
        assert_eq!(written, data.len(), "全データを受け付けること");
        with_guard(send.finish()).await.expect("FIN を送れる");
    });

    let tcp = with_guard(TcpStream::connect(addr))
        .await
        .expect("接続できる");
    let connector = TlsConnector::from(tls_client_config);
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost").expect("名前を変換できる");
    let tls = with_guard(connector.connect(server_name, tcp))
        .await
        .expect("TLS ハンドシェイクできる");
    assert_eq!(
        tls.get_ref().1.alpn_protocol(),
        Some(ALPN),
        "ALPN がネゴシエートされること"
    );

    let connection =
        QmuxConnection::new(QmuxRole::Client, tls, qmux_config()).expect("QMux 接続を作成できる");
    let (mut send, mut recv) = with_guard(connection.open_bidirectional())
        .await
        .expect("ストリームを開ける");
    with_guard(send.write(b"hello from qmux"))
        .await
        .expect("書き込める");
    with_guard(send.finish()).await.expect("FIN を送れる");

    let echoed = with_guard(read_all_qmux(&mut recv)).await;
    assert_eq!(echoed, b"hello from qmux", "エコーが一致すること");

    with_guard(server).await.expect("サーバータスクが終了する");
}

#[tokio::test]
async fn datagram_roundtrip_over_tls() {
    // aws-lc-sys をプロバイダとして使う
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // テスト用の CA と `localhost` のサーバー証明書を生成する
    let certificates = generate_certificates();
    let tls_server_config = server_config(&certificates);
    let tls_client_config = client_config(certificates.ca_cert.clone());

    let listener = with_guard(TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("listen できる");
    let addr = listener.local_addr().expect("addr を取得できる");

    let server = tokio::spawn(async move {
        let (tcp, _) = with_guard(listener.accept()).await.expect("accept できる");
        let acceptor = TlsAcceptor::from(tls_server_config);
        let tls = with_guard(acceptor.accept(tcp))
            .await
            .expect("TLS ハンドシェイクできる");
        assert_eq!(
            tls.get_ref().1.alpn_protocol(),
            Some(ALPN),
            "ALPN がネゴシエートされること"
        );

        let connection = tokio_dwnx::Connection::new(DwnxRole::Server, tls, dwnx_config())
            .expect("QMux 接続を作成できる");

        // クライアントからの DATAGRAM を受信し、エコーを返す
        let data = with_guard(connection.recv_datagram())
            .await
            .expect("受信に失敗しない")
            .expect("DATAGRAM が届く");
        assert_eq!(data, b"hello datagram", "受信した DATAGRAM が一致すること");
        with_guard(connection.send_datagram(&data))
            .await
            .expect("エコーの DATAGRAM を送れる");

        // 空の DATAGRAM も往復させる (RFC 9221 Section 4)
        let empty = with_guard(connection.recv_datagram())
            .await
            .expect("受信に失敗しない")
            .expect("空の DATAGRAM が届く");
        assert!(empty.is_empty(), "空の DATAGRAM が届くこと");
        with_guard(connection.send_datagram(&empty))
            .await
            .expect("空の DATAGRAM を送れる");
    });

    let tcp = with_guard(TcpStream::connect(addr))
        .await
        .expect("接続できる");
    let connector = TlsConnector::from(tls_client_config);
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost").expect("名前を変換できる");
    let tls = with_guard(connector.connect(server_name, tcp))
        .await
        .expect("TLS ハンドシェイクできる");
    assert_eq!(
        tls.get_ref().1.alpn_protocol(),
        Some(ALPN),
        "ALPN がネゴシエートされること"
    );

    let connection =
        QmuxConnection::new(QmuxRole::Client, tls, qmux_config()).expect("QMux 接続を作成できる");

    // tokio-qmux クライアントから tokio-dwnx サーバーへ DATAGRAM が届くこと
    with_guard(connection.send_datagram(b"hello datagram"))
        .await
        .expect("DATAGRAM を送れる");
    let echoed = with_guard(connection.recv_datagram())
        .await
        .expect("受信に失敗しない")
        .expect("エコーの DATAGRAM が届く");
    assert_eq!(
        echoed, b"hello datagram",
        "tokio-dwnx がエコーした DATAGRAM が一致すること"
    );

    // 空の DATAGRAM も往復すること
    with_guard(connection.send_datagram(b""))
        .await
        .expect("空の DATAGRAM を送れる");
    let echoed = with_guard(connection.recv_datagram())
        .await
        .expect("受信に失敗しない")
        .expect("空のエコーの DATAGRAM が届く");
    assert!(echoed.is_empty(), "空の DATAGRAM がエコーされること");

    with_guard(server).await.expect("サーバータスクが終了する");
}

#[tokio::test]
async fn dwnx_client_qmux_server_over_tls() {
    // 逆方向の役割の組み合わせ: tokio-dwnx クライアントから tokio-qmux サーバーへ
    // TLS で接続する。tokio-dwnx の open / finish / close (linger) と tokio-qmux の
    // accept の組み合わせを、ストリームと DATAGRAM の両方で検証する。
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let certificates = generate_certificates();
    let tls_server_config = server_config(&certificates);
    let tls_client_config = client_config(certificates.ca_cert.clone());

    let listener = with_guard(TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("listen できる");
    let addr = listener.local_addr().expect("addr を取得できる");

    let server = tokio::spawn(async move {
        let (tcp, _) = with_guard(listener.accept()).await.expect("accept できる");
        let acceptor = TlsAcceptor::from(tls_server_config);
        let tls = with_guard(acceptor.accept(tcp))
            .await
            .expect("TLS ハンドシェイクできる");
        assert_eq!(
            tls.get_ref().1.alpn_protocol(),
            Some(ALPN),
            "ALPN がネゴシエートされること"
        );

        let connection = QmuxConnection::new(QmuxRole::Server, tls, qmux_config())
            .expect("QMux 接続を作成できる");
        let (mut send, mut recv) = with_guard(connection.accept_bidirectional())
            .await
            .expect("accept に失敗しない")
            .expect("ストリームが届く");

        let data = with_guard(read_all_qmux(&mut recv)).await;
        assert_eq!(data, b"hello from dwnx", "受信データが一致すること");

        // エコーを返す
        with_guard(send.write(&data)).await.expect("書き込める");
        with_guard(send.finish()).await.expect("FIN を送れる");

        // DATAGRAM も逆方向に往復させる
        let datagram = with_guard(connection.recv_datagram())
            .await
            .expect("受信に失敗しない")
            .expect("DATAGRAM が届く");
        assert_eq!(
            datagram, b"dwnx datagram",
            "受信した DATAGRAM が一致すること"
        );
        with_guard(connection.send_datagram(&datagram))
            .await
            .expect("DATAGRAM を送れる");
    });

    let tcp = with_guard(TcpStream::connect(addr))
        .await
        .expect("接続できる");
    let connector = TlsConnector::from(tls_client_config);
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost").expect("名前を変換できる");
    let tls = with_guard(connector.connect(server_name, tcp))
        .await
        .expect("TLS ハンドシェイクできる");
    assert_eq!(
        tls.get_ref().1.alpn_protocol(),
        Some(ALPN),
        "ALPN がネゴシエートされること"
    );

    let connection = tokio_dwnx::Connection::new(DwnxRole::Client, tls, dwnx_config())
        .expect("QMux 接続を作成できる");

    // tokio-dwnx クライアントから tokio-qmux サーバーへ双方向ストリームで送る
    let (mut send, mut recv) = with_guard(connection.open_bidirectional())
        .await
        .expect("ストリームを開ける");
    with_guard(send.write(b"hello from dwnx"))
        .await
        .expect("書き込める");
    with_guard(send.finish()).await.expect("FIN を送れる");

    let echoed = with_guard(read_all_dwnx(&mut recv)).await;
    assert_eq!(echoed, b"hello from dwnx", "エコーが一致すること");

    // DATAGRAM も往復させる
    with_guard(connection.send_datagram(b"dwnx datagram"))
        .await
        .expect("DATAGRAM を送れる");
    let echoed = with_guard(connection.recv_datagram())
        .await
        .expect("受信に失敗しない")
        .expect("エコーの DATAGRAM が届く");
    assert_eq!(
        echoed, b"dwnx datagram",
        "tokio-qmux がエコーした DATAGRAM が一致すること"
    );

    // tokio-dwnx の close (linger) でサーバーが接続の終了を観測すること
    connection.close(0, "done");
    with_guard(server).await.expect("サーバータスクが終了する");
}
