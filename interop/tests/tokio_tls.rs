//! QMux (tokio-qmux) と dwnx (tokio-dwnx) の tokio + TLS 相互接続テスト
//!
//! TCP + TLS (rustls + aws-lc-sys) + ALPN で接続し、クライアントから
//! 双方向ストリームでデータを送り、サーバーがエコーを返すことを検証する。

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
        ..DwnxTransportParameters::default()
    }
}

/// ALPN を設定したサーバー設定を作る
fn server_config() -> Arc<rustls::ServerConfig> {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("自己署名証明書を作れる");
    let cert = certified.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());

    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key.into())
        .expect("証明書を設定できる");
    config.alpn_protocols = vec![ALPN.to_vec()];
    Arc::new(config)
}

/// 証明書を検証しないクライアント設定を作る (テスト専用)
fn client_config() -> Arc<rustls::ClientConfig> {
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    config.alpn_protocols = vec![ALPN.to_vec()];
    Arc::new(config)
}

/// 検証対象の証明書を無条件に受け入れる検証器 (テスト専用)
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
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

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listen できる");
    let addr = listener.local_addr().expect("addr を取得できる");

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept できる");
        let acceptor = TlsAcceptor::from(server_config());
        let tls = acceptor
            .accept(tcp)
            .await
            .expect("TLS ハンドシェイクできる");
        assert_eq!(
            tls.get_ref().1.alpn_protocol(),
            Some(ALPN),
            "ALPN がネゴシエートされること"
        );

        let mut connection = tokio_dwnx::Connection::new(DwnxRole::Server, tls, &dwnx_config());
        let (mut send, mut recv) = connection
            .accept_bidirectional()
            .await
            .expect("accept に失敗しない")
            .expect("ストリームが届く");

        let data = read_all_dwnx(&mut recv).await;
        assert_eq!(data, b"hello from qmux", "受信データが一致すること");

        // エコーを返す
        send.write(&data).await.expect("書き込める");
        send.finish().await.expect("FIN を送れる");
    });

    let tcp = TcpStream::connect(addr).await.expect("接続できる");
    let connector = TlsConnector::from(client_config());
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost").expect("名前を変換できる");
    let tls = connector
        .connect(server_name, tcp)
        .await
        .expect("TLS ハンドシェイクできる");
    assert_eq!(
        tls.get_ref().1.alpn_protocol(),
        Some(ALPN),
        "ALPN がネゴシエートされること"
    );

    let connection =
        QmuxConnection::new(QmuxRole::Client, tls, qmux_config()).expect("QMux 接続を作成できる");
    let (mut send, mut recv) = connection
        .open_bidirectional()
        .await
        .expect("ストリームを開ける");
    send.write(b"hello from qmux").await.expect("書き込める");
    send.finish().await.expect("FIN を送れる");

    let echoed = read_all_qmux(&mut recv).await;
    assert_eq!(echoed, b"hello from qmux", "エコーが一致すること");

    server.await.expect("サーバータスクが終了する");
}
