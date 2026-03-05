use anyhow::{Context, Result, bail};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};
use spargio_quic::quinn;
use std::path::Path;
use std::sync::Arc;

pub const ALPN_SPARSYNC: &[u8] = b"sparsync/1";

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create parent dir {}", parent.display()))?;
    }
    Ok(())
}

fn write_der(cert_path: &Path, cert_der: &[u8], key_path: &Path, key_der: &[u8]) -> Result<()> {
    ensure_parent(cert_path)?;
    ensure_parent(key_path)?;
    std::fs::write(cert_path, cert_der)
        .with_context(|| format!("write cert {}", cert_path.display()))?;
    std::fs::write(key_path, key_der)
        .with_context(|| format!("write key {}", key_path.display()))?;
    Ok(())
}

pub fn generate_self_signed(cert_path: &Path, key_path: &Path, names: &[String]) -> Result<()> {
    let cert =
        rcgen::generate_simple_self_signed(names.to_vec()).context("generate self-signed cert")?;
    write_der(
        cert_path,
        cert.cert.der().as_ref(),
        key_path,
        &cert.key_pair.serialize_der(),
    )
}

pub fn generate_ca(cert_path: &Path, key_path: &Path, common_name: &str) -> Result<()> {
    let mut params = CertificateParams::new(Vec::new()).context("create CA certificate params")?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    params.distinguished_name = dn;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];

    let key = KeyPair::generate().context("generate CA key")?;
    let cert = params.self_signed(&key).context("self-sign CA cert")?;
    write_der(
        cert_path,
        cert.der().as_ref(),
        key_path,
        &key.serialize_der(),
    )
}

pub fn issue_cert_signed_by_ca(
    ca_cert_path: &Path,
    ca_key_path: &Path,
    cert_path: &Path,
    key_path: &Path,
    common_name: &str,
    dns_names: &[String],
    client_uri: Option<&str>,
    client_auth: bool,
    server_auth: bool,
) -> Result<()> {
    let ca_cert_der = std::fs::read(ca_cert_path)
        .with_context(|| format!("read CA cert {}", ca_cert_path.display()))?;
    let ca_key_der = std::fs::read(ca_key_path)
        .with_context(|| format!("read CA key {}", ca_key_path.display()))?;
    let ca_key = KeyPair::try_from(ca_key_der.as_slice()).context("parse CA key")?;
    let ca_params = CertificateParams::from_ca_cert_der(&CertificateDer::from(ca_cert_der))
        .context("parse CA certificate params")?;
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .context("rebuild CA cert for signing")?;

    let mut params = CertificateParams::new(dns_names.to_vec()).context("build cert params")?;
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    if client_auth {
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
    }
    if server_auth {
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ServerAuth);
    }
    if let Some(uri) = client_uri {
        params.subject_alt_names.push(SanType::URI(
            uri.parse()
                .with_context(|| format!("parse SAN URI '{}'", uri))?,
        ));
    }

    let key = KeyPair::generate().context("generate leaf key")?;
    let cert = params
        .signed_by(&key, &ca_cert, &ca_key)
        .context("sign cert with CA")?;
    write_der(
        cert_path,
        cert.der().as_ref(),
        key_path,
        &key.serialize_der(),
    )
}

pub fn certificate_fingerprint_sha256(der: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(der);
    hex::encode(hasher.finalize())
}

pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: Option<&Path>,
) -> Result<quinn::ServerConfig> {
    let cert_der =
        std::fs::read(cert_path).with_context(|| format!("read cert {}", cert_path.display()))?;
    let key_der =
        std::fs::read(key_path).with_context(|| format!("read key {}", key_path.display()))?;

    let cert = rustls::pki_types::CertificateDer::from(cert_der);
    let key = PrivatePkcs8KeyDer::from(key_der).into();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("build rustls server config builder")?;

    let mut server_crypto = if let Some(ca_path) = client_ca_path {
        let ca_der = std::fs::read(ca_path)
            .with_context(|| format!("read client CA certificate {}", ca_path.display()))?;
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(ca_der))
            .context("add client CA cert to trust store")?;
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .context("build webpki client verifier")?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![cert], key)
            .context("build mTLS quic server config")?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .context("build quic server config")?
    };
    server_crypto.alpn_protocols = vec![ALPN_SPARSYNC.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
        .context("convert rustls server config to quic config")?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(quic_crypto)))
}

pub fn load_client_config(
    ca_path: &Path,
    client_cert_path: Option<&Path>,
    client_key_path: Option<&Path>,
) -> Result<quinn::ClientConfig> {
    let ca_der = std::fs::read(ca_path)
        .with_context(|| format!("read CA certificate {}", ca_path.display()))?;
    let cert = rustls::pki_types::CertificateDer::from(ca_der);

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).context("add CA cert to trust store")?;

    if client_cert_path.is_some() != client_key_path.is_some() {
        bail!("client cert and key must be provided together");
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("build rustls client config builder")?
        .with_root_certificates(Arc::new(roots));

    let mut client_crypto =
        if let (Some(cert_path), Some(key_path)) = (client_cert_path, client_key_path) {
            let cert_der = std::fs::read(cert_path)
                .with_context(|| format!("read client cert {}", cert_path.display()))?;
            let key_der = std::fs::read(key_path)
                .with_context(|| format!("read client key {}", key_path.display()))?;
            let cert = CertificateDer::from(cert_der);
            let key = PrivatePkcs8KeyDer::from(key_der).into();
            builder
                .with_client_auth_cert(vec![cert], key)
                .context("build client config with mTLS cert")?
        } else {
            builder.with_no_client_auth()
        };
    client_crypto.alpn_protocols = vec![ALPN_SPARSYNC.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)
        .context("convert rustls client config to quic config")?;
    Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)))
}
