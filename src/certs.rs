use anyhow::{Context, Result};
use spargio_quic::quinn;
use std::path::Path;
use std::sync::Arc;

pub fn generate_self_signed(cert_path: &Path, key_path: &Path, names: &[String]) -> Result<()> {
    if let Some(parent) = cert_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create cert dir {}", parent.display()))?;
    }
    if let Some(parent) = key_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create key dir {}", parent.display()))?;
    }

    let cert =
        rcgen::generate_simple_self_signed(names.to_vec()).context("generate self-signed cert")?;
    std::fs::write(cert_path, cert.cert.der().as_ref())
        .with_context(|| format!("write cert {}", cert_path.display()))?;
    std::fs::write(key_path, cert.key_pair.serialize_der())
        .with_context(|| format!("write key {}", key_path.display()))?;
    Ok(())
}

pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<quinn::ServerConfig> {
    let cert_der =
        std::fs::read(cert_path).with_context(|| format!("read cert {}", cert_path.display()))?;
    let key_der =
        std::fs::read(key_path).with_context(|| format!("read key {}", key_path.display()))?;

    let cert = rustls::pki_types::CertificateDer::from(cert_der);
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(key_der).into();

    let config = quinn::ServerConfig::with_single_cert(vec![cert], key)
        .context("build quic server config")?;
    Ok(config)
}

pub fn load_client_config(ca_path: &Path) -> Result<quinn::ClientConfig> {
    let ca_der = std::fs::read(ca_path)
        .with_context(|| format!("read CA certificate {}", ca_path.display()))?;
    let cert = rustls::pki_types::CertificateDer::from(ca_der);

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).context("add CA cert to trust store")?;

    let config = quinn::ClientConfig::with_root_certificates(Arc::new(roots))
        .context("build quic client config")?;
    Ok(config)
}
