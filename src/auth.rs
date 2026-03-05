use crate::certs;
use crate::util::sanitize_relative;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AuthLayout {
    pub root: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub client_ca_cert: PathBuf,
    pub client_ca_key: PathBuf,
    pub clients_dir: PathBuf,
    pub authz_path: PathBuf,
}

impl AuthLayout {
    pub fn under(root: &Path) -> Self {
        let root = root.to_path_buf();
        Self {
            server_cert: root.join("server.cert.der"),
            server_key: root.join("server.key.der"),
            client_ca_cert: root.join("client-ca.cert.der"),
            client_ca_key: root.join("client-ca.key.der"),
            clients_dir: root.join("clients"),
            authz_path: root.join("authz.json"),
            root,
        }
    }

    pub fn client_cert_path(&self, client_id: &str) -> PathBuf {
        self.clients_dir.join(format!("{client_id}.cert.der"))
    }

    pub fn client_key_path(&self, client_id: &str) -> PathBuf {
        self.clients_dir.join(format!("{client_id}.key.der"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzEntry {
    pub client_id: String,
    pub fingerprint_sha256: String,
    #[serde(default = "default_allowed_prefixes")]
    pub allowed_prefixes: Vec<String>,
    pub revoked: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthzPolicy {
    pub entries: Vec<AuthzEntry>,
}

impl AuthzPolicy {
    pub fn entry_for_fingerprint(&self, fingerprint: &str) -> Option<&AuthzEntry> {
        self.entries
            .iter()
            .find(|entry| entry.fingerprint_sha256 == fingerprint)
    }

    pub fn upsert_allow(
        &mut self,
        client_id: &str,
        fingerprint: String,
        allowed_prefixes: Vec<String>,
    ) {
        if let Some(entry) = self.entries.iter_mut().find(|v| v.client_id == client_id) {
            entry.fingerprint_sha256 = fingerprint;
            entry.allowed_prefixes = allowed_prefixes;
            entry.revoked = false;
            return;
        }
        self.entries.push(AuthzEntry {
            client_id: client_id.to_string(),
            fingerprint_sha256: fingerprint,
            allowed_prefixes,
            revoked: false,
        });
    }

    pub fn revoke(&mut self, client_id: &str) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|v| v.client_id == client_id) {
            entry.revoked = true;
            return true;
        }
        false
    }
}

fn default_allowed_prefixes() -> Vec<String> {
    vec!["/".to_string()]
}

fn normalize_prefix(prefix: &str) -> Result<String> {
    let trimmed = prefix.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return Ok("/".to_string());
    }

    let relative = trimmed.trim_start_matches('/');
    let cleaned = sanitize_relative(relative)
        .with_context(|| format!("invalid allowed prefix '{}'", prefix))?;
    let normalized = cleaned.to_string_lossy().replace('\\', "/");
    let normalized = normalized.trim_end_matches('/');
    if normalized.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(normalized.to_string())
    }
}

pub fn normalize_allowed_prefixes(prefixes: &[String]) -> Result<Vec<String>> {
    if prefixes.is_empty() {
        return Ok(default_allowed_prefixes());
    }

    let mut out = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        let normalized = normalize_prefix(prefix)?;
        if normalized == "/" {
            return Ok(default_allowed_prefixes());
        }
        if !out.iter().any(|existing| existing == &normalized) {
            out.push(normalized);
        }
    }

    if out.is_empty() {
        Ok(default_allowed_prefixes())
    } else {
        Ok(out)
    }
}

pub fn load_authz_policy(path: &Path) -> Result<AuthzPolicy> {
    let bytes = match std::fs::read(path) {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(AuthzPolicy::default()),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let policy: AuthzPolicy =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    Ok(policy)
}

pub fn save_authz_policy(path: &Path, policy: &AuthzPolicy) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create authz dir {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(policy).context("serialize authz policy")?;
    std::fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub struct InitServerResult {
    pub layout: AuthLayout,
}

pub fn init_server(layout: &AuthLayout, server_name: &str) -> Result<InitServerResult> {
    std::fs::create_dir_all(&layout.root)
        .with_context(|| format!("create auth root {}", layout.root.display()))?;
    std::fs::create_dir_all(&layout.clients_dir)
        .with_context(|| format!("create clients dir {}", layout.clients_dir.display()))?;

    if !layout.server_cert.exists() || !layout.server_key.exists() {
        certs::generate_self_signed(
            &layout.server_cert,
            &layout.server_key,
            &[server_name.to_string()],
        )
        .context("generate server certificate")?;
    }
    if !layout.client_ca_cert.exists() || !layout.client_ca_key.exists() {
        certs::generate_ca(
            &layout.client_ca_cert,
            &layout.client_ca_key,
            "sparsync-client-ca",
        )
        .context("generate client CA")?;
    }
    if !layout.authz_path.exists() {
        save_authz_policy(&layout.authz_path, &AuthzPolicy::default())
            .context("initialize authz policy")?;
    }

    Ok(InitServerResult {
        layout: layout.clone(),
    })
}

pub struct IssueClientResult {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub fingerprint_sha256: String,
    pub allowed_prefixes: Vec<String>,
}

pub fn issue_client(
    layout: &AuthLayout,
    client_id: &str,
    allowed_prefixes: &[String],
) -> Result<IssueClientResult> {
    if client_id.trim().is_empty() {
        bail!("client id cannot be empty");
    }
    std::fs::create_dir_all(&layout.clients_dir)
        .with_context(|| format!("create clients dir {}", layout.clients_dir.display()))?;

    let cert_path = layout.client_cert_path(client_id);
    let key_path = layout.client_key_path(client_id);
    certs::issue_cert_signed_by_ca(
        &layout.client_ca_cert,
        &layout.client_ca_key,
        &cert_path,
        &key_path,
        client_id,
        &[],
        Some(&format!("spiffe://sparsync/clients/{client_id}")),
        true,
        false,
    )
    .with_context(|| format!("issue client cert for {}", client_id))?;

    let cert_der =
        std::fs::read(&cert_path).with_context(|| format!("read {}", cert_path.display()))?;
    let fingerprint = certs::certificate_fingerprint_sha256(&cert_der);
    let allowed_prefixes = normalize_allowed_prefixes(allowed_prefixes)?;
    let mut policy = load_authz_policy(&layout.authz_path)?;
    policy.upsert_allow(client_id, fingerprint.clone(), allowed_prefixes.clone());
    save_authz_policy(&layout.authz_path, &policy)?;

    Ok(IssueClientResult {
        cert_path,
        key_path,
        fingerprint_sha256: fingerprint,
        allowed_prefixes,
    })
}

pub fn revoke_client(layout: &AuthLayout, client_id: &str) -> Result<bool> {
    let mut policy = load_authz_policy(&layout.authz_path)?;
    let found = policy.revoke(client_id);
    save_authz_policy(&layout.authz_path, &policy)?;
    Ok(found)
}

pub struct AuthStatus {
    pub total_clients: usize,
    pub active_clients: usize,
    pub revoked_clients: usize,
}

pub fn auth_status(layout: &AuthLayout) -> Result<AuthStatus> {
    let policy = load_authz_policy(&layout.authz_path)?;
    let total = policy.entries.len();
    let revoked = policy.entries.iter().filter(|v| v.revoked).count();
    Ok(AuthStatus {
        total_clients: total,
        active_clients: total.saturating_sub(revoked),
        revoked_clients: revoked,
    })
}

pub fn rotate_client_ca(layout: &AuthLayout) -> Result<()> {
    certs::generate_ca(
        &layout.client_ca_cert,
        &layout.client_ca_key,
        "sparsync-client-ca",
    )
    .context("rotate client CA")?;
    save_authz_policy(&layout.authz_path, &AuthzPolicy::default())
        .context("reset authz policy after client CA rotation")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AuthzPolicy, normalize_allowed_prefixes};

    #[test]
    fn authz_allow_and_revoke_flow() {
        let mut policy = AuthzPolicy::default();
        policy.upsert_allow("alice", "abc".to_string(), vec!["/".to_string()]);
        let active = policy
            .entry_for_fingerprint("abc")
            .expect("active fingerprint missing");
        assert!(!active.revoked);
        assert!(policy.entry_for_fingerprint("def").is_none());

        let changed = policy.revoke("alice");
        assert!(changed);
        let revoked = policy
            .entry_for_fingerprint("abc")
            .expect("revoked fingerprint missing");
        assert!(revoked.revoked);
    }

    #[test]
    fn normalize_prefixes_defaults_to_root() {
        let normalized = normalize_allowed_prefixes(&[]).expect("normalize prefixes");
        assert_eq!(normalized, vec!["/".to_string()]);
    }

    #[test]
    fn normalize_prefixes_collapses_root_and_deduplicates() {
        let normalized = normalize_allowed_prefixes(&[
            "team-a/".to_string(),
            "/team-a".to_string(),
            "team-b".to_string(),
        ])
        .expect("normalize prefixes");
        assert_eq!(normalized, vec!["team-a".to_string(), "team-b".to_string()]);

        let root = normalize_allowed_prefixes(&["/".to_string(), "team-c".to_string()])
            .expect("normalize root");
        assert_eq!(root, vec!["/".to_string()]);
    }
}
