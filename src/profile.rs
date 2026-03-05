use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncProfile {
    pub name: String,
    pub server: String,
    pub server_name: String,
    pub ca: PathBuf,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    pub ssh_target: Option<String>,
    pub destination: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ProfileStore {
    profiles: Vec<SyncProfile>,
}

fn env_root(var: &str) -> Option<PathBuf> {
    let value = std::env::var(var).ok()?;
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty() {
        return None;
    }
    Some(path)
}

fn config_root() -> PathBuf {
    if let Ok(dir) = std::env::var("SPARSYNC_CONFIG_DIR") {
        let path = PathBuf::from(dir);
        if !path.as_os_str().is_empty() {
            return path;
        }
    }

    if let Some(mut path) = env_root("XDG_CONFIG_HOME") {
        path.push("sparsync");
        return path;
    }

    if let Ok(home) = std::env::var("HOME") {
        let mut path = PathBuf::from(home);
        path.push(".config");
        path.push("sparsync");
        return path;
    }

    PathBuf::from(".sparsync")
}

fn data_root() -> PathBuf {
    if let Some(path) = env_root("SPARSYNC_DATA_DIR") {
        return path;
    }

    if let Some(mut path) = env_root("XDG_DATA_HOME") {
        path.push("sparsync");
        return path;
    }

    if let Ok(home) = std::env::var("HOME") {
        let mut path = PathBuf::from(home);
        path.push(".local");
        path.push("share");
        path.push("sparsync");
        return path;
    }

    PathBuf::from(".sparsync-data")
}

fn set_mode_if_unix(path: &std::path::Path, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        let mut perms = std::fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .permissions();
        perms.set_mode(mode);
        std::fs::set_permissions(path, perms)
            .with_context(|| format!("chmod {:o} {}", mode, path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
    Ok(())
}

fn ensure_dir(path: &std::path::Path, mode: u32) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
    set_mode_if_unix(path, mode)
}

pub fn ensure_config_root() -> Result<PathBuf> {
    let dir = config_root();
    ensure_dir(&dir, 0o755)?;
    Ok(dir)
}

pub fn ensure_data_root() -> Result<PathBuf> {
    let dir = data_root();
    ensure_dir(&dir, 0o755)?;
    Ok(dir)
}

pub fn ensure_secrets_root() -> Result<PathBuf> {
    let mut dir = ensure_data_root()?;
    dir.push("secrets");
    ensure_dir(&dir, 0o700)?;
    Ok(dir)
}

pub fn profiles_path() -> Result<PathBuf> {
    let mut path = ensure_config_root()?;
    path.push("profiles.json");
    Ok(path)
}

fn profile_component(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("profile name cannot be empty");
    }
    let out: String = trimmed
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.trim_matches('_').is_empty() {
        bail!("profile name '{}' has no usable path component", value);
    }
    Ok(out)
}

pub fn ensure_profile_secret_dir(profile_name: &str) -> Result<PathBuf> {
    let profile_component = profile_component(profile_name)?;
    let mut root = ensure_secrets_root()?;
    root.push("profiles");
    ensure_dir(&root, 0o700)?;

    let mut path = root;
    path.push(profile_component);
    ensure_dir(&path, 0o700)?;
    migrate_legacy_bootstrap(profile_name, &path)?;
    Ok(path)
}

fn migrate_legacy_bootstrap(profile_name: &str, target_dir: &std::path::Path) -> Result<()> {
    let mut legacy = ensure_config_root()?;
    legacy.push("bootstrap");
    legacy.push(profile_name);
    if !legacy.exists() || legacy == target_dir {
        return Ok(());
    }

    let entries = std::fs::read_dir(&legacy)
        .with_context(|| format!("read legacy bootstrap dir {}", legacy.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("read entry in {}", legacy.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }
        let dest = target_dir.join(entry.file_name());
        if dest.exists() {
            continue;
        }
        std::fs::copy(entry.path(), &dest).with_context(|| {
            format!(
                "copy legacy secret {} -> {}",
                entry.path().display(),
                dest.display()
            )
        })?;
        set_mode_if_unix(&dest, 0o600)?;
    }
    Ok(())
}

pub fn write_secret_file(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent, 0o700)?;
    }
    std::fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    set_mode_if_unix(path, 0o600)?;
    Ok(())
}

pub fn enforce_private_file(path: &std::path::Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        let mode = std::fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            bail!(
                "secret file {} has insecure permissions {:o} (expected 0600 or stricter)",
                path.display(),
                mode
            );
        }
    }
    Ok(())
}

fn load_store() -> Result<ProfileStore> {
    let path = profiles_path()?;
    let bytes = match std::fs::read(&path) {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ProfileStore::default());
        }
        Err(err) => {
            return Err(err).with_context(|| format!("read {}", path.display()));
        }
    };
    let store: ProfileStore =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    Ok(store)
}

fn save_store(store: &ProfileStore) -> Result<()> {
    let path = profiles_path()?;
    let bytes = serde_json::to_vec_pretty(store).context("serialize profile store")?;
    std::fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub fn get_profile(name: &str) -> Result<SyncProfile> {
    let store = load_store()?;
    let profile = store
        .profiles
        .into_iter()
        .find(|p| p.name == name)
        .ok_or_else(|| anyhow::anyhow!("profile '{}' not found", name))?;
    if let Some(client_key) = profile.client_key.as_deref() {
        enforce_private_file(client_key)?;
    }
    Ok(profile)
}

pub fn upsert_profile(profile: SyncProfile) -> Result<()> {
    if profile.name.trim().is_empty() {
        bail!("profile name cannot be empty");
    }
    let mut store = load_store()?;
    if let Some(existing) = store.profiles.iter_mut().find(|v| v.name == profile.name) {
        *existing = profile;
    } else {
        store.profiles.push(profile);
    }
    save_store(&store)
}
