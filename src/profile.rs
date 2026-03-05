use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
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

fn config_root() -> PathBuf {
    if let Ok(dir) = std::env::var("SPARSYNC_CONFIG_DIR") {
        let path = PathBuf::from(dir);
        if !path.as_os_str().is_empty() {
            return path;
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let mut path = PathBuf::from(home);
        path.push(".config");
        path.push("sparsync");
        return path;
    }

    PathBuf::from(".sparsync")
}

pub fn ensure_config_root() -> Result<PathBuf> {
    let dir = config_root();
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    Ok(dir)
}

pub fn profiles_path() -> Result<PathBuf> {
    let mut path = ensure_config_root()?;
    path.push("profiles.json");
    Ok(path)
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
    store
        .profiles
        .into_iter()
        .find(|p| p.name == name)
        .ok_or_else(|| anyhow::anyhow!("profile '{}' not found", name))
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
