use crate::protocol::{InitAction, InitFileRequest, InitFileResponse};
use anyhow::{Context, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use spargio::{RuntimeHandle, fs};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteFile {
    pub file_hash: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    #[serde(default)]
    pub xattr_sig: Option<u64>,
    pub total_chunks: usize,
    pub updated_at_sec: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialFile {
    pub file_hash: String,
    pub size: u64,
    pub chunk_size: usize,
    pub total_chunks: usize,
    pub completed_chunks: usize,
    pub updated_at_sec: i64,
}

#[derive(Debug, Clone)]
pub struct CompleteFileInput {
    pub relative_path: String,
    pub file_hash: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub xattr_sig: Option<u64>,
    pub total_chunks: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ResumeState {
    complete: HashMap<String, CompleteFile>,
    partial: HashMap<String, PartialFile>,
}

#[derive(Clone)]
pub struct StateStore {
    handle: RuntimeHandle,
    state_path: PathBuf,
    partial_root: PathBuf,
    inner: Arc<Mutex<ResumeState>>,
    persist_counter: Arc<AtomicUsize>,
    persist_stride: usize,
}

impl StateStore {
    pub async fn open(handle: RuntimeHandle, destination: &Path) -> Result<Self> {
        let state_root = destination.join(".sparsync");
        let partial_root = state_root.join("partials");
        fs::create_dir_all(&handle, &partial_root)
            .await
            .with_context(|| format!("create state root {}", partial_root.display()))?;

        let state_path = state_root.join("state.json");
        let state = match fs::read(&handle, &state_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parse state file {}", state_path.display()))?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => ResumeState::default(),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read state file {}", state_path.display()));
            }
        };

        Ok(Self {
            handle,
            state_path,
            partial_root,
            inner: Arc::new(Mutex::new(state)),
            persist_counter: Arc::new(AtomicUsize::new(0)),
            persist_stride: 1024,
        })
    }

    pub fn partial_root(&self) -> &Path {
        &self.partial_root
    }

    pub fn is_complete_match(
        &self,
        relative_path: &str,
        file_hash: &str,
        size: u64,
    ) -> Result<bool> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("state mutex poisoned"))?;
        Ok(guard
            .complete
            .get(relative_path)
            .is_some_and(|done| done.file_hash == file_hash && done.size == size))
    }

    pub async fn initialize_file(
        &self,
        req: &InitFileRequest,
        existing_partial_chunks: Option<usize>,
    ) -> Result<InitFileResponse> {
        let now = now_sec();
        let response = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("state mutex poisoned"))?;

            if let Some(done) = guard.complete.get(&req.relative_path) {
                if done.file_hash == req.file_hash && done.size == req.size {
                    let resp = InitFileResponse {
                        action: InitAction::Skip,
                        next_chunk: req.total_chunks,
                        metadata_sync_required: done.mode != req.mode
                            || done.mtime_sec != req.mtime_sec
                            || done.xattr_sig != req.xattr_sig,
                        message: "already up to date".to_string(),
                    };
                    return Ok(resp);
                }
            }

            let mut next_chunk = 0usize;
            if req.resume {
                if let Some(partial) = guard.partial.get(&req.relative_path) {
                    if partial.file_hash == req.file_hash
                        && partial.size == req.size
                        && partial.chunk_size == req.chunk_size
                    {
                        next_chunk = partial.completed_chunks.min(req.total_chunks);
                    }
                }
                if let Some(existing) = existing_partial_chunks {
                    next_chunk = next_chunk.max(existing.min(req.total_chunks));
                }
            }

            guard.partial.insert(
                req.relative_path.clone(),
                PartialFile {
                    file_hash: req.file_hash.clone(),
                    size: req.size,
                    chunk_size: req.chunk_size,
                    total_chunks: req.total_chunks,
                    completed_chunks: next_chunk,
                    updated_at_sec: now,
                },
            );

            InitFileResponse {
                action: InitAction::Upload,
                next_chunk,
                metadata_sync_required: false,
                message: if next_chunk > 0 {
                    format!("resuming at chunk {next_chunk}")
                } else {
                    "starting upload".to_string()
                },
            }
        };

        self.persist_if_needed(false).await?;
        Ok(response)
    }

    pub async fn current_chunk(&self, key: &str) -> Result<Option<usize>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("state mutex poisoned"))?;
        Ok(guard.partial.get(key).map(|p| p.completed_chunks))
    }

    pub async fn update_partial_progress(
        &self,
        key: &str,
        completed_chunks: usize,
        persist: bool,
    ) -> Result<usize> {
        let now = now_sec();
        let next_chunk = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("state mutex poisoned"))?;
            let partial = guard
                .partial
                .get_mut(key)
                .ok_or_else(|| anyhow::anyhow!("no partial state for {key}"))?;

            if completed_chunks > partial.completed_chunks {
                partial.completed_chunks = completed_chunks.min(partial.total_chunks);
            }
            partial.updated_at_sec = now;

            partial.completed_chunks
        };

        if persist {
            self.persist_if_needed(false).await?;
        }
        Ok(next_chunk)
    }

    pub async fn complete_file(
        &self,
        relative_path: &str,
        file_hash: &str,
        size: u64,
        mode: u32,
        mtime_sec: i64,
        xattr_sig: Option<u64>,
        total_chunks: usize,
    ) -> Result<()> {
        let now = now_sec();
        {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("state mutex poisoned"))?;

            guard.partial.remove(relative_path);
            guard.complete.insert(
                relative_path.to_string(),
                CompleteFile {
                    file_hash: file_hash.to_string(),
                    size,
                    mode,
                    mtime_sec,
                    xattr_sig,
                    total_chunks,
                    updated_at_sec: now,
                },
            );
        }

        self.persist_if_needed(false).await
    }

    pub async fn complete_files_batch(&self, entries: &[CompleteFileInput]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let now = now_sec();
        {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("state mutex poisoned"))?;

            for entry in entries {
                guard.partial.remove(&entry.relative_path);
                guard.complete.insert(
                    entry.relative_path.clone(),
                    CompleteFile {
                        file_hash: entry.file_hash.clone(),
                        size: entry.size,
                        mode: entry.mode,
                        mtime_sec: entry.mtime_sec,
                        xattr_sig: entry.xattr_sig,
                        total_chunks: entry.total_chunks,
                        updated_at_sec: now,
                    },
                );
            }
        }

        self.persist_if_needed(false).await
    }

    pub async fn flush(&self) -> Result<()> {
        self.persist_if_needed(true).await
    }

    async fn persist(&self, bytes: Vec<u8>) -> Result<()> {
        let tmp = self
            .state_path
            .with_extension(format!("json.tmp.{}", rand::rng().random::<u64>()));
        fs::write(&self.handle, &tmp, bytes)
            .await
            .with_context(|| format!("write state temp {}", tmp.display()))?;
        fs::rename(&self.handle, &tmp, &self.state_path)
            .await
            .with_context(|| {
                format!(
                    "replace state file {} from {}",
                    self.state_path.display(),
                    tmp.display()
                )
            })?;
        Ok(())
    }

    async fn persist_if_needed(&self, force: bool) -> Result<()> {
        let persist_now = if force {
            true
        } else {
            let count = self.persist_counter.fetch_add(1, Ordering::Relaxed) + 1;
            count % self.persist_stride == 0
        };

        if !persist_now {
            return Ok(());
        }

        let snapshot = {
            let guard = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("state mutex poisoned"))?;
            serde_json::to_vec(&*guard).context("serialize state")?
        };
        self.persist(snapshot).await
    }
}

fn now_sec() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}
