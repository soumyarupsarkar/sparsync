use crate::model::{FileManifest, Manifest};
use crate::util::{join_error, relative_path_string, runtime_error};
use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use spargio::{RuntimeHandle, fs};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const HASH_CACHE_FILE: &str = "hash_cache_v1.json";

#[derive(Debug, Clone, Copy)]
pub struct ScanOptions {
    pub chunk_size: usize,
    pub scan_workers: usize,
    pub hash_workers: usize,
}

#[derive(Debug, Clone)]
pub struct ScanStats {
    pub enumeration_elapsed: Duration,
    pub hash_elapsed: Duration,
    pub total_elapsed: Duration,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub relative_path: String,
    pub kind: FileEntryKind,
    pub symlink_target: Option<String>,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileEntryKind {
    File,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HashCacheEntry {
    size: u64,
    mtime_sec: i64,
    file_hash: String,
    total_chunks: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HashCache {
    chunk_size: usize,
    entries: HashMap<String, HashCacheEntry>,
}

pub async fn build_manifest(
    handle: RuntimeHandle,
    root: &Path,
    options: ScanOptions,
) -> Result<(Manifest, ScanStats)> {
    let total_started = Instant::now();
    let root = fs::canonicalize(&handle, root)
        .await
        .with_context(|| format!("canonicalize {}", root.display()))?;

    let enum_started = Instant::now();
    let files = enumerate_paths(handle.clone(), &root, options.scan_workers.max(1), false).await?;
    let enumeration_elapsed = enum_started.elapsed();
    let chunk_size = options.chunk_size.max(1);

    let hash_cache = load_hash_cache(handle.clone(), &root, chunk_size).await;

    let hash_started = Instant::now();
    let file_manifests = hash_files(
        handle.clone(),
        &root,
        files,
        chunk_size,
        options.hash_workers.max(1),
        Arc::new(hash_cache),
    )
    .await?;
    let hash_elapsed = hash_started.elapsed();

    persist_hash_cache(handle.clone(), &root, chunk_size, &file_manifests).await?;

    let total_bytes = file_manifests.iter().map(|item| item.size).sum();
    let manifest = Manifest {
        root: root.to_string_lossy().into_owned(),
        chunk_size,
        files: file_manifests,
        total_bytes,
    };

    let stats = ScanStats {
        enumeration_elapsed,
        hash_elapsed,
        total_elapsed: total_started.elapsed(),
    };

    Ok((manifest, stats))
}

pub async fn build_manifest_with_symlinks(
    handle: RuntimeHandle,
    root: &Path,
    options: ScanOptions,
) -> Result<(Manifest, Vec<FileEntry>, ScanStats)> {
    let total_started = Instant::now();
    let root = fs::canonicalize(&handle, root)
        .await
        .with_context(|| format!("canonicalize {}", root.display()))?;

    let enum_started = Instant::now();
    let (files, symlink_paths) =
        enumerate_paths_split(handle.clone(), &root, options.scan_workers.max(1), true).await?;
    let enumeration_elapsed = enum_started.elapsed();
    let chunk_size = options.chunk_size.max(1);

    let hash_cache = load_hash_cache(handle.clone(), &root, chunk_size).await;

    let hash_started = Instant::now();
    let file_manifests = hash_files(
        handle.clone(),
        &root,
        files,
        chunk_size,
        options.hash_workers.max(1),
        Arc::new(hash_cache),
    )
    .await?;
    let hash_elapsed = hash_started.elapsed();

    let symlink_entries = if symlink_paths.is_empty() {
        Vec::new()
    } else {
        collect_file_metadata(
            handle.clone(),
            &root,
            symlink_paths,
            options.hash_workers.max(1),
        )
        .await?
    };

    persist_hash_cache(handle.clone(), &root, chunk_size, &file_manifests).await?;

    let total_bytes = file_manifests.iter().map(|item| item.size).sum();
    let manifest = Manifest {
        root: root.to_string_lossy().into_owned(),
        chunk_size,
        files: file_manifests,
        total_bytes,
    };

    let stats = ScanStats {
        enumeration_elapsed,
        hash_elapsed,
        total_elapsed: total_started.elapsed(),
    };

    Ok((manifest, symlink_entries, stats))
}

pub async fn build_file_list(
    handle: RuntimeHandle,
    root: &Path,
    scan_workers: usize,
    metadata_workers: usize,
) -> Result<(PathBuf, Vec<FileEntry>, Duration, Duration)> {
    let root = fs::canonicalize(&handle, root)
        .await
        .with_context(|| format!("canonicalize {}", root.display()))?;

    let enum_started = Instant::now();
    let files = enumerate_paths(handle.clone(), &root, scan_workers.max(1), true).await?;
    let enumeration_elapsed = enum_started.elapsed();

    let metadata_started = Instant::now();
    let entries =
        collect_file_metadata(handle.clone(), &root, files, metadata_workers.max(1)).await?;
    let metadata_elapsed = metadata_started.elapsed();

    Ok((root, entries, enumeration_elapsed, metadata_elapsed))
}

async fn enumerate_paths(
    handle: RuntimeHandle,
    root: &Path,
    workers: usize,
    include_symlinks: bool,
) -> Result<Vec<PathBuf>> {
    let (mut files, symlinks) =
        enumerate_paths_split(handle, root, workers, include_symlinks).await?;
    if include_symlinks {
        files.extend(symlinks);
        files.sort();
    }
    Ok(files)
}

async fn enumerate_paths_split(
    handle: RuntimeHandle,
    root: &Path,
    workers: usize,
    include_symlinks: bool,
) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let directories = Arc::new(Mutex::new(VecDeque::from([root.to_path_buf()])));
    let files = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let symlinks = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let in_progress = Arc::new(AtomicUsize::new(0));

    let mut joins = Vec::with_capacity(workers);
    for _ in 0..workers {
        let handle = handle.clone();
        let task_handle = handle.clone();
        let directories = Arc::clone(&directories);
        let files = Arc::clone(&files);
        let symlinks = Arc::clone(&symlinks);
        let in_progress = Arc::clone(&in_progress);

        let join = handle
            .spawn_stealable(async move {
                loop {
                    let maybe_dir = {
                        let mut guard = directories
                            .lock()
                            .map_err(|_| anyhow::anyhow!("directory queue mutex poisoned"))?;
                        guard.pop_front()
                    };

                    if let Some(dir) = maybe_dir {
                        in_progress.fetch_add(1, Ordering::AcqRel);
                        let entries = fs::read_dir(&task_handle, &dir)
                            .await
                            .with_context(|| format!("read directory {}", dir.display()));
                        in_progress.fetch_sub(1, Ordering::AcqRel);

                        let entries = entries?;
                        for entry in entries {
                            match entry.entry_type {
                                fs::DirEntryType::Directory => {
                                    let mut guard = directories.lock().map_err(|_| {
                                        anyhow::anyhow!("directory queue mutex poisoned")
                                    })?;
                                    guard.push_back(entry.path);
                                }
                                fs::DirEntryType::File => {
                                    let mut guard = files
                                        .lock()
                                        .map_err(|_| anyhow::anyhow!("file list mutex poisoned"))?;
                                    guard.push(entry.path);
                                }
                                fs::DirEntryType::Symlink if include_symlinks => {
                                    let mut guard = symlinks.lock().map_err(|_| {
                                        anyhow::anyhow!("symlink list mutex poisoned")
                                    })?;
                                    guard.push(entry.path);
                                }
                                _ => {}
                            }
                        }
                        continue;
                    }

                    if in_progress.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    spargio::sleep(Duration::from_millis(1)).await;
                }

                Ok::<(), anyhow::Error>(())
            })
            .map_err(|err| runtime_error("spawn scanner task", err))?;
        joins.push(join);
    }

    for join in joins {
        let inner = join
            .await
            .map_err(|err| join_error("scanner task canceled", err))?;
        inner?;
    }

    let mut out = Arc::try_unwrap(files)
        .map_err(|_| anyhow::anyhow!("scanner still holds file list"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("file list mutex poisoned"))?;
    let mut out_symlinks = Arc::try_unwrap(symlinks)
        .map_err(|_| anyhow::anyhow!("scanner still holds symlink list"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("symlink list mutex poisoned"))?;

    out.sort();
    out_symlinks.sort();
    Ok((out, out_symlinks))
}

async fn hash_files(
    handle: RuntimeHandle,
    root: &Path,
    files: Vec<PathBuf>,
    chunk_size: usize,
    workers: usize,
    cache: Arc<HashMap<String, HashCacheEntry>>,
) -> Result<Vec<FileManifest>> {
    let root = root.to_path_buf();
    let mut iter = files.into_iter();
    let mut running = FuturesUnordered::new();

    for _ in 0..workers {
        if let Some(path) = iter.next() {
            running.push(spawn_hash_job(
                handle.clone(),
                root.clone(),
                path,
                chunk_size,
                cache.clone(),
            )?);
        }
    }

    let mut out = Vec::new();
    while let Some(joined) = running.next().await {
        let item = joined.map_err(|err| join_error("hash worker canceled", err))??;
        out.push(item);

        if let Some(path) = iter.next() {
            running.push(spawn_hash_job(
                handle.clone(),
                root.clone(),
                path,
                chunk_size,
                cache.clone(),
            )?);
        }
    }

    out.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(out)
}

async fn collect_file_metadata(
    handle: RuntimeHandle,
    root: &Path,
    files: Vec<PathBuf>,
    workers: usize,
) -> Result<Vec<FileEntry>> {
    let root = root.to_path_buf();
    let mut iter = files.into_iter();
    let mut running = FuturesUnordered::new();

    for _ in 0..workers {
        if let Some(path) = iter.next() {
            running.push(spawn_metadata_job(handle.clone(), root.clone(), path)?);
        }
    }

    let mut out = Vec::new();
    while let Some(joined) = running.next().await {
        let item = joined.map_err(|err| join_error("metadata worker canceled", err))??;
        out.push(item);

        if let Some(path) = iter.next() {
            running.push(spawn_metadata_job(handle.clone(), root.clone(), path)?);
        }
    }

    out.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(out)
}

fn spawn_hash_job(
    handle: RuntimeHandle,
    root: PathBuf,
    absolute_path: PathBuf,
    chunk_size: usize,
    cache: Arc<HashMap<String, HashCacheEntry>>,
) -> Result<spargio::JoinHandle<Result<FileManifest>>> {
    let task_handle = handle.clone();
    handle
        .spawn_stealable(async move {
            hash_one_file(
                task_handle.clone(),
                &root,
                &absolute_path,
                chunk_size,
                cache.as_ref(),
            )
            .await
        })
        .map_err(|err| runtime_error("spawn hash task", err))
}

fn spawn_metadata_job(
    handle: RuntimeHandle,
    root: PathBuf,
    absolute_path: PathBuf,
) -> Result<spargio::JoinHandle<Result<FileEntry>>> {
    let task_handle = handle.clone();
    handle
        .spawn_stealable(async move { metadata_one_file(task_handle, &root, &absolute_path).await })
        .map_err(|err| runtime_error("spawn metadata task", err))
}

async fn hash_one_file(
    handle: RuntimeHandle,
    root: &Path,
    absolute_path: &Path,
    chunk_size: usize,
    cache: &HashMap<String, HashCacheEntry>,
) -> Result<FileManifest> {
    let metadata = fs::metadata_lite(&handle, absolute_path)
        .await
        .with_context(|| format!("stat {}", absolute_path.display()))?;
    if !metadata.is_file() {
        return Err(anyhow::anyhow!("{} is not a file", absolute_path.display()));
    }

    let relative_path = relative_path_string(root, absolute_path)?;
    if let Some(entry) = cache.get(&relative_path) {
        if entry.size == metadata.size && entry.mtime_sec == metadata.mtime_sec {
            return Ok(FileManifest {
                relative_path,
                size: metadata.size,
                mode: metadata.mode as u32,
                mtime_sec: metadata.mtime_sec,
                uid: metadata.uid,
                gid: metadata.gid,
                file_hash: entry.file_hash.clone(),
                total_chunks: entry.total_chunks,
            });
        }
    }

    let file = fs::File::open(handle.clone(), absolute_path)
        .await
        .with_context(|| format!("open {}", absolute_path.display()))?;

    let mut offset = 0u64;
    let mut file_hasher = blake3::Hasher::new();
    let mut total_chunks = 0usize;

    loop {
        let chunk = file
            .read_at(offset, chunk_size)
            .await
            .with_context(|| format!("read {} at offset {}", absolute_path.display(), offset))?;

        if chunk.is_empty() {
            break;
        }

        file_hasher.update(&chunk);
        total_chunks = total_chunks.saturating_add(1);
        offset = offset.saturating_add(chunk.len() as u64);

        if chunk.len() < chunk_size {
            break;
        }
    }

    Ok(FileManifest {
        relative_path,
        size: metadata.size,
        mode: metadata.mode as u32,
        mtime_sec: metadata.mtime_sec,
        uid: metadata.uid,
        gid: metadata.gid,
        file_hash: file_hasher.finalize().to_hex().to_string(),
        total_chunks,
    })
}

async fn metadata_one_file(
    handle: RuntimeHandle,
    root: &Path,
    absolute_path: &Path,
) -> Result<FileEntry> {
    let relative_path = relative_path_string(root, absolute_path)?;
    let symlink_metadata = fs::symlink_metadata(&handle, absolute_path)
        .await
        .with_context(|| format!("lstat {}", absolute_path.display()))?;
    if symlink_metadata.file_type().is_symlink() {
        let target = std::fs::read_link(absolute_path)
            .with_context(|| format!("read symlink {}", absolute_path.display()))?;
        #[cfg(unix)]
        {
            return Ok(FileEntry {
                relative_path,
                kind: FileEntryKind::Symlink,
                symlink_target: Some(target.to_string_lossy().into_owned()),
                size: symlink_metadata.len(),
                mode: symlink_metadata.mode() as u32,
                mtime_sec: symlink_metadata.mtime(),
                uid: symlink_metadata.uid(),
                gid: symlink_metadata.gid(),
            });
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            return Ok(FileEntry {
                relative_path,
                kind: FileEntryKind::Symlink,
                symlink_target: None,
                size: symlink_metadata.len(),
                mode: 0,
                mtime_sec: 0,
                uid: 0,
                gid: 0,
            });
        }
    }

    let metadata = fs::metadata_lite(&handle, absolute_path)
        .await
        .with_context(|| format!("stat {}", absolute_path.display()))?;
    if !metadata.is_file() {
        return Err(anyhow::anyhow!(
            "{} is neither file nor symlink",
            absolute_path.display()
        ));
    }

    Ok(FileEntry {
        relative_path,
        kind: FileEntryKind::File,
        symlink_target: None,
        size: metadata.size,
        mode: metadata.mode as u32,
        mtime_sec: metadata.mtime_sec,
        uid: metadata.uid,
        gid: metadata.gid,
    })
}

async fn load_hash_cache(
    handle: RuntimeHandle,
    root: &Path,
    chunk_size: usize,
) -> HashMap<String, HashCacheEntry> {
    let path = hash_cache_file(root);
    let bytes = match fs::read(&handle, &path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
        Err(_) => return HashMap::new(),
    };

    let cache: HashCache = match serde_json::from_slice(&bytes) {
        Ok(cache) => cache,
        Err(_) => return HashMap::new(),
    };

    if cache.chunk_size != chunk_size {
        return HashMap::new();
    }

    cache.entries
}

async fn persist_hash_cache(
    handle: RuntimeHandle,
    root: &Path,
    chunk_size: usize,
    manifests: &[FileManifest],
) -> Result<()> {
    let path = hash_cache_file(root);
    let Some(dir) = path.parent() else {
        return Ok(());
    };
    fs::create_dir_all(&handle, &dir)
        .await
        .with_context(|| format!("create hash cache dir {}", dir.display()))?;

    let mut entries = HashMap::with_capacity(manifests.len());
    for file in manifests {
        entries.insert(
            file.relative_path.clone(),
            HashCacheEntry {
                size: file.size,
                mtime_sec: file.mtime_sec,
                file_hash: file.file_hash.clone(),
                total_chunks: file.total_chunks,
            },
        );
    }

    let bytes = serde_json::to_vec(&HashCache {
        chunk_size,
        entries,
    })
    .context("serialize hash cache")?;
    fs::write(&handle, &path, bytes)
        .await
        .with_context(|| format!("write hash cache {}", path.display()))?;
    Ok(())
}

fn hash_cache_file(root: &Path) -> PathBuf {
    let root_key = blake3::hash(root.to_string_lossy().as_bytes()).to_hex();
    let mut base = cache_base_dir();
    base.push("sparsync");
    base.push(root_key.as_str());
    base.push(HASH_CACHE_FILE);
    base
}

fn cache_base_dir() -> PathBuf {
    if let Some(value) = std::env::var_os("SPARSYNC_CACHE_DIR") {
        return PathBuf::from(value);
    }

    if let Some(value) = std::env::var_os("XDG_CACHE_HOME") {
        return PathBuf::from(value);
    }

    if let Some(value) = std::env::var_os("HOME") {
        let mut path = PathBuf::from(value);
        path.push(".cache");
        return path;
    }

    let mut fallback = PathBuf::from(OsString::from("/tmp"));
    fallback.push(".cache");
    fallback
}
