use crate::filter::PathFilter;
use crate::metadata;
use crate::util::remove_dir_tree;
use anyhow::{Context, Result};
use spargio::{RuntimeHandle, fs};
use std::collections::HashSet;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const COPY_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct LocalCopyOptions {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub preserve_metadata: bool,
    pub preserve_xattrs: bool,
    pub dry_run: bool,
    pub delete: bool,
    pub update_only: bool,
    pub filter: PathFilter,
    pub bwlimit_kbps: Option<u64>,
    pub progress: bool,
}

#[derive(Debug, Clone, Default)]
pub struct LocalCopySummary {
    pub files_copied: usize,
    pub files_skipped: usize,
    pub files_deleted: usize,
    pub bytes_copied: u64,
}

pub async fn copy_tree(
    handle: RuntimeHandle,
    options: LocalCopyOptions,
) -> Result<LocalCopySummary> {
    let mut summary = LocalCopySummary::default();
    let source = fs::canonicalize(&handle, &options.source)
        .await
        .with_context(|| format!("canonicalize source {}", options.source.display()))?;
    let source_meta = fs::metadata_lite(&handle, &source)
        .await
        .with_context(|| format!("stat source {}", source.display()))?;
    if !source_meta.is_dir() {
        return Err(anyhow::anyhow!(
            "source {} is not a directory",
            source.display()
        ));
    }

    if !options.dry_run {
        fs::create_dir_all(&handle, &options.destination)
            .await
            .with_context(|| {
                format!(
                    "create destination directory {}",
                    options.destination.display()
                )
            })?;
    }

    let bwlimit_bytes_per_sec = options
        .bwlimit_kbps
        .filter(|value| *value > 0)
        .map(|v| v * 1024);
    let mut bw_last_refill = Instant::now();
    let mut bw_tokens = bwlimit_bytes_per_sec.unwrap_or(0) as f64;

    let mut source_set = HashSet::new();
    let source_files = enumerate_relative_files(&handle, &source).await?;
    let total_files = source_files
        .iter()
        .filter(|rel| {
            options
                .filter
                .allows(&rel.to_string_lossy().replace('\\', "/"))
        })
        .count();
    let mut processed = 0usize;
    for rel in source_files {
        let rel_text = rel.to_string_lossy().replace('\\', "/");
        if !options.filter.allows(&rel_text) {
            continue;
        }
        source_set.insert(rel_text.clone());

        let src_path = source.join(&rel);
        let dst_path = options.destination.join(&rel);
        let src_link_meta = fs::symlink_metadata(&handle, &src_path)
            .await
            .with_context(|| format!("lstat source {}", src_path.display()))?;
        let is_symlink = src_link_meta.file_type().is_symlink();
        if is_symlink {
            #[cfg(unix)]
            let src_mtime = src_link_meta.mtime();
            #[cfg(not(unix))]
            let src_mtime = 0i64;
            #[cfg(unix)]
            let src_uid = src_link_meta.uid();
            #[cfg(not(unix))]
            let src_uid = 0u32;
            #[cfg(unix)]
            let src_gid = src_link_meta.gid();
            #[cfg(not(unix))]
            let src_gid = 0u32;
            #[cfg(unix)]
            let src_mode = src_link_meta.mode() as u32;
            #[cfg(not(unix))]
            let src_mode = 0u32;

            let target = metadata::read_link_target(&src_path)
                .with_context(|| format!("read source symlink {}", src_path.display()))?;
            let mut copy = true;
            if let Ok(existing) = fs::symlink_metadata(&handle, &dst_path).await {
                #[cfg(unix)]
                let existing_mtime = existing.mtime();
                #[cfg(not(unix))]
                let existing_mtime = 0i64;
                if options.update_only {
                    if existing_mtime >= src_mtime {
                        copy = false;
                    }
                } else if existing_mtime == src_mtime
                    && metadata::read_link_target(&dst_path)
                        .map(|value| value == target)
                        .unwrap_or(false)
                {
                    copy = false;
                }
            }

            if options.dry_run {
                if copy {
                    summary.files_copied = summary.files_copied.saturating_add(1);
                } else {
                    summary.files_skipped = summary.files_skipped.saturating_add(1);
                }
                processed = processed.saturating_add(1);
                maybe_print_progress(
                    options.progress,
                    processed,
                    total_files,
                    summary.bytes_copied,
                );
                continue;
            }

            if copy {
                if let Some(parent) = dst_path.parent() {
                    fs::create_dir_all(&handle, parent).await.with_context(|| {
                        format!("create destination parent {}", parent.display())
                    })?;
                }
                remove_existing_path_for_symlink(&handle, &dst_path).await?;
                fs::symlink(&handle, Path::new(&target), &dst_path)
                    .await
                    .with_context(|| {
                        format!("create symlink {} -> {}", dst_path.display(), target)
                    })?;
                if options.preserve_metadata {
                    let xattrs = if options.preserve_xattrs {
                        metadata::collect_xattrs(&src_path, false)
                            .with_context(|| format!("collect xattrs {}", src_path.display()))?
                    } else {
                        Vec::new()
                    };
                    apply_preserve_metadata(
                        &handle,
                        &dst_path,
                        src_mode,
                        src_mtime,
                        src_uid,
                        src_gid,
                        &xattrs,
                        options.preserve_xattrs,
                        false,
                    )
                    .await?;
                }
                summary.files_copied = summary.files_copied.saturating_add(1);
            } else {
                if options.preserve_metadata && !options.update_only {
                    let xattrs = if options.preserve_xattrs {
                        metadata::collect_xattrs(&src_path, false)
                            .with_context(|| format!("collect xattrs {}", src_path.display()))?
                    } else {
                        Vec::new()
                    };
                    apply_preserve_metadata(
                        &handle,
                        &dst_path,
                        src_mode,
                        src_mtime,
                        src_uid,
                        src_gid,
                        &xattrs,
                        options.preserve_xattrs,
                        false,
                    )
                    .await?;
                }
                summary.files_skipped = summary.files_skipped.saturating_add(1);
            }
            processed = processed.saturating_add(1);
            maybe_print_progress(
                options.progress,
                processed,
                total_files,
                summary.bytes_copied,
            );
            continue;
        }

        let src_meta = fs::metadata_lite(&handle, &src_path)
            .await
            .with_context(|| format!("stat source {}", src_path.display()))?;
        let dst_meta = fs::metadata_lite(&handle, &dst_path).await.ok();
        if let Some(dst_meta) = dst_meta {
            if options.update_only {
                if dst_meta.mtime_sec >= src_meta.mtime_sec {
                    summary.files_skipped = summary.files_skipped.saturating_add(1);
                    processed = processed.saturating_add(1);
                    maybe_print_progress(
                        options.progress,
                        processed,
                        total_files,
                        summary.bytes_copied,
                    );
                    continue;
                }
            } else if dst_meta.size == src_meta.size && dst_meta.mtime_sec == src_meta.mtime_sec {
                if options.preserve_metadata && !options.update_only && !options.dry_run {
                    let xattrs = if options.preserve_xattrs {
                        metadata::collect_xattrs(&src_path, true)
                            .with_context(|| format!("collect xattrs {}", src_path.display()))?
                    } else {
                        Vec::new()
                    };
                    apply_preserve_metadata(
                        &handle,
                        &dst_path,
                        src_meta.mode as u32,
                        src_meta.mtime_sec,
                        src_meta.uid,
                        src_meta.gid,
                        &xattrs,
                        options.preserve_xattrs,
                        true,
                    )
                    .await?;
                }
                summary.files_skipped = summary.files_skipped.saturating_add(1);
                processed = processed.saturating_add(1);
                maybe_print_progress(
                    options.progress,
                    processed,
                    total_files,
                    summary.bytes_copied,
                );
                continue;
            }
        }

        if options.dry_run {
            summary.files_copied = summary.files_copied.saturating_add(1);
            summary.bytes_copied = summary.bytes_copied.saturating_add(src_meta.size);
            processed = processed.saturating_add(1);
            maybe_print_progress(
                options.progress,
                processed,
                total_files,
                summary.bytes_copied,
            );
            continue;
        }

        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(&handle, parent)
                .await
                .with_context(|| format!("create destination parent {}", parent.display()))?;
        }

        let copied = copy_one_file(&handle, &src_path, &dst_path).await?;
        if options.preserve_metadata {
            let xattrs = if options.preserve_xattrs {
                metadata::collect_xattrs(&src_path, true)
                    .with_context(|| format!("collect xattrs {}", src_path.display()))?
            } else {
                Vec::new()
            };
            apply_preserve_metadata(
                &handle,
                &dst_path,
                src_meta.mode as u32,
                src_meta.mtime_sec,
                src_meta.uid,
                src_meta.gid,
                &xattrs,
                options.preserve_xattrs,
                true,
            )
            .await?;
        }
        summary.files_copied = summary.files_copied.saturating_add(1);
        summary.bytes_copied = summary.bytes_copied.saturating_add(copied);
        processed = processed.saturating_add(1);
        maybe_print_progress(
            options.progress,
            processed,
            total_files,
            summary.bytes_copied,
        );
        if let Some(limit) = bwlimit_bytes_per_sec {
            let copied = copied as f64;
            loop {
                let now = Instant::now();
                let refill =
                    now.saturating_duration_since(bw_last_refill).as_secs_f64() * limit as f64;
                bw_tokens = (bw_tokens + refill).min(limit as f64);
                bw_last_refill = now;
                if bw_tokens >= copied {
                    bw_tokens -= copied;
                    break;
                }
                let deficit = copied - bw_tokens;
                bw_tokens = 0.0;
                spargio::sleep(Duration::from_secs_f64(deficit / limit as f64)).await;
            }
        }
    }

    if options.delete {
        let deleted = prune_destination(
            &handle,
            &options.destination,
            &source_set,
            &options.filter,
            options.dry_run,
        )
        .await?;
        summary.files_deleted = summary.files_deleted.saturating_add(deleted);
    }

    Ok(summary)
}

fn maybe_print_progress(enabled: bool, done: usize, total: usize, bytes: u64) {
    if enabled {
        println!("progress files={done}/{total} bytes={bytes}");
    }
}

pub async fn prune_destination(
    handle: &RuntimeHandle,
    destination: &Path,
    keep: &HashSet<String>,
    filter: &PathFilter,
    dry_run: bool,
) -> Result<usize> {
    let mut deleted = 0usize;
    let destination_files = enumerate_relative_files(handle, destination).await?;
    for rel in destination_files {
        let rel_text = rel.to_string_lossy().replace('\\', "/");
        if !filter.allows(&rel_text) {
            continue;
        }
        if keep.contains(&rel_text) {
            continue;
        }
        let path = destination.join(&rel);
        if dry_run {
            deleted = deleted.saturating_add(1);
            continue;
        }
        match fs::remove_file(handle, &path).await {
            Ok(()) => {
                deleted = deleted.saturating_add(1);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| format!("delete {}", path.display()));
            }
        }
    }
    Ok(deleted)
}

async fn copy_one_file(handle: &RuntimeHandle, source: &Path, destination: &Path) -> Result<u64> {
    let source_file = fs::File::open(handle.clone(), source)
        .await
        .with_context(|| format!("open {}", source.display()))?;
    let dest_file = fs::File::create(handle.clone(), destination)
        .await
        .with_context(|| format!("create {}", destination.display()))?;

    let mut offset = 0u64;
    loop {
        let chunk = source_file
            .read_at(offset, COPY_CHUNK_SIZE)
            .await
            .with_context(|| format!("read {}", source.display()))?;
        if chunk.is_empty() {
            break;
        }
        dest_file
            .write_all_at(offset, chunk.as_ref())
            .await
            .with_context(|| format!("write {}", destination.display()))?;
        offset = offset.saturating_add(chunk.len() as u64);
        if chunk.len() < COPY_CHUNK_SIZE {
            break;
        }
    }
    dest_file
        .fsync()
        .await
        .with_context(|| format!("fsync {}", destination.display()))?;
    Ok(offset)
}

async fn remove_existing_path_for_symlink(handle: &RuntimeHandle, path: &Path) -> Result<()> {
    match fs::symlink_metadata(handle, path).await {
        Ok(meta) => {
            if meta.file_type().is_dir() && !meta.file_type().is_symlink() {
                remove_dir_tree(handle, path).await?;
            } else {
                fs::remove_file(handle, path)
                    .await
                    .with_context(|| format!("remove existing file {}", path.display()))?;
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("lstat {}", path.display())),
    }
}

async fn apply_preserve_metadata(
    handle: &RuntimeHandle,
    path: &Path,
    mode: u32,
    mtime_sec: i64,
    uid: u32,
    gid: u32,
    xattrs: &[crate::protocol::XattrEntry],
    preserve_xattrs: bool,
    follow_symlink: bool,
) -> Result<()> {
    #[cfg(unix)]
    {
        if follow_symlink {
            let perms = std::fs::Permissions::from_mode(mode & 0o7777);
            fs::set_permissions(handle, path, perms)
                .await
                .with_context(|| format!("set permissions {}", path.display()))?;
        }
    }
    metadata::set_owner(path, uid, gid, follow_symlink)?;
    if preserve_xattrs {
        metadata::apply_xattrs(path, xattrs, follow_symlink)?;
    }
    metadata::set_mtime(path, mtime_sec, follow_symlink)
}

async fn enumerate_relative_files(handle: &RuntimeHandle, root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    match fs::metadata_lite(handle, root).await {
        Ok(meta) => {
            if !meta.is_dir() {
                return Ok(out);
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err).with_context(|| format!("stat {}", root.display())),
    }

    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(handle, &dir)
            .await
            .with_context(|| format!("read directory {}", dir.display()))?;
        for entry in entries {
            match entry.entry_type {
                fs::DirEntryType::Directory => stack.push(entry.path),
                fs::DirEntryType::File => {
                    let rel = entry.path.strip_prefix(root).with_context(|| {
                        format!(
                            "path {} escaped root {}",
                            entry.path.display(),
                            root.display()
                        )
                    })?;
                    out.push(rel.to_path_buf());
                }
                fs::DirEntryType::Symlink => {
                    let rel = entry.path.strip_prefix(root).with_context(|| {
                        format!(
                            "path {} escaped root {}",
                            entry.path.display(),
                            root.display()
                        )
                    })?;
                    out.push(rel.to_path_buf());
                }
                _ => {}
            }
        }
    }
    out.sort();
    Ok(out)
}
