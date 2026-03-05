use crate::filter::PathFilter;
use anyhow::{Context, Result};
use filetime::FileTime;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct LocalCopyOptions {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub preserve_metadata: bool,
    pub dry_run: bool,
    pub delete: bool,
    pub update_only: bool,
    pub filter: PathFilter,
    pub bwlimit_kbps: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct LocalCopySummary {
    pub files_copied: usize,
    pub files_skipped: usize,
    pub files_deleted: usize,
    pub bytes_copied: u64,
}

pub fn copy_tree(options: LocalCopyOptions) -> Result<LocalCopySummary> {
    let mut summary = LocalCopySummary::default();
    let source = fs::canonicalize(&options.source)
        .with_context(|| format!("canonicalize source {}", options.source.display()))?;
    if !source.is_dir() {
        return Err(anyhow::anyhow!(
            "source {} is not a directory",
            source.display()
        ));
    }

    if !options.dry_run {
        fs::create_dir_all(&options.destination).with_context(|| {
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
    let bw_started = Instant::now();
    let mut bw_sent_bytes = 0u64;

    let mut source_set = HashSet::new();
    let source_files = enumerate_relative_files(&source)?;
    for rel in source_files {
        let rel_text = rel.to_string_lossy().replace('\\', "/");
        if !options.filter.allows(&rel_text) {
            continue;
        }
        source_set.insert(rel_text.clone());

        let src_path = source.join(&rel);
        let dst_path = options.destination.join(&rel);
        let src_meta = fs::metadata(&src_path)
            .with_context(|| format!("stat source {}", src_path.display()))?;
        if let Ok(dst_meta) = fs::metadata(&dst_path) {
            if options.update_only {
                let src_mtime = FileTime::from_last_modification_time(&src_meta);
                let dst_mtime = FileTime::from_last_modification_time(&dst_meta);
                if dst_mtime >= src_mtime {
                    summary.files_skipped = summary.files_skipped.saturating_add(1);
                    continue;
                }
            } else if dst_meta.len() == src_meta.len() {
                let src_mtime = FileTime::from_last_modification_time(&src_meta);
                let dst_mtime = FileTime::from_last_modification_time(&dst_meta);
                if dst_mtime == src_mtime {
                    summary.files_skipped = summary.files_skipped.saturating_add(1);
                    continue;
                }
            }
        }

        if options.dry_run {
            summary.files_copied = summary.files_copied.saturating_add(1);
            summary.bytes_copied = summary.bytes_copied.saturating_add(src_meta.len());
            continue;
        }

        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create destination parent {}", parent.display()))?;
        }
        fs::copy(&src_path, &dst_path).with_context(|| {
            format!(
                "copy source {} -> {}",
                src_path.display(),
                dst_path.display()
            )
        })?;
        if options.preserve_metadata {
            fs::set_permissions(&dst_path, src_meta.permissions())
                .with_context(|| format!("apply permissions {}", dst_path.display()))?;
            let mtime = FileTime::from_last_modification_time(&src_meta);
            let atime = FileTime::from_last_access_time(&src_meta);
            filetime::set_file_times(&dst_path, atime, mtime)
                .with_context(|| format!("apply timestamps {}", dst_path.display()))?;
        }
        summary.files_copied = summary.files_copied.saturating_add(1);
        summary.bytes_copied = summary.bytes_copied.saturating_add(src_meta.len());
        if let Some(limit) = bwlimit_bytes_per_sec {
            bw_sent_bytes = bw_sent_bytes.saturating_add(src_meta.len());
            let expected = Duration::from_secs_f64(bw_sent_bytes as f64 / limit as f64);
            let elapsed = bw_started.elapsed();
            if expected > elapsed {
                std::thread::sleep(expected.saturating_sub(elapsed));
            }
        }
    }

    if options.delete {
        let destination_files = enumerate_relative_files(&options.destination)?;
        for rel in destination_files {
            let rel_text = rel.to_string_lossy().replace('\\', "/");
            if !options.filter.allows(&rel_text) {
                continue;
            }
            if source_set.contains(&rel_text) {
                continue;
            }
            let path = options.destination.join(&rel);
            if options.dry_run {
                summary.files_deleted = summary.files_deleted.saturating_add(1);
                continue;
            }
            match fs::remove_file(&path) {
                Ok(()) => {
                    summary.files_deleted = summary.files_deleted.saturating_add(1);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| format!("delete {}", path.display()));
                }
            }
        }
    }

    Ok(summary)
}

pub fn prune_destination(
    destination: &Path,
    keep: &HashSet<String>,
    filter: &PathFilter,
    dry_run: bool,
) -> Result<usize> {
    let mut deleted = 0usize;
    let destination_files = enumerate_relative_files(destination)?;
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
        match fs::remove_file(&path) {
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

fn enumerate_relative_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries =
            fs::read_dir(&dir).with_context(|| format!("read directory {}", dir.display()))?;
        for entry in entries {
            let entry = entry.with_context(|| format!("read entry under {}", dir.display()))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .with_context(|| format!("stat {}", path.display()))?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let rel = path.strip_prefix(root).with_context(|| {
                format!("path {} escaped root {}", path.display(), root.display())
            })?;
            out.push(rel.to_path_buf());
        }
    }
    out.sort();
    Ok(out)
}
