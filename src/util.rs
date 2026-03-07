use anyhow::{Context, Result, anyhow, bail};
use spargio::{RuntimeHandle, fs};
use std::path::{Component, Path, PathBuf};

pub const QUICK_CHECK_TOKEN_PREFIX: &str = "qc:";

pub fn relative_path_string(root: &Path, absolute: &Path) -> Result<String> {
    let relative = absolute.strip_prefix(root).map_err(|_| {
        anyhow::anyhow!(
            "path {} is outside root {}",
            absolute.display(),
            root.display()
        )
    })?;

    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            _ => bail!("invalid relative path component in {}", relative.display()),
        }
    }

    if parts.is_empty() {
        bail!("empty relative path for {}", absolute.display());
    }

    Ok(parts.join("/"))
}

pub fn sanitize_relative(path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        bail!("empty relative path");
    }

    let input = Path::new(path);
    if input.is_absolute() {
        bail!("absolute paths are not allowed: {path}");
    }

    let mut clean = PathBuf::new();
    for component in input.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir => bail!("parent path traversal is not allowed: {path}"),
            Component::RootDir | Component::Prefix(_) => {
                bail!("absolute path component is not allowed: {path}")
            }
        }
    }

    if clean.as_os_str().is_empty() {
        bail!("path resolves to empty relative path: {path}");
    }

    Ok(clean)
}

pub fn partial_path(partial_root: &Path, relative: &Path) -> PathBuf {
    let mut out = partial_root.join(relative);
    let ext = out
        .extension()
        .and_then(|v| v.to_str())
        .map(|v| format!("{v}.part"))
        .unwrap_or_else(|| "part".to_string());
    out.set_extension(ext);
    out
}

pub fn runtime_error(context: &str, err: spargio::RuntimeError) -> anyhow::Error {
    anyhow!("{context}: {err:?}")
}

pub fn join_error(context: &str, err: spargio::JoinError) -> anyhow::Error {
    anyhow!("{context}: {err:?}")
}

pub fn quick_check_token(size: u64, mtime_sec: i64) -> String {
    format!("{QUICK_CHECK_TOKEN_PREFIX}{size}:{mtime_sec}")
}

pub fn parse_quick_check_token(value: &str) -> Option<(u64, i64)> {
    let rest = value.strip_prefix(QUICK_CHECK_TOKEN_PREFIX)?;
    let (size, mtime_sec) = rest.split_once(':')?;
    let size = size.parse::<u64>().ok()?;
    let mtime_sec = mtime_sec.parse::<i64>().ok()?;
    Some((size, mtime_sec))
}

pub fn is_quick_check_token(value: &str) -> bool {
    parse_quick_check_token(value).is_some()
}

pub fn total_chunks_for_size(size: u64, chunk_size: usize) -> usize {
    if size == 0 {
        return 0;
    }
    let chunk = chunk_size.max(1) as u64;
    ((size + chunk - 1) / chunk) as usize
}

pub async fn remove_dir_tree(handle: &RuntimeHandle, root: &Path) -> Result<()> {
    let mut pending = vec![root.to_path_buf()];
    let mut remove_order = Vec::new();

    while let Some(dir) = pending.pop() {
        remove_order.push(dir.clone());
        let entries = fs::read_dir(handle, &dir)
            .await
            .with_context(|| format!("read directory {}", dir.display()))?;
        for entry in entries {
            match entry.entry_type {
                fs::DirEntryType::Directory => pending.push(entry.path),
                fs::DirEntryType::File | fs::DirEntryType::Symlink => {
                    fs::remove_file(handle, &entry.path)
                        .await
                        .with_context(|| format!("remove {}", entry.path.display()))?;
                }
                _ => {}
            }
        }
    }

    for dir in remove_order.into_iter().rev() {
        fs::remove_dir(handle, &dir)
            .await
            .with_context(|| format!("remove directory {}", dir.display()))?;
    }
    Ok(())
}
