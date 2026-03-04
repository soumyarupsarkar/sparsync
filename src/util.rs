use anyhow::{Result, anyhow, bail};
use std::path::{Component, Path, PathBuf};

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
