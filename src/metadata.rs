use crate::protocol::XattrEntry;
use anyhow::{Context, Result};
use std::path::Path;

pub fn read_link_target(path: &Path) -> Result<String> {
    #[cfg(unix)]
    {
        let target =
            std::fs::read_link(path).with_context(|| format!("read symlink {}", path.display()))?;
        return Ok(target.to_string_lossy().into_owned());
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        anyhow::bail!("symlink support requires unix targets");
    }
}

pub fn collect_xattrs(path: &Path, follow_symlink: bool) -> Result<Vec<XattrEntry>> {
    #[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
    {
        use std::ffi::{CStr, CString};
        use std::os::unix::ffi::OsStrExt;

        fn os_error_from_rc(rc: isize) -> std::io::Result<usize> {
            if rc < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(rc as usize)
            }
        }

        let c_path = CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("encode xattr path {}", path.display()))?;

        let list_len = unsafe {
            if follow_symlink {
                os_error_from_rc(libc::listxattr(c_path.as_ptr(), std::ptr::null_mut(), 0) as isize)
            } else {
                os_error_from_rc(libc::llistxattr(c_path.as_ptr(), std::ptr::null_mut(), 0) as isize)
            }
        };
        let list_len = match list_len {
            Ok(value) => value,
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ENOTSUP) | Some(libc::ENODATA) | Some(libc::EPERM)
                ) =>
            {
                return Ok(Vec::new());
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "list xattrs follow_symlink={follow_symlink} {}",
                        path.display()
                    )
                });
            }
        };
        if list_len == 0 {
            return Ok(Vec::new());
        }

        let mut list_buf = vec![0u8; list_len];
        let read_len = unsafe {
            if follow_symlink {
                os_error_from_rc(libc::listxattr(
                    c_path.as_ptr(),
                    list_buf.as_mut_ptr() as *mut libc::c_char,
                    list_buf.len(),
                ) as isize)
            } else {
                os_error_from_rc(libc::llistxattr(
                    c_path.as_ptr(),
                    list_buf.as_mut_ptr() as *mut libc::c_char,
                    list_buf.len(),
                ) as isize)
            }
        }
        .with_context(|| format!("read xattr list {}", path.display()))?;
        list_buf.truncate(read_len);

        let mut out = Vec::new();
        let mut cursor = 0usize;
        while cursor < list_buf.len() {
            let rel = &list_buf[cursor..];
            let nul_idx = rel.iter().position(|b| *b == 0).ok_or_else(|| {
                anyhow::anyhow!("malformed xattr name list for {}", path.display())
            })?;
            let bytes = &rel[..nul_idx];
            cursor = cursor.saturating_add(nul_idx + 1);
            if bytes.is_empty() {
                continue;
            }
            let name_cstr = CStr::from_bytes_with_nul(&rel[..nul_idx + 1])
                .context("decode xattr name as C string")?;
            let name = name_cstr.to_string_lossy().into_owned();
            let c_name = CString::new(name.as_str()).context("encode xattr name")?;
            let value_len = unsafe {
                if follow_symlink {
                    os_error_from_rc(libc::getxattr(
                        c_path.as_ptr(),
                        c_name.as_ptr(),
                        std::ptr::null_mut(),
                        0,
                    ) as isize)
                } else {
                    os_error_from_rc(libc::lgetxattr(
                        c_path.as_ptr(),
                        c_name.as_ptr(),
                        std::ptr::null_mut(),
                        0,
                    ) as isize)
                }
            };
            let value_len = match value_len {
                Ok(value) => value,
                Err(err)
                    if matches!(
                        err.raw_os_error(),
                        Some(libc::ENODATA) | Some(libc::ENOTSUP)
                    ) =>
                {
                    continue;
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("query xattr '{}' on {}", name, path.display()));
                }
            };
            let mut value = vec![0u8; value_len];
            let read_value_len = unsafe {
                if follow_symlink {
                    os_error_from_rc(libc::getxattr(
                        c_path.as_ptr(),
                        c_name.as_ptr(),
                        value.as_mut_ptr() as *mut libc::c_void,
                        value.len(),
                    ) as isize)
                } else {
                    os_error_from_rc(libc::lgetxattr(
                        c_path.as_ptr(),
                        c_name.as_ptr(),
                        value.as_mut_ptr() as *mut libc::c_void,
                        value.len(),
                    ) as isize)
                }
            }
            .with_context(|| format!("read xattr '{}' from {}", name, path.display()))?;
            value.truncate(read_value_len);
            out.push(XattrEntry { name, value });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }
    #[cfg(not(all(unix, any(target_os = "linux", target_os = "android"))))]
    {
        let _ = (path, follow_symlink);
        Ok(Vec::new())
    }
}

pub fn has_xattrs(path: &Path, follow_symlink: bool) -> Result<bool> {
    #[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        fn os_error_from_rc(rc: isize) -> std::io::Result<usize> {
            if rc < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(rc as usize)
            }
        }

        let c_path = CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("encode xattr path {}", path.display()))?;
        let list_len = unsafe {
            if follow_symlink {
                os_error_from_rc(libc::listxattr(c_path.as_ptr(), std::ptr::null_mut(), 0) as isize)
            } else {
                os_error_from_rc(libc::llistxattr(c_path.as_ptr(), std::ptr::null_mut(), 0) as isize)
            }
        };
        return match list_len {
            Ok(value) => Ok(value > 0),
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ENOTSUP) | Some(libc::ENODATA) | Some(libc::EPERM)
                ) =>
            {
                Ok(false)
            }
            Err(err) => Err(err).with_context(|| {
                format!(
                    "list xattrs follow_symlink={follow_symlink} {}",
                    path.display()
                )
            }),
        };
    }
    #[cfg(not(all(unix, any(target_os = "linux", target_os = "android"))))]
    {
        let _ = (path, follow_symlink);
        Ok(false)
    }
}

pub fn xattr_signature(xattrs: &[XattrEntry]) -> Option<u64> {
    if xattrs.is_empty() {
        return None;
    }
    let mut ordered: Vec<&XattrEntry> = xattrs.iter().collect();
    ordered.sort_by(|a, b| a.name.cmp(&b.name));
    let mut hasher = blake3::Hasher::new();
    for attr in ordered {
        let name = attr.name.as_bytes();
        let value = attr.value.as_slice();
        hasher.update(&(name.len() as u32).to_be_bytes());
        hasher.update(name);
        hasher.update(&(value.len() as u32).to_be_bytes());
        hasher.update(value);
    }
    let digest = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest.as_bytes()[..8]);
    Some(u64::from_be_bytes(out))
}

pub fn path_xattr_signature(path: &Path, follow_symlink: bool) -> Result<Option<u64>> {
    if !has_xattrs(path, follow_symlink)? {
        return Ok(None);
    }
    let xattrs = collect_xattrs(path, follow_symlink)?;
    Ok(xattr_signature(&xattrs))
}

pub fn apply_xattrs(path: &Path, xattrs: &[XattrEntry], follow_symlink: bool) -> Result<()> {
    #[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
    {
        use std::collections::HashSet;
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("encode xattr path {}", path.display()))?;
        let desired: HashSet<&str> = xattrs.iter().map(|item| item.name.as_str()).collect();
        let current = collect_xattrs(path, follow_symlink)?;

        for stale in current {
            if desired.contains(stale.name.as_str()) {
                continue;
            }
            let c_name = CString::new(stale.name.as_str()).context("encode stale xattr name")?;
            let rc = unsafe {
                if follow_symlink {
                    libc::removexattr(c_path.as_ptr(), c_name.as_ptr())
                } else {
                    libc::lremovexattr(c_path.as_ptr(), c_name.as_ptr())
                }
            };
            if rc < 0 {
                let err = std::io::Error::last_os_error();
                if !matches!(
                    err.raw_os_error(),
                    Some(libc::ENODATA) | Some(libc::ENOTSUP) | Some(libc::EPERM)
                ) {
                    return Err(err).with_context(|| {
                        format!("remove stale xattr '{}' on {}", stale.name, path.display())
                    });
                }
            }
        }

        for attr in xattrs {
            let c_name = CString::new(attr.name.as_str())
                .with_context(|| format!("encode {}", attr.name))?;
            let rc = unsafe {
                if follow_symlink {
                    libc::setxattr(
                        c_path.as_ptr(),
                        c_name.as_ptr(),
                        attr.value.as_ptr() as *const libc::c_void,
                        attr.value.len(),
                        0,
                    )
                } else {
                    libc::lsetxattr(
                        c_path.as_ptr(),
                        c_name.as_ptr(),
                        attr.value.as_ptr() as *const libc::c_void,
                        attr.value.len(),
                        0,
                    )
                }
            };
            if rc < 0 {
                let err = std::io::Error::last_os_error();
                if !matches!(err.raw_os_error(), Some(libc::ENOTSUP) | Some(libc::EPERM)) {
                    return Err(err).with_context(|| {
                        format!(
                            "set xattr '{}' on {} ({} bytes)",
                            attr.name,
                            path.display(),
                            attr.value.len()
                        )
                    });
                }
            }
        }
        Ok(())
    }
    #[cfg(not(all(unix, any(target_os = "linux", target_os = "android"))))]
    {
        let _ = (path, xattrs, follow_symlink);
        Ok(())
    }
}

pub fn set_owner(path: &Path, uid: u32, gid: u32, follow_symlink: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("encode path for chown {}", path.display()))?;
        let rc = unsafe {
            if follow_symlink {
                libc::chown(c_path.as_ptr(), uid, gid)
            } else {
                libc::lchown(c_path.as_ptr(), uid, gid)
            }
        };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EPERM) {
                return Err(err).with_context(|| {
                    format!(
                        "set owner/group uid={} gid={} follow_symlink={} {}",
                        uid,
                        gid,
                        follow_symlink,
                        path.display()
                    )
                });
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, uid, gid, follow_symlink);
        Ok(())
    }
}

pub fn set_mtime(path: &Path, mtime_sec: i64, follow_symlink: bool) -> Result<()> {
    #[cfg(unix)]
    {
        if follow_symlink {
            let mtime = filetime::FileTime::from_unix_time(mtime_sec, 0);
            filetime::set_file_mtime(path, mtime)
                .with_context(|| format!("set mtime {}", path.display()))?;
            return Ok(());
        }

        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("encode symlink path {}", path.display()))?;
        let times = [
            libc::timespec {
                tv_sec: mtime_sec,
                tv_nsec: 0,
            },
            libc::timespec {
                tv_sec: mtime_sec,
                tv_nsec: 0,
            },
        ];
        let rc = unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                c_path.as_ptr(),
                times.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("set symlink mtime {}", path.display()));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let mtime = filetime::FileTime::from_unix_time(mtime_sec, 0);
        filetime::set_file_mtime(path, mtime)
            .with_context(|| format!("set mtime {}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sparsync-metadata-test-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
    fn can_set_user_xattr(path: &Path, follow_symlink: bool) -> bool {
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes()).expect("encode path");
        let c_name = CString::new("user.sparsync.test_probe").expect("encode name");
        let value = b"1";
        let rc = unsafe {
            if follow_symlink {
                libc::setxattr(
                    c_path.as_ptr(),
                    c_name.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    0,
                )
            } else {
                libc::lsetxattr(
                    c_path.as_ptr(),
                    c_name.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    0,
                )
            }
        };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            return !matches!(err.raw_os_error(), Some(libc::ENOTSUP) | Some(libc::EPERM));
        }
        let _ = unsafe {
            if follow_symlink {
                libc::removexattr(c_path.as_ptr(), c_name.as_ptr())
            } else {
                libc::lremovexattr(c_path.as_ptr(), c_name.as_ptr())
            }
        };
        true
    }

    #[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
    #[test]
    fn file_xattrs_roundtrip_and_stale_removal() {
        let root = temp_path("file");
        std::fs::create_dir_all(&root).expect("create temp dir");
        let path = root.join("data.bin");
        std::fs::write(&path, b"payload").expect("write temp file");

        if !can_set_user_xattr(&path, true) {
            let _ = std::fs::remove_dir_all(&root);
            return;
        }

        apply_xattrs(
            &path,
            &[
                XattrEntry {
                    name: "user.sparsync.keep".to_string(),
                    value: b"alpha".to_vec(),
                },
                XattrEntry {
                    name: "user.sparsync.stale".to_string(),
                    value: b"old".to_vec(),
                },
            ],
            true,
        )
        .expect("apply initial xattrs");

        apply_xattrs(
            &path,
            &[XattrEntry {
                name: "user.sparsync.keep".to_string(),
                value: b"beta".to_vec(),
            }],
            true,
        )
        .expect("apply updated xattrs");

        let attrs = collect_xattrs(&path, true).expect("collect file xattrs");
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].name, "user.sparsync.keep");
        assert_eq!(attrs[0].value, b"beta");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
    #[test]
    fn symlink_xattrs_use_nofollow_semantics() {
        use std::os::unix::fs::symlink;

        let root = temp_path("symlink");
        std::fs::create_dir_all(&root).expect("create temp dir");
        let target = root.join("target.bin");
        let link = root.join("link.bin");
        std::fs::write(&target, b"payload").expect("write target");
        symlink(&target, &link).expect("create symlink");

        if !can_set_user_xattr(&link, false) {
            let _ = std::fs::remove_dir_all(&root);
            return;
        }

        apply_xattrs(
            &link,
            &[XattrEntry {
                name: "user.sparsync.link".to_string(),
                value: b"link-value".to_vec(),
            }],
            false,
        )
        .expect("apply link xattrs");

        let link_attrs = collect_xattrs(&link, false).expect("collect symlink xattrs");
        assert_eq!(link_attrs.len(), 1);
        assert_eq!(link_attrs[0].name, "user.sparsync.link");
        assert_eq!(link_attrs[0].value, b"link-value");

        let target_attrs = collect_xattrs(&target, true).expect("collect target xattrs");
        assert!(
            !target_attrs
                .iter()
                .any(|attr| attr.name == "user.sparsync.link"),
            "symlink xattr should not be applied to target"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}
