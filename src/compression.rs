use anyhow::{Context, Result};
use std::borrow::Cow;

pub fn maybe_compress_vec(data: Vec<u8>, level: i32) -> Result<(Vec<u8>, bool)> {
    if level <= 0 {
        return Ok((data, false));
    }

    let compressed = zstd::bulk::compress(&data, level)
        .with_context(|| format!("zstd compression failed (level={level})"))?;

    if compressed.len().saturating_add(32) < data.len() {
        Ok((compressed, true))
    } else {
        Ok((data, false))
    }
}

pub fn maybe_decode<'a>(
    data: &'a [u8],
    compressed: bool,
    expected_len: usize,
) -> Result<Cow<'a, [u8]>> {
    if !compressed {
        return Ok(Cow::Borrowed(data));
    }

    let out = zstd::bulk::decompress(data, expected_len)
        .with_context(|| format!("zstd decompression failed (expected_len={expected_len})"))?;
    Ok(Cow::Owned(out))
}
