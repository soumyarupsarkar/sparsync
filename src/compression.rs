use anyhow::{Context, Result};

pub fn maybe_compress(data: &[u8], level: i32) -> Result<(Vec<u8>, bool)> {
    if level <= 0 {
        return Ok((data.to_vec(), false));
    }

    let compressed = zstd::bulk::compress(data, level)
        .with_context(|| format!("zstd compression failed (level={level})"))?;

    if compressed.len().saturating_add(32) < data.len() {
        Ok((compressed, true))
    } else {
        Ok((data.to_vec(), false))
    }
}

pub fn maybe_decompress(data: &[u8], compressed: bool, expected_len: usize) -> Result<Vec<u8>> {
    if !compressed {
        return Ok(data.to_vec());
    }

    let out = zstd::bulk::decompress(data, expected_len)
        .with_context(|| format!("zstd decompression failed (expected_len={expected_len})"))?;
    Ok(out)
}
