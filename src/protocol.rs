use anyhow::{Context, Result, bail};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum Frame {
    InitFileRequest(InitFileRequest),
    InitFileResponse(InitFileResponse),
    InitBatchRequest(InitBatchRequest),
    InitBatchResponse(InitBatchResponse),
    UploadBatchRequest(UploadBatchRequest),
    UploadBatchResponse(UploadBatchResponse),
    UploadSmallBatchRequest(UploadSmallBatchRequest),
    UploadSmallBatchResponse(UploadSmallBatchResponse),
    UploadColdBatchRequest(UploadColdBatchRequest),
    UploadColdBatchResponse(UploadColdBatchResponse),
    Error(ErrorFrame),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct InitFileRequest {
    pub relative_path: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub file_hash: String,
    pub chunk_size: usize,
    pub total_chunks: usize,
    pub resume: bool,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct InitFileResponse {
    pub action: InitAction,
    pub next_chunk: usize,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum InitAction {
    Skip,
    Upload,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadBatchRequest {
    pub relative_path: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub file_hash: String,
    pub total_chunks: usize,
    pub start_chunk: usize,
    pub chunk_size: usize,
    pub sent_chunks: usize,
    pub finalize: bool,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadBatchResponse {
    pub accepted: bool,
    pub message: String,
    pub next_chunk: usize,
    pub completed: bool,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct InitBatchRequest {
    pub files: Vec<InitFileRequest>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct InitBatchResponse {
    pub results: Vec<InitBatchResult>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct InitBatchResult {
    pub action: InitAction,
    pub next_chunk: usize,
    pub message: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadSmallBatchRequest {
    pub files: Vec<UploadSmallFileMeta>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadSmallFileMeta {
    pub relative_path: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub file_hash: String,
    pub total_chunks: usize,
    pub compressed: bool,
    pub raw_len: usize,
    pub data_len: usize,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadSmallBatchResponse {
    pub results: Vec<UploadSmallFileResult>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadSmallFileResult {
    pub accepted: bool,
    pub skipped: bool,
    pub message: String,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadColdBatchRequest {
    pub allow_skip: bool,
    pub files: Vec<UploadColdFileMeta>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadColdFileMeta {
    pub relative_path: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub file_hash: String,
    pub total_chunks: usize,
    pub compressed: bool,
    pub raw_len: usize,
    pub data_len: usize,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadColdBatchResponse {
    pub results: Vec<UploadColdFileResult>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct UploadColdFileResult {
    pub accepted: bool,
    pub skipped: bool,
    pub message: String,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct ErrorFrame {
    pub message: String,
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkPacketRef<'a> {
    pub raw_len: usize,
    pub compressed: bool,
    pub data: &'a [u8],
}

const CHUNK_ENTRY_HEADER_SIZE: usize = 9;
const CHUNK_FLAG_COMPRESSED: u8 = 1;

pub fn encode(frame: &Frame, payload: Option<&[u8]>) -> Result<Vec<u8>> {
    let payload_len = payload.map_or(0usize, |p| p.len());
    let mut out = encode_header(frame, payload_len)?;
    if let Some(bytes) = payload {
        out.extend_from_slice(bytes);
    }
    Ok(out)
}

pub fn encode_header(frame: &Frame, payload_len: usize) -> Result<Vec<u8>> {
    let frame_bytes = rkyv::to_bytes::<_, 4096>(frame)
        .context("serialize frame (rkyv)")?
        .to_vec();

    if frame_bytes.len() > u32::MAX as usize {
        bail!("frame header too large: {} bytes", frame_bytes.len());
    }
    if payload_len > u32::MAX as usize {
        bail!("frame payload too large: {payload_len} bytes");
    }

    let mut out = Vec::with_capacity(8 + frame_bytes.len());
    out.extend_from_slice(&(frame_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&(payload_len as u32).to_be_bytes());
    out.extend_from_slice(&frame_bytes);
    Ok(out)
}

pub fn decode(bytes: &[u8]) -> Result<(Frame, &[u8])> {
    if bytes.len() < 8 {
        bail!("frame is too short: {} bytes", bytes.len());
    }

    let header_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let payload_len = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;

    let expected = 8usize
        .checked_add(header_len)
        .and_then(|v| v.checked_add(payload_len))
        .ok_or_else(|| anyhow::anyhow!("frame length overflow"))?;

    if expected != bytes.len() {
        bail!(
            "frame size mismatch: expected {} got {}",
            expected,
            bytes.len()
        );
    }

    let header_start = 8;
    let header_end = header_start + header_len;
    let payload_start = header_end;
    let payload_end = payload_start + payload_len;

    // Framing validates exact header bounds; decode avoids additional validation pass.
    let frame = unsafe { rkyv::from_bytes_unchecked::<Frame>(&bytes[header_start..header_end]) }
        .context("deserialize frame (rkyv)")?;

    Ok((frame, &bytes[payload_start..payload_end]))
}

pub fn decode_chunk_batch<'a>(
    bytes: &'a [u8],
    expected_chunks: usize,
) -> Result<Vec<ChunkPacketRef<'a>>> {
    let mut offset = 0usize;
    let mut out = Vec::with_capacity(expected_chunks);

    for _ in 0..expected_chunks {
        if bytes.len().saturating_sub(offset) < CHUNK_ENTRY_HEADER_SIZE {
            bail!(
                "chunk batch truncated at offset {} (need {} bytes header)",
                offset,
                CHUNK_ENTRY_HEADER_SIZE
            );
        }

        let raw_len = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        let encoded_len = u32::from_be_bytes([
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]) as usize;
        let flags = bytes[offset + 8];
        offset += CHUNK_ENTRY_HEADER_SIZE;

        let end = offset
            .checked_add(encoded_len)
            .ok_or_else(|| anyhow::anyhow!("chunk encoded length overflow"))?;
        if end > bytes.len() {
            bail!(
                "chunk batch truncated payload: offset {} encoded_len {} total {}",
                offset,
                encoded_len,
                bytes.len()
            );
        }

        let data = &bytes[offset..end];
        offset = end;

        out.push(ChunkPacketRef {
            raw_len,
            compressed: (flags & CHUNK_FLAG_COMPRESSED) != 0,
            data,
        });
    }

    if offset != bytes.len() {
        bail!(
            "chunk batch trailing bytes: consumed {} total {}",
            offset,
            bytes.len()
        );
    }

    Ok(out)
}

pub fn split_small_file_payload<'a>(
    bytes: &'a [u8],
    files: &[UploadSmallFileMeta],
) -> Result<Vec<&'a [u8]>> {
    let mut offset = 0usize;
    let mut out = Vec::with_capacity(files.len());

    for file in files {
        let end = offset
            .checked_add(file.data_len)
            .ok_or_else(|| anyhow::anyhow!("small-file payload length overflow"))?;
        if end > bytes.len() {
            bail!(
                "small-file payload truncated for {}: need {} at offset {} total {}",
                file.relative_path,
                file.data_len,
                offset,
                bytes.len()
            );
        }
        out.push(&bytes[offset..end]);
        offset = end;
    }

    if offset != bytes.len() {
        bail!(
            "small-file payload trailing bytes: consumed {} total {}",
            offset,
            bytes.len()
        );
    }

    Ok(out)
}

pub fn split_cold_file_payload<'a>(
    bytes: &'a [u8],
    files: &[UploadColdFileMeta],
) -> Result<Vec<&'a [u8]>> {
    let mut offset = 0usize;
    let mut out = Vec::with_capacity(files.len());

    for file in files {
        let end = offset
            .checked_add(file.data_len)
            .ok_or_else(|| anyhow::anyhow!("cold-file payload length overflow"))?;
        if end > bytes.len() {
            bail!(
                "cold-file payload truncated for {}: need {} at offset {} total {}",
                file.relative_path,
                file.data_len,
                offset,
                bytes.len()
            );
        }
        out.push(&bytes[offset..end]);
        offset = end;
    }

    if offset != bytes.len() {
        bail!(
            "cold-file payload trailing bytes: consumed {} total {}",
            offset,
            bytes.len()
        );
    }

    Ok(out)
}
