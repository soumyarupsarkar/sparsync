use anyhow::{Context, Result, bail};
use rkyv::{Archive, Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;
pub const BINARY_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const WIRE_MAGIC: [u8; 4] = *b"SPS1";
pub const WIRE_CODEC_RKYV: u8 = 1;
pub const WIRE_ENDIAN_LITTLE: u8 = 1;
pub const WIRE_ENDIAN_BIG: u8 = 2;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum Frame {
    HelloRequest(HelloRequest),
    HelloResponse(HelloResponse),
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
#[archive(check_bytes)]
pub struct HelloRequest {
    pub protocol_version: u16,
    pub codec: WireCodec,
    pub endianness: WireEndianness,
    pub binary_version: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct HelloResponse {
    pub protocol_version: u16,
    pub codec: WireCodec,
    pub endianness: WireEndianness,
    pub binary_version: String,
    pub accepted: bool,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[archive(check_bytes)]
pub enum WireCodec {
    Rkyv,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[archive(check_bytes)]
pub enum WireEndianness {
    Little,
    Big,
}

pub fn local_wire_codec() -> WireCodec {
    WireCodec::Rkyv
}

pub fn local_wire_endianness() -> WireEndianness {
    #[cfg(target_endian = "little")]
    {
        WireEndianness::Little
    }
    #[cfg(target_endian = "big")]
    {
        WireEndianness::Big
    }
}

fn local_wire_endianness_id() -> u8 {
    match local_wire_endianness() {
        WireEndianness::Little => WIRE_ENDIAN_LITTLE,
        WireEndianness::Big => WIRE_ENDIAN_BIG,
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct InitFileRequest {
    pub relative_path: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub update_only: bool,
    pub file_hash: String,
    pub chunk_size: usize,
    pub total_chunks: usize,
    pub resume: bool,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct InitFileResponse {
    pub action: InitAction,
    pub next_chunk: usize,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[archive(check_bytes)]
pub enum InitAction {
    Skip,
    Upload,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
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
#[archive(check_bytes)]
pub struct UploadBatchResponse {
    pub accepted: bool,
    pub message: String,
    pub next_chunk: usize,
    pub completed: bool,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct InitBatchRequest {
    pub files: Vec<InitFileRequest>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct InitBatchResponse {
    pub results: Vec<InitBatchResult>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct InitBatchResult {
    pub action: InitAction,
    pub next_chunk: usize,
    pub message: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct UploadSmallBatchRequest {
    pub files: Vec<UploadSmallFileMeta>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
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
#[archive(check_bytes)]
pub struct UploadSmallBatchResponse {
    pub results: Vec<UploadSmallFileResult>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct UploadSmallFileResult {
    pub accepted: bool,
    pub skipped: bool,
    pub message: String,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct UploadColdBatchRequest {
    pub allow_skip: bool,
    pub files: Vec<UploadColdFileMeta>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
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
#[archive(check_bytes)]
pub struct UploadColdBatchResponse {
    pub results: Vec<UploadColdFileResult>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct UploadColdFileResult {
    pub accepted: bool,
    pub skipped: bool,
    pub message: String,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
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
pub const FRAME_PREFIX_LEN: usize = 16;

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

    let mut out = Vec::with_capacity(FRAME_PREFIX_LEN + frame_bytes.len());
    out.extend_from_slice(&WIRE_MAGIC);
    out.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    out.push(WIRE_CODEC_RKYV);
    out.push(local_wire_endianness_id());
    out.extend_from_slice(&(frame_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&(payload_len as u32).to_be_bytes());
    out.extend_from_slice(&frame_bytes);
    Ok(out)
}

pub fn frame_total_len(prefix: &[u8]) -> Result<usize> {
    if prefix.len() < FRAME_PREFIX_LEN {
        bail!(
            "frame prefix too short: {} bytes (need {})",
            prefix.len(),
            FRAME_PREFIX_LEN
        );
    }

    if prefix[0..4] != WIRE_MAGIC {
        bail!(
            "wire magic mismatch: got {:02x?} expected {:02x?}",
            &prefix[0..4],
            WIRE_MAGIC
        );
    }
    let wire_protocol = u16::from_be_bytes([prefix[4], prefix[5]]);
    if wire_protocol != PROTOCOL_VERSION {
        bail!(
            "wire protocol mismatch: peer={} local={}",
            wire_protocol,
            PROTOCOL_VERSION
        );
    }
    if prefix[6] != WIRE_CODEC_RKYV {
        bail!("unsupported wire codec id {}", prefix[6]);
    }
    if prefix[7] != local_wire_endianness_id() {
        bail!(
            "wire endianness mismatch: peer={} local={}",
            prefix[7],
            local_wire_endianness_id()
        );
    }

    let header_len = u32::from_be_bytes([prefix[8], prefix[9], prefix[10], prefix[11]]) as usize;
    let payload_len = u32::from_be_bytes([prefix[12], prefix[13], prefix[14], prefix[15]]) as usize;

    FRAME_PREFIX_LEN
        .checked_add(header_len)
        .and_then(|v| v.checked_add(payload_len))
        .ok_or_else(|| anyhow::anyhow!("frame length overflow"))
}

pub fn decode(bytes: &[u8]) -> Result<(Frame, &[u8])> {
    if bytes.len() < FRAME_PREFIX_LEN {
        bail!("frame is too short: {} bytes", bytes.len());
    }

    let expected = frame_total_len(bytes)?;
    let header_len = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let payload_len = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;

    if expected != bytes.len() {
        bail!(
            "frame size mismatch: expected {} got {}",
            expected,
            bytes.len()
        );
    }

    let header_start = FRAME_PREFIX_LEN;
    let header_end = header_start + header_len;
    let payload_start = header_end;
    let payload_end = payload_start + payload_len;

    let frame = rkyv::from_bytes::<Frame>(&bytes[header_start..header_end])
        .map_err(|err| anyhow::anyhow!("deserialize frame (rkyv + validation): {err}"))?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn hello_request() -> Frame {
        Frame::HelloRequest(HelloRequest {
            protocol_version: PROTOCOL_VERSION,
            codec: local_wire_codec(),
            endianness: local_wire_endianness(),
            binary_version: BINARY_VERSION.to_string(),
        })
    }

    #[test]
    fn roundtrip_hello_uses_wire_preamble() {
        let bytes = encode(&hello_request(), None).expect("encode hello");
        assert_eq!(&bytes[0..4], &WIRE_MAGIC);
        assert_eq!([bytes[4], bytes[5]], PROTOCOL_VERSION.to_be_bytes());
        assert_eq!(bytes[6], WIRE_CODEC_RKYV);

        let (frame, payload) = decode(&bytes).expect("decode hello");
        assert!(payload.is_empty());
        match frame {
            Frame::HelloRequest(req) => {
                assert_eq!(req.protocol_version, PROTOCOL_VERSION);
                assert_eq!(req.codec, local_wire_codec());
                assert_eq!(req.endianness, local_wire_endianness());
                assert_eq!(req.binary_version, BINARY_VERSION);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bad_wire_magic_before_rkyv_decode() {
        let mut bytes = encode(&hello_request(), None).expect("encode hello");
        bytes[0] ^= 0xff;
        let err = decode(&bytes).expect_err("decode should fail");
        assert!(err.to_string().contains("wire magic mismatch"));
    }

    #[test]
    fn decode_rejects_endianness_mismatch_before_rkyv_decode() {
        let mut bytes = encode(&hello_request(), None).expect("encode hello");
        bytes[7] = match bytes[7] {
            WIRE_ENDIAN_LITTLE => WIRE_ENDIAN_BIG,
            _ => WIRE_ENDIAN_LITTLE,
        };
        let err = decode(&bytes).expect_err("decode should fail");
        assert!(err.to_string().contains("wire endianness mismatch"));
    }
}
