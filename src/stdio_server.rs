use crate::compression;
use crate::filter::PathFilter;
use crate::local_copy;
use crate::metadata;
use crate::protocol::{
    DeletePlanRequest, DeletePlanResponse, DeletePlanStage, ErrorFrame, Frame, HelloRequest,
    HelloResponse, InitAction, InitBatchRequest, InitBatchResponse, InitBatchResult,
    InitFileRequest, InitFileResponse, PROTOCOL_VERSION, SyncFileMetadataBatchRequest,
    SyncFileMetadataBatchResponse, SyncFileMetadataResult, SyncSymlinkBatchRequest,
    SyncSymlinkBatchResponse, SyncSymlinkResult, UploadBatchRequest, UploadBatchResponse,
    UploadColdBatchRequest, UploadColdBatchResponse, UploadColdFileMeta, UploadColdFileResult,
    UploadSmallBatchRequest, UploadSmallBatchResponse, UploadSmallFileMeta, UploadSmallFileResult,
    XattrEntry,
};
use crate::state::{CompleteFileInput, StateStore};
use crate::util::{is_quick_check_token, partial_path, remove_dir_tree, sanitize_relative};
use anyhow::{Context, Result, bail};
use spargio::{RuntimeHandle, fs};
use std::collections::HashSet;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ServeStdioOptions {
    pub destination: PathBuf,
    pub max_stream_payload: usize,
    pub preserve_metadata: bool,
    pub preserve_xattrs: bool,
}

struct StdioServerContext {
    handle: RuntimeHandle,
    destination: PathBuf,
    state: StateStore,
    max_stream_payload: usize,
    preserve_metadata: bool,
    preserve_xattrs: bool,
    partials_maybe_empty: bool,
    delete_plan_state: Option<DeletePlanState>,
}

#[derive(Debug, Default)]
struct DeletePlanState {
    dry_run: bool,
    include: Vec<String>,
    exclude: Vec<String>,
    keep: HashSet<String>,
}

pub async fn run_server_stdio(handle: RuntimeHandle, options: ServeStdioOptions) -> Result<()> {
    fs::create_dir_all(&handle, &options.destination)
        .await
        .with_context(|| format!("create destination {}", options.destination.display()))?;

    let state = StateStore::open(handle.clone(), &options.destination)
        .await
        .context("open resume state")?;

    let partials_maybe_empty = match fs::read_dir(&handle, state.partial_root()).await {
        Ok(entries) => entries.is_empty(),
        Err(_) => true,
    };

    let mut context = StdioServerContext {
        handle: handle.clone(),
        destination: options.destination,
        state,
        max_stream_payload: options.max_stream_payload,
        preserve_metadata: options.preserve_metadata,
        preserve_xattrs: options.preserve_xattrs,
        partials_maybe_empty,
        delete_plan_state: None,
    };
    let mut frame_io =
        StdioFrameIo::new(handle, context.max_stream_payload).context("open stdio frame io")?;

    loop {
        let bytes = frame_io.read_next_frame().await?;
        let Some(bytes) = bytes else {
            break;
        };

        let (frame, payload) = crate::protocol::decode(&bytes).context("decode frame")?;
        let response = match process_one_frame(&mut context, frame, payload).await {
            Ok(frame) => frame,
            Err(err) => Frame::Error(ErrorFrame {
                message: format!("{err:#}"),
            }),
        };
        let response_bytes = crate::protocol::encode(&response, None).context("encode response")?;
        frame_io
            .write_all(&response_bytes)
            .await
            .context("write frame response to stdout")?;
    }

    context.state.flush().await.context("flush resume state")?;

    Ok(())
}

const STREAM_FD_OFFSET: u64 = u64::MAX;
const STDIO_WRITE_CHUNK_BYTES: usize = 128 * 1024;

fn stdio_nonblocking_enabled() -> bool {
    std::env::var("SPARSYNC_STDIO_NONBLOCK")
        .map(|value| value != "0")
        .unwrap_or(true)
}

struct StdioFrameIo {
    native: spargio::UringNativeAny,
    stdin_fd: RawFd,
    stdout_fd: RawFd,
    max_stream_payload: usize,
    read_buffer: Vec<u8>,
    read_frame_len: Option<usize>,
}

impl StdioFrameIo {
    fn new(handle: RuntimeHandle, max_stream_payload: usize) -> Result<Self> {
        let native = handle
            .uring_native_unbound()
            .map_err(runtime_error_to_io)?
            .clear_preferred_shard();

        let stdin_fd = std::io::stdin().as_raw_fd();
        let stdout_fd = std::io::stdout().as_raw_fd();
        if stdio_nonblocking_enabled() {
            set_nonblocking(stdin_fd, true).context("set stdin nonblocking")?;
            set_nonblocking(stdout_fd, true).context("set stdout nonblocking")?;
        }

        Ok(Self {
            native,
            stdin_fd,
            stdout_fd,
            max_stream_payload,
            read_buffer: Vec::with_capacity(64 * 1024),
            read_frame_len: None,
        })
    }

    async fn read_next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some(frame) = try_extract_frame(
                &mut self.read_buffer,
                &mut self.read_frame_len,
                self.max_stream_payload,
            )? {
                return Ok(Some(frame));
            }

            let remaining = self
                .max_stream_payload
                .saturating_sub(self.read_buffer.len())
                .min(256 * 1024)
                .max(1);
            match self
                .native
                .read_at(self.stdin_fd, STREAM_FD_OFFSET, remaining)
                .await
            {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        if self.read_buffer.is_empty() && self.read_frame_len.is_none() {
                            return Ok(None);
                        }
                        if let Some(expected) = self.read_frame_len {
                            bail!(
                                "stdin closed with partial frame: have {} expected {}",
                                self.read_buffer.len(),
                                expected
                            );
                        }
                        bail!(
                            "stdin closed with {} buffered bytes",
                            self.read_buffer.len()
                        );
                    }
                    self.read_buffer.extend_from_slice(bytes.as_ref());
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    wait_fd_ready(&self.native, self.stdin_fd, libc::POLLIN as u32)
                        .await
                        .context("wait for stdin readable")?;
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {}
                Err(err) => return Err(err).context("read frame bytes from stdin"),
            }
        }
    }

    async fn write_all(&self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let submit_len = bytes.len().min(STDIO_WRITE_CHUNK_BYTES);
            let submit = &bytes[..submit_len];
            match self
                .native
                .write_at(self.stdout_fd, STREAM_FD_OFFSET, submit)
                .await
            {
                Ok(0) => bail!("write returned zero bytes on stdout"),
                Ok(wrote) => {
                    let wrote = wrote.min(submit_len);
                    bytes = &bytes[wrote..];
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    wait_fd_ready(&self.native, self.stdout_fd, libc::POLLOUT as u32)
                        .await
                        .context("wait for stdout writable")?;
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {}
                Err(err) => return Err(err).context("write frame bytes to stdout"),
            }
        }
        Ok(())
    }
}

fn try_extract_frame(
    buffered: &mut Vec<u8>,
    expected_len: &mut Option<usize>,
    max_stream_payload: usize,
) -> Result<Option<Vec<u8>>> {
    if expected_len.is_none() && buffered.len() >= crate::protocol::FRAME_PREFIX_LEN {
        let frame_len =
            crate::protocol::frame_total_len(&buffered[..crate::protocol::FRAME_PREFIX_LEN])
                .context("decode frame prefix")?;
        if frame_len > max_stream_payload {
            bail!(
                "frame length {} exceeds max_stream_payload {}",
                frame_len,
                max_stream_payload
            );
        }
        *expected_len = Some(frame_len);
    }

    let Some(frame_len) = *expected_len else {
        return Ok(None);
    };

    if buffered.len() < frame_len {
        return Ok(None);
    }

    *expected_len = None;
    if buffered.len() == frame_len {
        return Ok(Some(std::mem::take(buffered)));
    }

    let tail = buffered.split_off(frame_len);
    let frame = std::mem::replace(buffered, tail);
    Ok(Some(frame))
}

fn runtime_error_to_io(err: spargio::RuntimeError) -> io::Error {
    match err {
        spargio::RuntimeError::InvalidConfig(msg) => {
            io::Error::new(io::ErrorKind::InvalidInput, msg)
        }
        spargio::RuntimeError::ThreadSpawn(io_err) => io_err,
        spargio::RuntimeError::InvalidShard(shard) => {
            io::Error::new(io::ErrorKind::NotFound, format!("invalid shard {shard}"))
        }
        spargio::RuntimeError::Closed => {
            io::Error::new(io::ErrorKind::BrokenPipe, "runtime closed")
        }
        spargio::RuntimeError::Overloaded => {
            io::Error::new(io::ErrorKind::WouldBlock, "runtime overloaded")
        }
        spargio::RuntimeError::UnsupportedBackend(msg) => {
            io::Error::new(io::ErrorKind::Unsupported, msg)
        }
        spargio::RuntimeError::IoUringInit(io_err) => io_err,
    }
}

async fn wait_fd_ready(native: &spargio::UringNativeAny, fd: RawFd, mask: u32) -> io::Result<()> {
    unsafe {
        native
            .submit_unsafe(
                (fd, mask),
                |state| {
                    let (fd, mask) = *state;
                    Ok(io_uring::opcode::PollAdd::new(io_uring::types::Fd(fd), mask).build())
                },
                |_, cqe| {
                    if cqe.result < 0 {
                        return Err(io::Error::from_raw_os_error(-cqe.result));
                    }
                    Ok(())
                },
            )
            .await
    }
}

fn set_nonblocking(fd: RawFd, enabled: bool) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut next = flags;
    if enabled {
        next |= libc::O_NONBLOCK;
    } else {
        next &= !libc::O_NONBLOCK;
    }
    if next != flags {
        let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, next) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

async fn process_one_frame(
    context: &mut StdioServerContext,
    frame: Frame,
    payload: &[u8],
) -> Result<Frame> {
    match frame {
        Frame::HelloRequest(req) => process_hello(req).await,
        Frame::InitFileRequest(req) => process_init_file(context, req).await,
        Frame::InitBatchRequest(req) => process_init_batch(context, req).await,
        Frame::UploadBatchRequest(req) => process_upload_batch(context, req, payload).await,
        Frame::UploadSmallBatchRequest(req) => {
            process_upload_small_batch(context, req, payload).await
        }
        Frame::UploadColdBatchRequest(req) => {
            process_upload_cold_batch(context, req, payload).await
        }
        Frame::SyncSymlinkBatchRequest(req) => {
            process_sync_symlink_batch(context, req, payload).await
        }
        Frame::SyncFileMetadataBatchRequest(req) => {
            process_sync_file_metadata_batch(context, req, payload).await
        }
        Frame::DeletePlanRequest(req) => process_delete_plan(context, req, payload).await,
        Frame::HelloResponse(_) => Ok(Frame::Error(ErrorFrame {
            message: "unexpected hello response on server".to_string(),
        })),
        other => Ok(Frame::Error(ErrorFrame {
            message: format!("frame not supported over stdio transport: {other:?}"),
        })),
    }
}

async fn process_hello(req: HelloRequest) -> Result<Frame> {
    if req.protocol_version != PROTOCOL_VERSION {
        return Ok(Frame::HelloResponse(HelloResponse {
            protocol_version: PROTOCOL_VERSION,
            codec: crate::protocol::local_wire_codec(),
            endianness: crate::protocol::local_wire_endianness(),
            binary_version: crate::protocol::BINARY_VERSION.to_string(),
            accepted: false,
            message: format!(
                "protocol mismatch: client={} server={}",
                req.protocol_version, PROTOCOL_VERSION
            ),
        }));
    }
    if req.codec != crate::protocol::local_wire_codec() {
        return Ok(Frame::HelloResponse(HelloResponse {
            protocol_version: PROTOCOL_VERSION,
            codec: crate::protocol::local_wire_codec(),
            endianness: crate::protocol::local_wire_endianness(),
            binary_version: crate::protocol::BINARY_VERSION.to_string(),
            accepted: false,
            message: format!(
                "codec mismatch: client={:?} server={:?}",
                req.codec,
                crate::protocol::local_wire_codec()
            ),
        }));
    }
    if req.endianness != crate::protocol::local_wire_endianness() {
        return Ok(Frame::HelloResponse(HelloResponse {
            protocol_version: PROTOCOL_VERSION,
            codec: crate::protocol::local_wire_codec(),
            endianness: crate::protocol::local_wire_endianness(),
            binary_version: crate::protocol::BINARY_VERSION.to_string(),
            accepted: false,
            message: format!(
                "endianness mismatch: client={:?} server={:?}",
                req.endianness,
                crate::protocol::local_wire_endianness()
            ),
        }));
    }
    if req.binary_version != crate::protocol::BINARY_VERSION {
        return Ok(Frame::HelloResponse(HelloResponse {
            protocol_version: PROTOCOL_VERSION,
            codec: crate::protocol::local_wire_codec(),
            endianness: crate::protocol::local_wire_endianness(),
            binary_version: crate::protocol::BINARY_VERSION.to_string(),
            accepted: false,
            message: format!(
                "binary version mismatch: client={} server={}",
                req.binary_version,
                crate::protocol::BINARY_VERSION
            ),
        }));
    }
    Ok(Frame::HelloResponse(HelloResponse {
        protocol_version: PROTOCOL_VERSION,
        codec: crate::protocol::local_wire_codec(),
        endianness: crate::protocol::local_wire_endianness(),
        binary_version: crate::protocol::BINARY_VERSION.to_string(),
        accepted: true,
        message: "ok".to_string(),
    }))
}

async fn process_init_file(
    context: &mut StdioServerContext,
    req: InitFileRequest,
) -> Result<Frame> {
    let relative = sanitize_relative(&req.relative_path)?;
    let response = initialize_one_file_with_relative(context, &req, &relative, true).await?;
    Ok(Frame::InitFileResponse(response))
}

async fn process_init_batch(
    context: &mut StdioServerContext,
    req: InitBatchRequest,
) -> Result<Frame> {
    let mut prepared = Vec::with_capacity(req.files.len());
    let mut final_parents = HashSet::new();
    let mut partial_parents = HashSet::new();

    for file in req.files {
        let relative = sanitize_relative(&file.relative_path)?;
        let skip =
            context
                .state
                .is_complete_match(&file.relative_path, &file.file_hash, file.size)?;

        if !skip {
            if let Some(parent) = context.destination.join(&relative).parent() {
                final_parents.insert(parent.to_path_buf());
            }
            let partial = partial_path(context.state.partial_root(), &relative);
            if let Some(parent) = partial.parent() {
                partial_parents.insert(parent.to_path_buf());
            }
        }

        prepared.push((file, relative, skip));
    }

    for parent in final_parents {
        fs::create_dir_all(&context.handle, &parent)
            .await
            .with_context(|| format!("create destination parent {}", parent.display()))?;
    }
    for parent in partial_parents {
        fs::create_dir_all(&context.handle, &parent)
            .await
            .with_context(|| format!("create partial parent {}", parent.display()))?;
    }

    let mut results = Vec::with_capacity(prepared.len());
    for (file, relative, _) in prepared {
        let response = initialize_one_file_with_relative(context, &file, &relative, false).await?;
        results.push(InitBatchResult {
            action: response.action,
            next_chunk: response.next_chunk,
            metadata_sync_required: response.metadata_sync_required,
            message: response.message,
        });
    }

    Ok(Frame::InitBatchResponse(InitBatchResponse { results }))
}

async fn initialize_one_file_with_relative(
    context: &mut StdioServerContext,
    req: &InitFileRequest,
    relative: &Path,
    prepare_dirs: bool,
) -> Result<InitFileResponse> {
    let final_path = context.destination.join(relative);
    let partial = partial_path(context.state.partial_root(), relative);
    if req.update_only {
        match fs::metadata_lite(&context.handle, &final_path).await {
            Ok(meta) if meta.is_file() && meta.mtime_sec > req.mtime_sec => {
                return Ok(InitFileResponse {
                    action: InitAction::Skip,
                    next_chunk: req.total_chunks,
                    metadata_sync_required: false,
                    message: format!(
                        "destination is newer (dst_mtime={} src_mtime={})",
                        meta.mtime_sec, req.mtime_sec
                    ),
                });
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("read destination metadata {}", final_path.display())
                });
            }
        }
    }

    if prepare_dirs {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(&context.handle, parent)
                .await
                .with_context(|| format!("create destination parent {}", parent.display()))?;
        }
        if let Some(parent) = partial.parent() {
            fs::create_dir_all(&context.handle, parent)
                .await
                .with_context(|| format!("create partial parent {}", parent.display()))?;
        }
    }

    let existing_partial_chunks = if req.resume && !context.partials_maybe_empty {
        match fs::metadata_lite(&context.handle, &partial).await {
            Ok(meta) if meta.is_file() => {
                if meta.size > req.size {
                    let _ = fs::remove_file(&context.handle, &partial).await;
                    None
                } else {
                    Some((meta.size / req.chunk_size as u64) as usize)
                }
            }
            Ok(_) => None,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read partial metadata {}", partial.display()));
            }
        }
    } else {
        None
    };

    let response = context
        .state
        .initialize_file(req, existing_partial_chunks)
        .await
        .with_context(|| format!("initialize file state {}", req.relative_path))?;

    if matches!(response.action, InitAction::Upload)
        && response.next_chunk == 0
        && existing_partial_chunks.is_some()
    {
        if let Err(err) = fs::remove_file(&context.handle, &partial).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err).with_context(|| format!("reset partial {}", partial.display()));
            }
        }
    }

    Ok(response)
}

async fn process_upload_small_batch(
    context: &mut StdioServerContext,
    req: UploadSmallBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    let payload_parts = crate::protocol::split_small_file_payload(payload, &req.files)
        .context("split small-file batch payload")?;
    let mut destination_parents = HashSet::new();
    let mut partial_parents = HashSet::new();
    for meta in &req.files {
        let relative = sanitize_relative(&meta.relative_path)?;
        if let Some(parent) = context.destination.join(&relative).parent() {
            destination_parents.insert(parent.to_path_buf());
        }
        let partial = partial_path(context.state.partial_root(), &relative);
        if let Some(parent) = partial.parent() {
            partial_parents.insert(parent.to_path_buf());
        }
    }

    for parent in destination_parents {
        fs::create_dir_all(&context.handle, &parent)
            .await
            .with_context(|| format!("create destination parent {}", parent.display()))?;
    }
    for parent in partial_parents {
        fs::create_dir_all(&context.handle, &parent)
            .await
            .with_context(|| format!("create partial parent {}", parent.display()))?;
    }

    let mut results = Vec::with_capacity(req.files.len());
    let mut completions = Vec::new();
    for (meta, encoded_payload) in req.files.into_iter().zip(payload_parts.into_iter()) {
        let one = process_upload_small_one(context, meta, encoded_payload).await?;
        if let Some(completion) = one.completion {
            completions.push(completion);
        }
        results.push(one.result);
    }

    context
        .state
        .complete_files_batch(&completions)
        .await
        .context("persist small batch completion state")?;

    Ok(Frame::UploadSmallBatchResponse(UploadSmallBatchResponse {
        results,
    }))
}

async fn process_upload_cold_batch(
    context: &mut StdioServerContext,
    req: UploadColdBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    let payload_parts = crate::protocol::split_cold_file_payload(payload, &req.files)
        .context("split cold-file batch payload")?;
    let allow_skip = req.allow_skip;

    let mut destination_parents = HashSet::new();
    for meta in &req.files {
        let relative = sanitize_relative(&meta.relative_path)?;
        if let Some(parent) = context.destination.join(&relative).parent() {
            destination_parents.insert(parent.to_path_buf());
        }
    }
    for parent in destination_parents {
        fs::create_dir_all(&context.handle, &parent)
            .await
            .with_context(|| format!("create destination parent {}", parent.display()))?;
    }

    let mut results = Vec::with_capacity(req.files.len());
    let mut completions = Vec::new();
    for (meta, encoded_payload) in req.files.into_iter().zip(payload_parts.into_iter()) {
        let one = process_upload_cold_one(context, meta, encoded_payload, allow_skip).await?;
        if let Some(completion) = one.completion {
            completions.push(completion);
        }
        results.push(one.result);
    }

    context
        .state
        .complete_files_batch(&completions)
        .await
        .context("persist cold batch completion state")?;

    Ok(Frame::UploadColdBatchResponse(UploadColdBatchResponse {
        results,
    }))
}

async fn process_upload_batch(
    context: &mut StdioServerContext,
    req: UploadBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    let relative = sanitize_relative(&req.relative_path)?;
    let key = req.relative_path.as_str();

    let expected_next = context.state.current_chunk(key).await?.ok_or_else(|| {
        anyhow::anyhow!(
            "upload batch received before init for {}",
            req.relative_path
        )
    })?;

    if req.start_chunk < expected_next {
        return Ok(Frame::UploadBatchResponse(UploadBatchResponse {
            accepted: true,
            message: "batch already recorded".to_string(),
            next_chunk: expected_next,
            completed: false,
            bytes_written: 0,
        }));
    }

    if req.start_chunk > expected_next {
        return Ok(Frame::UploadBatchResponse(UploadBatchResponse {
            accepted: false,
            message: format!("out-of-order batch: expected start_chunk={expected_next}"),
            next_chunk: expected_next,
            completed: false,
            bytes_written: 0,
        }));
    }

    let packets = crate::protocol::decode_chunk_batch(payload, req.sent_chunks)
        .with_context(|| format!("decode chunk batch for {}", req.relative_path))?;

    let partial = partial_path(context.state.partial_root(), &relative);
    if let Some(parent) = partial.parent() {
        fs::create_dir_all(&context.handle, parent)
            .await
            .with_context(|| format!("create partial parent {}", parent.display()))?;
    }
    context.partials_maybe_empty = false;

    let open = fs::OpenOptions::new().read(true).write(true).create(true);
    let open = if req.start_chunk == 0 {
        open.truncate(true)
    } else {
        open
    };
    let file = open
        .open(context.handle.clone(), &partial)
        .await
        .with_context(|| format!("open partial {}", partial.display()))?;

    let mut next_chunk = req.start_chunk;
    for packet in packets {
        let decoded = compression::maybe_decode(packet.data, packet.compressed, packet.raw_len)?;
        if decoded.len() != packet.raw_len {
            return Ok(Frame::UploadBatchResponse(UploadBatchResponse {
                accepted: false,
                message: format!(
                    "raw length mismatch: expected {} got {}",
                    packet.raw_len,
                    decoded.len()
                ),
                next_chunk,
                completed: false,
                bytes_written: 0,
            }));
        }

        let offset = (next_chunk as u64).saturating_mul(req.chunk_size as u64);
        file.write_all_at(offset, decoded.as_ref())
            .await
            .with_context(|| format!("write partial {} at {}", partial.display(), offset))?;
        next_chunk = next_chunk.saturating_add(1);
    }

    context
        .state
        .update_partial_progress(key, next_chunk, false)
        .await
        .with_context(|| format!("update batch progress {}", req.relative_path))?;

    if !req.finalize {
        return Ok(Frame::UploadBatchResponse(UploadBatchResponse {
            accepted: true,
            message: "batch accepted".to_string(),
            next_chunk,
            completed: false,
            bytes_written: 0,
        }));
    }

    if next_chunk != req.total_chunks {
        return Ok(Frame::UploadBatchResponse(UploadBatchResponse {
            accepted: false,
            message: format!(
                "finalize requested before all chunks uploaded: have {} need {}",
                next_chunk, req.total_chunks
            ),
            next_chunk,
            completed: false,
            bytes_written: 0,
        }));
    }

    let bytes_written = finalize_uploaded_file(context, &req, &relative, &partial).await?;
    Ok(Frame::UploadBatchResponse(UploadBatchResponse {
        accepted: true,
        message: "file finalized".to_string(),
        next_chunk,
        completed: true,
        bytes_written,
    }))
}

fn parse_keep_payload(payload: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(payload).context("decode delete keep-list payload as utf-8")?;
    let mut keep = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = sanitize_relative(trimmed)
            .with_context(|| format!("normalize keep path '{}'", trimmed))?
            .to_string_lossy()
            .replace('\\', "/");
        keep.push(normalized);
    }
    Ok(keep)
}

async fn process_delete_plan(
    context: &mut StdioServerContext,
    req: DeletePlanRequest,
    payload: &[u8],
) -> Result<Frame> {
    match req.stage {
        DeletePlanStage::Begin => {
            context.delete_plan_state = Some(DeletePlanState {
                dry_run: req.dry_run,
                include: req.include,
                exclude: req.exclude,
                keep: HashSet::new(),
            });
            Ok(Frame::DeletePlanResponse(DeletePlanResponse {
                accepted: true,
                message: "delete plan initialized".to_string(),
                deleted: 0,
                keep_paths: 0,
            }))
        }
        DeletePlanStage::AddKeepChunk => {
            let Some(plan) = context.delete_plan_state.as_mut() else {
                return Ok(Frame::DeletePlanResponse(DeletePlanResponse {
                    accepted: false,
                    message: "delete plan add-chunk before begin".to_string(),
                    deleted: 0,
                    keep_paths: 0,
                }));
            };
            for path in parse_keep_payload(payload)? {
                plan.keep.insert(path);
            }
            Ok(Frame::DeletePlanResponse(DeletePlanResponse {
                accepted: true,
                message: "delete plan chunk accepted".to_string(),
                deleted: 0,
                keep_paths: plan.keep.len() as u64,
            }))
        }
        DeletePlanStage::Apply => {
            let Some(plan) = context.delete_plan_state.take() else {
                return Ok(Frame::DeletePlanResponse(DeletePlanResponse {
                    accepted: false,
                    message: "delete plan apply before begin".to_string(),
                    deleted: 0,
                    keep_paths: 0,
                }));
            };
            let filter = PathFilter::from_patterns(&plan.include, &plan.exclude)
                .context("compile delete filter")?;
            let deleted = local_copy::prune_destination(
                &context.handle,
                &context.destination,
                &plan.keep,
                &filter,
                plan.dry_run,
            )
            .await
            .context("apply delete plan on stdio destination")?;
            Ok(Frame::DeletePlanResponse(DeletePlanResponse {
                accepted: true,
                message: "delete plan applied".to_string(),
                deleted: deleted as u64,
                keep_paths: plan.keep.len() as u64,
            }))
        }
    }
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

fn apply_symlink_meta(
    context: &StdioServerContext,
    destination: &Path,
    entry: &crate::protocol::SymlinkMeta,
) -> Result<()> {
    if !context.preserve_metadata {
        return Ok(());
    }
    metadata::set_owner(destination, entry.uid, entry.gid, false)?;
    if context.preserve_xattrs {
        metadata::apply_xattrs(destination, &entry.xattrs, false)?;
    }
    metadata::set_mtime(destination, entry.mtime_sec, false)?;
    Ok(())
}

async fn process_sync_symlink_batch(
    context: &mut StdioServerContext,
    req: SyncSymlinkBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    if !payload.is_empty() {
        bail!(
            "sync symlink batch request does not accept payload (received {} bytes)",
            payload.len()
        );
    }
    let mut results = Vec::with_capacity(req.entries.len());
    for entry in req.entries {
        let relative = sanitize_relative(&entry.relative_path)?;
        let destination = context.destination.join(&relative);

        let mut skipped = false;
        if let Ok(existing_meta) = fs::symlink_metadata(&context.handle, &destination).await {
            #[cfg(unix)]
            let existing_mtime = existing_meta.mtime();
            #[cfg(not(unix))]
            let existing_mtime = 0i64;
            if existing_meta.file_type().is_symlink()
                && existing_mtime == entry.mtime_sec
                && metadata::read_link_target(&destination)
                    .map(|value| value == entry.target)
                    .unwrap_or(false)
            {
                apply_symlink_meta(context, &destination, &entry)?;
                skipped = true;
            }
        }
        if skipped {
            results.push(SyncSymlinkResult {
                accepted: true,
                skipped: true,
                message: "already up to date".to_string(),
            });
            continue;
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(&context.handle, parent)
                .await
                .with_context(|| format!("create destination parent {}", parent.display()))?;
        }
        remove_existing_path_for_symlink(&context.handle, &destination).await?;
        fs::symlink(&context.handle, Path::new(&entry.target), &destination)
            .await
            .with_context(|| {
                format!(
                    "create destination symlink {} -> {}",
                    destination.display(),
                    entry.target
                )
            })?;
        apply_symlink_meta(context, &destination, &entry)?;
        results.push(SyncSymlinkResult {
            accepted: true,
            skipped: false,
            message: "symlink updated".to_string(),
        });
    }
    Ok(Frame::SyncSymlinkBatchResponse(SyncSymlinkBatchResponse {
        results,
    }))
}

async fn process_sync_file_metadata_batch(
    context: &mut StdioServerContext,
    req: SyncFileMetadataBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    if !payload.is_empty() {
        bail!(
            "sync file metadata request does not accept payload (received {} bytes)",
            payload.len()
        );
    }
    let mut results = Vec::with_capacity(req.entries.len());
    for entry in req.entries {
        let state_match =
            context
                .state
                .is_complete_match(&entry.relative_path, &entry.file_hash, entry.size)?;
        let quick_check = is_quick_check_token(&entry.file_hash);
        let relative = sanitize_relative(&entry.relative_path)?;
        let destination = context.destination.join(&relative);
        let existing = match fs::metadata_lite(&context.handle, &destination).await {
            Ok(meta) if meta.is_file() => meta,
            Ok(_) => {
                results.push(SyncFileMetadataResult {
                    accepted: false,
                    skipped: false,
                    message: "destination is not a regular file".to_string(),
                });
                continue;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                results.push(SyncFileMetadataResult {
                    accepted: false,
                    skipped: false,
                    message: "destination missing".to_string(),
                });
                continue;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("read destination metadata {}", destination.display())
                });
            }
        };
        if !state_match && !quick_check {
            results.push(SyncFileMetadataResult {
                accepted: false,
                skipped: false,
                message: "state mismatch; file no longer complete for hash/size".to_string(),
            });
            continue;
        }
        if existing.size != entry.size {
            results.push(SyncFileMetadataResult {
                accepted: false,
                skipped: false,
                message: format!(
                    "destination size mismatch: expected {} got {}",
                    entry.size, existing.size
                ),
            });
            continue;
        }
        if quick_check && existing.mtime_sec != entry.mtime_sec {
            results.push(SyncFileMetadataResult {
                accepted: false,
                skipped: false,
                message: format!(
                    "destination mtime mismatch: expected {} got {}",
                    entry.mtime_sec, existing.mtime_sec
                ),
            });
            continue;
        }
        if !context.preserve_metadata {
            results.push(SyncFileMetadataResult {
                accepted: true,
                skipped: true,
                message: "metadata preservation disabled".to_string(),
            });
            continue;
        }
        apply_metadata(
            &context.handle,
            &destination,
            entry.mode,
            entry.mtime_sec,
            entry.uid,
            entry.gid,
            &entry.xattrs,
            context.preserve_xattrs,
        )
        .await?;
        results.push(SyncFileMetadataResult {
            accepted: true,
            skipped: false,
            message: "metadata updated".to_string(),
        });
    }
    Ok(Frame::SyncFileMetadataBatchResponse(
        SyncFileMetadataBatchResponse { results },
    ))
}

struct ColdTaskResult {
    result: UploadColdFileResult,
    completion: Option<CompleteFileInput>,
}

struct SmallTaskResult {
    result: UploadSmallFileResult,
    completion: Option<CompleteFileInput>,
}

async fn process_upload_cold_one(
    context: &mut StdioServerContext,
    meta: UploadColdFileMeta,
    encoded: &[u8],
    allow_skip: bool,
) -> Result<ColdTaskResult> {
    let relative = sanitize_relative(&meta.relative_path)?;

    if allow_skip
        && context
            .state
            .is_complete_match(&meta.relative_path, &meta.file_hash, meta.size)?
    {
        if context.preserve_metadata {
            let final_path = context.destination.join(&relative);
            apply_metadata(
                &context.handle,
                &final_path,
                meta.mode,
                meta.mtime_sec,
                meta.uid,
                meta.gid,
                &meta.xattrs,
                context.preserve_xattrs,
            )
            .await?;
        }
        return Ok(ColdTaskResult {
            result: UploadColdFileResult {
                accepted: true,
                skipped: true,
                message: "already completed; metadata reconciled".to_string(),
                bytes_written: 0,
            },
            completion: None,
        });
    }

    let decoded = compression::maybe_decode(encoded, meta.compressed, meta.raw_len)
        .with_context(|| format!("decode cold-file payload {}", meta.relative_path))?;
    if decoded.len() != meta.raw_len {
        return Ok(ColdTaskResult {
            result: UploadColdFileResult {
                accepted: false,
                skipped: false,
                message: format!(
                    "raw length mismatch: expected {} got {}",
                    meta.raw_len,
                    decoded.len()
                ),
                bytes_written: 0,
            },
            completion: None,
        });
    }

    if decoded.len() as u64 != meta.size {
        return Ok(ColdTaskResult {
            result: UploadColdFileResult {
                accepted: false,
                skipped: false,
                message: format!(
                    "size mismatch: expected {} got {}",
                    meta.size,
                    decoded.len()
                ),
                bytes_written: 0,
            },
            completion: None,
        });
    }

    let final_path = context.destination.join(&relative);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(context.handle.clone(), &final_path)
        .await
        .with_context(|| format!("open destination {}", final_path.display()))?;

    file.write_all_at(0, decoded.as_ref())
        .await
        .with_context(|| format!("write destination {}", final_path.display()))?;

    if context.preserve_metadata {
        apply_metadata(
            &context.handle,
            &final_path,
            meta.mode,
            meta.mtime_sec,
            meta.uid,
            meta.gid,
            &meta.xattrs,
            context.preserve_xattrs,
        )
        .await?;
    }

    if !context.partials_maybe_empty {
        let stale_partial = partial_path(context.state.partial_root(), &relative);
        let _ = fs::remove_file(&context.handle, &stale_partial).await;
    }

    Ok(ColdTaskResult {
        result: UploadColdFileResult {
            accepted: true,
            skipped: false,
            message: "file finalized".to_string(),
            bytes_written: meta.size,
        },
        completion: Some(CompleteFileInput {
            relative_path: meta.relative_path,
            file_hash: meta.file_hash,
            size: meta.size,
            mode: meta.mode,
            mtime_sec: meta.mtime_sec,
            xattr_sig: metadata::xattr_signature(&meta.xattrs),
            total_chunks: meta.total_chunks,
        }),
    })
}

async fn process_upload_small_one(
    context: &mut StdioServerContext,
    meta: UploadSmallFileMeta,
    encoded: &[u8],
) -> Result<SmallTaskResult> {
    let relative = sanitize_relative(&meta.relative_path)?;
    let key = meta.relative_path.as_str();

    let expected_next = context.state.current_chunk(key).await?.ok_or_else(|| {
        anyhow::anyhow!(
            "small-file upload received before init for {}",
            meta.relative_path
        )
    })?;

    if meta.total_chunks > 0 && expected_next >= meta.total_chunks {
        return Ok(SmallTaskResult {
            result: UploadSmallFileResult {
                accepted: true,
                skipped: true,
                message: "already completed".to_string(),
                bytes_written: 0,
            },
            completion: None,
        });
    }

    if expected_next > 0 {
        return Ok(SmallTaskResult {
            result: UploadSmallFileResult {
                accepted: false,
                skipped: false,
                message: format!(
                    "cannot apply small-file batch to resumed state (next_chunk={expected_next})"
                ),
                bytes_written: 0,
            },
            completion: None,
        });
    }

    let decoded = compression::maybe_decode(encoded, meta.compressed, meta.raw_len)
        .with_context(|| format!("decode small-file payload {}", meta.relative_path))?;

    if decoded.len() != meta.raw_len {
        return Ok(SmallTaskResult {
            result: UploadSmallFileResult {
                accepted: false,
                skipped: false,
                message: format!(
                    "raw length mismatch: expected {} got {}",
                    meta.raw_len,
                    decoded.len()
                ),
                bytes_written: 0,
            },
            completion: None,
        });
    }

    if decoded.len() as u64 != meta.size {
        return Ok(SmallTaskResult {
            result: UploadSmallFileResult {
                accepted: false,
                skipped: false,
                message: format!(
                    "size mismatch: expected {} got {}",
                    meta.size,
                    decoded.len()
                ),
                bytes_written: 0,
            },
            completion: None,
        });
    }

    if meta.total_chunks <= 1 {
        let bytes_written =
            write_small_file_direct(context, &relative, &meta, decoded.as_ref()).await?;
        return Ok(SmallTaskResult {
            result: UploadSmallFileResult {
                accepted: true,
                skipped: false,
                message: "file finalized".to_string(),
                bytes_written,
            },
            completion: Some(CompleteFileInput {
                relative_path: meta.relative_path,
                file_hash: meta.file_hash,
                size: meta.size,
                mode: meta.mode,
                mtime_sec: meta.mtime_sec,
                xattr_sig: metadata::xattr_signature(&meta.xattrs),
                total_chunks: meta.total_chunks,
            }),
        });
    }

    let partial = partial_path(context.state.partial_root(), &relative);
    context.partials_maybe_empty = false;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(context.handle.clone(), &partial)
        .await
        .with_context(|| format!("open partial {}", partial.display()))?;

    file.write_all_at(0, decoded.as_ref())
        .await
        .with_context(|| format!("write partial {}", partial.display()))?;

    context
        .state
        .update_partial_progress(key, meta.total_chunks.max(1), false)
        .await
        .with_context(|| format!("update batch progress {}", meta.relative_path))?;

    let bytes_written = finalize_partial_file(
        context,
        &relative,
        &partial,
        FileFinalize {
            relative_path: &meta.relative_path,
            file_hash: &meta.file_hash,
            size: meta.size,
            mode: meta.mode,
            mtime_sec: meta.mtime_sec,
            uid: meta.uid,
            gid: meta.gid,
            xattrs: &meta.xattrs,
            total_chunks: meta.total_chunks,
        },
    )
    .await?;

    Ok(SmallTaskResult {
        result: UploadSmallFileResult {
            accepted: true,
            skipped: false,
            message: "file finalized".to_string(),
            bytes_written,
        },
        completion: None,
    })
}

async fn write_small_file_direct(
    context: &mut StdioServerContext,
    relative: &Path,
    meta: &UploadSmallFileMeta,
    decoded: &[u8],
) -> Result<u64> {
    let final_path = context.destination.join(relative);

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(context.handle.clone(), &final_path)
        .await
        .with_context(|| format!("open destination {}", final_path.display()))?;

    file.write_all_at(0, decoded)
        .await
        .with_context(|| format!("write destination {}", final_path.display()))?;

    if context.preserve_metadata {
        apply_metadata(
            &context.handle,
            &final_path,
            meta.mode,
            meta.mtime_sec,
            meta.uid,
            meta.gid,
            &meta.xattrs,
            context.preserve_xattrs,
        )
        .await?;
    }

    if !context.partials_maybe_empty {
        let stale_partial = partial_path(context.state.partial_root(), relative);
        let _ = fs::remove_file(&context.handle, &stale_partial).await;
    }

    Ok(meta.size)
}

struct FileFinalize<'a> {
    relative_path: &'a str,
    file_hash: &'a str,
    size: u64,
    mode: u32,
    mtime_sec: i64,
    uid: u32,
    gid: u32,
    xattrs: &'a [XattrEntry],
    total_chunks: usize,
}

async fn finalize_uploaded_file(
    context: &mut StdioServerContext,
    req: &UploadBatchRequest,
    relative: &Path,
    partial_path: &Path,
) -> Result<u64> {
    finalize_partial_file(
        context,
        relative,
        partial_path,
        FileFinalize {
            relative_path: &req.relative_path,
            file_hash: &req.file_hash,
            size: req.size,
            mode: req.mode,
            mtime_sec: req.mtime_sec,
            uid: req.uid,
            gid: req.gid,
            xattrs: &req.xattrs,
            total_chunks: req.total_chunks,
        },
    )
    .await
}

async fn finalize_partial_file(
    context: &mut StdioServerContext,
    relative: &Path,
    partial_path: &Path,
    meta: FileFinalize<'_>,
) -> Result<u64> {
    let final_path = context.destination.join(relative);
    let partial_meta = fs::metadata_lite(&context.handle, partial_path)
        .await
        .with_context(|| format!("metadata {}", partial_path.display()))?;
    if partial_meta.size != meta.size {
        bail!(
            "size mismatch before finalize: expected {} got {}",
            meta.size,
            partial_meta.size
        );
    }

    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(&context.handle, parent)
            .await
            .with_context(|| format!("create destination parent {}", parent.display()))?;
    }

    fs::rename(&context.handle, partial_path, &final_path)
        .await
        .with_context(|| {
            format!(
                "promote partial {} -> {}",
                partial_path.display(),
                final_path.display()
            )
        })?;

    if context.preserve_metadata {
        apply_metadata(
            &context.handle,
            &final_path,
            meta.mode,
            meta.mtime_sec,
            meta.uid,
            meta.gid,
            meta.xattrs,
            context.preserve_xattrs,
        )
        .await?;
    }

    context
        .state
        .complete_file(
            meta.relative_path,
            meta.file_hash,
            meta.size,
            meta.mode,
            meta.mtime_sec,
            metadata::xattr_signature(meta.xattrs),
            meta.total_chunks,
        )
        .await
        .with_context(|| format!("persist completion for {}", meta.relative_path))?;

    Ok(meta.size)
}

async fn apply_metadata(
    handle: &RuntimeHandle,
    path: &Path,
    mode: u32,
    mtime_sec: i64,
    uid: u32,
    gid: u32,
    xattrs: &[XattrEntry],
    preserve_xattrs: bool,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(mode & 0o7777);
        fs::set_permissions(handle, path, perms)
            .await
            .with_context(|| format!("set permissions for {}", path.display()))?;
    }
    metadata::set_owner(path, uid, gid, true)?;
    if preserve_xattrs {
        metadata::apply_xattrs(path, xattrs, true)?;
    }
    metadata::set_mtime(path, mtime_sec, true)?;

    Ok(())
}
