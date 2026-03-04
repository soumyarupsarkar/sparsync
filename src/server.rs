use crate::certs;
use crate::compression;
use crate::protocol::{
    ErrorFrame, Frame, InitAction, InitBatchRequest, InitBatchResponse, InitBatchResult,
    InitFileRequest, InitFileResponse, UploadBatchRequest, UploadBatchResponse,
    UploadColdBatchRequest, UploadColdBatchResponse, UploadColdFileMeta, UploadColdFileResult,
    UploadSmallBatchRequest, UploadSmallBatchResponse, UploadSmallFileMeta, UploadSmallFileResult,
};
use crate::state::{CompleteFileInput, StateStore};
use crate::util::{partial_path, sanitize_relative};
use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use spargio::{RuntimeHandle, fs};
use spargio_quic::{QuicEndpoint, QuicEndpointOptions};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub bind: SocketAddr,
    pub destination: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
    pub max_stream_payload: usize,
    pub preserve_metadata: bool,
}

#[derive(Clone)]
struct ServerContext {
    handle: RuntimeHandle,
    destination: Arc<PathBuf>,
    state: StateStore,
    max_stream_payload: usize,
    preserve_metadata: bool,
    partials_maybe_empty: Arc<AtomicBool>,
}

const BATCH_WRITE_CONCURRENCY_MIN: usize = 16;
const BATCH_WRITE_CONCURRENCY_MAX: usize = 96;

fn choose_write_concurrency(file_count: usize, total_bytes: u64) -> usize {
    if let Ok(value) = std::env::var("SPARSYNC_BATCH_WRITE_CONCURRENCY") {
        if let Ok(parsed) = value.parse::<usize>() {
            return parsed.max(1);
        }
    }

    if file_count == 0 {
        return BATCH_WRITE_CONCURRENCY_MIN;
    }

    let avg_bytes = total_bytes / file_count as u64;
    let target = if avg_bytes <= 8 * 1024 {
        24
    } else if avg_bytes <= 64 * 1024 {
        48
    } else if avg_bytes <= 512 * 1024 {
        64
    } else {
        BATCH_WRITE_CONCURRENCY_MAX
    };

    target
        .max(BATCH_WRITE_CONCURRENCY_MIN)
        .min(BATCH_WRITE_CONCURRENCY_MAX)
}

pub async fn run_server(handle: RuntimeHandle, options: ServeOptions) -> Result<()> {
    fs::create_dir_all(&handle, &options.destination)
        .await
        .with_context(|| format!("create destination {}", options.destination.display()))?;

    let server_config =
        certs::load_server_config(&options.cert, &options.key).with_context(|| {
            format!(
                "load cert/key cert={} key={}",
                options.cert.display(),
                options.key.display()
            )
        })?;

    let endpoint_options = QuicEndpointOptions::default()
        .with_accept_timeout(Duration::from_secs(30))
        .with_operation_timeout(Duration::from_secs(60))
        .with_max_inflight_ops(65_536);

    let endpoint = QuicEndpoint::server_with_options(server_config, options.bind, endpoint_options)
        .context("create quic server endpoint")?;

    info!(bind = %options.bind, destination = %options.destination.display(), "sparsync server started");

    let state = StateStore::open(handle.clone(), &options.destination)
        .await
        .context("open resume state")?;

    let partials_maybe_empty = match fs::read_dir(&handle, state.partial_root()).await {
        Ok(entries) => entries.is_empty(),
        Err(_) => true,
    };

    let context = ServerContext {
        handle: handle.clone(),
        destination: Arc::new(options.destination),
        state,
        max_stream_payload: options.max_stream_payload,
        preserve_metadata: options.preserve_metadata,
        partials_maybe_empty: Arc::new(AtomicBool::new(partials_maybe_empty)),
    };

    loop {
        let accepted = match endpoint.accept().await {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(err) => {
                return Err(err).context("accept incoming connection");
            }
        };

        let Some(connection) = accepted else {
            continue;
        };

        let context = context.clone();
        let conn_id = connection.stable_id();
        info!(connection_id = conn_id, "accepted QUIC connection");

        let spawn = handle.spawn_stealable(async move {
            if let Err(err) = handle_connection(context, connection).await {
                warn!(connection_id = conn_id, error = %err, "connection closed with error");
            }
        });
        if let Err(err) = spawn {
            error!(connection_id = conn_id, error = ?err, "failed to spawn connection task");
        }
    }
}

async fn handle_connection(
    context: ServerContext,
    connection: spargio_quic::QuicConnection,
) -> Result<()> {
    loop {
        match connection.accept_bi().await {
            Ok((send, recv)) => {
                let stream_conn_id = connection.stable_id();
                let stream_context = context.clone();
                let spawn = context.handle.spawn_stealable(async move {
                    if let Err(err) = handle_stream(stream_context, send, recv).await {
                        warn!(
                            connection_id = stream_conn_id,
                            error = %err,
                            "stream handling failed"
                        );
                    }
                });
                if let Err(err) = spawn {
                    warn!(
                        connection_id = stream_conn_id,
                        error = ?err,
                        "failed to spawn stream task"
                    );
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(err) if is_closed_error(&err) => {
                return Ok(());
            }
            Err(err) => {
                return Err(err).context("accept bidirectional stream");
            }
        }
    }
}

async fn handle_stream(
    context: ServerContext,
    mut send: spargio_quic::QuicSendStream,
    mut recv: spargio_quic::QuicRecvStream,
) -> Result<()> {
    let bytes = recv
        .read_to_end(context.max_stream_payload)
        .await
        .context("read stream payload")?;
    let (frame, payload) = crate::protocol::decode(&bytes).context("decode frame")?;

    let response = match frame {
        Frame::InitFileRequest(req) => process_init_file(&context, req).await,
        Frame::InitBatchRequest(req) => process_init_batch(&context, req).await,
        Frame::UploadBatchRequest(req) => process_upload_batch(&context, req, payload).await,
        Frame::UploadSmallBatchRequest(req) => {
            process_upload_small_batch(&context, req, payload).await
        }
        Frame::UploadColdBatchRequest(req) => {
            process_upload_cold_batch(&context, req, payload).await
        }
        other => Ok(Frame::Error(ErrorFrame {
            message: format!("unexpected frame on server: {other:?}"),
        })),
    };

    let response = match response {
        Ok(frame) => frame,
        Err(err) => Frame::Error(ErrorFrame {
            message: format!("{err:#}"),
        }),
    };

    let response_bytes = crate::protocol::encode(&response, None).context("encode response")?;
    send.write_all(&response_bytes)
        .await
        .context("write response")?;
    send.finish().context("finish response stream")?;
    Ok(())
}

async fn process_init_file(context: &ServerContext, req: InitFileRequest) -> Result<Frame> {
    let response = initialize_one_file(context, &req).await?;
    Ok(Frame::InitFileResponse(response))
}

async fn process_init_batch(context: &ServerContext, req: InitBatchRequest) -> Result<Frame> {
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
    for (file, relative, skip) in prepared {
        let response = if skip {
            InitFileResponse {
                action: InitAction::Skip,
                next_chunk: file.total_chunks,
                message: "already up to date".to_string(),
            }
        } else {
            initialize_one_file_with_relative(context, &file, &relative, false).await?
        };
        results.push(InitBatchResult {
            action: response.action,
            next_chunk: response.next_chunk,
            message: response.message,
        });
    }

    Ok(Frame::InitBatchResponse(InitBatchResponse { results }))
}

async fn initialize_one_file(
    context: &ServerContext,
    req: &InitFileRequest,
) -> Result<InitFileResponse> {
    let relative = sanitize_relative(&req.relative_path)?;
    initialize_one_file_with_relative(context, req, &relative, true).await
}

async fn initialize_one_file_with_relative(
    context: &ServerContext,
    req: &InitFileRequest,
    relative: &Path,
    prepare_dirs: bool,
) -> Result<InitFileResponse> {
    if context
        .state
        .is_complete_match(&req.relative_path, &req.file_hash, req.size)?
    {
        return Ok(InitFileResponse {
            action: InitAction::Skip,
            next_chunk: req.total_chunks,
            message: "already up to date".to_string(),
        });
    }

    let final_path = context.destination.join(relative);
    let partial_path = partial_path(context.state.partial_root(), &relative);
    if prepare_dirs {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(&context.handle, parent)
                .await
                .with_context(|| format!("create destination parent {}", parent.display()))?;
        }

        if let Some(parent) = partial_path.parent() {
            fs::create_dir_all(&context.handle, parent)
                .await
                .with_context(|| format!("create partial parent {}", parent.display()))?;
        }
    }

    let existing_partial_chunks = if req.resume
        && !context.partials_maybe_empty.load(Ordering::Relaxed)
    {
        match fs::metadata_lite(&context.handle, &partial_path).await {
            Ok(meta) if meta.is_file() => {
                if meta.size > req.size {
                    let _ = fs::remove_file(&context.handle, &partial_path).await;
                    None
                } else {
                    Some((meta.size / req.chunk_size as u64) as usize)
                }
            }
            Ok(_) => None,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read partial metadata {}", partial_path.display()));
            }
        }
    } else {
        None
    };

    let response = context
        .state
        .initialize_file(&req, existing_partial_chunks)
        .await
        .with_context(|| format!("initialize file state {}", req.relative_path))?;

    if matches!(response.action, InitAction::Upload)
        && response.next_chunk == 0
        && existing_partial_chunks.is_some()
    {
        if let Err(err) = fs::remove_file(&context.handle, &partial_path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err)
                    .with_context(|| format!("reset partial {}", partial_path.display()));
            }
        }
    }

    Ok(response)
}

async fn process_upload_batch(
    context: &ServerContext,
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

    let partial_path = partial_path(context.state.partial_root(), &relative);
    if let Some(parent) = partial_path.parent() {
        fs::create_dir_all(&context.handle, parent)
            .await
            .with_context(|| format!("create partial parent {}", parent.display()))?;
    }
    context.partials_maybe_empty.store(false, Ordering::Relaxed);

    let open = fs::OpenOptions::new().read(true).write(true).create(true);
    let open = if req.start_chunk == 0 {
        open.truncate(true)
    } else {
        open
    };

    let file = open
        .open(context.handle.clone(), &partial_path)
        .await
        .with_context(|| format!("open partial {}", partial_path.display()))?;

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
            .with_context(|| format!("write partial {} at {}", partial_path.display(), offset))?;
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

    let bytes_written = finalize_uploaded_file(context, &req, &relative, &partial_path).await?;
    Ok(Frame::UploadBatchResponse(UploadBatchResponse {
        accepted: true,
        message: "file finalized".to_string(),
        next_chunk,
        completed: true,
        bytes_written,
    }))
}

async fn process_upload_small_batch(
    context: &ServerContext,
    req: UploadSmallBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    let write_concurrency =
        choose_write_concurrency(req.files.len(), req.files.iter().map(|f| f.size).sum());
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

    let total_files = req.files.len();
    let mut pending = req
        .files
        .into_iter()
        .zip(payload_parts.into_iter())
        .enumerate();

    let mut running = FuturesUnordered::new();
    for _ in 0..write_concurrency {
        let Some((index, (meta, encoded_payload))) = pending.next() else {
            break;
        };
        running.push(run_small_upload_task_indexed(
            context.clone(),
            index,
            meta,
            encoded_payload,
        ));
    }

    let mut ordered = vec![None; total_files];
    let mut completions = Vec::new();
    while let Some((index, result)) = running.next().await {
        if let Some(completion) = result.completion {
            completions.push(completion);
        }
        ordered[index] = Some(result.result);
        if let Some((index, (meta, encoded_payload))) = pending.next() {
            running.push(run_small_upload_task_indexed(
                context.clone(),
                index,
                meta,
                encoded_payload,
            ));
        }
    }

    let mut results = Vec::with_capacity(total_files);
    for (index, item) in ordered.into_iter().enumerate() {
        let result = item.ok_or_else(|| anyhow::anyhow!("missing small-batch result {index}"))?;
        results.push(result);
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
    context: &ServerContext,
    req: UploadColdBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    let write_concurrency =
        choose_write_concurrency(req.files.len(), req.files.iter().map(|f| f.size).sum());
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

    let total_files = req.files.len();
    let mut pending = req
        .files
        .into_iter()
        .zip(payload_parts.into_iter())
        .enumerate();

    let mut running = FuturesUnordered::new();
    for _ in 0..write_concurrency {
        let Some((index, (meta, encoded_payload))) = pending.next() else {
            break;
        };
        running.push(run_cold_upload_task_indexed(
            context.clone(),
            index,
            meta,
            encoded_payload,
            allow_skip,
        ));
    }

    let mut ordered = vec![None; total_files];
    let mut completions = Vec::new();
    while let Some((index, result)) = running.next().await {
        if let Some(completion) = result.completion {
            completions.push(completion);
        }
        ordered[index] = Some(result.result);
        if let Some((index, (meta, encoded_payload))) = pending.next() {
            running.push(run_cold_upload_task_indexed(
                context.clone(),
                index,
                meta,
                encoded_payload,
                allow_skip,
            ));
        }
    }

    let mut results = Vec::with_capacity(total_files);
    for (index, item) in ordered.into_iter().enumerate() {
        let result = item.ok_or_else(|| anyhow::anyhow!("missing cold-batch result {index}"))?;
        results.push(result);
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

async fn run_small_upload_task(
    context: ServerContext,
    meta: UploadSmallFileMeta,
    encoded: &[u8],
) -> SmallTaskResult {
    match process_upload_small_one(&context, meta, encoded).await {
        Ok(ok) => ok,
        Err(err) => SmallTaskResult {
            result: UploadSmallFileResult {
                accepted: false,
                skipped: false,
                message: format!("{err:#}"),
                bytes_written: 0,
            },
            completion: None,
        },
    }
}

async fn run_small_upload_task_indexed(
    context: ServerContext,
    index: usize,
    meta: UploadSmallFileMeta,
    encoded: &[u8],
) -> (usize, SmallTaskResult) {
    let result = run_small_upload_task(context, meta, encoded).await;
    (index, result)
}

async fn run_cold_upload_task(
    context: ServerContext,
    meta: UploadColdFileMeta,
    encoded: &[u8],
    allow_skip: bool,
) -> ColdTaskResult {
    match process_upload_cold_one(&context, meta, encoded, allow_skip).await {
        Ok(ok) => ok,
        Err(err) => ColdTaskResult {
            result: UploadColdFileResult {
                accepted: false,
                skipped: false,
                message: format!("{err:#}"),
                bytes_written: 0,
            },
            completion: None,
        },
    }
}

async fn run_cold_upload_task_indexed(
    context: ServerContext,
    index: usize,
    meta: UploadColdFileMeta,
    encoded: &[u8],
    allow_skip: bool,
) -> (usize, ColdTaskResult) {
    let result = run_cold_upload_task(context, meta, encoded, allow_skip).await;
    (index, result)
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
    context: &ServerContext,
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
        return Ok(ColdTaskResult {
            result: UploadColdFileResult {
                accepted: true,
                skipped: true,
                message: "already completed".to_string(),
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
        apply_metadata(&context.handle, &final_path, meta.mode, meta.mtime_sec).await?;
    }

    if !context.partials_maybe_empty.load(Ordering::Relaxed) {
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
            total_chunks: meta.total_chunks,
        }),
    })
}

async fn process_upload_small_one(
    context: &ServerContext,
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
                total_chunks: meta.total_chunks,
            }),
        });
    }

    let partial_path = partial_path(context.state.partial_root(), &relative);
    context.partials_maybe_empty.store(false, Ordering::Relaxed);

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(context.handle.clone(), &partial_path)
        .await
        .with_context(|| format!("open partial {}", partial_path.display()))?;

    file.write_all_at(0, decoded.as_ref())
        .await
        .with_context(|| format!("write partial {}", partial_path.display()))?;

    context
        .state
        .update_partial_progress(key, meta.total_chunks.max(1), false)
        .await
        .with_context(|| format!("update batch progress {}", meta.relative_path))?;

    let bytes_written = finalize_partial_file(
        context,
        &relative,
        &partial_path,
        FileFinalize {
            relative_path: &meta.relative_path,
            file_hash: &meta.file_hash,
            size: meta.size,
            mode: meta.mode,
            mtime_sec: meta.mtime_sec,
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
    context: &ServerContext,
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
        apply_metadata(&context.handle, &final_path, meta.mode, meta.mtime_sec).await?;
    }

    if !context.partials_maybe_empty.load(Ordering::Relaxed) {
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
    total_chunks: usize,
}

async fn finalize_uploaded_file(
    context: &ServerContext,
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
            total_chunks: req.total_chunks,
        },
    )
    .await
}

async fn finalize_partial_file(
    context: &ServerContext,
    relative: &Path,
    partial_path: &Path,
    meta: FileFinalize<'_>,
) -> Result<u64> {
    let final_path = context.destination.join(relative);

    let partial_meta = fs::metadata_lite(&context.handle, partial_path)
        .await
        .with_context(|| format!("metadata {}", partial_path.display()))?;
    if partial_meta.size != meta.size {
        return Err(anyhow::anyhow!(
            "size mismatch before finalize: expected {} got {}",
            meta.size,
            partial_meta.size
        ));
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
        apply_metadata(&context.handle, &final_path, meta.mode, meta.mtime_sec).await?;
    }

    context
        .state
        .complete_file(
            meta.relative_path,
            meta.file_hash,
            meta.size,
            meta.mode,
            meta.mtime_sec,
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
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(mode & 0o7777);
        fs::set_permissions(handle, path, perms)
            .await
            .with_context(|| format!("set permissions for {}", path.display()))?;
    }

    let file_time = filetime::FileTime::from_unix_time(mtime_sec, 0);
    filetime::set_file_mtime(path, file_time)
        .with_context(|| format!("set file mtime for {}", path.display()))?;

    Ok(())
}

fn is_closed_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::NotConnected
    )
}
