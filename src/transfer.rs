use crate::certs;
use crate::compression;
use crate::model::FileManifest;
use crate::protocol::{
    Frame, InitAction, InitBatchRequest, InitFileRequest, InitFileResponse, UploadBatchRequest,
    UploadSmallBatchRequest, UploadSmallFileMeta,
};
use crate::scan::{self, ScanOptions};
use crate::util::{join_error, runtime_error};
use anyhow::{Context, Result, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use spargio::{RuntimeHandle, fs};
use spargio_quic::{QuicConnection, QuicEndpoint, QuicEndpointOptions};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const BATCH_TARGET_BYTES: usize = 8 * 1024 * 1024;
const SMALL_FILE_MAX_BYTES: u64 = 128 * 1024;
const SMALL_BATCH_MAX_FILES: usize = 4096;

#[derive(Debug, Clone)]
pub struct PushOptions {
    pub source: PathBuf,
    pub server: SocketAddr,
    pub server_name: String,
    pub ca: PathBuf,
    pub scan: ScanOptions,
    pub parallel_files: usize,
    pub connections: usize,
    pub compression_level: i32,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub max_stream_payload: usize,
    pub resume: bool,
    pub manifest_out: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct PushSummary {
    pub files_transferred: usize,
    pub files_skipped: usize,
    pub bytes_sent: u64,
    pub bytes_raw: u64,
    pub elapsed: Duration,
}

impl PushSummary {
    pub fn megabits_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= f64::EPSILON {
            return 0.0;
        }
        ((self.bytes_sent as f64) * 8.0) / secs / 1_000_000.0
    }
}

#[derive(Debug, Default, Clone)]
struct FileResult {
    transferred: bool,
    bytes_sent: u64,
    bytes_raw: u64,
}

#[derive(Debug, Default, Clone)]
struct BatchResult {
    files_transferred: usize,
    files_skipped: usize,
    bytes_sent: u64,
    bytes_raw: u64,
}

impl BatchResult {
    fn add_file_result(&mut self, result: FileResult) {
        if result.transferred {
            self.files_transferred = self.files_transferred.saturating_add(1);
        } else {
            self.files_skipped = self.files_skipped.saturating_add(1);
        }
        self.bytes_sent = self.bytes_sent.saturating_add(result.bytes_sent);
        self.bytes_raw = self.bytes_raw.saturating_add(result.bytes_raw);
    }

    fn merge(&mut self, other: BatchResult) {
        self.files_transferred = self
            .files_transferred
            .saturating_add(other.files_transferred);
        self.files_skipped = self.files_skipped.saturating_add(other.files_skipped);
        self.bytes_sent = self.bytes_sent.saturating_add(other.bytes_sent);
        self.bytes_raw = self.bytes_raw.saturating_add(other.bytes_raw);
    }
}

#[derive(Clone)]
struct FileTransferOptions {
    source_root: PathBuf,
    chunk_size: usize,
    compression_level: i32,
    max_stream_payload: usize,
    resume: bool,
}

pub async fn push_directory(handle: RuntimeHandle, options: PushOptions) -> Result<PushSummary> {
    let started = Instant::now();
    let client_config = certs::load_client_config(&options.ca)
        .with_context(|| format!("load CA {}", options.ca.display()))?;

    let endpoint_options = QuicEndpointOptions::default()
        .with_connect_timeout(options.connect_timeout)
        .with_operation_timeout(options.operation_timeout)
        .with_max_inflight_ops(65_536);

    let mut endpoint =
        QuicEndpoint::client_with_options("0.0.0.0:0".parse().unwrap(), endpoint_options)
            .context("create quic client endpoint")?;
    endpoint.set_default_client_config(client_config);

    let mut connections = Vec::with_capacity(options.connections.max(1));
    for _ in 0..options.connections.max(1) {
        let connection = endpoint
            .connect(options.server, &options.server_name)
            .await
            .with_context(|| format!("connect to {} ({})", options.server, options.server_name))?;
        connections.push(connection);
    }

    let (manifest, scan_stats) =
        scan::build_manifest(handle.clone(), &options.source, options.scan)
            .await
            .with_context(|| format!("build source manifest {}", options.source.display()))?;

    if let Some(path) = &options.manifest_out {
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        fs::write(&handle, path, bytes)
            .await
            .with_context(|| format!("write manifest {}", path.display()))?;
    }

    println!(
        "scan complete files={} bytes={} enumerate_ms={} hash_ms={}",
        manifest.files.len(),
        manifest.total_bytes,
        scan_stats.enumeration_elapsed.as_millis(),
        scan_stats.hash_elapsed.as_millis(),
    );

    let transfer_options = FileTransferOptions {
        source_root: PathBuf::from(&manifest.root),
        chunk_size: manifest.chunk_size,
        compression_level: options.compression_level,
        max_stream_payload: options.max_stream_payload,
        resume: options.resume,
    };

    let (small_files, large_files) =
        partition_small_files(manifest.files, transfer_options.chunk_size);

    let small_batches = build_small_batches(small_files, transfer_options.max_stream_payload);
    let small_join = if small_batches.is_empty() {
        None
    } else {
        let small_handle = handle.clone();
        let small_connection = connections[0].clone();
        let small_options = transfer_options.clone();
        Some(
            handle
                .spawn_stealable(async move {
                    let mut totals = BatchResult::default();
                    for batch in small_batches {
                        let result = transfer_small_batch(
                            &small_handle,
                            &small_connection,
                            &small_options,
                            &batch,
                        )
                        .await?;
                        totals.merge(result);
                    }
                    Ok::<BatchResult, anyhow::Error>(totals)
                })
                .map_err(|err| runtime_error("spawn small-batch transfer task", err))?,
        )
    };

    let mut totals = BatchResult::default();

    let mut files = large_files.into_iter();
    let mut running = FuturesUnordered::new();
    let mut next_connection = 0usize;
    for _ in 0..options.parallel_files.max(1) {
        if let Some(file) = files.next() {
            let connection = connections[next_connection % connections.len()].clone();
            next_connection = next_connection.saturating_add(1);
            running.push(spawn_transfer_job(
                handle.clone(),
                connection,
                transfer_options.clone(),
                file,
            )?);
        }
    }

    while let Some(joined) = running.next().await {
        let result = joined.map_err(|err| join_error("file transfer task canceled", err))??;
        totals.add_file_result(result);

        if let Some(file) = files.next() {
            let connection = connections[next_connection % connections.len()].clone();
            next_connection = next_connection.saturating_add(1);
            running.push(spawn_transfer_job(
                handle.clone(),
                connection,
                transfer_options.clone(),
                file,
            )?);
        }
    }

    if let Some(join) = small_join {
        let small_totals = join
            .await
            .map_err(|err| join_error("small-batch transfer task canceled", err))??;
        totals.merge(small_totals);
    }

    Ok(PushSummary {
        files_transferred: totals.files_transferred,
        files_skipped: totals.files_skipped,
        bytes_sent: totals.bytes_sent,
        bytes_raw: totals.bytes_raw,
        elapsed: started.elapsed(),
    })
}

fn partition_small_files(
    files: Vec<FileManifest>,
    chunk_size: usize,
) -> (Vec<FileManifest>, Vec<FileManifest>) {
    let mut small = Vec::new();
    let mut large = Vec::new();

    for file in files {
        if is_small_file_candidate(&file, chunk_size) {
            small.push(file);
        } else {
            large.push(file);
        }
    }

    (small, large)
}

fn is_small_file_candidate(file: &FileManifest, chunk_size: usize) -> bool {
    file.total_chunks == 1 && file.size <= SMALL_FILE_MAX_BYTES && file.size <= chunk_size as u64
}

fn build_small_batches(
    files: Vec<FileManifest>,
    max_stream_payload: usize,
) -> Vec<Vec<FileManifest>> {
    let batch_bytes_limit = max_stream_payload
        .saturating_sub(512 * 1024)
        .clamp(512 * 1024, 32 * 1024 * 1024);

    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;

    for file in files {
        let file_bytes = file.size.min(usize::MAX as u64) as usize;
        let would_overflow = !current.is_empty()
            && (current.len() >= SMALL_BATCH_MAX_FILES
                || current_bytes.saturating_add(file_bytes) > batch_bytes_limit);
        if would_overflow {
            batches.push(current);
            current = Vec::new();
            current_bytes = 0;
        }

        current_bytes = current_bytes.saturating_add(file_bytes);
        current.push(file);
    }

    if !current.is_empty() {
        batches.push(current);
    }

    batches
}

fn spawn_transfer_job(
    handle: RuntimeHandle,
    connection: QuicConnection,
    options: FileTransferOptions,
    file: FileManifest,
) -> Result<spargio::JoinHandle<Result<FileResult>>> {
    let task_handle = handle.clone();
    handle
        .spawn_stealable(async move {
            transfer_one_file(task_handle.clone(), connection, options, file)
                .await
                .with_context(|| "transfer one file")
        })
        .map_err(|err| runtime_error("spawn transfer task", err))
}

async fn transfer_one_file(
    handle: RuntimeHandle,
    connection: QuicConnection,
    options: FileTransferOptions,
    file: FileManifest,
) -> Result<FileResult> {
    let init_request = Frame::InitFileRequest(InitFileRequest {
        relative_path: file.relative_path.clone(),
        size: file.size,
        mode: file.mode,
        mtime_sec: file.mtime_sec,
        file_hash: file.file_hash.clone(),
        chunk_size: options.chunk_size,
        total_chunks: file.total_chunks,
        resume: options.resume,
    });

    let init_response =
        send_frame_roundtrip(&connection, init_request, None, options.max_stream_payload).await?;

    let init = match init_response {
        Frame::InitFileResponse(resp) => resp,
        Frame::Error(err) => bail!("init rejected for {}: {}", file.relative_path, err.message),
        other => bail!(
            "unexpected init response for {}: {other:?}",
            file.relative_path
        ),
    };

    if matches!(init.action, InitAction::Skip) {
        return Ok(FileResult {
            transferred: false,
            bytes_sent: 0,
            bytes_raw: 0,
        });
    }

    upload_file_batches(&handle, &connection, &options, &file, &init).await
}

async fn transfer_small_batch(
    handle: &RuntimeHandle,
    connection: &QuicConnection,
    options: &FileTransferOptions,
    files: &[FileManifest],
) -> Result<BatchResult> {
    if files.is_empty() {
        return Ok(BatchResult::default());
    }

    let init_request = Frame::InitBatchRequest(InitBatchRequest {
        files: files
            .iter()
            .map(|file| InitFileRequest {
                relative_path: file.relative_path.clone(),
                size: file.size,
                mode: file.mode,
                mtime_sec: file.mtime_sec,
                file_hash: file.file_hash.clone(),
                chunk_size: options.chunk_size,
                total_chunks: file.total_chunks,
                resume: options.resume,
            })
            .collect(),
    });

    let init_response =
        send_frame_roundtrip(connection, init_request, None, options.max_stream_payload).await?;
    let init = match init_response {
        Frame::InitBatchResponse(resp) => resp,
        Frame::Error(err) => bail!("small batch init rejected: {}", err.message),
        other => bail!("unexpected small batch init response: {other:?}"),
    };

    let mut init_by_path = HashMap::with_capacity(init.results.len());
    for result in init.results {
        init_by_path.insert(result.relative_path.clone(), result);
    }

    let mut totals = BatchResult::default();
    let mut upload_metas = Vec::new();
    let mut upload_payload = Vec::new();
    let mut upload_paths = Vec::new();
    let mut fallback = Vec::new();

    for file in files {
        let result = init_by_path
            .remove(&file.relative_path)
            .ok_or_else(|| anyhow::anyhow!("missing init response for {}", file.relative_path))?;

        if matches!(result.action, InitAction::Skip) {
            totals.files_skipped = totals.files_skipped.saturating_add(1);
            continue;
        }

        if result.next_chunk > 0 {
            fallback.push((
                file.clone(),
                InitFileResponse {
                    action: InitAction::Upload,
                    next_chunk: result.next_chunk,
                    message: result.message,
                },
            ));
            continue;
        }

        let source_path = options.source_root.join(Path::new(&file.relative_path));
        let raw = fs::read(handle, &source_path)
            .await
            .with_context(|| format!("read source {}", source_path.display()))?;
        if raw.len() as u64 != file.size {
            bail!(
                "small file size changed while reading {}: expected {} got {}",
                file.relative_path,
                file.size,
                raw.len()
            );
        }

        let (encoded, compressed) = compression::maybe_compress(&raw, options.compression_level)
            .with_context(|| format!("compress {}", file.relative_path))?;

        totals.bytes_raw = totals.bytes_raw.saturating_add(raw.len() as u64);
        totals.bytes_sent = totals.bytes_sent.saturating_add(encoded.len() as u64);
        upload_payload.extend_from_slice(&encoded);
        upload_paths.push(file.relative_path.clone());
        upload_metas.push(UploadSmallFileMeta {
            relative_path: file.relative_path.clone(),
            size: file.size,
            mode: file.mode,
            mtime_sec: file.mtime_sec,
            file_hash: file.file_hash.clone(),
            total_chunks: file.total_chunks,
            compressed,
            raw_len: raw.len(),
            data_len: encoded.len(),
        });
    }

    if !upload_metas.is_empty() {
        let upload_request = Frame::UploadSmallBatchRequest(UploadSmallBatchRequest {
            files: upload_metas,
        });

        let upload_response = send_frame_roundtrip(
            connection,
            upload_request,
            Some(&upload_payload),
            options.max_stream_payload,
        )
        .await?;

        let upload = match upload_response {
            Frame::UploadSmallBatchResponse(resp) => resp,
            Frame::Error(err) => bail!("small batch upload rejected: {}", err.message),
            other => bail!("unexpected small batch upload response: {other:?}"),
        };

        let mut response_by_path = HashMap::with_capacity(upload.results.len());
        for result in upload.results {
            response_by_path.insert(result.relative_path.clone(), result);
        }

        for path in upload_paths {
            let result = response_by_path
                .remove(&path)
                .ok_or_else(|| anyhow::anyhow!("missing upload response for {path}"))?;
            if !result.accepted {
                bail!("small-file upload rejected for {path}: {}", result.message);
            }
            if result.skipped {
                totals.files_skipped = totals.files_skipped.saturating_add(1);
            } else {
                totals.files_transferred = totals.files_transferred.saturating_add(1);
            }
        }
    }

    for (file, init) in fallback {
        let result = upload_file_batches(handle, connection, options, &file, &init).await?;
        totals.add_file_result(result);
    }

    Ok(totals)
}

async fn upload_file_batches(
    handle: &RuntimeHandle,
    connection: &QuicConnection,
    options: &FileTransferOptions,
    file: &FileManifest,
    init: &InitFileResponse,
) -> Result<FileResult> {
    let source_path = options.source_root.join(Path::new(&file.relative_path));
    let source_file = fs::File::open(handle.clone(), &source_path)
        .await
        .with_context(|| format!("open source {}", source_path.display()))?;

    let payload_budget = options
        .max_stream_payload
        .saturating_sub(64 * 1024)
        .max(1024);
    let per_chunk_budget = options.chunk_size.saturating_add(16).max(1);
    let max_chunks_per_batch = (payload_budget / per_chunk_budget).max(1);

    let mut next_chunk = init.next_chunk.min(file.total_chunks);
    let mut sent_bytes = 0u64;
    let mut raw_bytes = 0u64;

    loop {
        let start_chunk = next_chunk;
        let mut packets = Vec::new();
        let mut batch_estimate = 0usize;
        for _ in 0..max_chunks_per_batch {
            if next_chunk >= file.total_chunks {
                break;
            }
            let offset = (next_chunk as u64).saturating_mul(options.chunk_size as u64);
            let chunk = source_file
                .read_at(offset, options.chunk_size)
                .await
                .with_context(|| {
                    format!(
                        "read chunk {} from {} at offset {}",
                        next_chunk,
                        source_path.display(),
                        offset
                    )
                })?;

            if chunk.is_empty() {
                break;
            }

            let (encoded_payload, compressed) =
                compression::maybe_compress(&chunk, options.compression_level)?;
            let encoded_len = encoded_payload.len();
            packets.push(crate::protocol::ChunkPacket {
                raw_len: chunk.len(),
                compressed,
                data: encoded_payload,
            });
            batch_estimate = batch_estimate.saturating_add(9).saturating_add(encoded_len);
            next_chunk = next_chunk.saturating_add(1);

            if batch_estimate >= BATCH_TARGET_BYTES {
                break;
            }
        }

        let finalize = next_chunk >= file.total_chunks;
        if packets.is_empty() && !finalize {
            bail!(
                "batch assembly produced no packets before finalize for {}",
                file.relative_path
            );
        }

        let batch_payload = crate::protocol::encode_chunk_batch(&packets)?;
        let frame = Frame::UploadBatchRequest(UploadBatchRequest {
            relative_path: file.relative_path.clone(),
            size: file.size,
            mode: file.mode,
            mtime_sec: file.mtime_sec,
            file_hash: file.file_hash.clone(),
            total_chunks: file.total_chunks,
            start_chunk,
            chunk_size: options.chunk_size,
            sent_chunks: packets.len(),
            finalize,
        });

        let response = send_frame_roundtrip(
            connection,
            frame,
            Some(&batch_payload),
            options.max_stream_payload,
        )
        .await?;

        let response = match response {
            Frame::UploadBatchResponse(resp) => resp,
            Frame::Error(err) => {
                bail!(
                    "upload batch failed for {}: {}",
                    file.relative_path,
                    err.message
                )
            }
            other => bail!(
                "unexpected batch response for {}: {other:?}",
                file.relative_path
            ),
        };

        if !response.accepted {
            bail!(
                "upload batch rejected for {} at chunk {}: {}",
                file.relative_path,
                start_chunk,
                response.message
            );
        }

        let batch_sent = packets
            .iter()
            .map(|packet| packet.data.len() as u64)
            .sum::<u64>();
        let batch_raw = packets
            .iter()
            .map(|packet| packet.raw_len as u64)
            .sum::<u64>();
        sent_bytes = sent_bytes.saturating_add(batch_sent);
        raw_bytes = raw_bytes.saturating_add(batch_raw);

        if response.completed {
            return Ok(FileResult {
                transferred: true,
                bytes_sent: sent_bytes,
                bytes_raw: raw_bytes,
            });
        }

        if response.next_chunk <= start_chunk {
            bail!(
                "non-progress batch ack for {}: start={} next={} message={}",
                file.relative_path,
                start_chunk,
                response.next_chunk,
                response.message
            );
        }
        next_chunk = response.next_chunk.min(file.total_chunks);
    }
}

async fn send_frame_roundtrip(
    connection: &QuicConnection,
    request: Frame,
    payload: Option<&[u8]>,
    max_stream_payload: usize,
) -> Result<Frame> {
    let bytes = crate::protocol::encode(&request, payload).context("encode request frame")?;
    if bytes.len() > max_stream_payload {
        bail!(
            "encoded frame too large: {} > max_stream_payload {}",
            bytes.len(),
            max_stream_payload
        );
    }

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("open bidirectional stream")?;

    send.write_all(&bytes)
        .await
        .context("write frame request")?;
    send.finish().context("finish request stream")?;

    let response = recv
        .read_to_end(max_stream_payload)
        .await
        .context("read frame response")?;
    let (frame, payload) = crate::protocol::decode(&response).context("decode frame response")?;
    if !payload.is_empty() {
        bail!(
            "unexpected payload in response frame ({} bytes)",
            payload.len()
        );
    }
    Ok(frame)
}
