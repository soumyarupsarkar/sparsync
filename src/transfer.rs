use crate::certs;
use crate::compression;
use crate::model::FileManifest;
use crate::protocol::{
    Frame, InitAction, InitBatchRequest, InitFileRequest, InitFileResponse, UploadBatchRequest,
    UploadColdBatchRequest, UploadColdFileMeta, UploadSmallBatchRequest, UploadSmallFileMeta,
};
use crate::scan::{self, ScanOptions};
use crate::util::{join_error, runtime_error};
use anyhow::{Context, Result, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use spargio::{RuntimeHandle, fs};
use spargio_quic::{
    QuicConnection, QuicEndpoint, QuicEndpointOptions, QuicRecvStream, QuicSendStream,
};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BATCH_TARGET_BYTES: usize = 8 * 1024 * 1024;
const COLD_BATCH_TARGET_BYTES: usize = 32 * 1024 * 1024;
const DIRECT_BATCH_TARGET_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SMALL_FILE_MAX_BYTES: u64 = 128 * 1024;
const DEFAULT_DIRECT_FILE_MAX_BYTES: u64 = 4 * 1024 * 1024;
const SMALL_BATCH_MAX_FILES: usize = 4096;
const INIT_BATCH_MAX_FILES: usize = 4096;
const DEFAULT_UPLOAD_WINDOW: usize = 4;

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
    pub cold_start: bool,
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
    stats: TransferStats,
    small_file_max_bytes: u64,
    direct_file_max_bytes: u64,
}

#[derive(Default)]
struct TransferStatsInner {
    connections: AtomicU64,
    control_frames: AtomicU64,
    streams_opened: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    disk_read_bytes: AtomicU64,
    encoded_bytes: AtomicU64,
    connect_ns: AtomicU64,
    disk_read_ns: AtomicU64,
    encode_ns: AtomicU64,
    roundtrip_ns: AtomicU64,
}

#[derive(Clone, Default)]
struct TransferStats {
    enabled: bool,
    inner: Arc<TransferStatsInner>,
}

impl TransferStats {
    fn from_env() -> Self {
        let enabled = std::env::var("SPARSYNC_PROFILE")
            .map(|value| value != "0")
            .unwrap_or(false);
        Self {
            enabled,
            inner: Arc::new(TransferStatsInner::default()),
        }
    }

    fn add_connections(&self, count: usize) {
        self.inner
            .connections
            .store(count as u64, Ordering::Relaxed);
    }

    fn add_connect_time(&self, elapsed: Duration) {
        self.inner
            .connect_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_disk_read(&self, elapsed: Duration, bytes: u64) {
        self.inner
            .disk_read_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.inner
            .disk_read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn add_encode(&self, elapsed: Duration, bytes: usize) {
        self.inner
            .encode_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.inner
            .encoded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn add_control_frame(&self) {
        self.inner.control_frames.fetch_add(1, Ordering::Relaxed);
    }

    fn add_stream_opened(&self) {
        self.inner.streams_opened.fetch_add(1, Ordering::Relaxed);
    }

    fn add_request_bytes(&self, bytes: usize) {
        self.inner
            .request_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn add_response_bytes(&self, bytes: usize) {
        self.inner
            .response_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn add_roundtrip_time(&self, elapsed: Duration) {
        self.inner
            .roundtrip_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn print_if_enabled(&self, total_elapsed: Duration, cold_start: bool) {
        if !self.enabled {
            return;
        }
        let label = if cold_start { "cold" } else { "normal" };
        println!(
            "profile mode={} connections={} control_frames={} streams_opened={} request_bytes={} response_bytes={} disk_read_bytes={} encoded_bytes={} connect_ms={} disk_read_ms={} encode_ms={} roundtrip_ms={} total_ms={}",
            label,
            self.inner.connections.load(Ordering::Relaxed),
            self.inner.control_frames.load(Ordering::Relaxed),
            self.inner.streams_opened.load(Ordering::Relaxed),
            self.inner.request_bytes.load(Ordering::Relaxed),
            self.inner.response_bytes.load(Ordering::Relaxed),
            self.inner.disk_read_bytes.load(Ordering::Relaxed),
            self.inner.encoded_bytes.load(Ordering::Relaxed),
            Duration::from_nanos(self.inner.connect_ns.load(Ordering::Relaxed)).as_millis(),
            Duration::from_nanos(self.inner.disk_read_ns.load(Ordering::Relaxed)).as_millis(),
            Duration::from_nanos(self.inner.encode_ns.load(Ordering::Relaxed)).as_millis(),
            Duration::from_nanos(self.inner.roundtrip_ns.load(Ordering::Relaxed)).as_millis(),
            total_elapsed.as_millis(),
        );
    }
}

fn small_file_max_bytes_from_env() -> u64 {
    std::env::var("SPARSYNC_SMALL_FILE_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_SMALL_FILE_MAX_BYTES)
}

fn direct_file_max_bytes_from_env() -> u64 {
    std::env::var("SPARSYNC_DIRECT_FILE_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_DIRECT_FILE_MAX_BYTES)
}

fn auto_connections_enabled() -> bool {
    std::env::var("SPARSYNC_AUTO_CONNECTIONS")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn upload_window_from_env() -> usize {
    std::env::var("SPARSYNC_UPLOAD_WINDOW")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_UPLOAD_WINDOW)
}

pub async fn push_directory(handle: RuntimeHandle, options: PushOptions) -> Result<PushSummary> {
    let started = Instant::now();
    let stats = TransferStats::from_env();
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
    let connect_started = Instant::now();
    for _ in 0..options.connections.max(1) {
        let connection = endpoint
            .connect(options.server, &options.server_name)
            .await
            .with_context(|| format!("connect to {} ({})", options.server, options.server_name))?;
        connections.push(connection);
    }
    stats.add_connect_time(connect_started.elapsed());
    stats.add_connections(connections.len());

    if options.cold_start {
        let summary =
            push_directory_cold(handle.clone(), &options, &connections, started, stats).await?;
        return Ok(summary);
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
        stats: stats.clone(),
        small_file_max_bytes: small_file_max_bytes_from_env(),
        direct_file_max_bytes: direct_file_max_bytes_from_env(),
    };

    let (small_files, large_files) = partition_small_files(
        manifest.files,
        transfer_options.chunk_size,
        transfer_options.small_file_max_bytes,
    );

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
    let (init_totals, uploads) =
        initialize_large_files(&connections[0], &transfer_options, large_files).await?;
    totals.merge(init_totals);

    if auto_connections_enabled() && options.connections == 1 {
        let upload_bytes = uploads.iter().map(|(file, _)| file.size).sum::<u64>();
        if !uploads.is_empty() && upload_bytes >= 8 * 1024 * 1024 {
            let connect_started = Instant::now();
            let connection = endpoint
                .connect(options.server, &options.server_name)
                .await
                .with_context(|| {
                    format!(
                        "connect additional stream to {} ({})",
                        options.server, options.server_name
                    )
                })?;
            connections.push(connection);
            stats.add_connect_time(connect_started.elapsed());
            stats.add_connections(connections.len());
        }
    }

    let (direct_files, streamed_files) =
        split_direct_uploads(uploads, transfer_options.direct_file_max_bytes);
    let direct_totals = transfer_initialized_direct_batches(
        handle.clone(),
        &connections,
        &transfer_options,
        direct_files,
    )
    .await?;
    totals.merge(direct_totals);

    let mut files = streamed_files.into_iter();
    let mut running = FuturesUnordered::new();
    let mut next_connection = 0usize;
    for _ in 0..options.parallel_files.max(1) {
        if let Some((file, init)) = files.next() {
            let connection = connections[next_connection % connections.len()].clone();
            next_connection = next_connection.saturating_add(1);
            running.push(spawn_upload_job(
                handle.clone(),
                connection,
                transfer_options.clone(),
                file,
                init,
            )?);
        }
    }

    while let Some(joined) = running.next().await {
        let result = joined.map_err(|err| join_error("file transfer task canceled", err))??;
        totals.add_file_result(result);

        if let Some((file, init)) = files.next() {
            let connection = connections[next_connection % connections.len()].clone();
            next_connection = next_connection.saturating_add(1);
            running.push(spawn_upload_job(
                handle.clone(),
                connection,
                transfer_options.clone(),
                file,
                init,
            )?);
        }
    }

    if let Some(join) = small_join {
        let small_totals = join
            .await
            .map_err(|err| join_error("small-batch transfer task canceled", err))??;
        totals.merge(small_totals);
    }

    let elapsed = started.elapsed();
    stats.print_if_enabled(elapsed, false);

    Ok(PushSummary {
        files_transferred: totals.files_transferred,
        files_skipped: totals.files_skipped,
        bytes_sent: totals.bytes_sent,
        bytes_raw: totals.bytes_raw,
        elapsed,
    })
}

async fn push_directory_cold(
    handle: RuntimeHandle,
    options: &PushOptions,
    connections: &[QuicConnection],
    started: Instant,
    stats: TransferStats,
) -> Result<PushSummary> {
    let chunk_size = options.scan.chunk_size.max(1);
    let (source_root, files, enumeration_elapsed, metadata_elapsed) = scan::build_file_list(
        handle.clone(),
        &options.source,
        options.scan.scan_workers.max(1),
        options.scan.hash_workers.max(1),
    )
    .await
    .with_context(|| format!("build cold file list {}", options.source.display()))?;

    let total_bytes = files.iter().map(|item| item.size).sum::<u64>();
    println!(
        "scan complete files={} bytes={} enumerate_ms={} hash_ms={}",
        files.len(),
        total_bytes,
        enumeration_elapsed.as_millis(),
        metadata_elapsed.as_millis(),
    );

    if let Some(path) = &options.manifest_out {
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "root": source_root,
            "chunk_size": chunk_size,
            "files": files.iter().map(|item| serde_json::json!({
                "relative_path": item.relative_path,
                "size": item.size,
                "mode": item.mode,
                "mtime_sec": item.mtime_sec,
            })).collect::<Vec<_>>(),
            "total_bytes": total_bytes,
            "cold_start": true,
        }))?;
        fs::write(&handle, path, bytes)
            .await
            .with_context(|| format!("write manifest {}", path.display()))?;
    }

    let mut batch_limit = options
        .max_stream_payload
        .saturating_sub(512 * 1024)
        .clamp(512 * 1024, COLD_BATCH_TARGET_BYTES);
    if connections.len() > 1 {
        let split_target = (total_bytes as usize)
            .saturating_div(connections.len())
            .max(4 * 1024 * 1024);
        batch_limit = batch_limit.min(split_target);
    }
    let max_inflight_batches = connections.len().saturating_mul(2).max(1);

    let mut totals = BatchResult::default();
    let mut payload = Vec::new();
    let mut metas = Vec::new();
    let mut pending = Vec::new();
    let mut running = FuturesUnordered::new();
    let mut next_connection = 0usize;

    for file in files {
        let source_path = source_root.join(Path::new(&file.relative_path));
        let read_started = Instant::now();
        let raw = fs::read(&handle, &source_path)
            .await
            .with_context(|| format!("read source {}", source_path.display()))?;
        stats.add_disk_read(read_started.elapsed(), raw.len() as u64);
        if raw.len() as u64 != file.size {
            bail!(
                "file size changed during cold read for {}: expected {} got {}",
                file.relative_path,
                file.size,
                raw.len()
            );
        }

        let raw_len = raw.len();
        let file_hash = blake3::hash(&raw).to_hex().to_string();
        let total_chunks = if file.size == 0 {
            0
        } else {
            ((file.size + (chunk_size as u64).saturating_sub(1)) / chunk_size as u64) as usize
        };
        let encode_started = Instant::now();
        let (encoded, compressed) = compression::maybe_compress_vec(raw, options.compression_level)
            .with_context(|| format!("compress {}", file.relative_path))?;
        stats.add_encode(encode_started.elapsed(), encoded.len());
        let encoded_len = encoded.len();

        if !metas.is_empty() && payload.len().saturating_add(encoded_len) > batch_limit {
            let connection = connections[next_connection % connections.len()].clone();
            next_connection = next_connection.saturating_add(1);
            running.push(upload_cold_batch(
                connection,
                std::mem::take(&mut metas),
                std::mem::take(&mut payload),
                std::mem::take(&mut pending),
                options.max_stream_payload,
                stats.clone(),
            ));

            if running.len() >= max_inflight_batches {
                let joined = running
                    .next()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("missing in-flight cold upload result"))??;
                totals.merge(joined);
            }
        }

        payload.extend_from_slice(&encoded);
        pending.push((
            file.relative_path.clone(),
            raw_len as u64,
            encoded_len as u64,
        ));
        metas.push(UploadColdFileMeta {
            relative_path: file.relative_path,
            size: file.size,
            mode: file.mode,
            mtime_sec: file.mtime_sec,
            file_hash,
            total_chunks,
            compressed,
            raw_len,
            data_len: encoded_len,
        });
    }

    if !metas.is_empty() {
        let connection = connections[next_connection % connections.len()].clone();
        running.push(upload_cold_batch(
            connection,
            std::mem::take(&mut metas),
            std::mem::take(&mut payload),
            std::mem::take(&mut pending),
            options.max_stream_payload,
            stats.clone(),
        ));
    }

    while let Some(joined) = running.next().await {
        totals.merge(joined?);
    }

    let elapsed = started.elapsed();
    stats.print_if_enabled(elapsed, true);

    Ok(PushSummary {
        files_transferred: totals.files_transferred,
        files_skipped: totals.files_skipped,
        bytes_sent: totals.bytes_sent,
        bytes_raw: totals.bytes_raw,
        elapsed,
    })
}

async fn upload_cold_batch(
    connection: QuicConnection,
    metas: Vec<UploadColdFileMeta>,
    payload: Vec<u8>,
    pending: Vec<(String, u64, u64)>,
    max_stream_payload: usize,
    stats: TransferStats,
) -> Result<BatchResult> {
    if metas.is_empty() {
        return Ok(BatchResult::default());
    }

    let response = send_frame_roundtrip(
        &connection,
        Frame::UploadColdBatchRequest(UploadColdBatchRequest {
            allow_skip: false,
            files: metas,
        }),
        Some(&payload),
        max_stream_payload,
        &stats,
    )
    .await?;

    let response = match response {
        Frame::UploadColdBatchResponse(resp) => resp,
        Frame::Error(err) => bail!("cold batch upload rejected: {}", err.message),
        other => bail!("unexpected cold batch response: {other:?}"),
    };

    if response.results.len() != pending.len() {
        bail!(
            "cold batch response size mismatch: got {} expected {}",
            response.results.len(),
            pending.len()
        );
    }

    let mut totals = BatchResult::default();
    for (result, (path, raw, encoded)) in response.results.into_iter().zip(pending) {
        if !result.accepted {
            bail!("cold batch file rejected for {path}: {}", result.message);
        }
        if result.skipped {
            totals.files_skipped = totals.files_skipped.saturating_add(1);
        } else {
            totals.files_transferred = totals.files_transferred.saturating_add(1);
            totals.bytes_raw = totals.bytes_raw.saturating_add(raw);
            totals.bytes_sent = totals.bytes_sent.saturating_add(encoded);
        }
    }

    Ok(totals)
}

async fn transfer_initialized_direct_batches(
    handle: RuntimeHandle,
    connections: &[QuicConnection],
    options: &FileTransferOptions,
    files: Vec<FileManifest>,
) -> Result<BatchResult> {
    if files.is_empty() {
        return Ok(BatchResult::default());
    }

    let total_bytes = files.iter().map(|file| file.size).sum::<u64>();
    let mut batch_limit = options
        .max_stream_payload
        .saturating_sub(512 * 1024)
        .clamp(512 * 1024, DIRECT_BATCH_TARGET_BYTES);
    if connections.len() > 1 {
        let split_target = (total_bytes as usize)
            .saturating_div(connections.len())
            .max(4 * 1024 * 1024);
        batch_limit = batch_limit.min(split_target);
    }
    let max_inflight_batches = connections.len().saturating_mul(2).max(1);

    let mut totals = BatchResult::default();
    let mut payload = Vec::new();
    let mut metas = Vec::new();
    let mut pending = Vec::new();
    let mut running = FuturesUnordered::new();
    let mut next_connection = 0usize;

    for file in files {
        let source_path = options.source_root.join(Path::new(&file.relative_path));
        let read_started = Instant::now();
        let raw = fs::read(&handle, &source_path)
            .await
            .with_context(|| format!("read source {}", source_path.display()))?;
        options
            .stats
            .add_disk_read(read_started.elapsed(), raw.len() as u64);
        if raw.len() as u64 != file.size {
            bail!(
                "file size changed during direct read for {}: expected {} got {}",
                file.relative_path,
                file.size,
                raw.len()
            );
        }

        let raw_len = raw.len();
        let encode_started = Instant::now();
        let (encoded, compressed) = compression::maybe_compress_vec(raw, options.compression_level)
            .with_context(|| format!("compress {}", file.relative_path))?;
        options
            .stats
            .add_encode(encode_started.elapsed(), encoded.len());
        let encoded_len = encoded.len();

        if !metas.is_empty() && payload.len().saturating_add(encoded_len) > batch_limit {
            let connection = connections[next_connection % connections.len()].clone();
            next_connection = next_connection.saturating_add(1);
            running.push(upload_cold_batch(
                connection,
                std::mem::take(&mut metas),
                std::mem::take(&mut payload),
                std::mem::take(&mut pending),
                options.max_stream_payload,
                options.stats.clone(),
            ));

            if running.len() >= max_inflight_batches {
                let joined = running
                    .next()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("missing in-flight direct upload result"))??;
                totals.merge(joined);
            }
        }

        payload.extend_from_slice(&encoded);
        pending.push((
            file.relative_path.clone(),
            raw_len as u64,
            encoded_len as u64,
        ));
        metas.push(UploadColdFileMeta {
            relative_path: file.relative_path,
            size: file.size,
            mode: file.mode,
            mtime_sec: file.mtime_sec,
            file_hash: file.file_hash,
            total_chunks: file.total_chunks,
            compressed,
            raw_len,
            data_len: encoded_len,
        });
    }

    if !metas.is_empty() {
        let connection = connections[next_connection % connections.len()].clone();
        running.push(upload_cold_batch(
            connection,
            std::mem::take(&mut metas),
            std::mem::take(&mut payload),
            std::mem::take(&mut pending),
            options.max_stream_payload,
            options.stats.clone(),
        ));
    }

    while let Some(joined) = running.next().await {
        totals.merge(joined?);
    }

    Ok(totals)
}

fn partition_small_files(
    files: Vec<FileManifest>,
    chunk_size: usize,
    small_file_max_bytes: u64,
) -> (Vec<FileManifest>, Vec<FileManifest>) {
    let mut small = Vec::new();
    let mut large = Vec::new();

    for file in files {
        if is_small_file_candidate(&file, chunk_size, small_file_max_bytes) {
            small.push(file);
        } else {
            large.push(file);
        }
    }

    (small, large)
}

fn split_direct_uploads(
    uploads: Vec<(FileManifest, InitFileResponse)>,
    direct_file_max_bytes: u64,
) -> (Vec<FileManifest>, Vec<(FileManifest, InitFileResponse)>) {
    let mut direct = Vec::new();
    let mut streamed = Vec::new();

    for (file, init) in uploads {
        if init.next_chunk == 0 && file.size <= direct_file_max_bytes {
            direct.push(file);
        } else {
            streamed.push((file, init));
        }
    }

    (direct, streamed)
}

fn is_small_file_candidate(
    file: &FileManifest,
    chunk_size: usize,
    small_file_max_bytes: u64,
) -> bool {
    file.total_chunks == 1 && file.size <= small_file_max_bytes && file.size <= chunk_size as u64
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

fn build_init_batches(
    files: Vec<FileManifest>,
    max_stream_payload: usize,
) -> Vec<Vec<FileManifest>> {
    let header_budget = max_stream_payload
        .saturating_sub(512 * 1024)
        .clamp(256 * 1024, 8 * 1024 * 1024);

    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut estimated = 0usize;

    for file in files {
        // Rough estimate for rkyv request metadata size per entry.
        let per_file = 128usize.saturating_add(file.relative_path.len());
        let would_overflow = !current.is_empty()
            && (current.len() >= INIT_BATCH_MAX_FILES
                || estimated.saturating_add(per_file) > header_budget);
        if would_overflow {
            batches.push(current);
            current = Vec::new();
            estimated = 0;
        }
        estimated = estimated.saturating_add(per_file);
        current.push(file);
    }

    if !current.is_empty() {
        batches.push(current);
    }

    batches
}

fn spawn_upload_job(
    handle: RuntimeHandle,
    connection: QuicConnection,
    options: FileTransferOptions,
    file: FileManifest,
    init: InitFileResponse,
) -> Result<spargio::JoinHandle<Result<FileResult>>> {
    let task_handle = handle.clone();
    handle
        .spawn_stealable(async move {
            upload_file_batches(&task_handle, &connection, &options, &file, &init)
                .await
                .with_context(|| "transfer one file")
        })
        .map_err(|err| runtime_error("spawn transfer task", err))
}

async fn initialize_large_files(
    connection: &QuicConnection,
    options: &FileTransferOptions,
    files: Vec<FileManifest>,
) -> Result<(BatchResult, Vec<(FileManifest, InitFileResponse)>)> {
    let init_batches = build_init_batches(files, options.max_stream_payload);
    let mut totals = BatchResult::default();
    let mut uploads = Vec::new();
    if init_batches.is_empty() {
        return Ok((totals, uploads));
    }
    let mut session = FrameSession::open(connection, options.max_stream_payload, &options.stats)
        .await
        .context("open large-file init stream")?;

    for batch in init_batches {
        let init_request = Frame::InitBatchRequest(InitBatchRequest {
            files: batch
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

        let init_response = session
            .roundtrip(init_request, None)
            .await
            .context("roundtrip large-file init batch")?;
        let init = match init_response {
            Frame::InitBatchResponse(resp) => resp,
            Frame::Error(err) => bail!("large batch init rejected: {}", err.message),
            other => bail!("unexpected large batch init response: {other:?}"),
        };

        if init.results.len() != batch.len() {
            bail!(
                "large batch init response size mismatch: got {} expected {}",
                init.results.len(),
                batch.len()
            );
        }

        for (file, result) in batch.into_iter().zip(init.results.into_iter()) {
            if matches!(result.action, InitAction::Skip) {
                totals.files_skipped = totals.files_skipped.saturating_add(1);
                continue;
            }
            uploads.push((
                file,
                InitFileResponse {
                    action: InitAction::Upload,
                    next_chunk: result.next_chunk,
                    message: result.message,
                },
            ));
        }
    }

    session.finish().context("finish large-file init stream")?;

    Ok((totals, uploads))
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

    let mut session = FrameSession::open(connection, options.max_stream_payload, &options.stats)
        .await
        .context("open small-batch stream")?;

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

    let init_response = session
        .roundtrip(init_request, None)
        .await
        .context("roundtrip small-batch init")?;
    let init = match init_response {
        Frame::InitBatchResponse(resp) => resp,
        Frame::Error(err) => bail!("small batch init rejected: {}", err.message),
        other => bail!("unexpected small batch init response: {other:?}"),
    };

    if init.results.len() != files.len() {
        bail!(
            "small batch init response size mismatch: got {} expected {}",
            init.results.len(),
            files.len()
        );
    }

    let mut totals = BatchResult::default();
    let mut upload_metas = Vec::new();
    let mut upload_payload = Vec::new();
    let mut upload_paths = Vec::new();
    let mut fallback = Vec::new();

    for (file, result) in files.iter().zip(init.results.into_iter()) {
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
        let read_started = Instant::now();
        let raw = fs::read(handle, &source_path)
            .await
            .with_context(|| format!("read source {}", source_path.display()))?;
        options
            .stats
            .add_disk_read(read_started.elapsed(), raw.len() as u64);
        if raw.len() as u64 != file.size {
            bail!(
                "small file size changed while reading {}: expected {} got {}",
                file.relative_path,
                file.size,
                raw.len()
            );
        }

        let raw_len = raw.len();
        let encode_started = Instant::now();
        let (encoded, compressed) = compression::maybe_compress_vec(raw, options.compression_level)
            .with_context(|| format!("compress {}", file.relative_path))?;
        options
            .stats
            .add_encode(encode_started.elapsed(), encoded.len());

        totals.bytes_raw = totals.bytes_raw.saturating_add(raw_len as u64);
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
            raw_len,
            data_len: encoded.len(),
        });
    }

    if !upload_metas.is_empty() {
        let upload_request = Frame::UploadSmallBatchRequest(UploadSmallBatchRequest {
            files: upload_metas,
        });

        let upload_response = session
            .roundtrip(upload_request, Some(&upload_payload))
            .await
            .context("roundtrip small-batch upload")?;

        let upload = match upload_response {
            Frame::UploadSmallBatchResponse(resp) => resp,
            Frame::Error(err) => bail!("small batch upload rejected: {}", err.message),
            other => bail!("unexpected small batch upload response: {other:?}"),
        };

        if upload.results.len() != upload_paths.len() {
            bail!(
                "small batch upload response size mismatch: got {} expected {}",
                upload.results.len(),
                upload_paths.len()
            );
        }

        for (path, result) in upload_paths.iter().zip(upload.results.into_iter()) {
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

    session.finish().context("finish small-batch stream")?;

    for (file, init) in fallback {
        let result = upload_file_batches(handle, connection, options, &file, &init).await?;
        totals.add_file_result(result);
    }

    Ok(totals)
}

#[derive(Debug)]
struct PreparedUploadBatch {
    frame: Frame,
    start_chunk: usize,
    end_chunk: usize,
    sent_chunks: usize,
    finalize: bool,
    batch_sent: u64,
    batch_raw: u64,
}

#[derive(Debug)]
struct InflightUploadBatch {
    start_chunk: usize,
    end_chunk: usize,
    sent_chunks: usize,
    finalize: bool,
    batch_sent: u64,
    batch_raw: u64,
    sent_at: Instant,
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
    let upload_window = upload_window_from_env().max(1);
    let payload_capacity = payload_budget.min(BATCH_TARGET_BYTES);

    let mut next_chunk_to_send = init.next_chunk.min(file.total_chunks);
    let mut finalize_sent = false;
    let mut sent_bytes = 0u64;
    let mut raw_bytes = 0u64;
    let mut inflight = VecDeque::with_capacity(upload_window);
    let mut payload_pool: Vec<Vec<u8>> = Vec::with_capacity(upload_window);
    let mut session = FrameSession::open(connection, options.max_stream_payload, &options.stats)
        .await
        .with_context(|| format!("open upload stream for {}", file.relative_path))?;

    loop {
        while inflight.len() < upload_window && !finalize_sent {
            let mut batch_payload = payload_pool
                .pop()
                .unwrap_or_else(|| Vec::with_capacity(payload_capacity.min(256 * 1024)));
            let prepared = prepare_upload_batch(
                &source_file,
                &source_path,
                options,
                file,
                next_chunk_to_send,
                max_chunks_per_batch,
                &mut batch_payload,
            )
            .await?;

            let sent_at = session
                .send_request(prepared.frame, Some(&batch_payload))
                .await
                .with_context(|| format!("send upload batch for {}", file.relative_path))?;
            batch_payload.clear();
            payload_pool.push(batch_payload);

            next_chunk_to_send = prepared.end_chunk;
            finalize_sent |= prepared.finalize;
            inflight.push_back(InflightUploadBatch {
                start_chunk: prepared.start_chunk,
                end_chunk: prepared.end_chunk,
                sent_chunks: prepared.sent_chunks,
                finalize: prepared.finalize,
                batch_sent: prepared.batch_sent,
                batch_raw: prepared.batch_raw,
                sent_at,
            });
        }

        let inflight_batch = inflight.pop_front().ok_or_else(|| {
            anyhow::anyhow!(
                "upload pipeline stalled without inflight batch for {}",
                file.relative_path
            )
        })?;

        let response = session
            .read_response(inflight_batch.sent_at)
            .await
            .with_context(|| format!("read upload batch response for {}", file.relative_path))?;

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
                inflight_batch.start_chunk,
                response.message
            );
        }

        sent_bytes = sent_bytes.saturating_add(inflight_batch.batch_sent);
        raw_bytes = raw_bytes.saturating_add(inflight_batch.batch_raw);

        if response.completed {
            if !inflight.is_empty() {
                bail!(
                    "upload batch completed for {} but {} pipeline requests are still in-flight",
                    file.relative_path,
                    inflight.len()
                );
            }
            if !inflight_batch.finalize {
                bail!(
                    "upload batch completed for {} before finalize batch was sent",
                    file.relative_path
                );
            }
            session
                .finish()
                .with_context(|| format!("finish upload stream for {}", file.relative_path))?;
            return Ok(FileResult {
                transferred: true,
                bytes_sent: sent_bytes,
                bytes_raw: raw_bytes,
            });
        }

        if response.next_chunk <= inflight_batch.start_chunk {
            bail!(
                "non-progress batch ack for {}: start={} next={} message={}",
                file.relative_path,
                inflight_batch.start_chunk,
                response.next_chunk,
                response.message
            );
        }

        if response.next_chunk != inflight_batch.end_chunk {
            bail!(
                "unexpected batch ack progression for {}: start={} sent_chunks={} expected_next={} got_next={}",
                file.relative_path,
                inflight_batch.start_chunk,
                inflight_batch.sent_chunks,
                inflight_batch.end_chunk,
                response.next_chunk
            );
        }
    }
}

async fn prepare_upload_batch(
    source_file: &fs::File,
    source_path: &Path,
    options: &FileTransferOptions,
    file: &FileManifest,
    start_chunk: usize,
    max_chunks_per_batch: usize,
    batch_payload: &mut Vec<u8>,
) -> Result<PreparedUploadBatch> {
    let mut next_chunk = start_chunk;
    let mut sent_chunks = 0usize;
    batch_payload.clear();
    let mut batch_estimate = 0usize;
    let mut batch_sent = 0u64;
    let mut batch_raw = 0u64;
    for _ in 0..max_chunks_per_batch {
        if next_chunk >= file.total_chunks {
            break;
        }
        let offset = (next_chunk as u64).saturating_mul(options.chunk_size as u64);
        let read_started = Instant::now();
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
        options
            .stats
            .add_disk_read(read_started.elapsed(), chunk.len() as u64);

        if chunk.is_empty() {
            break;
        }

        let raw_len = chunk.len();
        let encode_started = Instant::now();
        let (encoded_payload, compressed) =
            compression::maybe_compress_vec(chunk, options.compression_level)?;
        options
            .stats
            .add_encode(encode_started.elapsed(), encoded_payload.len());
        let encoded_len = encoded_payload.len();
        if raw_len > u32::MAX as usize {
            bail!("chunk raw length too large for framing: {}", raw_len);
        }
        if encoded_len > u32::MAX as usize {
            bail!(
                "chunk encoded length too large for framing: {}",
                encoded_len
            );
        }
        batch_payload.extend_from_slice(&(raw_len as u32).to_be_bytes());
        batch_payload.extend_from_slice(&(encoded_len as u32).to_be_bytes());
        batch_payload.push(if compressed { 1 } else { 0 });
        batch_payload.extend_from_slice(&encoded_payload);
        batch_sent = batch_sent.saturating_add(encoded_len as u64);
        batch_raw = batch_raw.saturating_add(raw_len as u64);
        sent_chunks = sent_chunks.saturating_add(1);
        batch_estimate = batch_estimate.saturating_add(9).saturating_add(encoded_len);
        next_chunk = next_chunk.saturating_add(1);

        if batch_estimate >= BATCH_TARGET_BYTES {
            break;
        }
    }

    let finalize = next_chunk >= file.total_chunks;
    if sent_chunks == 0 && !finalize {
        bail!(
            "batch assembly produced no packets before finalize for {}",
            file.relative_path
        );
    }

    let frame = Frame::UploadBatchRequest(UploadBatchRequest {
        relative_path: file.relative_path.clone(),
        size: file.size,
        mode: file.mode,
        mtime_sec: file.mtime_sec,
        file_hash: file.file_hash.clone(),
        total_chunks: file.total_chunks,
        start_chunk,
        chunk_size: options.chunk_size,
        sent_chunks,
        finalize,
    });

    Ok(PreparedUploadBatch {
        frame,
        start_chunk,
        end_chunk: next_chunk,
        sent_chunks,
        finalize,
        batch_sent,
        batch_raw,
    })
}

async fn send_frame_roundtrip(
    connection: &QuicConnection,
    request: Frame,
    payload: Option<&[u8]>,
    max_stream_payload: usize,
    stats: &TransferStats,
) -> Result<Frame> {
    let mut session = FrameSession::open(connection, max_stream_payload, stats).await?;
    let frame = session.roundtrip(request, payload).await?;
    session.finish().context("finish request stream")?;
    Ok(frame)
}

struct FrameSession<'a> {
    send: QuicSendStream,
    recv: QuicRecvStream,
    max_stream_payload: usize,
    stats: &'a TransferStats,
    response_buffer: Vec<u8>,
    response_frame_len: Option<usize>,
}

impl<'a> FrameSession<'a> {
    async fn open(
        connection: &QuicConnection,
        max_stream_payload: usize,
        stats: &'a TransferStats,
    ) -> Result<Self> {
        let (send, recv) = connection
            .open_bi()
            .await
            .context("open bidirectional stream")?;
        stats.add_stream_opened();
        Ok(Self {
            send,
            recv,
            max_stream_payload,
            stats,
            response_buffer: Vec::with_capacity(64 * 1024),
            response_frame_len: None,
        })
    }

    async fn roundtrip(&mut self, request: Frame, payload: Option<&[u8]>) -> Result<Frame> {
        let sent_at = self.send_request(request, payload).await?;
        self.read_response(sent_at).await
    }

    async fn send_request(&mut self, request: Frame, payload: Option<&[u8]>) -> Result<Instant> {
        let encode_started = Instant::now();
        let payload_len = payload.map_or(0usize, |p| p.len());
        let frame_header = crate::protocol::encode_header(&request, payload_len)
            .context("encode request frame")?;
        let encoded_len = frame_header.len().saturating_add(payload_len);
        self.stats
            .add_encode(encode_started.elapsed(), frame_header.len());
        if encoded_len > self.max_stream_payload {
            bail!(
                "encoded frame too large: {} > max_stream_payload {}",
                encoded_len,
                self.max_stream_payload
            );
        }
        self.stats.add_control_frame();
        self.stats.add_request_bytes(encoded_len);

        let sent_at = Instant::now();
        self.send
            .write_all(&frame_header)
            .await
            .context("write frame request")?;
        if let Some(payload) = payload {
            self.send
                .write_all(payload)
                .await
                .context("write frame request payload")?;
        }
        Ok(sent_at)
    }

    async fn read_response(&mut self, request_started: Instant) -> Result<Frame> {
        let response = self.read_response_frame().await?;
        self.stats.add_response_bytes(response.len());
        let (frame, payload) =
            crate::protocol::decode(&response).context("decode frame response")?;
        if !payload.is_empty() {
            bail!(
                "unexpected payload in response frame ({} bytes)",
                payload.len()
            );
        }
        self.stats.add_roundtrip_time(request_started.elapsed());
        Ok(frame)
    }

    fn finish(&mut self) -> Result<()> {
        self.send.finish().context("finish request stream")
    }

    async fn read_response_frame(&mut self) -> Result<Vec<u8>> {
        loop {
            if let Some(frame) = try_extract_frame(
                &mut self.response_buffer,
                &mut self.response_frame_len,
                self.max_stream_payload,
            )? {
                return Ok(frame);
            }

            let remaining = self
                .max_stream_payload
                .saturating_sub(self.response_buffer.len());
            if remaining == 0 {
                bail!(
                    "response frame exceeded max_stream_payload {}",
                    self.max_stream_payload
                );
            }

            let chunk = self
                .recv
                .read_chunk(remaining)
                .await
                .context("read frame response chunk")?;
            let Some(chunk) = chunk else {
                if let Some(expected) = self.response_frame_len {
                    bail!(
                        "response stream closed with partial frame: have {} expected {}",
                        self.response_buffer.len(),
                        expected
                    );
                }
                bail!("response stream closed before response frame");
            };
            if !chunk.is_empty() {
                self.response_buffer.extend_from_slice(chunk.as_ref());
            }
        }
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
