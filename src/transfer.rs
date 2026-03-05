use crate::certs;
use crate::compression;
use crate::endpoint::RemoteEndpoint;
use crate::filter::PathFilter;
use crate::model::FileManifest;
use crate::protocol::{
    Frame, HelloRequest, HelloResponse, InitAction, InitBatchRequest, InitFileRequest,
    InitFileResponse, PROTOCOL_VERSION, UploadBatchRequest, UploadColdBatchRequest,
    UploadColdFileMeta, UploadSmallBatchRequest, UploadSmallFileMeta,
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
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
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
const STDIO_PIPE_TARGET_BYTES: usize = 1 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct PushOptions {
    pub source: PathBuf,
    pub server: SocketAddr,
    pub server_name: String,
    pub ca: PathBuf,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    pub scan: ScanOptions,
    pub parallel_files: usize,
    pub connections: usize,
    pub compression_level: i32,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub max_stream_payload: usize,
    pub resume: bool,
    pub update_only: bool,
    pub cold_start: bool,
    pub manifest_out: Option<PathBuf>,
    pub path_filter: Option<PathFilter>,
    pub bwlimit_kbps: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PushOverSshOptions {
    pub source: PathBuf,
    pub remote: RemoteEndpoint,
    pub destination: String,
    pub remote_shell_prefix: Option<String>,
    pub scan: ScanOptions,
    pub compression_level: i32,
    pub max_stream_payload: usize,
    pub resume: bool,
    pub update_only: bool,
    pub cold_start: bool,
    pub manifest_out: Option<PathBuf>,
    pub preserve_metadata: bool,
    pub path_filter: Option<PathFilter>,
    pub bwlimit_kbps: Option<u64>,
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
    update_only: bool,
    stats: TransferStats,
    small_file_max_bytes: u64,
    direct_file_max_bytes: u64,
    bw_limiter: Option<BwLimiter>,
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

#[derive(Default)]
struct SshProfileInner {
    frames_sent: AtomicU64,
    frames_received: AtomicU64,
    write_calls: AtomicU64,
    read_calls: AtomicU64,
    flush_calls: AtomicU64,
    scan_ns: AtomicU64,
    init_ns: AtomicU64,
    upload_ns: AtomicU64,
}

#[derive(Clone, Default)]
struct SshProfile {
    enabled: bool,
    inner: Arc<SshProfileInner>,
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

impl SshProfile {
    fn from_env() -> Self {
        let enabled = std::env::var("SPARSYNC_PROFILE")
            .map(|value| value != "0")
            .unwrap_or(false);
        Self {
            enabled,
            inner: Arc::new(SshProfileInner::default()),
        }
    }

    fn add_frame_sent(&self) {
        self.inner.frames_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn add_frame_received(&self) {
        self.inner.frames_received.fetch_add(1, Ordering::Relaxed);
    }

    fn add_write_call(&self) {
        self.inner.write_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn add_read_call(&self) {
        self.inner.read_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn add_flush_call(&self) {
        self.inner.flush_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn add_init_time(&self, elapsed: Duration) {
        self.inner
            .init_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_scan_time(&self, elapsed: Duration) {
        self.inner
            .scan_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_upload_time(&self, elapsed: Duration) {
        self.inner
            .upload_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn print_if_enabled(&self) {
        if !self.enabled {
            return;
        }
        println!(
            "profile transport=ssh frames_sent={} frames_received={} write_calls={} read_calls={} flush_calls={} scan_ms={} init_ms={} upload_ms={}",
            self.inner.frames_sent.load(Ordering::Relaxed),
            self.inner.frames_received.load(Ordering::Relaxed),
            self.inner.write_calls.load(Ordering::Relaxed),
            self.inner.read_calls.load(Ordering::Relaxed),
            self.inner.flush_calls.load(Ordering::Relaxed),
            Duration::from_nanos(self.inner.scan_ns.load(Ordering::Relaxed)).as_millis(),
            Duration::from_nanos(self.inner.init_ns.load(Ordering::Relaxed)).as_millis(),
            Duration::from_nanos(self.inner.upload_ns.load(Ordering::Relaxed)).as_millis(),
        );
    }
}

#[derive(Clone)]
struct BwLimiter {
    bytes_per_sec: u64,
    state: Arc<Mutex<BwLimiterState>>,
}

#[derive(Debug)]
struct BwLimiterState {
    started: Option<Instant>,
    sent_bytes: u64,
}

impl BwLimiter {
    fn from_kbps(kbps: Option<u64>) -> Option<Self> {
        let kbps = kbps.filter(|value| *value > 0)?;
        Some(Self {
            bytes_per_sec: kbps.saturating_mul(1024),
            state: Arc::new(Mutex::new(BwLimiterState {
                started: None,
                sent_bytes: 0,
            })),
        })
    }

    async fn throttle(&self, bytes: usize) {
        if bytes == 0 || self.bytes_per_sec == 0 {
            return;
        }
        let sleep_for = {
            let mut guard = match self.state.lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            let now = Instant::now();
            let started = *guard.started.get_or_insert(now);
            guard.sent_bytes = guard.sent_bytes.saturating_add(bytes as u64);
            let expected =
                Duration::from_secs_f64(guard.sent_bytes as f64 / self.bytes_per_sec as f64);
            let elapsed = started.elapsed();
            expected.saturating_sub(elapsed)
        };
        if !sleep_for.is_zero() {
            spargio::sleep(sleep_for).await;
        }
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

fn stdio_nonblocking_enabled() -> bool {
    std::env::var("SPARSYNC_STDIO_NONBLOCK")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn stdio_pipe_size_bytes_from_env() -> usize {
    std::env::var("SPARSYNC_STDIO_PIPE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value >= 64 * 1024)
        .unwrap_or(STDIO_PIPE_TARGET_BYTES)
}

pub async fn push_directory(handle: RuntimeHandle, options: PushOptions) -> Result<PushSummary> {
    let started = Instant::now();
    let stats = TransferStats::from_env();
    let client_config = certs::load_client_config(
        &options.ca,
        options.client_cert.as_deref(),
        options.client_key.as_deref(),
    )
    .with_context(|| {
        format!(
            "load client TLS config ca={} client_cert={} client_key={}",
            options.ca.display(),
            options
                .client_cert
                .as_ref()
                .map(|v| v.display().to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            options
                .client_key
                .as_ref()
                .map(|v| v.display().to_string())
                .unwrap_or_else(|| "<none>".to_string())
        )
    })?;

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
        verify_protocol(&connection, options.max_stream_payload, &stats)
            .await
            .context("validate protocol compatibility")?;
        connections.push(connection);
    }
    stats.add_connect_time(connect_started.elapsed());
    stats.add_connections(connections.len());

    if options.cold_start {
        let summary =
            push_directory_cold(handle.clone(), &options, &connections, started, stats).await?;
        return Ok(summary);
    }

    let (mut manifest, scan_stats) =
        scan::build_manifest(handle.clone(), &options.source, options.scan)
            .await
            .with_context(|| format!("build source manifest {}", options.source.display()))?;
    if let Some(path_filter) = options.path_filter.as_ref() {
        let before = manifest.files.len();
        manifest
            .files
            .retain(|file| path_filter.allows(&file.relative_path));
        manifest.total_bytes = manifest.files.iter().map(|item| item.size).sum();
        if manifest.files.len() != before {
            println!(
                "filter applied kept_files={} dropped_files={}",
                manifest.files.len(),
                before.saturating_sub(manifest.files.len())
            );
        }
    }

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
        update_only: options.update_only,
        stats: stats.clone(),
        small_file_max_bytes: small_file_max_bytes_from_env(),
        direct_file_max_bytes: direct_file_max_bytes_from_env(),
        bw_limiter: BwLimiter::from_kbps(options.bwlimit_kbps),
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

pub async fn push_directory_over_ssh_stdio(
    handle: RuntimeHandle,
    options: PushOverSshOptions,
) -> Result<PushSummary> {
    let started = Instant::now();
    let stats = TransferStats::from_env();
    let ssh_profile = SshProfile::from_env();
    let bw_limiter = BwLimiter::from_kbps(options.bwlimit_kbps);

    let connect_started = Instant::now();
    let mut session = SshFrameSession::connect(
        handle.clone(),
        &options.remote,
        &options.destination,
        options.remote_shell_prefix.as_deref(),
        options.max_stream_payload,
        options.preserve_metadata,
        &stats,
        ssh_profile.clone(),
        bw_limiter.clone(),
    )
    .await
    .context("open SSH stdio data session")?;
    stats.add_connect_time(connect_started.elapsed());
    stats.add_connections(1);

    verify_protocol_over_ssh(&mut session)
        .await
        .context("validate protocol compatibility over ssh")?;

    if options.cold_start {
        let summary = push_directory_over_ssh_cold(
            handle.clone(),
            &options,
            &mut session,
            &stats,
            &ssh_profile,
            started,
        )
        .await?;
        session.finish().context("finish SSH stdio session")?;
        return Ok(summary);
    }

    let (mut manifest, scan_stats) =
        scan::build_manifest(handle.clone(), &options.source, options.scan)
            .await
            .with_context(|| format!("build source manifest {}", options.source.display()))?;
    if let Some(path_filter) = options.path_filter.as_ref() {
        let before = manifest.files.len();
        manifest
            .files
            .retain(|file| path_filter.allows(&file.relative_path));
        manifest.total_bytes = manifest.files.iter().map(|item| item.size).sum();
        if manifest.files.len() != before {
            println!(
                "filter applied kept_files={} dropped_files={}",
                manifest.files.len(),
                before.saturating_sub(manifest.files.len())
            );
        }
    }
    ssh_profile.add_scan_time(scan_stats.total_elapsed);

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
        update_only: options.update_only,
        stats: stats.clone(),
        small_file_max_bytes: small_file_max_bytes_from_env(),
        direct_file_max_bytes: direct_file_max_bytes_from_env(),
        bw_limiter: BwLimiter::from_kbps(options.bwlimit_kbps),
    };

    let (small_files, large_files) = partition_small_files(
        manifest.files,
        transfer_options.chunk_size,
        transfer_options.small_file_max_bytes,
    );
    let small_batches = build_small_batches(small_files, transfer_options.max_stream_payload);

    let mut totals = BatchResult::default();
    let small_started = Instant::now();
    for batch in small_batches {
        let result =
            transfer_small_batch_over_ssh(&handle, &mut session, &transfer_options, &batch).await?;
        totals.merge(result);
    }
    ssh_profile.add_init_time(small_started.elapsed());

    let init_started = Instant::now();
    let (init_totals, uploads) =
        initialize_large_files_over_ssh(&mut session, &transfer_options, large_files).await?;
    totals.merge(init_totals);
    ssh_profile.add_init_time(init_started.elapsed());

    let (direct_files, streamed_files) =
        split_direct_uploads(uploads, transfer_options.direct_file_max_bytes);
    let upload_started = Instant::now();
    let direct_totals = transfer_initialized_direct_batches_over_ssh(
        &handle,
        &mut session,
        &transfer_options,
        direct_files,
    )
    .await?;
    totals.merge(direct_totals);

    for (file, init) in streamed_files {
        let result =
            upload_file_batches_over_ssh(&handle, &mut session, &transfer_options, &file, &init)
                .await
                .with_context(|| format!("upload file {}", file.relative_path))?;
        totals.add_file_result(result);
    }
    ssh_profile.add_upload_time(upload_started.elapsed());

    session.finish().context("finish SSH stdio session")?;

    let elapsed = started.elapsed();
    stats.print_if_enabled(elapsed, false);
    ssh_profile.print_if_enabled();

    Ok(PushSummary {
        files_transferred: totals.files_transferred,
        files_skipped: totals.files_skipped,
        bytes_sent: totals.bytes_sent,
        bytes_raw: totals.bytes_raw,
        elapsed,
    })
}

async fn push_directory_over_ssh_cold(
    handle: RuntimeHandle,
    options: &PushOverSshOptions,
    session: &mut SshFrameSession<'_>,
    stats: &TransferStats,
    ssh_profile: &SshProfile,
    started: Instant,
) -> Result<PushSummary> {
    let chunk_size = options.scan.chunk_size.max(1);
    let (source_root, mut files, enumeration_elapsed, metadata_elapsed) = scan::build_file_list(
        handle.clone(),
        &options.source,
        options.scan.scan_workers.max(1),
        options.scan.hash_workers.max(1),
    )
    .await
    .with_context(|| format!("build cold file list {}", options.source.display()))?;
    if let Some(path_filter) = options.path_filter.as_ref() {
        let before = files.len();
        files.retain(|file| path_filter.allows(&file.relative_path));
        if files.len() != before {
            println!(
                "filter applied kept_files={} dropped_files={}",
                files.len(),
                before.saturating_sub(files.len())
            );
        }
    }
    ssh_profile.add_scan_time(enumeration_elapsed.saturating_add(metadata_elapsed));

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

    let batch_limit = options
        .max_stream_payload
        .saturating_sub(512 * 1024)
        .clamp(512 * 1024, COLD_BATCH_TARGET_BYTES);
    let mut totals = BatchResult::default();
    let mut payload = Vec::new();
    let mut metas = Vec::new();
    let mut pending = Vec::new();

    let upload_started = Instant::now();
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
            let batch = upload_cold_batch_over_ssh(
                session,
                std::mem::take(&mut metas),
                std::mem::take(&mut payload),
                std::mem::take(&mut pending),
            )
            .await?;
            totals.merge(batch);
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
        let batch = upload_cold_batch_over_ssh(
            session,
            std::mem::take(&mut metas),
            std::mem::take(&mut payload),
            std::mem::take(&mut pending),
        )
        .await?;
        totals.merge(batch);
    }
    ssh_profile.add_upload_time(upload_started.elapsed());

    let elapsed = started.elapsed();
    stats.print_if_enabled(elapsed, true);
    ssh_profile.print_if_enabled();

    Ok(PushSummary {
        files_transferred: totals.files_transferred,
        files_skipped: totals.files_skipped,
        bytes_sent: totals.bytes_sent,
        bytes_raw: totals.bytes_raw,
        elapsed,
    })
}

async fn initialize_large_files_over_ssh(
    session: &mut SshFrameSession<'_>,
    options: &FileTransferOptions,
    files: Vec<FileManifest>,
) -> Result<(BatchResult, Vec<(FileManifest, InitFileResponse)>)> {
    let init_batches = build_init_batches(files, options.max_stream_payload);
    let mut totals = BatchResult::default();
    let mut uploads = Vec::new();
    if init_batches.is_empty() {
        return Ok((totals, uploads));
    }

    for batch in init_batches {
        let init_request = Frame::InitBatchRequest(InitBatchRequest {
            files: batch
                .iter()
                .map(|file| InitFileRequest {
                    relative_path: file.relative_path.clone(),
                    size: file.size,
                    mode: file.mode,
                    mtime_sec: file.mtime_sec,
                    update_only: options.update_only,
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
            .context("roundtrip large-file init batch over ssh")?;
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

    Ok((totals, uploads))
}

async fn transfer_small_batch_over_ssh(
    handle: &RuntimeHandle,
    session: &mut SshFrameSession<'_>,
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
                update_only: options.update_only,
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
        .context("roundtrip small-batch init over ssh")?;
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
            .context("roundtrip small-batch upload over ssh")?;

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

    for (file, init) in fallback {
        let result = upload_file_batches_over_ssh(handle, session, options, &file, &init).await?;
        totals.add_file_result(result);
    }

    Ok(totals)
}

async fn transfer_initialized_direct_batches_over_ssh(
    handle: &RuntimeHandle,
    session: &mut SshFrameSession<'_>,
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
    let split_target = total_bytes as usize;
    if split_target > 4 * 1024 * 1024 {
        batch_limit = batch_limit.min(split_target);
    }

    let mut totals = BatchResult::default();
    let mut payload = Vec::new();
    let mut metas = Vec::new();
    let mut pending = Vec::new();

    for file in files {
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
            let batch = upload_cold_batch_over_ssh(
                session,
                std::mem::take(&mut metas),
                std::mem::take(&mut payload),
                std::mem::take(&mut pending),
            )
            .await?;
            totals.merge(batch);
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
        let batch = upload_cold_batch_over_ssh(
            session,
            std::mem::take(&mut metas),
            std::mem::take(&mut payload),
            std::mem::take(&mut pending),
        )
        .await?;
        totals.merge(batch);
    }

    Ok(totals)
}

async fn upload_cold_batch_over_ssh(
    session: &mut SshFrameSession<'_>,
    metas: Vec<UploadColdFileMeta>,
    payload: Vec<u8>,
    pending: Vec<(String, u64, u64)>,
) -> Result<BatchResult> {
    if metas.is_empty() {
        return Ok(BatchResult::default());
    }

    let response = session
        .roundtrip(
            Frame::UploadColdBatchRequest(UploadColdBatchRequest {
                allow_skip: false,
                files: metas,
            }),
            Some(&payload),
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

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn ssh_base_command(remote: &RemoteEndpoint) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.arg("-o").arg("BatchMode=yes");
    cmd.arg("-o").arg("StrictHostKeyChecking=accept-new");
    if let Ok(identity) = std::env::var("SPARSYNC_SSH_IDENTITY") {
        if !identity.trim().is_empty() {
            cmd.arg("-i").arg(identity);
        }
    }
    if let Some(port) = remote.port {
        cmd.arg("-p").arg(port.to_string());
    }
    cmd.arg(remote.ssh_target());
    cmd
}

const STREAM_FD_OFFSET: u64 = u64::MAX;

struct SshFrameSession<'a> {
    child: Child,
    stdin_fd: Option<OwnedFd>,
    stdout_fd: OwnedFd,
    native: spargio::UringNativeAny,
    max_stream_payload: usize,
    stats: &'a TransferStats,
    profile: SshProfile,
    response_buffer: Vec<u8>,
    response_frame_len: Option<usize>,
    finished: bool,
    bw_limiter: Option<BwLimiter>,
}

impl<'a> SshFrameSession<'a> {
    async fn connect(
        handle: RuntimeHandle,
        remote: &RemoteEndpoint,
        destination: &str,
        remote_shell_prefix: Option<&str>,
        max_stream_payload: usize,
        preserve_metadata: bool,
        stats: &'a TransferStats,
        profile: SshProfile,
        bw_limiter: Option<BwLimiter>,
    ) -> Result<Self> {
        let remote_shell_prefix =
            remote_shell_prefix.unwrap_or("PATH=\"$HOME/.local/bin:$PATH\" sparsync");
        let mut remote_cmd = format!(
            "{} serve-stdio --destination {} --max-stream-payload {}",
            remote_shell_prefix,
            sh_quote(destination),
            max_stream_payload
        );
        if preserve_metadata {
            remote_cmd.push_str(" --preserve-metadata");
        }

        let mut child = ssh_base_command(remote)
            .arg(remote_cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn SSH session to {}", remote.ssh_target()))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing SSH stdin pipe"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing SSH stdout pipe"))?;

        let stdin_fd = unsafe { OwnedFd::from_raw_fd(stdin.into_raw_fd()) };
        let stdout_fd = unsafe { OwnedFd::from_raw_fd(stdout.into_raw_fd()) };
        if stdio_nonblocking_enabled() {
            set_nonblocking(stdin_fd.as_raw_fd(), true).context("set SSH stdin nonblocking")?;
            set_nonblocking(stdout_fd.as_raw_fd(), true).context("set SSH stdout nonblocking")?;
        }
        let pipe_target = stdio_pipe_size_bytes_from_env();
        configure_pipe_size(stdin_fd.as_raw_fd(), pipe_target);
        configure_pipe_size(stdout_fd.as_raw_fd(), pipe_target);

        let native = handle
            .uring_native_unbound()
            .map_err(runtime_error_to_io)?
            .clear_preferred_shard();
        stats.add_stream_opened();

        Ok(Self {
            child,
            stdin_fd: Some(stdin_fd),
            stdout_fd,
            native,
            max_stream_payload,
            stats,
            profile,
            response_buffer: Vec::with_capacity(64 * 1024),
            response_frame_len: None,
            finished: false,
            bw_limiter,
        })
    }

    async fn roundtrip(&mut self, request: Frame, payload: Option<&[u8]>) -> Result<Frame> {
        let sent_at = self.send_request(request, payload).await?;
        self.read_response(sent_at).await
    }

    async fn send_request(&mut self, request: Frame, payload: Option<&[u8]>) -> Result<Instant> {
        let encode_started = Instant::now();
        let payload_len = payload.map_or(0usize, |bytes| bytes.len());
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
        self.profile.add_frame_sent();
        if let Some(limiter) = &self.bw_limiter {
            limiter.throttle(encoded_len).await;
        }
        let sent_at = Instant::now();

        self.write_all(frame_header.as_ref())
            .await
            .context("write request frame to SSH stdin")?;
        if let Some(payload) = payload {
            self.write_all(payload)
                .await
                .context("write request payload to SSH stdin")?;
        }
        self.profile.add_flush_call();

        Ok(sent_at)
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        let fd = self
            .stdin_fd
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("SSH session stdin already closed"))?
            .as_raw_fd();

        let payload: Arc<[u8]> = Arc::from(bytes);
        let mut offset = 0usize;
        while offset < payload.len() {
            match write_arc_to_fd_once(&self.native, fd, payload.clone(), offset).await {
                Ok(0) => {
                    return Err(anyhow::anyhow!("write returned zero bytes to SSH stdin"));
                }
                Ok(wrote) => {
                    self.profile.add_write_call();
                    let remain = payload.len().saturating_sub(offset);
                    let wrote = wrote.min(remain);
                    offset = offset.saturating_add(wrote);
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    wait_fd_ready(&self.native, fd, libc::POLLOUT as u32)
                        .await
                        .context("wait for SSH stdin writable")?;
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {}
                Err(err) => return Err(err).context("write request bytes to SSH stdin"),
            }
        }
        Ok(())
    }

    async fn read_response(&mut self, request_started: Instant) -> Result<Frame> {
        let response = self.read_response_frame().await?;
        self.stats.add_response_bytes(response.len());
        self.profile.add_frame_received();
        self.stats.add_roundtrip_time(request_started.elapsed());
        let (frame, payload) =
            crate::protocol::decode(&response).context("decode response frame")?;
        if !payload.is_empty() {
            bail!(
                "unexpected payload in response frame ({} bytes)",
                payload.len()
            );
        }
        Ok(frame)
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
                .read_chunk(remaining)
                .await
                .context("read SSH response frame chunk")?;
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
            self.response_buffer.extend_from_slice(chunk.as_ref());
        }
    }

    async fn read_chunk(&mut self, max_len: usize) -> Result<Option<Vec<u8>>> {
        let read_len = max_len.min(256 * 1024).max(1);
        let fd = self.stdout_fd.as_raw_fd();
        loop {
            match self.native.read_at(fd, STREAM_FD_OFFSET, read_len).await {
                Ok(bytes) => {
                    self.profile.add_read_call();
                    if bytes.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(bytes));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    wait_fd_ready(&self.native, fd, libc::POLLIN as u32)
                        .await
                        .context("wait for SSH stdout readable")?;
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {}
                Err(err) => return Err(err).context("read response bytes from SSH stdout"),
            }
        }
    }

    fn finish(mut self) -> Result<()> {
        self.stdin_fd.take();
        let status = self
            .child
            .wait()
            .context("wait for SSH stdio server process")?;
        self.finished = true;
        if !status.success() {
            bail!("SSH stdio server exited with status {}", status);
        }
        Ok(())
    }
}

impl Drop for SshFrameSession<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.stdin_fd.take();
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            _ => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
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

fn configure_pipe_size(fd: RawFd, target_bytes: usize) {
    let Ok(target) = i32::try_from(target_bytes) else {
        return;
    };
    unsafe {
        let _ = libc::fcntl(fd, libc::F_SETPIPE_SZ, target);
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

async fn write_arc_to_fd_once(
    native: &spargio::UringNativeAny,
    fd: RawFd,
    payload: Arc<[u8]>,
    offset: usize,
) -> io::Result<usize> {
    if offset >= payload.len() {
        return Ok(0);
    }
    unsafe {
        native
            .submit_unsafe(
                (fd, payload, offset),
                |state| {
                    let (fd, payload, offset) = state;
                    let remain = payload.len().saturating_sub(*offset);
                    let len = u32::try_from(remain).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "write length exceeds u32::MAX")
                    })?;
                    let ptr = payload.as_ptr().wrapping_add(*offset);
                    Ok(
                        io_uring::opcode::Write::new(io_uring::types::Fd(*fd), ptr, len)
                            .offset(STREAM_FD_OFFSET)
                            .build(),
                    )
                },
                |_, cqe| {
                    if cqe.result < 0 {
                        return Err(io::Error::from_raw_os_error(-cqe.result));
                    }
                    Ok(cqe.result as usize)
                },
            )
            .await
    }
}

async fn verify_protocol_over_ssh(session: &mut SshFrameSession<'_>) -> Result<()> {
    let response = session
        .roundtrip(
            Frame::HelloRequest(HelloRequest {
                protocol_version: PROTOCOL_VERSION,
                codec: crate::protocol::local_wire_codec(),
                endianness: crate::protocol::local_wire_endianness(),
                binary_version: crate::protocol::BINARY_VERSION.to_string(),
            }),
            None,
        )
        .await
        .context("send hello request")?;

    match response {
        Frame::HelloResponse(HelloResponse {
            accepted: true,
            protocol_version,
            codec,
            endianness,
            binary_version,
            ..
        }) if protocol_version == PROTOCOL_VERSION
            && codec == crate::protocol::local_wire_codec()
            && endianness == crate::protocol::local_wire_endianness()
            && binary_version == crate::protocol::BINARY_VERSION =>
        {
            Ok(())
        }
        Frame::HelloResponse(resp) => bail!(
            "server rejected protocol: {} (client={} server={} codec={:?}/{:?} endianness={:?}/{:?} binary={}/{})",
            resp.message,
            PROTOCOL_VERSION,
            resp.protocol_version,
            crate::protocol::local_wire_codec(),
            resp.codec,
            crate::protocol::local_wire_endianness(),
            resp.endianness,
            crate::protocol::BINARY_VERSION,
            resp.binary_version
        ),
        Frame::Error(err) => bail!("server hello failed: {}", err.message),
        other => bail!("unexpected hello response frame: {other:?}"),
    }
}

async fn upload_file_batches_over_ssh(
    handle: &RuntimeHandle,
    session: &mut SshFrameSession<'_>,
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
    let mut batch_payload = Vec::with_capacity(payload_capacity.min(256 * 1024));

    loop {
        while inflight.len() < upload_window && !finalize_sent {
            batch_payload.clear();
            if let Some(mut reused) = payload_pool.pop() {
                reused.clear();
                batch_payload = reused;
            }

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
            payload_pool.push(std::mem::take(&mut batch_payload));
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
            Frame::Error(err) => bail!(
                "upload batch failed for {}: {}",
                file.relative_path,
                err.message
            ),
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

async fn push_directory_cold(
    handle: RuntimeHandle,
    options: &PushOptions,
    connections: &[QuicConnection],
    started: Instant,
    stats: TransferStats,
) -> Result<PushSummary> {
    let chunk_size = options.scan.chunk_size.max(1);
    let (source_root, mut files, enumeration_elapsed, metadata_elapsed) = scan::build_file_list(
        handle.clone(),
        &options.source,
        options.scan.scan_workers.max(1),
        options.scan.hash_workers.max(1),
    )
    .await
    .with_context(|| format!("build cold file list {}", options.source.display()))?;
    if let Some(path_filter) = options.path_filter.as_ref() {
        let before = files.len();
        files.retain(|file| path_filter.allows(&file.relative_path));
        if files.len() != before {
            println!(
                "filter applied kept_files={} dropped_files={}",
                files.len(),
                before.saturating_sub(files.len())
            );
        }
    }

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
    let mut session = FrameSession::open(
        connection,
        options.max_stream_payload,
        &options.stats,
        options.bw_limiter.clone(),
    )
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
                    update_only: options.update_only,
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

    let mut session = FrameSession::open(
        connection,
        options.max_stream_payload,
        &options.stats,
        options.bw_limiter.clone(),
    )
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
                update_only: options.update_only,
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
    let mut session = FrameSession::open(
        connection,
        options.max_stream_payload,
        &options.stats,
        options.bw_limiter.clone(),
    )
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
    let mut session = FrameSession::open(connection, max_stream_payload, stats, None).await?;
    let frame = session.roundtrip(request, payload).await?;
    session.finish().context("finish request stream")?;
    Ok(frame)
}

async fn verify_protocol(
    connection: &QuicConnection,
    max_stream_payload: usize,
    stats: &TransferStats,
) -> Result<()> {
    let response = send_frame_roundtrip(
        connection,
        Frame::HelloRequest(HelloRequest {
            protocol_version: PROTOCOL_VERSION,
            codec: crate::protocol::local_wire_codec(),
            endianness: crate::protocol::local_wire_endianness(),
            binary_version: crate::protocol::BINARY_VERSION.to_string(),
        }),
        None,
        max_stream_payload,
        stats,
    )
    .await
    .context("send hello request")?;
    match response {
        Frame::HelloResponse(HelloResponse {
            accepted: true,
            protocol_version,
            codec,
            endianness,
            binary_version,
            ..
        }) if protocol_version == PROTOCOL_VERSION
            && codec == crate::protocol::local_wire_codec()
            && endianness == crate::protocol::local_wire_endianness()
            && binary_version == crate::protocol::BINARY_VERSION =>
        {
            Ok(())
        }
        Frame::HelloResponse(resp) => bail!(
            "server rejected protocol: {} (client={} server={} codec={:?}/{:?} endianness={:?}/{:?} binary={}/{})",
            resp.message,
            PROTOCOL_VERSION,
            resp.protocol_version,
            crate::protocol::local_wire_codec(),
            resp.codec,
            crate::protocol::local_wire_endianness(),
            resp.endianness,
            crate::protocol::BINARY_VERSION,
            resp.binary_version
        ),
        Frame::Error(err) => bail!("server hello failed: {}", err.message),
        other => bail!("unexpected hello response frame: {other:?}"),
    }
}

struct FrameSession<'a> {
    send: QuicSendStream,
    recv: QuicRecvStream,
    max_stream_payload: usize,
    stats: &'a TransferStats,
    response_buffer: Vec<u8>,
    response_frame_len: Option<usize>,
    bw_limiter: Option<BwLimiter>,
}

impl<'a> FrameSession<'a> {
    async fn open(
        connection: &QuicConnection,
        max_stream_payload: usize,
        stats: &'a TransferStats,
        bw_limiter: Option<BwLimiter>,
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
            bw_limiter,
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
        if let Some(limiter) = &self.bw_limiter {
            limiter.throttle(encoded_len).await;
        }

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
