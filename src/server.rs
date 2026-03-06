use crate::auth::{AuthzPolicy, load_authz_policy};
use crate::certs;
use crate::compression;
use crate::filter::PathFilter;
use crate::metadata;
use crate::protocol::{
    DeletePlanRequest, DeletePlanResponse, DeletePlanStage, ErrorFrame, Frame, HelloRequest,
    HelloResponse, InitAction, InitBatchRequest, InitBatchResponse, InitBatchResult,
    InitFileRequest, InitFileResponse, PROTOCOL_VERSION, SourceEntryKind, SourceStreamChunk,
    SourceStreamDone, SourceStreamFileEnd, SourceStreamFileStart, SourceStreamRequest, SymlinkMeta,
    SyncFileMetadataBatchRequest, SyncFileMetadataBatchResponse, SyncFileMetadataResult,
    SyncSymlinkBatchRequest, SyncSymlinkBatchResponse, SyncSymlinkResult, UploadBatchRequest,
    UploadBatchResponse, UploadColdBatchRequest, UploadColdBatchResponse, UploadColdFileMeta,
    UploadColdFileResult, UploadSmallBatchRequest, UploadSmallBatchResponse, UploadSmallFileMeta,
    UploadSmallFileResult, XattrEntry,
};
use crate::state::{CompleteFileInput, StateStore};
use crate::util::{partial_path, sanitize_relative};
use anyhow::{Context, Result, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use spargio::{RuntimeHandle, fs};
use spargio_quic::{QuicBackend, QuicEndpoint, QuicEndpointOptions};
use std::collections::HashSet;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub bind: SocketAddr,
    pub destination: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: Option<PathBuf>,
    pub authz: Option<PathBuf>,
    pub max_stream_payload: usize,
    pub preserve_metadata: bool,
    pub preserve_xattrs: bool,
    pub once: bool,
    pub once_idle_timeout: Duration,
}

#[derive(Clone)]
struct ServerContext {
    handle: RuntimeHandle,
    destination: Arc<PathBuf>,
    state: StateStore,
    max_stream_payload: usize,
    preserve_metadata: bool,
    preserve_xattrs: bool,
    partials_maybe_empty: Arc<AtomicBool>,
    authz_policy: Option<Arc<AuthzPolicy>>,
    connection_auth: Option<Arc<ConnectionAuth>>,
    profile: ServerProfile,
}

#[derive(Debug)]
struct ConnectionAuth {
    client_id: String,
    fingerprint_sha256: String,
    allowed_prefixes: Vec<String>,
    bytes_written: AtomicU64,
}

#[derive(Debug, Default)]
struct DeletePlanState {
    dry_run: bool,
    include: Vec<String>,
    exclude: Vec<String>,
    keep: HashSet<String>,
}

#[derive(Default)]
struct ServerProfileInner {
    streams: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    read_ns: AtomicU64,
    decode_ns: AtomicU64,
    process_ns: AtomicU64,
    encode_ns: AtomicU64,
    write_ns: AtomicU64,
    batch_split_ns: AtomicU64,
    batch_write_ns: AtomicU64,
    state_commit_ns: AtomicU64,
}

#[derive(Clone, Default)]
struct ServerProfile {
    enabled: bool,
    inner: Arc<ServerProfileInner>,
}

impl ServerProfile {
    fn from_env() -> Self {
        let enabled = std::env::var("SPARSYNC_PROFILE")
            .map(|value| value != "0")
            .unwrap_or(false);
        Self {
            enabled,
            inner: Arc::new(ServerProfileInner::default()),
        }
    }

    fn add_stream(&self) -> u64 {
        self.inner.streams.fetch_add(1, Ordering::Relaxed) + 1
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

    fn add_read(&self, elapsed: Duration) {
        self.inner
            .read_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_decode(&self, elapsed: Duration) {
        self.inner
            .decode_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_process(&self, elapsed: Duration) {
        self.inner
            .process_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_encode(&self, elapsed: Duration) {
        self.inner
            .encode_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_write(&self, elapsed: Duration) {
        self.inner
            .write_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_batch_split(&self, elapsed: Duration) {
        self.inner
            .batch_split_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_batch_write(&self, elapsed: Duration) {
        self.inner
            .batch_write_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn add_state_commit(&self, elapsed: Duration) {
        self.inner
            .state_commit_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn maybe_log_stream_totals(&self, stream_idx: u64) {
        if !self.enabled {
            return;
        }
        if stream_idx % 8 != 0 {
            return;
        }
        info!(
            target: "sparsync::server_profile",
            streams = self.inner.streams.load(Ordering::Relaxed),
            request_bytes = self.inner.request_bytes.load(Ordering::Relaxed),
            response_bytes = self.inner.response_bytes.load(Ordering::Relaxed),
            read_ms = Duration::from_nanos(self.inner.read_ns.load(Ordering::Relaxed)).as_millis(),
            decode_ms = Duration::from_nanos(self.inner.decode_ns.load(Ordering::Relaxed)).as_millis(),
            process_ms = Duration::from_nanos(self.inner.process_ns.load(Ordering::Relaxed)).as_millis(),
            encode_ms = Duration::from_nanos(self.inner.encode_ns.load(Ordering::Relaxed)).as_millis(),
            write_ms = Duration::from_nanos(self.inner.write_ns.load(Ordering::Relaxed)).as_millis(),
            batch_split_ms = Duration::from_nanos(self.inner.batch_split_ns.load(Ordering::Relaxed)).as_millis(),
            batch_write_ms = Duration::from_nanos(self.inner.batch_write_ns.load(Ordering::Relaxed)).as_millis(),
            state_commit_ms = Duration::from_nanos(self.inner.state_commit_ns.load(Ordering::Relaxed)).as_millis(),
            "server_profile"
        );
    }
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
        certs::load_server_config(&options.cert, &options.key, options.client_ca.as_deref())
            .with_context(|| {
                format!(
                    "load server config cert={} key={} client_ca={}",
                    options.cert.display(),
                    options.key.display(),
                    options
                        .client_ca
                        .as_ref()
                        .map(|v| v.display().to_string())
                        .unwrap_or_else(|| "<none>".to_string())
                )
            })?;

    let mut endpoint_options = QuicEndpointOptions::default()
        .with_accept_timeout(Duration::from_secs(30))
        .with_operation_timeout(Duration::from_secs(60))
        .with_max_inflight_ops(65_536);
    if options.authz.is_some() {
        endpoint_options = endpoint_options.with_backend(QuicBackend::Bridge);
    }

    let endpoint = QuicEndpoint::server_with_options(server_config, options.bind, endpoint_options)
        .context("create quic server endpoint")?;

    info!(
        bind = %options.bind,
        destination = %options.destination.display(),
        client_auth = options.client_ca.is_some(),
        authz = options
            .authz
            .as_ref()
            .map(|v| v.display().to_string())
            .unwrap_or_else(|| "<none>".to_string()),
        once = options.once,
        once_idle_timeout_ms = options.once_idle_timeout.as_millis(),
        "sparsync server started"
    );

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
        preserve_xattrs: options.preserve_xattrs,
        partials_maybe_empty: Arc::new(AtomicBool::new(partials_maybe_empty)),
        authz_policy: options
            .authz
            .as_deref()
            .map(load_authz_policy)
            .transpose()
            .context("load authz policy")?
            .map(Arc::new),
        connection_auth: None,
        profile: ServerProfile::from_env(),
    };

    let once_deadline = options
        .once
        .then(|| Instant::now() + options.once_idle_timeout);
    loop {
        let accepted = match if let Some(deadline) = once_deadline {
            match spargio::timeout_at(deadline, endpoint.accept()).await {
                Ok(value) => value,
                Err(_) => {
                    warn!(
                        timeout_ms = options.once_idle_timeout.as_millis(),
                        "serve --once idle timeout elapsed before first connection; exiting"
                    );
                    context.state.flush().await.context("flush resume state")?;
                    return Ok(());
                }
            }
        } else {
            endpoint.accept().await
        } {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
                spargio::sleep(Duration::from_millis(2)).await;
                continue;
            }
            Err(err) => {
                return Err(err).context("accept incoming connection");
            }
        };

        let Some(connection) = accepted else {
            if let Some(deadline) = once_deadline {
                if Instant::now() >= deadline {
                    warn!(
                        timeout_ms = options.once_idle_timeout.as_millis(),
                        "serve --once idle timeout elapsed while waiting for connection; exiting"
                    );
                    context.state.flush().await.context("flush resume state")?;
                    return Ok(());
                }
            }
            spargio::sleep(Duration::from_millis(2)).await;
            continue;
        };
        let context = context.clone();
        let conn_id = connection.stable_id();
        info!(connection_id = conn_id, "accepted QUIC connection");

        if options.once {
            if let Err(err) =
                handle_connection(context.clone(), connection, Some(options.once_idle_timeout))
                    .await
            {
                warn!(
                    connection_id = conn_id,
                    error = %err,
                    "connection closed with error"
                );
            }
            context.state.flush().await.context("flush resume state")?;
            return Ok(());
        } else {
            let spawn = handle.spawn_stealable(async move {
                if let Err(err) = handle_connection(context, connection, None).await {
                    warn!(connection_id = conn_id, error = %err, "connection closed with error");
                }
            });
            if let Err(err) = spawn {
                error!(connection_id = conn_id, error = ?err, "failed to spawn connection task");
            }
        }
    }
}

async fn handle_connection(
    context: ServerContext,
    connection: spargio_quic::QuicConnection,
    idle_timeout: Option<Duration>,
) -> Result<()> {
    let connection_id = connection.stable_id();
    let connection_auth = if let Some(policy) = &context.authz_policy {
        Some(authorize_connection(
            policy,
            &connection,
            context.destination.as_path(),
            connection_id,
        )?)
    } else {
        None
    };
    let context = ServerContext {
        connection_auth,
        ..context
    };

    let mut idle_deadline = idle_timeout.map(|timeout| Instant::now() + timeout);
    let outcome = loop {
        let accepted_stream = if let Some(deadline) = idle_deadline {
            match spargio::timeout_at(deadline, connection.accept_bi()).await {
                Ok(value) => value,
                Err(_) => {
                    warn!(
                        connection_id,
                        timeout_ms = idle_timeout.unwrap_or_default().as_millis(),
                        "connection idle timeout elapsed in serve --once; closing"
                    );
                    break Ok(());
                }
            }
        } else {
            connection.accept_bi().await
        };

        match accepted_stream {
            Ok((send, recv)) => {
                idle_deadline = idle_timeout.map(|timeout| Instant::now() + timeout);
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
                if let Some(deadline) = idle_deadline {
                    if Instant::now() >= deadline {
                        warn!(
                            connection_id,
                            timeout_ms = idle_timeout.unwrap_or_default().as_millis(),
                            "connection idle timeout elapsed in serve --once; closing"
                        );
                        break Ok(());
                    }
                }
                spargio::sleep(Duration::from_millis(2)).await;
                continue;
            }
            Err(err) if is_closed_error(&err) => {
                break Ok(());
            }
            Err(err) => {
                break Err(err).context("accept bidirectional stream");
            }
        }
    };

    if let Some(auth) = &context.connection_auth {
        info!(
            target: "sparsync::audit",
            connection_id,
            client_id = %auth.client_id,
            fingerprint_sha256 = %auth.fingerprint_sha256,
            decision = "allow",
            destination_root = %context.destination.display(),
            bytes_written = auth.bytes_written.load(Ordering::Relaxed),
            "mTLS connection closed"
        );
    }

    outcome
}

fn authorize_connection(
    policy: &AuthzPolicy,
    connection: &spargio_quic::QuicConnection,
    destination_root: &Path,
    connection_id: usize,
) -> Result<Arc<ConnectionAuth>> {
    let certs = connection
        .peer_cert_chain_der()
        .context("read peer certificate chain from QUIC connection")?;
    let Some(leaf) = certs.first() else {
        warn!(
            target: "sparsync::audit",
            connection_id,
            decision = "deny",
            reason = "no_certificate",
            destination_root = %destination_root.display(),
            "mTLS authorization denied"
        );
        bail!("peer presented no certificate");
    };
    let fingerprint = crate::certs::certificate_fingerprint_sha256(leaf);
    let Some(entry) = policy.entry_for_fingerprint(&fingerprint) else {
        warn!(
            target: "sparsync::audit",
            connection_id,
            decision = "deny",
            reason = "unknown_fingerprint",
            fingerprint_sha256 = %fingerprint,
            destination_root = %destination_root.display(),
            "mTLS authorization denied"
        );
        bail!("peer certificate fingerprint is not authorized");
    };
    if entry.revoked {
        warn!(
            target: "sparsync::audit",
            connection_id,
            decision = "deny",
            reason = "revoked",
            client_id = %entry.client_id,
            fingerprint_sha256 = %fingerprint,
            destination_root = %destination_root.display(),
            "mTLS authorization denied"
        );
        bail!("peer certificate fingerprint is revoked");
    }

    let allowed_prefixes = crate::auth::normalize_allowed_prefixes(&entry.allowed_prefixes)
        .context("normalize authz prefixes from policy")?;
    info!(
        target: "sparsync::audit",
        connection_id,
        decision = "allow",
        client_id = %entry.client_id,
        fingerprint_sha256 = %fingerprint,
        destination_root = %destination_root.display(),
        allowed_prefixes = %allowed_prefixes.join(","),
        "mTLS authorization allowed"
    );

    Ok(Arc::new(ConnectionAuth {
        client_id: entry.client_id.clone(),
        fingerprint_sha256: fingerprint,
        allowed_prefixes,
        bytes_written: AtomicU64::new(0),
    }))
}

fn normalize_relative_for_auth(relative_path: &str) -> Result<String> {
    let cleaned = sanitize_relative(relative_path)?;
    Ok(cleaned.to_string_lossy().replace('\\', "/"))
}

fn is_path_allowed(allowed_prefixes: &[String], relative_path: &str) -> bool {
    if allowed_prefixes.iter().any(|prefix| prefix == "/") {
        return true;
    }

    allowed_prefixes.iter().any(|prefix| {
        if relative_path == prefix {
            return true;
        }
        let mut with_slash = String::with_capacity(prefix.len() + 1);
        with_slash.push_str(prefix);
        with_slash.push('/');
        relative_path.starts_with(&with_slash)
    })
}

fn authorize_relative_path(context: &ServerContext, relative_path: &str) -> Result<()> {
    let Some(auth) = &context.connection_auth else {
        return Ok(());
    };

    let normalized = normalize_relative_for_auth(relative_path)?;
    if is_path_allowed(&auth.allowed_prefixes, &normalized) {
        return Ok(());
    }

    warn!(
        target: "sparsync::audit",
        client_id = %auth.client_id,
        fingerprint_sha256 = %auth.fingerprint_sha256,
        decision = "deny",
        reason = "prefix_not_allowed",
        destination_path = %normalized,
        allowed_prefixes = %auth.allowed_prefixes.join(","),
        "mTLS path authorization denied"
    );
    bail!(
        "destination path '{}' is not authorized for client '{}'",
        relative_path,
        auth.client_id
    );
}

fn response_bytes_written(frame: &Frame) -> u64 {
    match frame {
        Frame::UploadBatchResponse(resp) => resp.bytes_written,
        Frame::UploadSmallBatchResponse(resp) => resp.results.iter().map(|v| v.bytes_written).sum(),
        Frame::UploadColdBatchResponse(resp) => resp.results.iter().map(|v| v.bytes_written).sum(),
        _ => 0,
    }
}

fn track_written_bytes(context: &ServerContext, frame: &Frame) {
    let Some(auth) = &context.connection_auth else {
        return;
    };
    let bytes = response_bytes_written(frame);
    if bytes > 0 {
        auth.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }
}

async fn handle_stream(
    context: ServerContext,
    mut send: spargio_quic::QuicSendStream,
    mut recv: spargio_quic::QuicRecvStream,
) -> Result<()> {
    let stream_idx = context.profile.add_stream();
    let mut read_buffer = Vec::with_capacity(64 * 1024);
    let mut expected_len = None;
    let mut delete_plan_state: Option<DeletePlanState> = None;

    loop {
        let bytes =
            read_next_frame(&context, &mut recv, &mut read_buffer, &mut expected_len).await?;
        let Some(bytes) = bytes else {
            break;
        };
        context.profile.add_request_bytes(bytes.len());

        let decode_started = Instant::now();
        let (frame, payload) = crate::protocol::decode(&bytes).context("decode frame")?;
        context.profile.add_decode(decode_started.elapsed());

        let process_started = Instant::now();
        if let Frame::SourceStreamRequest(req) = frame {
            let stream_result =
                process_source_stream_request(&context, &mut send, req, payload).await;
            if let Err(err) = stream_result {
                let response = Frame::Error(ErrorFrame {
                    message: format!("{err:#}"),
                });
                let response_bytes = crate::protocol::encode(&response, None)
                    .context("encode source stream error response")?;
                send.write_all(&response_bytes)
                    .await
                    .context("write source stream error response")?;
            }
            context.profile.add_process(process_started.elapsed());
            continue;
        }
        let response = match frame {
            Frame::HelloRequest(req) => process_hello(req).await,
            Frame::InitFileRequest(req) => process_init_file(&context, req).await,
            Frame::InitBatchRequest(req) => process_init_batch(&context, req).await,
            Frame::UploadBatchRequest(req) => process_upload_batch(&context, req, payload).await,
            Frame::UploadSmallBatchRequest(req) => {
                process_upload_small_batch(&context, req, payload).await
            }
            Frame::UploadColdBatchRequest(req) => {
                process_upload_cold_batch(&context, req, payload).await
            }
            Frame::SyncSymlinkBatchRequest(req) => {
                process_sync_symlink_batch(&context, req, payload).await
            }
            Frame::SyncFileMetadataBatchRequest(req) => {
                process_sync_file_metadata_batch(&context, req, payload).await
            }
            Frame::DeletePlanRequest(req) => {
                process_delete_plan(&context, &mut delete_plan_state, req, payload).await
            }
            Frame::HelloResponse(_) => Ok(Frame::Error(ErrorFrame {
                message: "unexpected hello response on server".to_string(),
            })),
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
        track_written_bytes(&context, &response);
        context.profile.add_process(process_started.elapsed());

        let encode_started = Instant::now();
        let response_bytes = crate::protocol::encode(&response, None).context("encode response")?;
        context.profile.add_encode(encode_started.elapsed());
        context.profile.add_response_bytes(response_bytes.len());
        let write_started = Instant::now();
        send.write_all(&response_bytes)
            .await
            .context("write response")?;
        context.profile.add_write(write_started.elapsed());
    }

    let finish_started = Instant::now();
    send.finish().context("finish response stream")?;
    context.profile.add_write(finish_started.elapsed());
    context.profile.maybe_log_stream_totals(stream_idx);
    Ok(())
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

async fn read_next_frame(
    context: &ServerContext,
    recv: &mut spargio_quic::QuicRecvStream,
    read_buffer: &mut Vec<u8>,
    expected_len: &mut Option<usize>,
) -> Result<Option<Vec<u8>>> {
    loop {
        if let Some(frame) =
            try_extract_frame(read_buffer, expected_len, context.max_stream_payload)?
        {
            return Ok(Some(frame));
        }

        let remaining = context.max_stream_payload.saturating_sub(read_buffer.len());
        if remaining == 0 {
            bail!(
                "request frame exceeded max_stream_payload {}",
                context.max_stream_payload
            );
        }

        let read_started = Instant::now();
        let chunk = recv
            .read_chunk(remaining)
            .await
            .context("read stream chunk")?;
        context.profile.add_read(read_started.elapsed());

        match chunk {
            Some(bytes) => {
                if !bytes.is_empty() {
                    read_buffer.extend_from_slice(bytes.as_ref());
                }
            }
            None => {
                if read_buffer.is_empty() && expected_len.is_none() {
                    return Ok(None);
                }
                if let Some(total) = expected_len {
                    bail!(
                        "stream closed with partial frame: have {} expected {}",
                        read_buffer.len(),
                        total
                    );
                }
                bail!("stream closed with {} buffered bytes", read_buffer.len());
            }
        }
    }
}

fn try_extract_frame(
    read_buffer: &mut Vec<u8>,
    expected_len: &mut Option<usize>,
    max_stream_payload: usize,
) -> Result<Option<Vec<u8>>> {
    if expected_len.is_none() && read_buffer.len() >= crate::protocol::FRAME_PREFIX_LEN {
        let total_len =
            crate::protocol::frame_total_len(&read_buffer[..crate::protocol::FRAME_PREFIX_LEN])
                .context("decode frame prefix")?;
        if total_len > max_stream_payload {
            bail!(
                "frame length {} exceeds max_stream_payload {}",
                total_len,
                max_stream_payload
            );
        }
        *expected_len = Some(total_len);
    }

    let Some(total_len) = *expected_len else {
        return Ok(None);
    };

    if read_buffer.len() < total_len {
        return Ok(None);
    }

    *expected_len = None;
    if read_buffer.len() == total_len {
        return Ok(Some(std::mem::take(read_buffer)));
    }

    let tail = read_buffer.split_off(total_len);
    let frame = std::mem::replace(read_buffer, tail);
    Ok(Some(frame))
}

async fn process_init_file(context: &ServerContext, req: InitFileRequest) -> Result<Frame> {
    authorize_relative_path(context, &req.relative_path)?;
    let response = initialize_one_file(context, &req).await?;
    Ok(Frame::InitFileResponse(response))
}

async fn process_init_batch(context: &ServerContext, req: InitBatchRequest) -> Result<Frame> {
    let mut prepared = Vec::with_capacity(req.files.len());
    let mut final_parents = HashSet::new();
    let mut partial_parents = HashSet::new();

    for file in req.files {
        authorize_relative_path(context, &file.relative_path)?;
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

fn is_delete_path_allowed(context: &ServerContext, relative_path: &str) -> bool {
    let Some(auth) = &context.connection_auth else {
        return true;
    };
    is_path_allowed(&auth.allowed_prefixes, relative_path)
}

async fn enumerate_relative_files(handle: &RuntimeHandle, root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    match fs::metadata_lite(handle, root).await {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => return Ok(out),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err).with_context(|| format!("stat {}", root.display())),
    }

    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(handle, &dir)
            .await
            .with_context(|| format!("read directory {}", dir.display()))?;
        for entry in entries {
            match entry.entry_type {
                fs::DirEntryType::Directory => stack.push(entry.path),
                fs::DirEntryType::File => {
                    let rel = entry.path.strip_prefix(root).with_context(|| {
                        format!(
                            "path {} escaped root {}",
                            entry.path.display(),
                            root.display()
                        )
                    })?;
                    out.push(rel.to_path_buf());
                }
                fs::DirEntryType::Symlink => {
                    let rel = entry.path.strip_prefix(root).with_context(|| {
                        format!(
                            "path {} escaped root {}",
                            entry.path.display(),
                            root.display()
                        )
                    })?;
                    out.push(rel.to_path_buf());
                }
                _ => {}
            }
        }
    }
    out.sort();
    Ok(out)
}

async fn process_delete_plan(
    context: &ServerContext,
    state: &mut Option<DeletePlanState>,
    req: DeletePlanRequest,
    payload: &[u8],
) -> Result<Frame> {
    match req.stage {
        DeletePlanStage::Begin => {
            *state = Some(DeletePlanState {
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
            let Some(plan) = state.as_mut() else {
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
            let Some(plan) = state.take() else {
                return Ok(Frame::DeletePlanResponse(DeletePlanResponse {
                    accepted: false,
                    message: "delete plan apply before begin".to_string(),
                    deleted: 0,
                    keep_paths: 0,
                }));
            };
            let filter = PathFilter::from_patterns(&plan.include, &plan.exclude)
                .context("compile delete filter")?;
            let mut deleted = 0u64;
            let destination_files =
                enumerate_relative_files(&context.handle, context.destination.as_ref())
                    .await
                    .context("enumerate destination files for delete planning")?;
            for rel in destination_files {
                let rel_text = rel.to_string_lossy().replace('\\', "/");
                if !filter.allows(&rel_text) {
                    continue;
                }
                if !is_delete_path_allowed(context, &rel_text) {
                    continue;
                }
                if plan.keep.contains(&rel_text) {
                    continue;
                }
                let path = context.destination.join(&rel);
                if plan.dry_run {
                    deleted = deleted.saturating_add(1);
                    continue;
                }
                match fs::remove_file(&context.handle, &path).await {
                    Ok(()) => {
                        deleted = deleted.saturating_add(1);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(err).with_context(|| format!("delete {}", path.display()));
                    }
                }
            }
            Ok(Frame::DeletePlanResponse(DeletePlanResponse {
                accepted: true,
                message: "delete plan applied".to_string(),
                deleted,
                keep_paths: plan.keep.len() as u64,
            }))
        }
    }
}

async fn write_stream_frame(
    send: &mut spargio_quic::QuicSendStream,
    max_stream_payload: usize,
    frame: &Frame,
    payload: Option<&[u8]>,
) -> Result<()> {
    let payload_len = payload.map_or(0usize, |bytes| bytes.len());
    let header =
        crate::protocol::encode_header(frame, payload_len).context("encode source stream frame")?;
    let encoded_len = header.len().saturating_add(payload_len);
    if encoded_len > max_stream_payload {
        bail!(
            "source stream frame too large: {} > max_stream_payload {}",
            encoded_len,
            max_stream_payload
        );
    }
    send.write_all(&header)
        .await
        .context("write source stream frame header")?;
    if let Some(payload) = payload {
        send.write_all(payload)
            .await
            .context("write source stream frame payload")?;
    }
    Ok(())
}

async fn process_source_stream_request(
    context: &ServerContext,
    send: &mut spargio_quic::QuicSendStream,
    req: SourceStreamRequest,
    payload: &[u8],
) -> Result<()> {
    if !payload.is_empty() {
        bail!(
            "source stream request does not accept payload (received {} bytes)",
            payload.len()
        );
    }
    let filter =
        PathFilter::from_patterns(&req.include, &req.exclude).context("compile source filter")?;
    let chunk_size = req.chunk_size.max(1);

    let mut files_streamed = 0u64;
    let mut bytes_streamed = 0u64;
    let files = enumerate_relative_files(&context.handle, context.destination.as_ref())
        .await
        .context("enumerate source files for stream request")?;

    for rel in files {
        let rel_text = rel.to_string_lossy().replace('\\', "/");
        if !filter.allows(&rel_text) {
            continue;
        }
        if !is_delete_path_allowed(context, &rel_text) {
            continue;
        }
        let source_path = context.destination.join(&rel);
        let symlink_meta = fs::symlink_metadata(&context.handle, &source_path)
            .await
            .with_context(|| format!("lstat {}", source_path.display()))?;
        let is_symlink = symlink_meta.file_type().is_symlink();
        let (entry_kind, symlink_target, size, mode, mtime_sec, uid, gid, follow_symlink) =
            if is_symlink {
                #[cfg(unix)]
                let mode = symlink_meta.mode() as u32;
                #[cfg(not(unix))]
                let mode = 0u32;
                #[cfg(unix)]
                let mtime_sec = symlink_meta.mtime();
                #[cfg(not(unix))]
                let mtime_sec = 0i64;
                #[cfg(unix)]
                let uid = symlink_meta.uid();
                #[cfg(not(unix))]
                let uid = 0u32;
                #[cfg(unix)]
                let gid = symlink_meta.gid();
                #[cfg(not(unix))]
                let gid = 0u32;
                let target = metadata::read_link_target(&source_path)
                    .with_context(|| format!("read symlink {}", source_path.display()))?;
                (
                    SourceEntryKind::Symlink,
                    Some(target),
                    0,
                    mode,
                    mtime_sec,
                    uid,
                    gid,
                    false,
                )
            } else {
                let meta = fs::metadata_lite(&context.handle, &source_path)
                    .await
                    .with_context(|| format!("metadata {}", source_path.display()))?;
                if !meta.is_file() {
                    continue;
                }
                (
                    SourceEntryKind::File,
                    None,
                    meta.size,
                    meta.mode as u32,
                    meta.mtime_sec,
                    meta.uid,
                    meta.gid,
                    true,
                )
            };
        let xattrs = if req.preserve_xattrs {
            metadata::collect_xattrs(&source_path, follow_symlink)
                .with_context(|| format!("collect xattrs {}", source_path.display()))?
        } else {
            Vec::new()
        };
        let start = Frame::SourceStreamFileStart(SourceStreamFileStart {
            relative_path: rel_text.clone(),
            entry_kind,
            symlink_target,
            size,
            mode,
            mtime_sec,
            uid,
            gid,
            xattrs,
        });
        write_stream_frame(send, context.max_stream_payload, &start, None).await?;

        if !req.metadata_only && !is_symlink {
            let file = fs::File::open(context.handle.clone(), &source_path)
                .await
                .with_context(|| format!("open source stream file {}", source_path.display()))?;
            let mut offset = 0u64;
            loop {
                let chunk = file.read_at(offset, chunk_size).await.with_context(|| {
                    format!("read source stream file {}", source_path.display())
                })?;
                if chunk.is_empty() {
                    break;
                }
                let chunk_len = u32::try_from(chunk.len()).with_context(|| {
                    format!(
                        "source stream chunk too large ({} bytes) for {}",
                        chunk.len(),
                        source_path.display()
                    )
                })?;
                let chunk_frame = Frame::SourceStreamChunk(SourceStreamChunk { chunk_len });
                write_stream_frame(
                    send,
                    context.max_stream_payload,
                    &chunk_frame,
                    Some(chunk.as_ref()),
                )
                .await?;
                offset = offset.saturating_add(chunk.len() as u64);
                bytes_streamed = bytes_streamed.saturating_add(chunk.len() as u64);
                if chunk.len() < chunk_size {
                    break;
                }
            }
        }

        let end = Frame::SourceStreamFileEnd(SourceStreamFileEnd {
            relative_path: rel_text,
        });
        write_stream_frame(send, context.max_stream_payload, &end, None).await?;
        files_streamed = files_streamed.saturating_add(1);
    }

    let done = Frame::SourceStreamDone(SourceStreamDone {
        files: files_streamed,
        bytes: bytes_streamed,
    });
    write_stream_frame(send, context.max_stream_payload, &done, None).await?;
    Ok(())
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
    let final_path = context.destination.join(relative);
    let partial_path = partial_path(context.state.partial_root(), &relative);
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
    authorize_relative_path(context, &req.relative_path)?;
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

    let split_started = Instant::now();
    let packets = crate::protocol::decode_chunk_batch(payload, req.sent_chunks)
        .with_context(|| format!("decode chunk batch for {}", req.relative_path))?;
    context.profile.add_batch_split(split_started.elapsed());

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

    let write_started = Instant::now();
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
    context.profile.add_batch_write(write_started.elapsed());

    let state_started = Instant::now();
    context
        .state
        .update_partial_progress(key, next_chunk, false)
        .await
        .with_context(|| format!("update batch progress {}", req.relative_path))?;
    context.profile.add_state_commit(state_started.elapsed());

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

    let finalize_started = Instant::now();
    let bytes_written = finalize_uploaded_file(context, &req, &relative, &partial_path).await?;
    context.profile.add_state_commit(finalize_started.elapsed());
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
    for meta in &req.files {
        authorize_relative_path(context, &meta.relative_path)?;
    }
    let write_concurrency =
        choose_write_concurrency(req.files.len(), req.files.iter().map(|f| f.size).sum());
    let split_started = Instant::now();
    let payload_parts = crate::protocol::split_small_file_payload(payload, &req.files)
        .context("split small-file batch payload")?;
    context.profile.add_batch_split(split_started.elapsed());

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

    let write_started = Instant::now();
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
    context.profile.add_batch_write(write_started.elapsed());

    let state_started = Instant::now();
    context
        .state
        .complete_files_batch(&completions)
        .await
        .context("persist small batch completion state")?;
    context.profile.add_state_commit(state_started.elapsed());

    Ok(Frame::UploadSmallBatchResponse(UploadSmallBatchResponse {
        results,
    }))
}

async fn process_upload_cold_batch(
    context: &ServerContext,
    req: UploadColdBatchRequest,
    payload: &[u8],
) -> Result<Frame> {
    for meta in &req.files {
        authorize_relative_path(context, &meta.relative_path)?;
    }
    let write_concurrency =
        choose_write_concurrency(req.files.len(), req.files.iter().map(|f| f.size).sum());
    let split_started = Instant::now();
    let payload_parts = crate::protocol::split_cold_file_payload(payload, &req.files)
        .context("split cold-file batch payload")?;
    context.profile.add_batch_split(split_started.elapsed());
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

    let write_started = Instant::now();
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
    context.profile.add_batch_write(write_started.elapsed());

    let state_started = Instant::now();
    context
        .state
        .complete_files_batch(&completions)
        .await
        .context("persist cold batch completion state")?;
    context.profile.add_state_commit(state_started.elapsed());

    Ok(Frame::UploadColdBatchResponse(UploadColdBatchResponse {
        results,
    }))
}

async fn remove_existing_path_for_symlink(handle: &RuntimeHandle, path: &Path) -> Result<()> {
    match fs::symlink_metadata(handle, path).await {
        Ok(meta) => {
            if meta.file_type().is_dir() && !meta.file_type().is_symlink() {
                std::fs::remove_dir_all(path)
                    .with_context(|| format!("remove existing directory {}", path.display()))?;
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

async fn apply_symlink_meta(
    context: &ServerContext,
    path: &Path,
    meta: &SymlinkMeta,
) -> Result<()> {
    if !context.preserve_metadata {
        return Ok(());
    }
    metadata::set_owner(path, meta.uid, meta.gid, false)?;
    if context.preserve_xattrs {
        metadata::apply_xattrs(path, &meta.xattrs, false)?;
    }
    metadata::set_mtime(path, meta.mtime_sec, false)?;
    Ok(())
}

async fn process_sync_symlink_batch(
    context: &ServerContext,
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
        authorize_relative_path(context, &entry.relative_path)?;
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
                apply_symlink_meta(context, &destination, &entry).await?;
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
        apply_symlink_meta(context, &destination, &entry).await?;
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
    context: &ServerContext,
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
        authorize_relative_path(context, &entry.relative_path)?;
        if !context
            .state
            .is_complete_match(&entry.relative_path, &entry.file_hash, entry.size)?
        {
            results.push(SyncFileMetadataResult {
                accepted: false,
                skipped: false,
                message: "state mismatch; file no longer complete for hash/size".to_string(),
            });
            continue;
        }
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
            xattr_sig: metadata::xattr_signature(&meta.xattrs),
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
                xattr_sig: metadata::xattr_signature(&meta.xattrs),
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
    uid: u32,
    gid: u32,
    xattrs: &'a [XattrEntry],
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
            uid: req.uid,
            gid: req.gid,
            xattrs: &req.xattrs,
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
