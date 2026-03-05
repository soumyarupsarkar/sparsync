mod auth;
mod bench;
mod bootstrap;
mod certs;
mod compression;
mod endpoint;
mod filter;
mod local_copy;
mod model;
mod profile;
mod protocol;
mod scan;
mod server;
mod state;
mod stdio_server;
mod transfer;
mod util;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use spargio::RuntimeHandle;
use std::collections::HashSet;
use std::ffi::OsString;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "sparsync",
    about = "Spargio-powered rsync-style transfer tool",
    disable_help_flag = true
)]
struct Cli {
    #[arg(long, default_value_t = num_cpus::get().max(1))]
    shards: usize,

    #[arg(long = "help", action = clap::ArgAction::HelpLong, global = true)]
    help: Option<bool>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve(ServeArgs),
    #[command(name = "serve-stdio", hide = true)]
    ServeStdio(ServeStdioArgs),
    Push(PushArgs),
    Sync(SyncArgs),
    Enroll(EnrollArgs),
    Server(ServerArgs),
    Auth(AuthArgs),
    Scan(ScanArgs),
    Bench(BenchArgs),
    GenCert(GenCertArgs),
}

#[derive(Debug, Args, Clone)]
struct ServeArgs {
    #[arg(long, default_value = "0.0.0.0:7844")]
    bind: SocketAddr,

    #[arg(long)]
    destination: PathBuf,

    #[arg(long)]
    cert: PathBuf,

    #[arg(long)]
    key: PathBuf,

    #[arg(long)]
    client_ca: Option<PathBuf>,

    #[arg(long)]
    authz: Option<PathBuf>,

    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_stream_payload: usize,

    #[arg(long)]
    preserve_metadata: bool,

    #[arg(long)]
    once: bool,

    #[arg(long, default_value_t = 60_000)]
    once_idle_timeout_ms: u64,
}

#[derive(Debug, Args, Clone)]
struct ServeStdioArgs {
    #[arg(long)]
    destination: PathBuf,

    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_stream_payload: usize,

    #[arg(long)]
    preserve_metadata: bool,
}

#[derive(Debug, Args, Clone)]
struct PushArgs {
    #[arg(long)]
    source: PathBuf,

    #[arg(long)]
    server: Option<SocketAddr>,

    #[arg(long)]
    server_name: Option<String>,

    #[arg(long)]
    ca: Option<PathBuf>,

    #[arg(long)]
    client_cert: Option<PathBuf>,

    #[arg(long)]
    client_key: Option<PathBuf>,

    #[arg(long)]
    profile: Option<String>,

    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1) * 4)]
    parallel_files: usize,

    #[arg(long, default_value_t = 1)]
    connections: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1))]
    scan_workers: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1) * 2)]
    hash_workers: usize,

    #[arg(long, default_value_t = 3)]
    compression_level: i32,

    #[arg(long, default_value_t = 5000)]
    connect_timeout_ms: u64,

    #[arg(long, default_value_t = 15000)]
    op_timeout_ms: u64,

    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_stream_payload: usize,

    #[arg(long)]
    no_resume: bool,

    #[arg(short = 'u', long = "update")]
    update_only: bool,

    #[arg(long = "exclude")]
    exclude: Vec<String>,

    #[arg(long = "include")]
    include: Vec<String>,

    #[arg(long = "bwlimit")]
    bwlimit: Option<String>,

    #[arg(long)]
    cold_start: bool,

    #[arg(long)]
    manifest_out: Option<PathBuf>,
}

#[derive(Debug, Args, Clone)]
struct SyncArgs {
    source: String,
    destination: String,

    #[arg(short = 'a', long = "archive")]
    archive: bool,

    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    #[arg(short = 'z', long = "compress")]
    rsync_compress: bool,

    #[arg(short = 'h', long = "human-readable")]
    human_readable: bool,

    #[arg(short = 'P', long = "partial-progress")]
    partial_progress: bool,

    #[arg(short = 'n', long = "dry-run")]
    dry_run: bool,

    #[arg(long)]
    delete: bool,

    #[arg(long = "exclude")]
    exclude: Vec<String>,

    #[arg(long = "include")]
    include: Vec<String>,

    #[arg(short = 'u', long = "update")]
    update_only: bool,

    #[arg(long = "bwlimit")]
    bwlimit: Option<String>,

    #[arg(long)]
    profile: Option<String>,

    #[arg(long, default_value = "none")]
    bootstrap: String,

    #[arg(long, default_value = "auto")]
    transport: String,

    #[arg(long, default_value = "ephemeral")]
    install: String,

    #[arg(long)]
    ssh_target: Option<String>,

    #[arg(long, default_value_t = 7844)]
    server_port: u16,

    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1) * 4)]
    parallel_files: usize,

    #[arg(long, default_value_t = 1)]
    connections: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1))]
    scan_workers: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1) * 2)]
    hash_workers: usize,

    #[arg(long, default_value_t = 0)]
    compression_level: i32,

    #[arg(long, default_value_t = 5000)]
    connect_timeout_ms: u64,

    #[arg(long, default_value_t = 15000)]
    op_timeout_ms: u64,

    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_stream_payload: usize,

    #[arg(long)]
    no_resume: bool,

    #[arg(long)]
    cold_start: bool,

    #[arg(long)]
    manifest_out: Option<PathBuf>,

    #[arg(long)]
    preserve_metadata: bool,
}

#[derive(Debug, Args, Clone)]
struct EnrollArgs {
    endpoint: String,

    #[arg(long)]
    profile: Option<String>,

    #[arg(long, default_value = "ephemeral")]
    install: String,

    #[arg(long, default_value_t = 7844)]
    server_port: u16,

    #[arg(long)]
    server_name: Option<String>,

    #[arg(long)]
    client_id: Option<String>,

    #[arg(long)]
    start_server: bool,

    #[arg(long)]
    preserve_metadata: bool,
}

#[derive(Debug, Args, Clone)]
struct ServerArgs {
    #[command(subcommand)]
    command: ServerCommand,
}

#[derive(Debug, Subcommand, Clone)]
enum ServerCommand {
    Start(ServerStartArgs),
    Stop(ServerStopArgs),
    Status(ServerStatusArgs),
}

#[derive(Debug, Args, Clone)]
struct ServerStartArgs {
    target: String,

    #[arg(long)]
    destination: Option<String>,

    #[arg(long)]
    profile: Option<String>,

    #[arg(long, default_value = "auto")]
    install: String,

    #[arg(long, default_value_t = 7844)]
    server_port: u16,

    #[arg(long)]
    server_name: Option<String>,

    #[arg(long)]
    client_id: Option<String>,

    #[arg(long)]
    preserve_metadata: bool,
}

#[derive(Debug, Args, Clone)]
struct ServerStopArgs {
    target: String,

    #[arg(long)]
    profile: Option<String>,
}

#[derive(Debug, Args, Clone)]
struct ServerStatusArgs {
    target: String,

    #[arg(long)]
    profile: Option<String>,
}

#[derive(Debug, Args, Clone)]
struct AuthArgs {
    #[command(subcommand)]
    command: AuthCommand,
}

#[derive(Debug, Subcommand, Clone)]
enum AuthCommand {
    InitServer(AuthInitServerArgs),
    IssueClient(AuthIssueClientArgs),
    RevokeClient(AuthRevokeClientArgs),
    Rotate(AuthRotateArgs),
    Status(AuthStatusArgs),
}

#[derive(Debug, Args, Clone)]
struct AuthInitServerArgs {
    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long, default_value = "localhost")]
    server_name: String,
}

#[derive(Debug, Args, Clone)]
struct AuthIssueClientArgs {
    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long)]
    client_id: String,

    #[arg(long = "allow-prefix")]
    allow_prefixes: Vec<String>,
}

#[derive(Debug, Args, Clone)]
struct AuthRevokeClientArgs {
    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long)]
    client_id: String,
}

#[derive(Debug, Args, Clone)]
struct AuthRotateArgs {
    #[arg(long)]
    dir: Option<PathBuf>,
}

#[derive(Debug, Args, Clone)]
struct AuthStatusArgs {
    #[arg(long)]
    dir: Option<PathBuf>,
}

#[derive(Debug, Args, Clone)]
struct ScanArgs {
    #[arg(long)]
    source: PathBuf,

    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1))]
    scan_workers: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1) * 2)]
    hash_workers: usize,

    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args, Clone)]
struct BenchArgs {
    #[arg(long, default_value_t = 1_000_000)]
    files: usize,

    #[arg(long, default_value_t = 500_000)]
    tasks: usize,

    #[arg(long, default_value_t = 100_000)]
    io_ops: usize,

    #[arg(long, default_value_t = 8_192)]
    in_flight: usize,

    #[arg(long, default_value_t = 4096)]
    io_size: usize,

    #[arg(long)]
    rsync_command: Option<String>,

    #[arg(long)]
    syncthing_command: Option<String>,
}

#[derive(Debug, Args, Clone)]
struct GenCertArgs {
    #[arg(long)]
    cert: PathBuf,

    #[arg(long)]
    key: PathBuf,

    #[arg(long = "name", default_values_t = vec![String::from("localhost")])]
    names: Vec<String>,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn should_insert_sync_subcommand(args: &[OsString]) -> bool {
    if args.len() <= 1 {
        return false;
    }

    const SUBCOMMANDS: &[&str] = &[
        "serve",
        "serve-stdio",
        "push",
        "sync",
        "enroll",
        "server",
        "auth",
        "scan",
        "bench",
        "gen-cert",
    ];

    let mut idx = 1usize;
    while idx < args.len() {
        let token = args[idx].to_string_lossy();
        if token == "--help" || token == "--version" {
            return false;
        }
        if token == "--shards" {
            idx = idx.saturating_add(2);
            continue;
        }
        if token.starts_with("--shards=") {
            idx = idx.saturating_add(1);
            continue;
        }
        if token.starts_with('-') {
            idx = idx.saturating_add(1);
            continue;
        }
        return !SUBCOMMANDS.contains(&token.as_ref());
    }

    false
}

fn parse_cli() -> Result<Cli> {
    let mut args: Vec<OsString> = std::env::args_os().collect();
    if should_insert_sync_subcommand(&args) {
        args.insert(1, OsString::from("sync"));
    }
    match Cli::try_parse_from(args) {
        Ok(cli) => Ok(cli),
        Err(err) => {
            use clap::error::ErrorKind;
            if matches!(
                err.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) {
                print!("{err}");
                std::process::exit(0);
            }
            Err(anyhow::anyhow!(err.to_string()))
        }
    }
}

fn main() -> Result<()> {
    init_tracing();
    let cli = parse_cli()?;
    let builder = spargio::Runtime::builder().shards(cli.shards.max(1));

    let outcome = futures::executor::block_on(async move {
        spargio::run_with(builder, move |handle| async move {
            run_command(handle, cli).await
        })
        .await
    });

    match outcome {
        Ok(inner) => inner,
        Err(err) => Err(anyhow::anyhow!("spargio runtime failed: {err:?}")),
    }
}

async fn run_command(handle: RuntimeHandle, cli: Cli) -> Result<()> {
    match cli.command {
        Command::Serve(args) => {
            let options = server::ServeOptions {
                bind: args.bind,
                destination: args.destination,
                cert: args.cert,
                key: args.key,
                client_ca: args.client_ca,
                authz: args.authz,
                max_stream_payload: args.max_stream_payload,
                preserve_metadata: args.preserve_metadata,
                once: args.once,
                once_idle_timeout: Duration::from_millis(args.once_idle_timeout_ms),
            };
            server::run_server(handle, options).await
        }
        Command::ServeStdio(args) => {
            let options = stdio_server::ServeStdioOptions {
                destination: args.destination,
                max_stream_payload: args.max_stream_payload,
                preserve_metadata: args.preserve_metadata,
            };
            stdio_server::run_server_stdio(handle, options).await
        }
        Command::Push(args) => {
            let profile = if let Some(name) = args.profile.as_deref() {
                Some(
                    profile::get_profile(name)
                        .with_context(|| format!("load profile '{}'", name))?,
                )
            } else {
                None
            };

            let server = match (args.server, profile.as_ref().map(|v| v.server.as_str())) {
                (Some(v), _) => v,
                (None, Some(raw)) => raw
                    .parse::<SocketAddr>()
                    .with_context(|| format!("parse server socket '{}' from profile", raw))?,
                (None, None) => bail!("missing --server (or provide --profile with server)"),
            };
            let server_name = match (args.server_name.clone(), profile.as_ref()) {
                (Some(v), _) => v,
                (None, Some(p)) => p.server_name.clone(),
                (None, None) => "localhost".to_string(),
            };
            let ca = match (args.ca.clone(), profile.as_ref()) {
                (Some(v), _) => v,
                (None, Some(p)) => p.ca.clone(),
                (None, None) => bail!("missing --ca (or provide --profile with ca)"),
            };
            let client_cert = args
                .client_cert
                .clone()
                .or_else(|| profile.as_ref().and_then(|p| p.client_cert.clone()));
            let client_key = args
                .client_key
                .clone()
                .or_else(|| profile.as_ref().and_then(|p| p.client_key.clone()));

            let scan_options = scan::ScanOptions {
                chunk_size: args.chunk_size,
                scan_workers: args.scan_workers.max(1),
                hash_workers: args.hash_workers.max(1),
            };
            let path_filter = filter::PathFilter::from_patterns(&args.include, &args.exclude)
                .context("compile include/exclude patterns")?;
            let bwlimit_kbps = parse_bwlimit_kbps(args.bwlimit.as_deref())?;
            let options = transfer::PushOptions {
                source: args.source,
                server,
                server_name,
                ca,
                client_cert,
                client_key,
                scan: scan_options,
                parallel_files: args.parallel_files.max(1),
                connections: args.connections.max(1),
                compression_level: args.compression_level,
                connect_timeout: Duration::from_millis(args.connect_timeout_ms),
                operation_timeout: Duration::from_millis(args.op_timeout_ms),
                max_stream_payload: args.max_stream_payload,
                resume: !args.no_resume,
                update_only: args.update_only,
                cold_start: args.cold_start,
                manifest_out: args.manifest_out,
                path_filter: Some(path_filter),
                bwlimit_kbps,
            };
            let summary = transfer::push_directory(handle, options).await?;
            println!(
                "pushed files={} skipped={} bytes_sent={} bytes_raw={} elapsed_ms={} throughput_mbps={:.2}",
                summary.files_transferred,
                summary.files_skipped,
                summary.bytes_sent,
                summary.bytes_raw,
                summary.elapsed.as_millis(),
                summary.megabits_per_sec()
            );
            Ok(())
        }
        Command::Sync(args) => run_sync_command(handle, args).await,
        Command::Enroll(args) => run_enroll_command(args),
        Command::Server(args) => run_server_command(args),
        Command::Auth(args) => {
            let resolve_dir = |input: Option<PathBuf>| -> Result<PathBuf> {
                if let Some(dir) = input {
                    return Ok(dir);
                }
                let mut dir = profile::ensure_config_root()?;
                dir.push("auth");
                Ok(dir)
            };

            match args.command {
                AuthCommand::InitServer(cmd) => {
                    let dir = resolve_dir(cmd.dir)?;
                    let layout = auth::AuthLayout::under(&dir);
                    let out = auth::init_server(&layout, &cmd.server_name)?;
                    println!(
                        "auth initialized dir={} server_cert={} client_ca={} authz={}",
                        out.layout.root.display(),
                        out.layout.server_cert.display(),
                        out.layout.client_ca_cert.display(),
                        out.layout.authz_path.display()
                    );
                    Ok(())
                }
                AuthCommand::IssueClient(cmd) => {
                    let dir = resolve_dir(cmd.dir)?;
                    let layout = auth::AuthLayout::under(&dir);
                    let out = auth::issue_client(&layout, &cmd.client_id, &cmd.allow_prefixes)?;
                    println!(
                        "issued client id={} cert={} key={} fingerprint_sha256={} allow_prefixes={}",
                        cmd.client_id,
                        out.cert_path.display(),
                        out.key_path.display(),
                        out.fingerprint_sha256,
                        out.allowed_prefixes.join(",")
                    );
                    Ok(())
                }
                AuthCommand::RevokeClient(cmd) => {
                    let dir = resolve_dir(cmd.dir)?;
                    let layout = auth::AuthLayout::under(&dir);
                    let found = auth::revoke_client(&layout, &cmd.client_id)?;
                    println!(
                        "revoke client id={} result={}",
                        cmd.client_id,
                        if found { "updated" } else { "not_found" }
                    );
                    Ok(())
                }
                AuthCommand::Rotate(cmd) => {
                    let dir = resolve_dir(cmd.dir)?;
                    let layout = auth::AuthLayout::under(&dir);
                    auth::rotate_client_ca(&layout)?;
                    println!(
                        "rotated client CA and reset authz dir={}",
                        layout.root.display()
                    );
                    Ok(())
                }
                AuthCommand::Status(cmd) => {
                    let dir = resolve_dir(cmd.dir)?;
                    let layout = auth::AuthLayout::under(&dir);
                    let status = auth::auth_status(&layout)?;
                    println!(
                        "auth status dir={} total_clients={} active_clients={} revoked_clients={}",
                        layout.root.display(),
                        status.total_clients,
                        status.active_clients,
                        status.revoked_clients
                    );
                    Ok(())
                }
            }
        }
        Command::Scan(args) => {
            let options = scan::ScanOptions {
                chunk_size: args.chunk_size,
                scan_workers: args.scan_workers.max(1),
                hash_workers: args.hash_workers.max(1),
            };
            let (manifest, stats) = scan::build_manifest(handle.clone(), &args.source, options)
                .await
                .with_context(|| format!("scan failed for {}", args.source.display()))?;

            println!(
                "scanned files={} bytes={} enumerate_ms={} hash_ms={} total_ms={}",
                manifest.files.len(),
                manifest.total_bytes,
                stats.enumeration_elapsed.as_millis(),
                stats.hash_elapsed.as_millis(),
                stats.total_elapsed.as_millis(),
            );

            if let Some(path) = args.output {
                let bytes = serde_json::to_vec_pretty(&manifest)?;
                spargio::fs::write(&handle, &path, bytes)
                    .await
                    .with_context(|| format!("failed to write manifest {}", path.display()))?;
                println!("manifest written to {}", path.display());
            }
            Ok(())
        }
        Command::Bench(args) => {
            let report = bench::run_benchmark(
                handle,
                bench::BenchmarkOptions {
                    files: args.files,
                    tasks: args.tasks,
                    io_ops: args.io_ops,
                    in_flight: args.in_flight.max(1),
                    io_size: args.io_size.max(1),
                    rsync_command: args.rsync_command,
                    syncthing_command: args.syncthing_command,
                },
            )
            .await?;

            println!(
                "bench scan_rate={:.2}/s task_rate={:.2}/s io_rate={:.2}/s",
                report.scan_items_per_sec, report.tasks_per_sec, report.io_ops_per_sec
            );
            if let Some(ms) = report.rsync_elapsed_ms {
                println!("bench rsync_ms={ms}");
            }
            if let Some(ms) = report.syncthing_elapsed_ms {
                println!("bench syncthing_ms={ms}");
            }
            Ok(())
        }
        Command::GenCert(args) => {
            certs::generate_self_signed(&args.cert, &args.key, &args.names)
                .with_context(|| format!("failed to generate cert at {}", args.cert.display()))?;
            println!(
                "generated cert={} key={} names={}",
                args.cert.display(),
                args.key.display(),
                args.names.join(",")
            );
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectiveTransport {
    Local,
    Ssh,
    Quic,
}

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn parse_socket_address(raw: &str) -> Result<SocketAddr> {
    raw.parse::<SocketAddr>()
        .or_else(|_| {
            raw.to_socket_addrs()
                .map(|mut iter| iter.next())
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
                .and_then(|addr| {
                    addr.ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "no socket address resolved",
                        )
                    })
                })
        })
        .with_context(|| format!("parse socket address '{}'", raw))
}

fn default_profile_name(remote: &endpoint::RemoteEndpoint, server_port: u16) -> String {
    format!("{}-{}", remote.host, remote.port.unwrap_or(server_port))
}

fn parse_transport(
    raw: &str,
    remote: Option<&endpoint::RemoteEndpoint>,
) -> Result<EffectiveTransport> {
    match raw {
        "local" => Ok(EffectiveTransport::Local),
        "ssh" => Ok(EffectiveTransport::Ssh),
        "quic" => Ok(EffectiveTransport::Quic),
        "auto" => {
            let Some(remote) = remote else {
                return Ok(EffectiveTransport::Local);
            };
            if remote.is_quic() {
                Ok(EffectiveTransport::Quic)
            } else {
                Ok(EffectiveTransport::Ssh)
            }
        }
        other => bail!(
            "invalid transport '{}' (expected auto|local|quic|ssh)",
            other
        ),
    }
}

fn parse_ssh_endpoint(target: &str, path: &str) -> Result<endpoint::RemoteEndpoint> {
    let path_component = {
        let trimmed = path.trim_start_matches('/');
        if trimmed.is_empty() {
            "__sparsync__"
        } else {
            trimmed
        }
    };
    let normalized = if target.starts_with("ssh://") {
        if target.trim_end_matches('/').contains('/') {
            target.to_string()
        } else {
            format!("{}/{}", target.trim_end_matches('/'), path_component)
        }
    } else {
        format!("ssh://{}/{}", target, path_component)
    };
    let endpoint = endpoint::parse_endpoint(&normalized)
        .with_context(|| format!("parse ssh target '{}'", target))?;
    match endpoint {
        endpoint::Endpoint::Remote(remote) if remote.is_ssh() => Ok(remote),
        _ => bail!("target '{}' is not an ssh endpoint", target),
    }
}

fn resolve_bootstrap_remote(
    sync_remote: &endpoint::RemoteEndpoint,
    profile_name: &str,
    ssh_target_override: Option<&str>,
) -> Result<endpoint::RemoteEndpoint> {
    if sync_remote.is_ssh() {
        return Ok(sync_remote.clone());
    }
    if let Some(target) = ssh_target_override {
        return parse_ssh_endpoint(target, &sync_remote.path);
    }
    if let Ok(profile) = profile::get_profile(profile_name) {
        if let Some(target) = profile.ssh_target.as_deref() {
            return parse_ssh_endpoint(target, &sync_remote.path);
        }
    }
    bail!(
        "missing ssh target for '{}' (provide --ssh-target or a profile with ssh_target)",
        sync_remote.host
    )
}

fn upsert_profile_from_bootstrap(
    profile_name: &str,
    remote: &endpoint::RemoteEndpoint,
    enrollment: &bootstrap::Enrollment,
    destination: &str,
) -> Result<()> {
    profile::upsert_profile(profile::SyncProfile {
        name: profile_name.to_string(),
        server: enrollment.server.to_string(),
        server_name: enrollment.server_name.clone(),
        ca: enrollment.ca.clone(),
        client_cert: Some(enrollment.client_cert.clone()),
        client_key: Some(enrollment.client_key.clone()),
        ssh_target: Some(remote.ssh_target()),
        destination: Some(destination.to_string()),
    })
}

fn reserve_local_port() -> Result<u16> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("bind ephemeral local port for sync helper")?;
    let port = listener
        .local_addr()
        .context("read local listener address")?
        .port();
    drop(listener);
    Ok(port)
}

fn start_local_once_server(
    destination: &PathBuf,
    port: u16,
    cert: &PathBuf,
    key: &PathBuf,
    preserve_metadata: bool,
) -> Result<std::process::Child> {
    let exe = std::env::current_exe().context("resolve sparsync executable for local helper")?;
    let mut command = std::process::Command::new(exe);
    command
        .arg("serve")
        .arg("--bind")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--destination")
        .arg(destination)
        .arg("--cert")
        .arg(cert)
        .arg("--key")
        .arg(key)
        .arg("--once")
        .arg("--once-idle-timeout-ms")
        .arg("120000")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if preserve_metadata {
        command.arg("--preserve-metadata");
    }
    command.spawn().context("start local one-shot receiver")
}

fn run_remote_push_over_reverse_ssh(
    remote: &endpoint::RemoteEndpoint,
    remote_forward_port: u16,
    local_port: u16,
    remote_command: &str,
) -> Result<()> {
    let mut command = std::process::Command::new("ssh");
    command.arg("-o").arg("BatchMode=yes");
    command.arg("-o").arg("StrictHostKeyChecking=accept-new");
    if let Ok(identity) = std::env::var("SPARSYNC_SSH_IDENTITY") {
        if !identity.trim().is_empty() {
            command.arg("-i").arg(identity);
        }
    }
    if let Some(port) = remote.port {
        command.arg("-p").arg(port.to_string());
    }
    command
        .arg("-R")
        .arg(format!("{remote_forward_port}:127.0.0.1:{local_port}"))
        .arg(remote.ssh_target())
        .arg(remote_command)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = command
        .status()
        .with_context(|| format!("run reverse ssh push command on {}", remote.ssh_target()))?;
    if !status.success() {
        bail!("remote reverse push command failed with status {}", status);
    }
    Ok(())
}

async fn apply_remote_delete_over_ssh(
    handle: RuntimeHandle,
    remote: &endpoint::RemoteEndpoint,
    destination: &str,
    source_root: &PathBuf,
    scan_options: scan::ScanOptions,
    filter: &filter::PathFilter,
) -> Result<u64> {
    let (_, files, _, _) = scan::build_file_list(
        handle,
        source_root,
        scan_options.scan_workers.max(1),
        scan_options.hash_workers.max(1),
    )
    .await
    .with_context(|| format!("build delete keep-list from {}", source_root.display()))?;
    let mut keep = Vec::with_capacity(files.len());
    for file in files {
        if filter.allows(&file.relative_path) {
            keep.push(file.relative_path);
        }
    }

    let mut local_temp = profile::ensure_data_root()?;
    local_temp.push("transient");
    std::fs::create_dir_all(&local_temp)
        .with_context(|| format!("create {}", local_temp.display()))?;
    let keep_file = local_temp.join(format!(
        "delete-keep-{}-{}.txt",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    let keep_data = if keep.is_empty() {
        Vec::new()
    } else {
        let mut data = keep.join("\n").into_bytes();
        data.push(b'\n');
        data
    };
    std::fs::write(&keep_file, keep_data)
        .with_context(|| format!("write keep-list {}", keep_file.display()))?;

    let remote_keep_file = bootstrap::upload_temp_file_via_ssh(remote, &keep_file, "sparsync-keep")
        .context("upload keep-list to remote")?;
    let script = format!(
        "python3 - <<'PY' {dest} {keep}\n\
import os, sys\n\
dest = sys.argv[1]\n\
keep_path = sys.argv[2]\n\
keep = set()\n\
with open(keep_path, 'r', encoding='utf-8') as fh:\n\
    for line in fh:\n\
        line = line.strip()\n\
        if line:\n\
            keep.add(line)\n\
deleted = 0\n\
for root, dirs, files in os.walk(dest, topdown=False):\n\
    rel_root = os.path.relpath(root, dest)\n\
    for name in files:\n\
        rel = os.path.normpath(os.path.join(rel_root, name)) if rel_root != '.' else name\n\
        rel = rel.replace(os.sep, '/')\n\
        if rel.startswith('.sparsync/'):\n\
            continue\n\
        if rel not in keep:\n\
            try:\n\
                os.remove(os.path.join(root, name))\n\
                deleted += 1\n\
            except FileNotFoundError:\n\
                pass\n\
    for name in dirs:\n\
        if name == '.sparsync':\n\
            continue\n\
        path = os.path.join(root, name)\n\
        try:\n\
            os.rmdir(path)\n\
        except OSError:\n\
            pass\n\
print(deleted)\n\
PY",
        dest = sh_quote(destination),
        keep = sh_quote(&remote_keep_file),
    );
    let output =
        bootstrap::run_remote_shell(remote, &script).context("apply remote delete plan")?;
    let _ = bootstrap::remove_remote_file(remote, &remote_keep_file);
    let _ = std::fs::remove_file(&keep_file);

    let deleted_text = String::from_utf8(output).context("decode remote delete output")?;
    let deleted = deleted_text
        .lines()
        .last()
        .unwrap_or("0")
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    Ok(deleted)
}

fn fetch_remote_file_list(
    remote: &endpoint::RemoteEndpoint,
    source_root: &str,
    filter: &filter::PathFilter,
) -> Result<Vec<(String, u64)>> {
    let script = format!(
        "python3 - <<'PY' {source}\n\
import os, sys\n\
root = sys.argv[1]\n\
for base, _, files in os.walk(root):\n\
    for name in files:\n\
        path = os.path.join(base, name)\n\
        rel = os.path.relpath(path, root).replace(os.sep, '/')\n\
        try:\n\
            size = os.path.getsize(path)\n\
        except OSError:\n\
            continue\n\
        print(f\"{{rel}}\\t{{size}}\")\n\
PY",
        source = sh_quote(source_root)
    );
    let output = bootstrap::run_remote_shell(remote, &script)
        .with_context(|| format!("enumerate remote files under {}", source_root))?;
    let text = String::from_utf8(output).context("decode remote file list")?;
    let mut out = Vec::new();
    for line in text.lines() {
        let Some((path, size_raw)) = line.split_once('\t') else {
            continue;
        };
        if !filter.allows(path) {
            continue;
        }
        let size = size_raw
            .trim()
            .parse::<u64>()
            .with_context(|| format!("parse remote file size '{}'", size_raw.trim()))?;
        out.push((path.to_string(), size));
    }
    Ok(out)
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len().saturating_sub(1) {
        value /= 1024.0;
        unit = unit.saturating_add(1);
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn print_push_summary(prefix: &str, summary: &transfer::PushSummary, human_readable: bool) {
    if human_readable {
        println!(
            "{prefix} files={} skipped={} bytes_sent={} ({}) bytes_raw={} ({}) elapsed_ms={} throughput_mbps={:.2}",
            summary.files_transferred,
            summary.files_skipped,
            summary.bytes_sent,
            human_bytes(summary.bytes_sent),
            summary.bytes_raw,
            human_bytes(summary.bytes_raw),
            summary.elapsed.as_millis(),
            summary.megabits_per_sec()
        );
    } else {
        println!(
            "{prefix} files={} skipped={} bytes_sent={} bytes_raw={} elapsed_ms={} throughput_mbps={:.2}",
            summary.files_transferred,
            summary.files_skipped,
            summary.bytes_sent,
            summary.bytes_raw,
            summary.elapsed.as_millis(),
            summary.megabits_per_sec()
        );
    }
}

fn parse_bwlimit_kbps(raw: Option<&str>) -> Result<Option<u64>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("--bwlimit cannot be empty");
    }
    let (digits, multiplier) = if let Some(value) = trimmed.strip_suffix(['k', 'K']) {
        (value.trim(), 1u64)
    } else if let Some(value) = trimmed.strip_suffix(['m', 'M']) {
        (value.trim(), 1024u64)
    } else if let Some(value) = trimmed.strip_suffix(['g', 'G']) {
        (value.trim(), 1024u64 * 1024u64)
    } else {
        (trimmed, 1u64)
    };
    let base = digits
        .parse::<u64>()
        .with_context(|| format!("parse --bwlimit value '{}'", raw))?;
    Ok(Some(base.saturating_mul(multiplier)))
}

fn run_enroll_command(args: EnrollArgs) -> Result<()> {
    let endpoint = endpoint::parse_endpoint(&args.endpoint)
        .with_context(|| format!("parse endpoint '{}'", args.endpoint))?;
    let remote = match endpoint {
        endpoint::Endpoint::Remote(remote) => remote,
        endpoint::Endpoint::Local(_) => bail!("enroll requires a remote endpoint"),
    };

    let profile_name = args
        .profile
        .clone()
        .unwrap_or_else(|| default_profile_name(&remote, args.server_port));
    let bootstrap_remote = resolve_bootstrap_remote(&remote, &profile_name, None)?;
    let install_mode = bootstrap::InstallMode::parse(&args.install)?;
    let client_id = args.client_id.unwrap_or_else(bootstrap::default_client_id);
    let server_name = args.server_name.unwrap_or_else(|| remote.host.clone());
    let bootstrap_options = bootstrap::BootstrapOptions {
        remote: bootstrap_remote.clone(),
        destination: remote.path.clone(),
        server_port: args.server_port,
        server_name,
        client_id,
        profile_name: profile_name.clone(),
        install_mode,
        preserve_metadata: args.preserve_metadata,
    };

    let enrollment = if args.start_server {
        bootstrap::start_remote_server(&bootstrap_options)
            .context("start remote server during enrollment")?
    } else {
        bootstrap::enroll_remote(&bootstrap_options).context("enroll remote endpoint")?
    };
    upsert_profile_from_bootstrap(&profile_name, &bootstrap_remote, &enrollment, &remote.path)?;
    println!(
        "enrolled profile={} server={} destination={} ssh_target={} started_server={}",
        profile_name,
        enrollment.server,
        remote.path,
        bootstrap_remote.ssh_target(),
        args.start_server
    );
    Ok(())
}

fn resolve_server_target(
    target: &str,
    profile_name: Option<&str>,
    destination_override: Option<&str>,
) -> Result<(endpoint::RemoteEndpoint, String, String)> {
    if let Some(name) = profile_name {
        let profile = profile::get_profile(name)
            .with_context(|| format!("load profile '{}' for server command", name))?;
        let ssh_target = profile
            .ssh_target
            .clone()
            .ok_or_else(|| anyhow::anyhow!("profile '{}' has no ssh_target", name))?;
        let destination = destination_override
            .map(ToString::to_string)
            .or(profile.destination.clone())
            .ok_or_else(|| anyhow::anyhow!("profile '{}' has no destination", name))?;
        let remote = parse_ssh_endpoint(&ssh_target, &destination)?;
        return Ok((remote, destination, name.to_string()));
    }

    let parsed = endpoint::parse_endpoint(target)
        .with_context(|| format!("parse server target '{}'", target))?;
    match parsed {
        endpoint::Endpoint::Remote(remote) => {
            let destination = destination_override
                .map(ToString::to_string)
                .or_else(|| {
                    if remote.path == "/" {
                        None
                    } else {
                        Some(remote.path.clone())
                    }
                })
                .ok_or_else(|| anyhow::anyhow!("missing destination (pass --destination)"))?;
            let profile_name = default_profile_name(&remote, 7844);
            let remote = if remote.is_ssh() {
                remote
            } else {
                resolve_bootstrap_remote(&remote, &profile_name, None)?
            };
            Ok((remote, destination, profile_name))
        }
        endpoint::Endpoint::Local(_) => {
            let remote = parse_ssh_endpoint(target, destination_override.unwrap_or("/"))?;
            let destination = destination_override
                .map(ToString::to_string)
                .ok_or_else(|| anyhow::anyhow!("missing destination (pass --destination)"))?;
            let profile_name = default_profile_name(&remote, 7844);
            Ok((remote, destination, profile_name))
        }
    }
}

fn run_server_command(args: ServerArgs) -> Result<()> {
    match args.command {
        ServerCommand::Start(cmd) => {
            let (remote, destination, profile_name) = resolve_server_target(
                &cmd.target,
                cmd.profile.as_deref(),
                cmd.destination.as_deref(),
            )?;
            let install_mode = bootstrap::InstallMode::parse(&cmd.install)?;
            let server_name = cmd.server_name.unwrap_or_else(|| remote.host.clone());
            let client_id = cmd.client_id.unwrap_or_else(bootstrap::default_client_id);
            let options = bootstrap::BootstrapOptions {
                remote: remote.clone(),
                destination: destination.clone(),
                server_port: cmd.server_port,
                server_name,
                client_id,
                profile_name: profile_name.clone(),
                install_mode,
                preserve_metadata: cmd.preserve_metadata,
            };
            let enrollment =
                bootstrap::start_remote_server(&options).context("start remote server")?;
            upsert_profile_from_bootstrap(&profile_name, &remote, &enrollment, &destination)?;
            println!(
                "server started profile={} ssh_target={} bind={} destination={}",
                profile_name,
                remote.ssh_target(),
                enrollment.server,
                destination
            );
            Ok(())
        }
        ServerCommand::Stop(cmd) => {
            let (remote, _, profile_name) =
                resolve_server_target(&cmd.target, cmd.profile.as_deref(), Some("/tmp"))?;
            let stopped_running =
                bootstrap::stop_remote_server(&remote).context("stop remote server")?;
            println!(
                "server stop profile={} ssh_target={} was_running={}",
                profile_name,
                remote.ssh_target(),
                stopped_running
            );
            Ok(())
        }
        ServerCommand::Status(cmd) => {
            let (remote, _, profile_name) =
                resolve_server_target(&cmd.target, cmd.profile.as_deref(), Some("/tmp"))?;
            let status = bootstrap::remote_server_status(&remote).context("query server status")?;
            println!(
                "server status profile={} ssh_target={} running={} pid={}",
                profile_name,
                remote.ssh_target(),
                status.running,
                status
                    .pid
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            );
            Ok(())
        }
    }
}

async fn run_sync_command(handle: RuntimeHandle, args: SyncArgs) -> Result<()> {
    let source = endpoint::parse_endpoint(&args.source)
        .with_context(|| format!("parse source endpoint '{}'", args.source))?;
    let destination = endpoint::parse_endpoint(&args.destination)
        .with_context(|| format!("parse destination endpoint '{}'", args.destination))?;
    let filter = filter::PathFilter::from_patterns(&args.include, &args.exclude)?;
    let preserve_metadata = args.archive || args.preserve_metadata;
    let compression_level = if args.rsync_compress {
        args.compression_level.max(1)
    } else {
        0
    };
    let bwlimit_kbps = parse_bwlimit_kbps(args.bwlimit.as_deref())?;
    if args.verbose {
        println!(
            "sync options transport={} bootstrap={} archive={} compress={} update_only={} delete={} include_patterns={} exclude_patterns={} bwlimit_kbps={}",
            args.transport,
            args.bootstrap,
            args.archive,
            args.rsync_compress,
            args.update_only,
            args.delete,
            args.include.len(),
            args.exclude.len(),
            bwlimit_kbps
                .map(|v| v.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        );
    }

    match (source, destination) {
        (endpoint::Endpoint::Local(source), endpoint::Endpoint::Local(destination)) => {
            let summary = local_copy::copy_tree(local_copy::LocalCopyOptions {
                source,
                destination,
                preserve_metadata,
                dry_run: args.dry_run,
                delete: args.delete,
                update_only: args.update_only,
                filter: filter.clone(),
                bwlimit_kbps,
            })?;
            if args.human_readable {
                println!(
                    "synced-local copied={} skipped={} deleted={} bytes={} ({})",
                    summary.files_copied,
                    summary.files_skipped,
                    summary.files_deleted,
                    summary.bytes_copied,
                    human_bytes(summary.bytes_copied)
                );
            } else {
                println!(
                    "synced-local copied={} skipped={} deleted={} bytes={}",
                    summary.files_copied,
                    summary.files_skipped,
                    summary.files_deleted,
                    summary.bytes_copied
                );
            }
            Ok(())
        }
        (endpoint::Endpoint::Local(source), endpoint::Endpoint::Remote(remote)) => {
            if args.dry_run {
                let (mut manifest, stats) = scan::build_manifest(
                    handle.clone(),
                    &source,
                    scan::ScanOptions {
                        chunk_size: args.chunk_size,
                        scan_workers: args.scan_workers.max(1),
                        hash_workers: args.hash_workers.max(1),
                    },
                )
                .await
                .with_context(|| format!("scan source {}", source.display()))?;
                let before = manifest.files.len();
                manifest
                    .files
                    .retain(|file| filter.allows(&file.relative_path));
                let kept_bytes = manifest.files.iter().map(|v| v.size).sum::<u64>();
                println!(
                    "dry-run source={} files_kept={} files_dropped={} bytes_kept={} enumerate_ms={} hash_ms={} total_ms={}",
                    source.display(),
                    manifest.files.len(),
                    before.saturating_sub(manifest.files.len()),
                    kept_bytes,
                    stats.enumeration_elapsed.as_millis(),
                    stats.hash_elapsed.as_millis(),
                    stats.total_elapsed.as_millis()
                );
                return Ok(());
            }

            let profile_name = args
                .profile
                .clone()
                .unwrap_or_else(|| default_profile_name(&remote, args.server_port));
            let scan_options = scan::ScanOptions {
                chunk_size: args.chunk_size,
                scan_workers: args.scan_workers.max(1),
                hash_workers: args.hash_workers.max(1),
            };
            let delete_remote_target = if args.delete {
                Some(resolve_bootstrap_remote(
                    &remote,
                    &profile_name,
                    args.ssh_target.as_deref(),
                )?)
            } else {
                None
            };
            match parse_transport(&args.transport, Some(&remote))? {
                EffectiveTransport::Ssh => {
                    let bootstrap_remote = resolve_bootstrap_remote(
                        &remote,
                        &profile_name,
                        args.ssh_target.as_deref(),
                    )?;
                    let mut remote_binary_lease = None;
                    let remote_shell_prefix = match args.bootstrap.as_str() {
                        "ssh" => {
                            let install_mode = bootstrap::InstallMode::parse(&args.install)?;
                            let lease = bootstrap::ensure_remote_binary_available(
                                &bootstrap_remote,
                                install_mode,
                            )?;
                            let shell_prefix = lease.shell_prefix().to_string();
                            remote_binary_lease = Some(lease);
                            Some(shell_prefix)
                        }
                        "none" => None,
                        other => {
                            bail!("invalid bootstrap mode '{}' (expected ssh|none)", other);
                        }
                    };
                    let summary = transfer::push_directory_over_ssh_stdio(
                        handle.clone(),
                        transfer::PushOverSshOptions {
                            source: source.clone(),
                            remote: bootstrap_remote.clone(),
                            destination: remote.path.clone(),
                            remote_shell_prefix,
                            scan: scan_options,
                            compression_level,
                            max_stream_payload: args.max_stream_payload,
                            resume: !args.no_resume || args.partial_progress,
                            update_only: args.update_only,
                            cold_start: args.cold_start,
                            manifest_out: args.manifest_out,
                            preserve_metadata,
                            path_filter: Some(filter.clone()),
                            bwlimit_kbps,
                        },
                    )
                    .await?;
                    drop(remote_binary_lease);
                    print_push_summary("synced", &summary, args.human_readable);
                    if args.delete {
                        let deleted = apply_remote_delete_over_ssh(
                            handle.clone(),
                            &bootstrap_remote,
                            &remote.path,
                            &source,
                            scan_options,
                            &filter,
                        )
                        .await?;
                        println!("delete complete removed_files={deleted}");
                    }
                    Ok(())
                }
                EffectiveTransport::Quic => {
                    let (server, server_name, ca, client_cert, client_key, bootstrap_session) =
                        if args.bootstrap == "ssh" {
                            let bootstrap_remote = resolve_bootstrap_remote(
                                &remote,
                                &profile_name,
                                args.ssh_target.as_deref(),
                            )?;
                            let install_mode = bootstrap::InstallMode::parse(&args.install)?;
                            let client_id = bootstrap::default_client_id();
                            let session =
                                bootstrap::bootstrap_remote_push(&bootstrap::BootstrapOptions {
                                    remote: bootstrap_remote.clone(),
                                    destination: remote.path.clone(),
                                    server_port: remote.port.unwrap_or(args.server_port),
                                    server_name: remote.host.clone(),
                                    client_id,
                                    profile_name: profile_name.clone(),
                                    install_mode,
                                    preserve_metadata,
                                })?;
                            upsert_profile_from_bootstrap(
                                &profile_name,
                                &bootstrap_remote,
                                &bootstrap::Enrollment {
                                    server: session.server,
                                    server_name: session.server_name.clone(),
                                    ca: session.ca.clone(),
                                    client_cert: session.client_cert.clone(),
                                    client_key: session.client_key.clone(),
                                },
                                &remote.path,
                            )?;
                            (
                                session.server,
                                session.server_name.clone(),
                                session.ca.clone(),
                                Some(session.client_cert.clone()),
                                Some(session.client_key.clone()),
                                Some(session),
                            )
                        } else if args.bootstrap == "none" {
                            let profile =
                                profile::get_profile(&profile_name).with_context(|| {
                                    format!("load profile '{}' for --bootstrap none", profile_name)
                                })?;
                            (
                                parse_socket_address(&profile.server)?,
                                profile.server_name,
                                profile.ca,
                                profile.client_cert,
                                profile.client_key,
                                None,
                            )
                        } else {
                            bail!(
                                "invalid bootstrap mode '{}' (expected ssh|none)",
                                args.bootstrap
                            );
                        };

                    let summary = transfer::push_directory(
                        handle.clone(),
                        transfer::PushOptions {
                            source: source.clone(),
                            server,
                            server_name,
                            ca,
                            client_cert,
                            client_key,
                            scan: scan_options,
                            parallel_files: args.parallel_files.max(1),
                            connections: args.connections.max(1),
                            compression_level,
                            connect_timeout: Duration::from_millis(args.connect_timeout_ms),
                            operation_timeout: Duration::from_millis(args.op_timeout_ms),
                            max_stream_payload: args.max_stream_payload,
                            resume: !args.no_resume || args.partial_progress,
                            update_only: args.update_only,
                            cold_start: args.cold_start,
                            manifest_out: args.manifest_out,
                            path_filter: Some(filter.clone()),
                            bwlimit_kbps,
                        },
                    )
                    .await?;
                    print_push_summary("synced", &summary, args.human_readable);
                    if let Some(session) = bootstrap_session {
                        session.wait()?;
                    }
                    if let Some(delete_remote) = delete_remote_target.as_ref() {
                        let deleted = apply_remote_delete_over_ssh(
                            handle.clone(),
                            delete_remote,
                            &remote.path,
                            &source,
                            scan_options,
                            &filter,
                        )
                        .await?;
                        println!("delete complete removed_files={deleted}");
                    }
                    Ok(())
                }
                EffectiveTransport::Local => {
                    bail!("invalid transport local for local->remote endpoint")
                }
            }
        }
        (endpoint::Endpoint::Remote(remote), endpoint::Endpoint::Local(destination)) => {
            let profile_name = args
                .profile
                .clone()
                .unwrap_or_else(|| default_profile_name(&remote, args.server_port));
            let bootstrap_remote =
                resolve_bootstrap_remote(&remote, &profile_name, args.ssh_target.as_deref())?;
            let remote_file_list =
                fetch_remote_file_list(&bootstrap_remote, &remote.path, &filter)?;
            let remote_total_bytes = remote_file_list
                .iter()
                .fold(0u64, |acc, (_, size)| acc.saturating_add(*size));
            let remote_keep: HashSet<String> = remote_file_list
                .iter()
                .map(|(path, _)| path.clone())
                .collect();

            if args.dry_run {
                let would_delete = if args.delete {
                    local_copy::prune_destination(&destination, &remote_keep, &filter, true)?
                } else {
                    0usize
                };
                println!(
                    "dry-run remote->local source={} destination={} files_kept={} bytes_kept={} would_delete={}",
                    remote.path,
                    destination.display(),
                    remote_keep.len(),
                    remote_total_bytes,
                    would_delete
                );
                return Ok(());
            }

            let install_mode = bootstrap::InstallMode::parse(&args.install)?;
            let mut remote_binary_lease = None;
            let shell_prefix = match args.bootstrap.as_str() {
                "ssh" => {
                    let lease =
                        bootstrap::ensure_remote_binary_available(&bootstrap_remote, install_mode)?;
                    let shell = lease.shell_prefix().to_string();
                    remote_binary_lease = Some(lease);
                    shell
                }
                "none" => "sparsync".to_string(),
                other => bail!("invalid bootstrap mode '{}' (expected ssh|none)", other),
            };

            let local_port = reserve_local_port()?;
            let remote_forward_port = reserve_local_port()?;

            let mut temp_root = profile::ensure_data_root()?;
            temp_root.push("transient");
            std::fs::create_dir_all(&temp_root)
                .with_context(|| format!("create {}", temp_root.display()))?;
            let unique = format!(
                "pull-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or_default()
            );
            temp_root.push(unique);
            std::fs::create_dir_all(&temp_root)
                .with_context(|| format!("create {}", temp_root.display()))?;
            let cert_path = temp_root.join("server.cert.der");
            let key_path = temp_root.join("server.key.der");
            certs::generate_self_signed(
                &cert_path,
                &key_path,
                &["localhost".to_string(), "127.0.0.1".to_string()],
            )
            .context("generate local reverse-sync certificate")?;

            let mut local_server = start_local_once_server(
                &destination,
                local_port,
                &cert_path,
                &key_path,
                preserve_metadata,
            )?;
            let remote_ca_path =
                bootstrap::upload_temp_file_via_ssh(&bootstrap_remote, &cert_path, "sparsync-ca")?;

            let mut remote_push_cmd = format!(
                "{shell_prefix} push --source {source} --server 127.0.0.1:{remote_port} --server-name localhost --ca {ca} --chunk-size {chunk_size} --parallel-files {parallel} --connections {connections} --scan-workers {scan_workers} --hash-workers {hash_workers} --compression-level {compression}",
                shell_prefix = shell_prefix,
                source = sh_quote(&remote.path),
                remote_port = remote_forward_port,
                ca = sh_quote(&remote_ca_path),
                chunk_size = args.chunk_size,
                parallel = args.parallel_files.max(1),
                connections = args.connections.max(1),
                scan_workers = args.scan_workers.max(1),
                hash_workers = args.hash_workers.max(1),
                compression = compression_level,
            );
            if args.no_resume && !args.partial_progress {
                remote_push_cmd.push_str(" --no-resume");
            }
            if args.update_only {
                remote_push_cmd.push_str(" -u");
            }
            if let Some(limit) = bwlimit_kbps {
                remote_push_cmd.push_str(" --bwlimit ");
                remote_push_cmd.push_str(&limit.to_string());
            }
            for pattern in &args.include {
                remote_push_cmd.push_str(" --include ");
                remote_push_cmd.push_str(&sh_quote(pattern));
            }
            for pattern in &args.exclude {
                remote_push_cmd.push_str(" --exclude ");
                remote_push_cmd.push_str(&sh_quote(pattern));
            }
            if args.cold_start {
                remote_push_cmd.push_str(" --cold-start");
            }

            let push_result = run_remote_push_over_reverse_ssh(
                &bootstrap_remote,
                remote_forward_port,
                local_port,
                &remote_push_cmd,
            );
            let _ = bootstrap::remove_remote_file(&bootstrap_remote, &remote_ca_path);

            match local_server.try_wait() {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    let _ = local_server.kill();
                    let _ = local_server.wait();
                }
            }
            drop(remote_binary_lease);
            push_result?;
            if args.delete {
                let deleted =
                    local_copy::prune_destination(&destination, &remote_keep, &filter, false)?;
                println!("delete complete removed_files={deleted}");
            }
            println!(
                "synced remote->local source={} destination={}",
                remote.path,
                destination.display()
            );
            Ok(())
        }
        (endpoint::Endpoint::Remote(_), endpoint::Endpoint::Remote(_)) => {
            bail!("remote->remote sync is not supported yet")
        }
    }
}
