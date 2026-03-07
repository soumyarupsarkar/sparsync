mod auth;
mod bench;
mod bootstrap;
mod certs;
mod compression;
mod endpoint;
mod filter;
mod local_copy;
mod metadata;
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
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "sparsync",
    about = "Spargio-powered rsync-style transfer tool",
    disable_help_flag = true,
    version
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
    #[command(name = "stream-source", hide = true)]
    StreamSource(StreamSourceArgs),
    #[command(name = "prune-destination", hide = true)]
    PruneDestination(PruneDestinationArgs),
    #[command(name = "service-daemon", hide = true)]
    ServiceDaemon(ServiceDaemonArgs),
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
    preserve_xattrs: bool,

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

    #[arg(long)]
    preserve_xattrs: bool,
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

    #[arg(short = 'c', long = "checksum")]
    checksum: bool,

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

    #[arg(long = "partial")]
    partial: bool,

    #[arg(long = "progress")]
    progress: bool,

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

    #[arg(short = 'c', long = "checksum")]
    checksum: bool,

    #[arg(short = 'X', long = "xattrs")]
    xattrs: bool,

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

#[derive(Debug, Args, Clone)]
struct StreamSourceArgs {
    #[arg(long)]
    source: PathBuf,

    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1))]
    scan_workers: usize,

    #[arg(long, default_value_t = num_cpus::get().max(1) * 2)]
    hash_workers: usize,

    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_stream_payload: usize,

    #[arg(long = "include")]
    include: Vec<String>,

    #[arg(long = "exclude")]
    exclude: Vec<String>,

    #[arg(long)]
    metadata_only: bool,

    #[arg(long)]
    preserve_metadata: bool,

    #[arg(long)]
    preserve_xattrs: bool,
}

#[derive(Debug, Args, Clone)]
struct PruneDestinationArgs {
    #[arg(long)]
    destination: PathBuf,

    #[arg(long)]
    dry_run: bool,

    #[arg(long = "include")]
    include: Vec<String>,

    #[arg(long = "exclude")]
    exclude: Vec<String>,
}

#[derive(Debug, Args, Clone)]
struct ServiceDaemonArgs {
    #[command(subcommand)]
    command: ServiceDaemonCommand,
}

#[derive(Debug, Subcommand, Clone)]
enum ServiceDaemonCommand {
    Start(ServiceDaemonStartArgs),
    Stop(ServiceDaemonStopArgs),
    Status(ServiceDaemonStatusArgs),
}

#[derive(Debug, Args, Clone)]
struct ServiceDaemonStartArgs {
    #[arg(long)]
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

    #[arg(long)]
    preserve_metadata: bool,

    #[arg(long)]
    pid_file: PathBuf,

    #[arg(long)]
    log_file: PathBuf,
}

#[derive(Debug, Args, Clone)]
struct ServiceDaemonStopArgs {
    #[arg(long)]
    pid_file: PathBuf,
}

#[derive(Debug, Args, Clone)]
struct ServiceDaemonStatusArgs {
    #[arg(long)]
    pid_file: PathBuf,
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
        "stream-source",
        "prune-destination",
        "service-daemon",
        "push",
        "sync",
        "help",
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

fn parse_cli_from(mut args: Vec<OsString>) -> Result<Cli> {
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
            if matches!(err.kind(), ErrorKind::UnknownArgument) {
                let rendered = err.to_string();
                if rendered.contains("'-h'") || rendered.contains(" -h ") {
                    return Err(anyhow::anyhow!(
                        "`-h` is reserved for rsync-compatible `sync` human-readable output. Use `--help` for top-level help or `sparsync sync -h` for transfer output formatting."
                    ));
                }
                return Err(anyhow::anyhow!(
                    "{}\nunknown flag. Run `sparsync --help` for supported options.",
                    rendered.trim_end()
                ));
            }
            Err(anyhow::anyhow!(err.to_string()))
        }
    }
}

fn parse_cli() -> Result<Cli> {
    parse_cli_from(std::env::args_os().collect())
}

fn main() -> Result<()> {
    init_tracing();
    let cli = parse_cli()?;
    let builder = spargio::Runtime::builder().shards(cli.shards.max(1));

    let outcome = spargio::__private::block_on(async move {
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
                preserve_xattrs: args.preserve_xattrs,
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
                preserve_xattrs: args.preserve_xattrs,
            };
            stdio_server::run_server_stdio(handle, options).await
        }
        Command::StreamSource(args) => {
            let path_filter = filter::PathFilter::from_patterns(&args.include, &args.exclude)
                .context("compile include/exclude patterns")?;
            transfer::stream_source_to_stdout(
                handle,
                transfer::StreamSourceOptions {
                    source: args.source,
                    scan: scan::ScanOptions {
                        chunk_size: args.chunk_size.max(1),
                        scan_workers: args.scan_workers.max(1),
                        hash_workers: args.hash_workers.max(1),
                    },
                    path_filter: Some(path_filter),
                    chunk_size: args.chunk_size.max(1),
                    max_stream_payload: args.max_stream_payload,
                    metadata_only: args.metadata_only,
                    preserve_metadata: args.preserve_metadata,
                    preserve_xattrs: args.preserve_xattrs,
                },
            )
            .await
        }
        Command::PruneDestination(args) => {
            let path_filter = filter::PathFilter::from_patterns(&args.include, &args.exclude)
                .context("compile include/exclude patterns")?;
            let mut keep_data = String::new();
            std::io::stdin()
                .read_to_string(&mut keep_data)
                .context("read keep-list from stdin")?;
            let keep: HashSet<String> = keep_data
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(ToString::to_string)
                .collect();
            let deleted = local_copy::prune_destination(
                &handle,
                &args.destination,
                &keep,
                &path_filter,
                args.dry_run,
            )
            .await?;
            println!("{deleted}");
            Ok(())
        }
        Command::ServiceDaemon(args) => run_service_daemon_command(args),
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
                checksum: args.checksum,
                cold_start: args.cold_start,
                manifest_out: args.manifest_out,
                preserve_metadata: false,
                preserve_xattrs: false,
                path_filter: Some(path_filter),
                bwlimit_kbps,
                progress: false,
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
        Command::Enroll(args) => {
            run_blocking_task(&handle, "enroll", move || run_enroll_command(args)).await
        }
        Command::Server(args) => {
            run_blocking_task(&handle, "server", move || run_server_command(args)).await
        }
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

async fn run_blocking_task<T, F>(handle: &RuntimeHandle, label: &'static str, job: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let join = handle
        .spawn_blocking(job)
        .map_err(|err| anyhow::anyhow!("spawn blocking task '{label}': {:?}", err))?;
    let inner: Result<T> = join
        .await
        .map_err(|err| anyhow::anyhow!("blocking task '{label}' cancelled: {:?}", err))?;
    inner
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectiveTransport {
    Local,
    Ssh,
    Quic,
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

async fn build_delete_keep_paths(
    handle: RuntimeHandle,
    source_root: &PathBuf,
    scan_options: scan::ScanOptions,
    filter: &filter::PathFilter,
) -> Result<Vec<String>> {
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
    Ok(keep)
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
        preserve_xattrs: args.preserve_metadata,
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

fn resolve_server_management_target(
    target: &str,
    profile_name: Option<&str>,
) -> Result<(endpoint::RemoteEndpoint, String)> {
    if let Some(name) = profile_name {
        let profile = profile::get_profile(name)
            .with_context(|| format!("load profile '{}' for server command", name))?;
        let ssh_target = profile
            .ssh_target
            .clone()
            .ok_or_else(|| anyhow::anyhow!("profile '{}' has no ssh_target", name))?;
        let remote = endpoint::parse_ssh_target(&ssh_target)
            .with_context(|| format!("parse profile ssh_target '{}'", ssh_target))?;
        return Ok((remote, name.to_string()));
    }

    if let Ok(remote) = endpoint::parse_ssh_target(target) {
        let profile_name = default_profile_name(&remote, 7844);
        return Ok((remote, profile_name));
    }

    let parsed = endpoint::parse_endpoint(target)
        .with_context(|| format!("parse server management target '{}'", target))?;
    let remote = match parsed {
        endpoint::Endpoint::Remote(remote) if remote.is_ssh() => endpoint::RemoteEndpoint {
            path: String::new(),
            ..remote
        },
        endpoint::Endpoint::Remote(remote) => {
            let profile_name = default_profile_name(&remote, 7844);
            endpoint::RemoteEndpoint {
                path: String::new(),
                ..resolve_bootstrap_remote(&remote, &profile_name, None)?
            }
        }
        endpoint::Endpoint::Local(_) => {
            bail!("server management target must be an ssh host or remote endpoint")
        }
    };
    let profile_name = default_profile_name(&remote, 7844);
    Ok((remote, profile_name))
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
                preserve_xattrs: cmd.preserve_metadata,
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
            let (remote, profile_name) =
                resolve_server_management_target(&cmd.target, cmd.profile.as_deref())?;
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
            let (remote, profile_name) =
                resolve_server_management_target(&cmd.target, cmd.profile.as_deref())?;
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

fn run_service_daemon_command(args: ServiceDaemonArgs) -> Result<()> {
    match args.command {
        ServiceDaemonCommand::Start(cmd) => {
            if let Some(parent) = cmd.pid_file.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            if let Some(parent) = cmd.log_file.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            if let Some(existing_pid) = read_pid_file(&cmd.pid_file)? {
                if process_is_running(existing_pid) {
                    println!("running {existing_pid}");
                    return Ok(());
                }
            }

            let exe = std::env::current_exe().context("resolve current executable")?;
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&cmd.log_file)
                .with_context(|| format!("open log file {}", cmd.log_file.display()))?;
            let err_log = log
                .try_clone()
                .with_context(|| format!("clone log fd {}", cmd.log_file.display()))?;
            let mut child_cmd = std::process::Command::new(exe);
            child_cmd
                .arg("serve")
                .arg("--bind")
                .arg(cmd.bind.to_string())
                .arg("--destination")
                .arg(&cmd.destination)
                .arg("--cert")
                .arg(&cmd.cert)
                .arg("--key")
                .arg(&cmd.key);
            if let Some(client_ca) = cmd.client_ca.as_ref() {
                child_cmd.arg("--client-ca").arg(client_ca);
            }
            if let Some(authz) = cmd.authz.as_ref() {
                child_cmd.arg("--authz").arg(authz);
            }
            if cmd.preserve_metadata {
                child_cmd.arg("--preserve-metadata");
                child_cmd.arg("--preserve-xattrs");
            }
            child_cmd
                .stdin(Stdio::null())
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(err_log));

            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                unsafe {
                    child_cmd.pre_exec(|| {
                        if libc::setsid() < 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }

            let mut child = child_cmd
                .spawn()
                .context("spawn background serve process")?;
            std::thread::sleep(Duration::from_millis(100));
            if let Some(status) = child
                .try_wait()
                .context("poll background serve process after spawn")?
            {
                bail!(
                    "background serve process exited early with status {}",
                    status
                );
            }
            let pid = child.id();
            std::fs::write(&cmd.pid_file, format!("{pid}\n"))
                .with_context(|| format!("write pid file {}", cmd.pid_file.display()))?;
            println!("started {pid}");
            Ok(())
        }
        ServiceDaemonCommand::Stop(cmd) => {
            let Some(pid) = read_pid_file(&cmd.pid_file)? else {
                let _ = std::fs::remove_file(&cmd.pid_file);
                println!("stopped_not_running");
                return Ok(());
            };
            if !process_is_running(pid) {
                let _ = std::fs::remove_file(&cmd.pid_file);
                println!("stopped_not_running");
                return Ok(());
            }

            send_signal(pid, libc::SIGTERM);
            let mut stopped = false;
            for _ in 0..40 {
                if !process_is_running(pid) {
                    stopped = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if !stopped && process_is_running(pid) {
                send_signal(pid, libc::SIGKILL);
                for _ in 0..20 {
                    if !process_is_running(pid) {
                        stopped = true;
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }

            let _ = std::fs::remove_file(&cmd.pid_file);
            if stopped || !process_is_running(pid) {
                println!("stopped_running");
            } else {
                println!("stopped_not_running");
            }
            Ok(())
        }
        ServiceDaemonCommand::Status(cmd) => {
            if let Some(pid) = read_pid_file(&cmd.pid_file)? {
                if process_is_running(pid) {
                    println!("running {pid}");
                    return Ok(());
                }
            }
            println!("stopped");
            Ok(())
        }
    }
}

fn read_pid_file(path: &PathBuf) -> Result<Option<i32>> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let pid = trimmed
        .parse::<i32>()
        .with_context(|| format!("parse pid from {}", path.display()))?;
    Ok(Some(pid))
}

fn process_is_running(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    let rc = unsafe { libc::kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn send_signal(pid: i32, signal: i32) {
    if pid > 0 {
        unsafe {
            let _ = libc::kill(pid, signal);
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
    let preserve_xattrs = args.xattrs || args.preserve_metadata;
    let compression_level = if args.rsync_compress {
        args.compression_level.max(1)
    } else {
        0
    };
    let bwlimit_kbps = parse_bwlimit_kbps(args.bwlimit.as_deref())?;
    let partial_enabled = args.partial_progress || args.partial;
    let progress_enabled = args.partial_progress || args.progress;
    if args.verbose {
        println!(
            "sync options transport={} bootstrap={} archive={} compress={} checksum={} update_only={} delete={} include_patterns={} exclude_patterns={} bwlimit_kbps={}",
            args.transport,
            args.bootstrap,
            args.archive,
            args.rsync_compress,
            args.checksum,
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
            let summary = local_copy::copy_tree(
                handle.clone(),
                local_copy::LocalCopyOptions {
                    source,
                    destination,
                    preserve_metadata,
                    preserve_xattrs,
                    dry_run: args.dry_run,
                    delete: args.delete,
                    update_only: args.update_only,
                    filter: filter.clone(),
                    bwlimit_kbps,
                    progress: args.partial_progress || args.progress,
                },
            )
            .await?;
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
            let profile_name = args
                .profile
                .clone()
                .unwrap_or_else(|| default_profile_name(&remote, args.server_port));
            let scan_options = scan::ScanOptions {
                chunk_size: args.chunk_size,
                scan_workers: args.scan_workers.max(1),
                hash_workers: args.hash_workers.max(1),
            };
            let effective_transport = parse_transport(&args.transport, Some(&remote))?;
            if args.dry_run {
                let (mut manifest, stats) =
                    scan::build_manifest(handle.clone(), &source, scan_options)
                        .await
                        .with_context(|| format!("scan source {}", source.display()))?;
                let before = manifest.files.len();
                manifest
                    .files
                    .retain(|file| filter.allows(&file.relative_path));
                let kept_bytes = manifest.files.iter().map(|v| v.size).sum::<u64>();
                let keep_paths: Vec<String> = manifest
                    .files
                    .iter()
                    .map(|file| file.relative_path.clone())
                    .collect();
                let mut would_delete = 0u64;
                if args.delete {
                    match effective_transport {
                        EffectiveTransport::Ssh => {
                            let bootstrap_remote = resolve_bootstrap_remote(
                                &remote,
                                &profile_name,
                                args.ssh_target.as_deref(),
                            )?;
                            let mut remote_binary_lease = None;
                            let remote_shell_prefix = match args.bootstrap.as_str() {
                                "ssh" => {
                                    let install_mode =
                                        bootstrap::InstallMode::parse(&args.install)?;
                                    let lease =
                                        run_blocking_task(&handle, "ensure-remote-binary", {
                                            let bootstrap_remote = bootstrap_remote.clone();
                                            move || {
                                                bootstrap::ensure_remote_binary_available(
                                                    &bootstrap_remote,
                                                    install_mode,
                                                )
                                            }
                                        })
                                        .await?;
                                    let shell_prefix = lease.shell_prefix().to_string();
                                    remote_binary_lease = Some(lease);
                                    Some(shell_prefix)
                                }
                                "none" => None,
                                other => {
                                    bail!("invalid bootstrap mode '{}' (expected ssh|none)", other);
                                }
                            };
                            would_delete = transfer::apply_delete_plan_over_ssh_stdio(
                                handle.clone(),
                                transfer::DeletePlanOverSshOptions {
                                    remote: bootstrap_remote,
                                    destination: remote.path.clone(),
                                    remote_shell_prefix,
                                    max_stream_payload: args.max_stream_payload,
                                    include_patterns: args.include.clone(),
                                    exclude_patterns: args.exclude.clone(),
                                    keep_paths: keep_paths.clone(),
                                    dry_run: true,
                                },
                            )
                            .await?;
                            drop(remote_binary_lease);
                        }
                        EffectiveTransport::Quic => {
                            if args.bootstrap == "ssh" {
                                let bootstrap_remote = resolve_bootstrap_remote(
                                    &remote,
                                    &profile_name,
                                    args.ssh_target.as_deref(),
                                )?;
                                let install_mode = bootstrap::InstallMode::parse(&args.install)?;
                                let client_id = bootstrap::default_client_id();
                                let session =
                                    run_blocking_task(&handle, "bootstrap-remote-push", {
                                        let options = bootstrap::BootstrapOptions {
                                            remote: bootstrap_remote.clone(),
                                            destination: remote.path.clone(),
                                            server_port: remote.port.unwrap_or(args.server_port),
                                            server_name: remote.host.clone(),
                                            client_id,
                                            profile_name: profile_name.clone(),
                                            install_mode,
                                            preserve_metadata,
                                            preserve_xattrs,
                                        };
                                        move || bootstrap::bootstrap_remote_push(&options)
                                    })
                                    .await?;
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
                                would_delete = transfer::apply_delete_plan_over_quic(
                                    handle.clone(),
                                    transfer::DeletePlanOverQuicOptions {
                                        server: session.server,
                                        server_name: session.server_name.clone(),
                                        ca: session.ca.clone(),
                                        client_cert: Some(session.client_cert.clone()),
                                        client_key: Some(session.client_key.clone()),
                                        max_stream_payload: args.max_stream_payload,
                                        connect_timeout: Duration::from_millis(
                                            args.connect_timeout_ms,
                                        ),
                                        operation_timeout: Duration::from_millis(
                                            args.op_timeout_ms,
                                        ),
                                        include_patterns: args.include.clone(),
                                        exclude_patterns: args.exclude.clone(),
                                        keep_paths: keep_paths.clone(),
                                        dry_run: true,
                                    },
                                )
                                .await?;
                                run_blocking_task(&handle, "bootstrap-session-wait", move || {
                                    session.wait()
                                })
                                .await?;
                            } else if args.bootstrap == "none" {
                                let profile =
                                    profile::get_profile(&profile_name).with_context(|| {
                                        format!(
                                            "load profile '{}' for --bootstrap none",
                                            profile_name
                                        )
                                    })?;
                                would_delete = transfer::apply_delete_plan_over_quic(
                                    handle.clone(),
                                    transfer::DeletePlanOverQuicOptions {
                                        server: parse_socket_address(&profile.server)?,
                                        server_name: profile.server_name,
                                        ca: profile.ca,
                                        client_cert: profile.client_cert,
                                        client_key: profile.client_key,
                                        max_stream_payload: args.max_stream_payload,
                                        connect_timeout: Duration::from_millis(
                                            args.connect_timeout_ms,
                                        ),
                                        operation_timeout: Duration::from_millis(
                                            args.op_timeout_ms,
                                        ),
                                        include_patterns: args.include.clone(),
                                        exclude_patterns: args.exclude.clone(),
                                        keep_paths: keep_paths.clone(),
                                        dry_run: true,
                                    },
                                )
                                .await?;
                            } else {
                                bail!(
                                    "invalid bootstrap mode '{}' (expected ssh|none)",
                                    args.bootstrap
                                );
                            }
                        }
                        EffectiveTransport::Local => {
                            bail!("invalid transport local for local->remote endpoint");
                        }
                    }
                }
                println!(
                    "dry-run source={} files_kept={} files_dropped={} bytes_kept={} would_delete={} enumerate_ms={} hash_ms={} total_ms={}",
                    source.display(),
                    manifest.files.len(),
                    before.saturating_sub(manifest.files.len()),
                    kept_bytes,
                    would_delete,
                    stats.enumeration_elapsed.as_millis(),
                    stats.hash_elapsed.as_millis(),
                    stats.total_elapsed.as_millis()
                );
                return Ok(());
            }
            match effective_transport {
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
                            let lease = run_blocking_task(&handle, "ensure-remote-binary", {
                                let bootstrap_remote = bootstrap_remote.clone();
                                move || {
                                    bootstrap::ensure_remote_binary_available(
                                        &bootstrap_remote,
                                        install_mode,
                                    )
                                }
                            })
                            .await?;
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
                            remote_shell_prefix: remote_shell_prefix.clone(),
                            scan: scan_options,
                            compression_level,
                            max_stream_payload: args.max_stream_payload,
                            resume: !args.no_resume || partial_enabled,
                            update_only: args.update_only,
                            checksum: args.checksum,
                            cold_start: args.cold_start,
                            manifest_out: args.manifest_out,
                            preserve_metadata,
                            preserve_xattrs,
                            path_filter: Some(filter.clone()),
                            bwlimit_kbps,
                            progress: progress_enabled,
                        },
                    )
                    .await?;
                    print_push_summary("synced", &summary, args.human_readable);
                    if args.delete {
                        let keep_paths =
                            build_delete_keep_paths(handle.clone(), &source, scan_options, &filter)
                                .await?;
                        let deleted = transfer::apply_delete_plan_over_ssh_stdio(
                            handle.clone(),
                            transfer::DeletePlanOverSshOptions {
                                remote: bootstrap_remote.clone(),
                                destination: remote.path.clone(),
                                remote_shell_prefix: remote_shell_prefix.clone(),
                                max_stream_payload: args.max_stream_payload,
                                include_patterns: args.include.clone(),
                                exclude_patterns: args.exclude.clone(),
                                keep_paths,
                                dry_run: false,
                            },
                        )
                        .await?;
                        println!("delete complete removed_files={deleted}");
                    }
                    drop(remote_binary_lease);
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
                            let session = run_blocking_task(&handle, "bootstrap-remote-push", {
                                let options = bootstrap::BootstrapOptions {
                                    remote: bootstrap_remote.clone(),
                                    destination: remote.path.clone(),
                                    server_port: remote.port.unwrap_or(args.server_port),
                                    server_name: remote.host.clone(),
                                    client_id,
                                    profile_name: profile_name.clone(),
                                    install_mode,
                                    preserve_metadata,
                                    preserve_xattrs,
                                };
                                move || bootstrap::bootstrap_remote_push(&options)
                            })
                            .await?;
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
                    let delete_ca = ca.clone();
                    let delete_client_cert = client_cert.clone();
                    let delete_client_key = client_key.clone();
                    let delete_server_name = server_name.clone();

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
                            resume: !args.no_resume || partial_enabled,
                            update_only: args.update_only,
                            checksum: args.checksum,
                            cold_start: args.cold_start,
                            manifest_out: args.manifest_out,
                            preserve_metadata,
                            preserve_xattrs,
                            path_filter: Some(filter.clone()),
                            bwlimit_kbps,
                            progress: progress_enabled,
                        },
                    )
                    .await?;
                    print_push_summary("synced", &summary, args.human_readable);
                    if args.delete {
                        let keep_paths =
                            build_delete_keep_paths(handle.clone(), &source, scan_options, &filter)
                                .await?;
                        let deleted = transfer::apply_delete_plan_over_quic(
                            handle.clone(),
                            transfer::DeletePlanOverQuicOptions {
                                server,
                                server_name: delete_server_name,
                                ca: delete_ca,
                                client_cert: delete_client_cert,
                                client_key: delete_client_key,
                                max_stream_payload: args.max_stream_payload,
                                connect_timeout: Duration::from_millis(args.connect_timeout_ms),
                                operation_timeout: Duration::from_millis(args.op_timeout_ms),
                                include_patterns: args.include.clone(),
                                exclude_patterns: args.exclude.clone(),
                                keep_paths,
                                dry_run: false,
                            },
                        )
                        .await?;
                        println!("delete complete removed_files={deleted}");
                    }
                    if let Some(session) = bootstrap_session {
                        run_blocking_task(&handle, "bootstrap-session-wait", move || {
                            session.wait()
                        })
                        .await?;
                    }
                    Ok(())
                }
                EffectiveTransport::Local => {
                    bail!("invalid transport local for local->remote endpoint")
                }
            }
        }
        (endpoint::Endpoint::Remote(remote), endpoint::Endpoint::Local(destination)) => {
            let effective_transport = parse_transport(&args.transport, Some(&remote))?;
            if matches!(effective_transport, EffectiveTransport::Local) {
                bail!("invalid transport local for remote->local endpoint");
            }
            let profile_name = args
                .profile
                .clone()
                .unwrap_or_else(|| default_profile_name(&remote, args.server_port));
            let pull = match effective_transport {
                EffectiveTransport::Ssh => {
                    let bootstrap_remote = resolve_bootstrap_remote(
                        &remote,
                        &profile_name,
                        args.ssh_target.as_deref(),
                    )?;
                    let install_mode = bootstrap::InstallMode::parse(&args.install)?;
                    let mut remote_binary_lease = None;
                    let remote_shell_prefix = match args.bootstrap.as_str() {
                        "ssh" => {
                            let lease = run_blocking_task(&handle, "ensure-remote-binary", {
                                let bootstrap_remote = bootstrap_remote.clone();
                                move || {
                                    bootstrap::ensure_remote_binary_available(
                                        &bootstrap_remote,
                                        install_mode,
                                    )
                                }
                            })
                            .await?;
                            let shell = lease.shell_prefix().to_string();
                            remote_binary_lease = Some(lease);
                            Some(shell)
                        }
                        "none" => None,
                        other => bail!("invalid bootstrap mode '{}' (expected ssh|none)", other),
                    };
                    let pull = transfer::pull_directory_over_ssh_stream(
                        handle.clone(),
                        transfer::PullOverSshStreamOptions {
                            remote: bootstrap_remote.clone(),
                            source: remote.path.clone(),
                            destination: destination.clone(),
                            remote_shell_prefix: remote_shell_prefix.clone(),
                            scan: scan::ScanOptions {
                                chunk_size: args.chunk_size.max(1),
                                scan_workers: args.scan_workers.max(1),
                                hash_workers: args.hash_workers.max(1),
                            },
                            path_filter: filter.clone(),
                            update_only: args.update_only,
                            preserve_metadata,
                            preserve_xattrs,
                            delete: args.delete,
                            dry_run: args.dry_run,
                            progress: progress_enabled,
                            chunk_size: args.chunk_size.max(1),
                            max_stream_payload: args.max_stream_payload,
                            metadata_only: args.dry_run,
                            bwlimit_kbps,
                            include_patterns: args.include.clone(),
                            exclude_patterns: args.exclude.clone(),
                        },
                    )
                    .await?;
                    drop(remote_binary_lease);
                    pull
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
                            let session = run_blocking_task(&handle, "bootstrap-remote-pull", {
                                let options = bootstrap::BootstrapOptions {
                                    remote: bootstrap_remote.clone(),
                                    destination: remote.path.clone(),
                                    server_port: remote.port.unwrap_or(args.server_port),
                                    server_name: remote.host.clone(),
                                    client_id,
                                    profile_name: profile_name.clone(),
                                    install_mode,
                                    preserve_metadata,
                                    preserve_xattrs,
                                };
                                move || bootstrap::bootstrap_remote_push(&options)
                            })
                            .await?;
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
                    let pull = transfer::pull_directory_over_quic(
                        handle.clone(),
                        transfer::PullOverQuicOptions {
                            server,
                            server_name,
                            ca,
                            client_cert,
                            client_key,
                            destination: destination.clone(),
                            path_filter: filter.clone(),
                            update_only: args.update_only,
                            preserve_metadata,
                            preserve_xattrs,
                            delete: args.delete,
                            dry_run: args.dry_run,
                            progress: progress_enabled,
                            chunk_size: args.chunk_size.max(1),
                            max_stream_payload: args.max_stream_payload,
                            metadata_only: args.dry_run,
                            bwlimit_kbps,
                            include_patterns: args.include.clone(),
                            exclude_patterns: args.exclude.clone(),
                            connect_timeout: Duration::from_millis(args.connect_timeout_ms),
                            operation_timeout: Duration::from_millis(args.op_timeout_ms),
                        },
                    )
                    .await?;
                    if let Some(session) = bootstrap_session {
                        run_blocking_task(&handle, "bootstrap-session-wait", move || {
                            session.wait()
                        })
                        .await?;
                    }
                    pull
                }
                EffectiveTransport::Local => {
                    bail!("invalid transport local for remote->local endpoint")
                }
            };

            if args.dry_run {
                println!(
                    "dry-run remote->local source={} destination={} files_copy={} files_skip={} bytes_copy={} would_delete={}",
                    remote.path,
                    destination.display(),
                    pull.files_copied,
                    pull.files_skipped,
                    pull.bytes_received,
                    pull.files_deleted
                );
                return Ok(());
            }

            println!(
                "synced remote->local source={} destination={} copied={} skipped={} deleted={} bytes={} elapsed_ms={}",
                remote.path,
                destination.display(),
                pull.files_copied,
                pull.files_skipped,
                pull.files_deleted,
                pull.bytes_received,
                pull.elapsed.as_millis()
            );
            Ok(())
        }
        (endpoint::Endpoint::Remote(_), endpoint::Endpoint::Remote(_)) => {
            bail!("remote->remote sync is not supported yet")
        }
    }
}

#[cfg(test)]
mod cli_tests {
    use super::{
        EffectiveTransport, endpoint, parse_cli_from, parse_transport,
        should_insert_sync_subcommand,
    };
    use std::ffi::OsString;

    fn args(items: &[&str]) -> Vec<OsString> {
        items.iter().map(|item| OsString::from(*item)).collect()
    }

    #[test]
    fn help_subcommand_is_not_rewritten() {
        assert!(!should_insert_sync_subcommand(&args(&[
            "sparsync", "help", "sync",
        ])));
    }

    #[test]
    fn top_level_help_flag_is_not_rewritten() {
        assert!(!should_insert_sync_subcommand(&args(&[
            "sparsync", "--help"
        ])));
    }

    #[test]
    fn rsync_style_invocation_is_rewritten() {
        assert!(should_insert_sync_subcommand(&args(&[
            "sparsync",
            "-av",
            "/src",
            "user@host:/dst",
        ])));
    }

    #[test]
    fn unknown_flag_error_includes_help_hint() {
        let err = parse_cli_from(args(&["sparsync", "--unknown-flag"])).expect_err("must fail");
        let text = err.to_string();
        assert!(text.contains("unknown flag"));
        assert!(text.contains("sparsync --help"));
    }

    #[test]
    fn top_level_short_help_has_compat_hint() {
        let err = parse_cli_from(args(&["sparsync", "-h"])).expect_err("must fail");
        assert!(err.to_string().contains("`-h` is reserved"));
    }

    #[test]
    fn auto_transport_chooses_from_endpoint_kind() {
        let ssh = endpoint::RemoteEndpoint {
            user: None,
            host: "example.com".to_string(),
            port: None,
            path: "/tmp".to_string(),
            kind: endpoint::RemoteKind::Ssh,
        };
        let quic = endpoint::RemoteEndpoint {
            kind: endpoint::RemoteKind::Quic,
            ..ssh.clone()
        };

        assert_eq!(
            parse_transport("auto", Some(&ssh)).expect("ssh transport"),
            EffectiveTransport::Ssh
        );
        assert_eq!(
            parse_transport("auto", Some(&quic)).expect("quic transport"),
            EffectiveTransport::Quic
        );
        assert_eq!(
            parse_transport("auto", None).expect("local transport"),
            EffectiveTransport::Local
        );
    }
}
