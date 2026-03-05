mod auth;
mod bench;
mod bootstrap;
mod certs;
mod compression;
mod endpoint;
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
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "sparsync", about = "Spargio-powered rsync-style transfer tool")]
struct Cli {
    #[arg(long, default_value_t = num_cpus::get().max(1))]
    shards: usize,

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

    #[arg(long)]
    cold_start: bool,

    #[arg(long)]
    manifest_out: Option<PathBuf>,
}

#[derive(Debug, Args, Clone)]
struct SyncArgs {
    source: String,
    destination: String,

    #[arg(long)]
    profile: Option<String>,

    #[arg(long, default_value = "ssh")]
    bootstrap: String,

    #[arg(long, default_value = "quic")]
    transport: String,

    #[arg(long, default_value = "ephemeral")]
    install: String,

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

    #[arg(long)]
    cold_start: bool,

    #[arg(long)]
    manifest_out: Option<PathBuf>,

    #[arg(long)]
    preserve_metadata: bool,
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

fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
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
                cold_start: args.cold_start,
                manifest_out: args.manifest_out,
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
        Command::Sync(args) => {
            let source = endpoint::parse_endpoint(&args.source)
                .with_context(|| format!("parse source endpoint '{}'", args.source))?;
            let destination = endpoint::parse_endpoint(&args.destination)
                .with_context(|| format!("parse destination endpoint '{}'", args.destination))?;

            let (source_path, remote) = match (source, destination) {
                (endpoint::Endpoint::Local(path), endpoint::Endpoint::Remote(remote)) => {
                    (path, remote)
                }
                _ => bail!(
                    "sync currently supports local->remote only (example: /src user@host:/dest)"
                ),
            };

            let profile_name = args
                .profile
                .clone()
                .unwrap_or_else(|| format!("{}-{}", remote.host, args.server_port));
            let scan_options = scan::ScanOptions {
                chunk_size: args.chunk_size,
                scan_workers: args.scan_workers.max(1),
                hash_workers: args.hash_workers.max(1),
            };

            match args.transport.as_str() {
                "quic" => {
                    let (server, server_name, ca, client_cert, client_key, bootstrap_session) =
                        if args.bootstrap == "ssh" {
                            let install_mode = bootstrap::InstallMode::parse(&args.install)?;
                            let client_id = bootstrap::default_client_id();
                            let session =
                                bootstrap::bootstrap_remote_push(&bootstrap::BootstrapOptions {
                                    remote: remote.clone(),
                                    destination: remote.path.clone(),
                                    server_port: args.server_port,
                                    server_name: remote.host.clone(),
                                    client_id,
                                    profile_name: profile_name.clone(),
                                    install_mode,
                                    preserve_metadata: args.preserve_metadata,
                                })?;

                            profile::upsert_profile(profile::SyncProfile {
                                name: profile_name.clone(),
                                server: session.server.to_string(),
                                server_name: session.server_name.clone(),
                                ca: session.ca.clone(),
                                client_cert: Some(session.client_cert.clone()),
                                client_key: Some(session.client_key.clone()),
                                ssh_target: Some(remote.ssh_target()),
                                destination: Some(remote.path.clone()),
                            })?;

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
                            let server = profile
                                .server
                                .parse::<SocketAddr>()
                                .or_else(|_| {
                                    profile
                                        .server
                                        .to_socket_addrs()
                                        .map(|mut iter| iter.next())
                                        .map_err(|err| {
                                            std::io::Error::new(
                                                std::io::ErrorKind::InvalidInput,
                                                err,
                                            )
                                        })
                                        .and_then(|addr| {
                                            addr.ok_or_else(|| {
                                                std::io::Error::new(
                                                    std::io::ErrorKind::InvalidInput,
                                                    "no socket address resolved for profile server",
                                                )
                                            })
                                        })
                                })
                                .with_context(|| {
                                    format!("parse profile server '{}'", profile.server)
                                })?;
                            (
                                server,
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
                        handle,
                        transfer::PushOptions {
                            source: source_path,
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
                            cold_start: args.cold_start,
                            manifest_out: args.manifest_out,
                        },
                    )
                    .await?;

                    println!(
                        "synced files={} skipped={} bytes_sent={} bytes_raw={} elapsed_ms={} throughput_mbps={:.2}",
                        summary.files_transferred,
                        summary.files_skipped,
                        summary.bytes_sent,
                        summary.bytes_raw,
                        summary.elapsed.as_millis(),
                        summary.megabits_per_sec()
                    );

                    if let Some(session) = bootstrap_session {
                        session.wait()?;
                    }
                    Ok(())
                }
                "ssh" => {
                    let mut remote_binary_lease = None;
                    let remote_shell_prefix = match args.bootstrap.as_str() {
                        "ssh" => {
                            let install_mode = bootstrap::InstallMode::parse(&args.install)?;
                            let lease =
                                bootstrap::ensure_remote_binary_available(&remote, install_mode)?;
                            let shell_prefix = lease.shell_prefix().to_string();
                            remote_binary_lease = Some(lease);
                            Some(shell_prefix)
                        }
                        "none" => None,
                        other => {
                            bail!("invalid bootstrap mode '{}' (expected ssh|none)", other);
                        }
                    };

                    let remote_destination = remote.path.clone();
                    let summary = transfer::push_directory_over_ssh_stdio(
                        handle,
                        transfer::PushOverSshOptions {
                            source: source_path,
                            remote,
                            destination: remote_destination,
                            remote_shell_prefix,
                            scan: scan_options,
                            compression_level: args.compression_level,
                            max_stream_payload: args.max_stream_payload,
                            resume: !args.no_resume,
                            cold_start: args.cold_start,
                            manifest_out: args.manifest_out,
                            preserve_metadata: args.preserve_metadata,
                        },
                    )
                    .await?;

                    println!(
                        "synced files={} skipped={} bytes_sent={} bytes_raw={} elapsed_ms={} throughput_mbps={:.2}",
                        summary.files_transferred,
                        summary.files_skipped,
                        summary.bytes_sent,
                        summary.bytes_raw,
                        summary.elapsed.as_millis(),
                        summary.megabits_per_sec()
                    );
                    drop(remote_binary_lease);
                    Ok(())
                }
                other => bail!("invalid transport '{}' (expected quic|ssh)", other),
            }
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
