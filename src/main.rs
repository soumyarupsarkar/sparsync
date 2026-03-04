mod bench;
mod certs;
mod compression;
mod model;
mod protocol;
mod scan;
mod server;
mod state;
mod transfer;
mod util;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use spargio::RuntimeHandle;
use std::net::SocketAddr;
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
    Push(PushArgs),
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
    server: SocketAddr,

    #[arg(long, default_value = "localhost")]
    server_name: String,

    #[arg(long)]
    ca: PathBuf,

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
                max_stream_payload: args.max_stream_payload,
                preserve_metadata: args.preserve_metadata,
            };
            server::run_server(handle, options).await
        }
        Command::Push(args) => {
            let scan_options = scan::ScanOptions {
                chunk_size: args.chunk_size,
                scan_workers: args.scan_workers.max(1),
                hash_workers: args.hash_workers.max(1),
            };
            let options = transfer::PushOptions {
                source: args.source,
                server: args.server,
                server_name: args.server_name,
                ca: args.ca,
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
