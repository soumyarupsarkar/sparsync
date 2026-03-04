use crate::util::{join_error, runtime_error};
use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use spargio::{RuntimeHandle, fs};
use std::cmp::min;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct BenchmarkOptions {
    pub files: usize,
    pub tasks: usize,
    pub io_ops: usize,
    pub in_flight: usize,
    pub io_size: usize,
    pub rsync_command: Option<String>,
    pub syncthing_command: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BenchmarkReport {
    pub scan_items_per_sec: f64,
    pub tasks_per_sec: f64,
    pub io_ops_per_sec: f64,
    pub rsync_elapsed_ms: Option<u128>,
    pub syncthing_elapsed_ms: Option<u128>,
}

pub async fn run_benchmark(
    handle: RuntimeHandle,
    options: BenchmarkOptions,
) -> Result<BenchmarkReport> {
    let scan_elapsed = benchmark_scan_like(options.files);
    let scan_items_per_sec = rate(options.files, scan_elapsed);

    let (task_elapsed, task_digest) =
        benchmark_tasks(handle.clone(), options.tasks, options.in_flight).await?;
    let tasks_per_sec = rate(options.tasks, task_elapsed);

    let (io_elapsed, io_digest) = benchmark_io(
        handle.clone(),
        options.io_ops,
        options.in_flight,
        options.io_size,
    )
    .await?;
    let io_ops_per_sec = rate(options.io_ops, io_elapsed);

    println!(
        "bench details scan_elapsed_ms={} task_elapsed_ms={} io_elapsed_ms={} task_digest={} io_digest={}",
        scan_elapsed.as_millis(),
        task_elapsed.as_millis(),
        io_elapsed.as_millis(),
        task_digest,
        io_digest,
    );

    let rsync_elapsed_ms = if let Some(cmd) = options.rsync_command {
        Some(
            run_external_command(handle.clone(), "rsync", cmd)
                .await?
                .as_millis(),
        )
    } else {
        None
    };

    let syncthing_elapsed_ms = if let Some(cmd) = options.syncthing_command {
        Some(
            run_external_command(handle.clone(), "syncthing", cmd)
                .await?
                .as_millis(),
        )
    } else {
        None
    };

    Ok(BenchmarkReport {
        scan_items_per_sec,
        tasks_per_sec,
        io_ops_per_sec,
        rsync_elapsed_ms,
        syncthing_elapsed_ms,
    })
}

fn benchmark_scan_like(files: usize) -> Duration {
    let started = Instant::now();
    let mut checksum = 0u64;

    for idx in 0..files {
        let pseudo_path = format!("root/{:04}/group/{:08}.bin", idx % 10_000, idx);
        let hash = blake3::hash(pseudo_path.as_bytes());
        checksum ^= hash.as_bytes()[0] as u64;
    }

    println!("bench scan checksum={checksum}");
    started.elapsed()
}

async fn benchmark_tasks(
    handle: RuntimeHandle,
    tasks: usize,
    in_flight: usize,
) -> Result<(Duration, u64)> {
    if tasks == 0 {
        return Ok((Duration::ZERO, 0));
    }

    let started = Instant::now();
    let mut next_task = 0usize;
    let mut digest = 0u64;
    let mut running = FuturesUnordered::new();

    let initial = min(tasks, in_flight.max(1));
    for _ in 0..initial {
        running.push(spawn_task_job(handle.clone(), next_task)?);
        next_task += 1;
    }

    while let Some(joined) = running.next().await {
        let value = joined.map_err(|err| join_error("task bench worker canceled", err))??;
        digest ^= value;

        if next_task < tasks {
            running.push(spawn_task_job(handle.clone(), next_task)?);
            next_task += 1;
        }
    }

    Ok((started.elapsed(), digest))
}

fn spawn_task_job(handle: RuntimeHandle, index: usize) -> Result<spargio::JoinHandle<Result<u64>>> {
    handle
        .spawn_stealable(async move {
            let mut data = [0u8; 1024];
            for (idx, byte) in data.iter_mut().enumerate() {
                *byte = ((index as u64 + idx as u64) % 255) as u8;
            }
            let hash = blake3::hash(&data);
            Ok(hash.as_bytes()[0] as u64)
        })
        .map_err(|err| runtime_error("spawn task bench worker", err))
}

async fn benchmark_io(
    handle: RuntimeHandle,
    io_ops: usize,
    in_flight: usize,
    io_size: usize,
) -> Result<(Duration, u64)> {
    if io_ops == 0 {
        return Ok((Duration::ZERO, 0));
    }

    let tmp_path = std::env::temp_dir().join(format!(
        "sparsync-bench-{}-{}.bin",
        std::process::id(),
        rand::rng().random::<u64>()
    ));

    let mut payload = vec![0u8; io_size];
    rand::rng().fill(payload.as_mut_slice());

    fs::write(&handle, &tmp_path, payload)
        .await
        .with_context(|| format!("write benchmark seed file {}", tmp_path.display()))?;

    let file = fs::File::open(handle.clone(), &tmp_path)
        .await
        .with_context(|| format!("open benchmark seed file {}", tmp_path.display()))?;

    let started = Instant::now();
    let mut next_task = 0usize;
    let mut digest = 0u64;
    let mut running = FuturesUnordered::new();

    let initial = min(io_ops, in_flight.max(1));
    for _ in 0..initial {
        running.push(spawn_io_job(
            handle.clone(),
            file.clone(),
            io_size,
            next_task,
        )?);
        next_task += 1;
    }

    while let Some(joined) = running.next().await {
        let value = joined.map_err(|err| join_error("io bench worker canceled", err))??;
        digest ^= value;

        if next_task < io_ops {
            running.push(spawn_io_job(
                handle.clone(),
                file.clone(),
                io_size,
                next_task,
            )?);
            next_task += 1;
        }
    }

    let elapsed = started.elapsed();
    let _ = fs::remove_file(&handle, &tmp_path).await;

    Ok((elapsed, digest))
}

fn spawn_io_job(
    handle: RuntimeHandle,
    file: fs::File,
    io_size: usize,
    index: usize,
) -> Result<spargio::JoinHandle<Result<u64>>> {
    handle
        .spawn_stealable(async move {
            let chunk = file.read_at(0, io_size).await?;
            if chunk.is_empty() {
                return Ok(0);
            }
            Ok((blake3::hash(&chunk).as_bytes()[0] as u64) ^ (index as u64))
        })
        .map_err(|err| runtime_error("spawn io bench worker", err))
}

async fn run_external_command(
    handle: RuntimeHandle,
    label: &str,
    command: String,
) -> Result<Duration> {
    let join = handle
        .spawn_blocking(move || {
            let started = Instant::now();
            let status = std::process::Command::new("bash")
                .arg("-lc")
                .arg(&command)
                .status()
                .with_context(|| format!("execute external command: {command}"))?;
            if !status.success() {
                anyhow::bail!("external command failed with status {status}");
            }
            Ok::<Duration, anyhow::Error>(started.elapsed())
        })
        .map_err(|err| runtime_error("spawn external benchmark command", err))?;

    let elapsed = join
        .await
        .map_err(|err| join_error("external benchmark canceled", err))??;
    println!(
        "bench external label={} elapsed_ms={}",
        label,
        elapsed.as_millis()
    );
    Ok(elapsed)
}

fn rate(items: usize, elapsed: Duration) -> f64 {
    if items == 0 {
        return 0.0;
    }
    let secs = elapsed.as_secs_f64();
    if secs <= f64::EPSILON {
        return items as f64;
    }
    (items as f64) / secs
}
