use crate::endpoint::RemoteEndpoint;
use crate::profile;
use anyhow::{Context, Result, bail};
use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

const DEFAULT_REMOTE_AUTH_SUBPATH: &str = ".config/sparsync/auth";
const DEFAULT_REMOTE_INSTALL_SUBPATH: &str = ".local/bin/sparsync";
const DEFAULT_REMOTE_SERVICE_SUBPATH: &str = ".config/sparsync/service";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InstallMode {
    Auto,
    Off,
    UploadLocalBinary,
    Ephemeral,
}

impl InstallMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "auto" => Ok(Self::Auto),
            "off" => Ok(Self::Off),
            "upload-local-binary" => Ok(Self::UploadLocalBinary),
            "ephemeral" => Ok(Self::Ephemeral),
            other => bail!(
                "invalid install mode '{}' (expected auto|off|upload-local-binary|ephemeral)",
                other
            ),
        }
    }
}

#[derive(Debug)]
pub struct BootstrapOptions {
    pub remote: RemoteEndpoint,
    pub destination: String,
    pub server_port: u16,
    pub server_name: String,
    pub client_id: String,
    pub profile_name: String,
    pub install_mode: InstallMode,
    pub preserve_metadata: bool,
}

pub struct BootstrapSession {
    pub server: SocketAddr,
    pub server_name: String,
    pub ca: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
    _binary_lease: RemoteBinaryLease,
    child: Option<Child>,
}

pub struct Enrollment {
    pub server: SocketAddr,
    pub server_name: String,
    pub ca: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

pub struct RemoteServerStatus {
    pub running: bool,
    pub pid: Option<u32>,
}

pub struct RemoteBinaryLease {
    remote: RemoteEndpoint,
    shell_prefix: String,
    cleanup_path: Option<String>,
}

impl RemoteBinaryLease {
    pub fn shell_prefix(&self) -> &str {
        &self.shell_prefix
    }
}

impl Drop for RemoteBinaryLease {
    fn drop(&mut self) {
        let Some(path) = self.cleanup_path.take() else {
            return;
        };
        let _ = run_ssh_status(&self.remote, &format!("rm -f {}", sh_quote(&path)));
    }
}

impl BootstrapSession {
    pub fn wait(mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let mut waited = Duration::ZERO;
        let step = Duration::from_millis(100);
        let timeout = Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .context("poll remote serve-session ssh process")?
            {
                break status;
            }
            if waited >= timeout {
                let _ = child.kill();
                let _ = child.wait();
                return Ok(());
            }
            thread::sleep(step);
            waited += step;
        };
        if !status.success() {
            bail!("remote serve-session exited with status {}", status);
        }
        Ok(())
    }
}

impl Drop for BootstrapSession {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        match child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
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

fn run_ssh(remote: &RemoteEndpoint, script: &str) -> Result<Vec<u8>> {
    let output = ssh_base_command(remote)
        .arg(script)
        .output()
        .with_context(|| format!("run ssh command on {}", remote.ssh_target()))?;
    if !output.status.success() {
        bail!(
            "ssh command failed on {}: {}",
            remote.ssh_target(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

fn run_ssh_status(remote: &RemoteEndpoint, script: &str) -> Result<bool> {
    let status = ssh_base_command(remote)
        .arg(script)
        .status()
        .with_context(|| format!("run ssh status command on {}", remote.ssh_target()))?;
    Ok(status.success())
}

fn remote_home(remote: &RemoteEndpoint) -> Result<String> {
    let bytes = run_ssh(remote, "printf %s \"$HOME\"").context("resolve remote HOME")?;
    let home = String::from_utf8(bytes).context("decode remote HOME output")?;
    let home = home.trim();
    if home.is_empty() {
        bail!("remote HOME is empty");
    }
    Ok(home.to_string())
}

fn remote_install_path(remote: &RemoteEndpoint) -> Result<String> {
    let home = remote_home(remote)?;
    Ok(format!("{home}/{DEFAULT_REMOTE_INSTALL_SUBPATH}"))
}

fn remote_auth_dir(remote: &RemoteEndpoint) -> Result<String> {
    let home = remote_home(remote)?;
    Ok(format!("{home}/{DEFAULT_REMOTE_AUTH_SUBPATH}"))
}

fn remote_service_dir(remote: &RemoteEndpoint) -> Result<String> {
    let home = remote_home(remote)?;
    Ok(format!("{home}/{DEFAULT_REMOTE_SERVICE_SUBPATH}"))
}

fn mktemp_remote_binary_path(remote: &RemoteEndpoint) -> Result<String> {
    let bytes =
        run_ssh(remote, "mktemp /tmp/sparsync.XXXXXX").context("create remote temp path")?;
    let path = String::from_utf8(bytes).context("decode remote temp path")?;
    let path = path.trim();
    if path.is_empty() {
        bail!("remote mktemp returned empty path");
    }
    Ok(path.to_string())
}

fn mktemp_remote_file_path(remote: &RemoteEndpoint, prefix: &str) -> Result<String> {
    let safe_prefix: String = prefix
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let bytes = run_ssh(remote, &format!("mktemp /tmp/{safe_prefix}.XXXXXX"))
        .context("create remote temp file path")?;
    let path = String::from_utf8(bytes).context("decode remote temp file path")?;
    let path = path.trim();
    if path.is_empty() {
        bail!("remote mktemp returned empty temp file path");
    }
    Ok(path.to_string())
}

fn upload_file_via_ssh(remote: &RemoteEndpoint, local: &Path, remote_path: &str) -> Result<()> {
    let data =
        std::fs::read(local).with_context(|| format!("read local file {}", local.display()))?;
    let remote_path_q = sh_quote(remote_path);
    let script = format!(
        "mkdir -p \"$(dirname {remote_path})\" && cat > {remote_path} && chmod 0755 {remote_path}",
        remote_path = remote_path_q
    );
    let mut child = ssh_base_command(remote)
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn ssh upload to {}", remote.ssh_target()))?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(&data)
            .with_context(|| format!("stream {} to remote", local.display()))?;
    }
    let output = child
        .wait_with_output()
        .context("wait for ssh upload command")?;
    if !output.status.success() {
        bail!(
            "upload to remote failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn fetch_file_via_ssh(remote: &RemoteEndpoint, remote_path: &str, local_path: &Path) -> Result<()> {
    let script = format!("cat {}", sh_quote(remote_path));
    let data =
        run_ssh(remote, &script).with_context(|| format!("fetch remote file {}", remote_path))?;
    profile::write_secret_file(local_path, &data)
        .with_context(|| format!("write {}", local_path.display()))?;
    Ok(())
}

fn prepare_remote_binary(
    remote: &RemoteEndpoint,
    install_mode: InstallMode,
) -> Result<RemoteBinaryLease> {
    let has_remote_sparsync = run_ssh_status(remote, "command -v sparsync >/dev/null 2>&1")?;
    match install_mode {
        InstallMode::Off => {
            if !has_remote_sparsync {
                bail!("remote sparsync is missing and install mode is off");
            }
            Ok(RemoteBinaryLease {
                remote: remote.clone(),
                shell_prefix: "sparsync".to_string(),
                cleanup_path: None,
            })
        }
        InstallMode::Auto => {
            if has_remote_sparsync {
                return Ok(RemoteBinaryLease {
                    remote: remote.clone(),
                    shell_prefix: "sparsync".to_string(),
                    cleanup_path: None,
                });
            }
            let local =
                std::env::current_exe().context("resolve local sparsync executable path")?;
            let install_path = remote_install_path(remote)?;
            upload_file_via_ssh(remote, &local, &install_path)
                .context("upload sparsync binary to remote")?;
            if !run_ssh_status(remote, &format!("[ -x {} ]", sh_quote(&install_path)))? {
                bail!("remote sparsync install failed");
            }
            Ok(RemoteBinaryLease {
                remote: remote.clone(),
                shell_prefix: sh_quote(&install_path),
                cleanup_path: None,
            })
        }
        InstallMode::UploadLocalBinary => {
            let local =
                std::env::current_exe().context("resolve local sparsync executable path")?;
            let install_path = remote_install_path(remote)?;
            upload_file_via_ssh(remote, &local, &install_path)
                .context("upload sparsync binary to remote")?;
            if !run_ssh_status(remote, &format!("[ -x {} ]", sh_quote(&install_path)))? {
                bail!("remote sparsync install failed");
            }
            Ok(RemoteBinaryLease {
                remote: remote.clone(),
                shell_prefix: sh_quote(&install_path),
                cleanup_path: None,
            })
        }
        InstallMode::Ephemeral => {
            let local =
                std::env::current_exe().context("resolve local sparsync executable path")?;
            let temp_path = mktemp_remote_binary_path(remote)?;
            upload_file_via_ssh(remote, &local, &temp_path)
                .context("upload ephemeral sparsync binary to remote")?;
            if !run_ssh_status(remote, &format!("[ -x {} ]", sh_quote(&temp_path)))? {
                bail!("ephemeral remote sparsync upload failed");
            }
            Ok(RemoteBinaryLease {
                remote: remote.clone(),
                shell_prefix: sh_quote(&temp_path),
                cleanup_path: Some(temp_path),
            })
        }
    }
}

pub fn ensure_remote_binary_available(
    remote: &RemoteEndpoint,
    install_mode: InstallMode,
) -> Result<RemoteBinaryLease> {
    prepare_remote_binary(remote, install_mode)
}

pub fn run_remote_shell(remote: &RemoteEndpoint, script: &str) -> Result<Vec<u8>> {
    run_ssh(remote, script)
}

pub fn upload_temp_file_via_ssh(
    remote: &RemoteEndpoint,
    local_path: &Path,
    prefix: &str,
) -> Result<String> {
    let remote_path = mktemp_remote_file_path(remote, prefix)?;
    upload_file_via_ssh(remote, local_path, &remote_path).with_context(|| {
        format!(
            "upload {} to remote temp file {}",
            local_path.display(),
            remote_path
        )
    })?;
    Ok(remote_path)
}

pub fn remove_remote_file(remote: &RemoteEndpoint, path: &str) -> Result<()> {
    let _ = run_ssh_status(remote, &format!("rm -f {}", sh_quote(path)))?;
    Ok(())
}

fn resolve_server_addr(remote: &RemoteEndpoint, port: u16) -> Result<SocketAddr> {
    let addr = format!("{}:{port}", remote.host);
    addr.to_socket_addrs()
        .with_context(|| format!("resolve remote host '{}'", remote.host))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("no socket address resolved for {}", addr))
}

pub fn default_client_id() -> String {
    let user = std::env::var("USER").unwrap_or_else(|_| "client".to_string());
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".to_string());
    let sanitize = |value: String| -> String {
        value
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .collect()
    };
    format!("{}-{}", sanitize(user), sanitize(host))
}

fn wait_for_server_ready(server: SocketAddr, timeout: Duration) -> Result<()> {
    let started = std::time::Instant::now();
    let step = Duration::from_millis(50);
    loop {
        if TcpStream::connect_timeout(&server, Duration::from_millis(250)).is_ok() {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            bail!("timed out waiting for server readiness at {}", server);
        }
        thread::sleep(step);
    }
}

fn enroll_remote_with_lease(
    options: &BootstrapOptions,
    binary_lease: &RemoteBinaryLease,
) -> Result<Enrollment> {
    let remote_auth_dir = remote_auth_dir(&options.remote)?;

    run_ssh(
        &options.remote,
        &format!(
            "{} auth init-server --dir {} --server-name {}",
            binary_lease.shell_prefix(),
            sh_quote(&remote_auth_dir),
            sh_quote(&options.server_name)
        ),
    )
    .context("initialize remote auth material")?;

    run_ssh(
        &options.remote,
        &format!(
            "{} auth issue-client --dir {} --client-id {} --allow-prefix /",
            binary_lease.shell_prefix(),
            sh_quote(&remote_auth_dir),
            sh_quote(&options.client_id)
        ),
    )
    .context("issue remote client certificate")?;

    let local_root = profile::ensure_profile_secret_dir(&options.profile_name)?;

    let ca = local_root.join("server.cert.der");
    let client_cert = local_root.join("client.cert.der");
    let client_key = local_root.join("client.key.der");

    fetch_file_via_ssh(
        &options.remote,
        &format!("{remote_auth_dir}/server.cert.der"),
        &ca,
    )?;
    fetch_file_via_ssh(
        &options.remote,
        &format!("{remote_auth_dir}/clients/{}.cert.der", options.client_id),
        &client_cert,
    )?;
    fetch_file_via_ssh(
        &options.remote,
        &format!("{remote_auth_dir}/clients/{}.key.der", options.client_id),
        &client_key,
    )?;

    let server = resolve_server_addr(&options.remote, options.server_port)?;

    Ok(Enrollment {
        server,
        server_name: options.server_name.clone(),
        ca,
        client_cert,
        client_key,
    })
}

pub fn enroll_remote(options: &BootstrapOptions) -> Result<Enrollment> {
    let binary_lease = prepare_remote_binary(&options.remote, options.install_mode)?;
    enroll_remote_with_lease(options, &binary_lease)
}

pub fn bootstrap_remote_push(options: &BootstrapOptions) -> Result<BootstrapSession> {
    let binary_lease = prepare_remote_binary(&options.remote, options.install_mode)?;
    let enrollment = enroll_remote_with_lease(options, &binary_lease)?;
    let remote_auth_dir = remote_auth_dir(&options.remote)?;

    let mut remote_cmd = format!(
        "{} serve --bind 0.0.0.0:{} --destination {} --cert {} --key {} --client-ca {} --authz {} --once",
        binary_lease.shell_prefix(),
        options.server_port,
        sh_quote(&options.destination),
        sh_quote(&format!("{remote_auth_dir}/server.cert.der")),
        sh_quote(&format!("{remote_auth_dir}/server.key.der")),
        sh_quote(&format!("{remote_auth_dir}/client-ca.cert.der")),
        sh_quote(&format!("{remote_auth_dir}/authz.json"))
    );
    if options.preserve_metadata {
        remote_cmd.push_str(" --preserve-metadata");
    }

    let child = ssh_base_command(&options.remote)
        .arg(remote_cmd)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!(
                "spawn remote serve-session on {}",
                options.remote.ssh_target()
            )
        })?;

    wait_for_server_ready(enrollment.server, Duration::from_secs(10))
        .context("wait for remote one-shot server readiness")?;

    Ok(BootstrapSession {
        server: enrollment.server,
        server_name: enrollment.server_name,
        ca: enrollment.ca,
        client_cert: enrollment.client_cert,
        client_key: enrollment.client_key,
        _binary_lease: binary_lease,
        child: Some(child),
    })
}

pub fn start_remote_server(options: &BootstrapOptions) -> Result<Enrollment> {
    let binary_lease = prepare_remote_binary(&options.remote, options.install_mode)?;
    let enrollment = enroll_remote_with_lease(options, &binary_lease)?;
    let remote_auth_dir = remote_auth_dir(&options.remote)?;
    let service_dir = remote_service_dir(&options.remote)?;
    let pid_file = format!("{service_dir}/server.pid");
    let log_file = format!("{service_dir}/server.log");

    let mut serve_cmd = format!(
        "{} serve --bind 0.0.0.0:{} --destination {} --cert {} --key {} --client-ca {} --authz {}",
        binary_lease.shell_prefix(),
        options.server_port,
        sh_quote(&options.destination),
        sh_quote(&format!("{remote_auth_dir}/server.cert.der")),
        sh_quote(&format!("{remote_auth_dir}/server.key.der")),
        sh_quote(&format!("{remote_auth_dir}/client-ca.cert.der")),
        sh_quote(&format!("{remote_auth_dir}/authz.json"))
    );
    if options.preserve_metadata {
        serve_cmd.push_str(" --preserve-metadata");
    }

    let script = format!(
        "mkdir -p {service_dir_q}; \
         if [ -f {pid_file_q} ]; then \
           pid=$(cat {pid_file_q} 2>/dev/null || true); \
           if [ -n \"$pid\" ] && kill -0 \"$pid\" >/dev/null 2>&1; then exit 0; fi; \
         fi; \
         nohup {serve_cmd} > {log_file_q} 2>&1 < /dev/null & \
         echo $! > {pid_file_q}",
        service_dir_q = sh_quote(&service_dir),
        pid_file_q = sh_quote(&pid_file),
        log_file_q = sh_quote(&log_file),
        serve_cmd = serve_cmd,
    );

    run_ssh(&options.remote, &script).context("start remote sparsync server")?;
    wait_for_server_ready(enrollment.server, Duration::from_secs(10))
        .context("wait for remote persistent server readiness")?;
    Ok(enrollment)
}

pub fn stop_remote_server(remote: &RemoteEndpoint) -> Result<bool> {
    let service_dir = remote_service_dir(remote)?;
    let pid_file = format!("{service_dir}/server.pid");
    let script = format!(
        "if [ -f {pid_file_q} ]; then \
           pid=$(cat {pid_file_q} 2>/dev/null || true); \
           if [ -n \"$pid\" ] && kill -0 \"$pid\" >/dev/null 2>&1; then \
             kill \"$pid\" >/dev/null 2>&1 || true; \
             sleep 0.2; \
             if kill -0 \"$pid\" >/dev/null 2>&1; then kill -9 \"$pid\" >/dev/null 2>&1 || true; fi; \
             rm -f {pid_file_q}; \
             echo stopped_running; \
             exit 0; \
           fi; \
           rm -f {pid_file_q}; \
         fi; \
         echo stopped_not_running",
        pid_file_q = sh_quote(&pid_file),
    );
    let output = run_ssh(remote, &script).context("stop remote sparsync server")?;
    let text = String::from_utf8(output).context("decode remote stop output")?;
    Ok(text.trim() == "stopped_running")
}

pub fn remote_server_status(remote: &RemoteEndpoint) -> Result<RemoteServerStatus> {
    let service_dir = remote_service_dir(remote)?;
    let pid_file = format!("{service_dir}/server.pid");
    let script = format!(
        "if [ -f {pid_file_q} ]; then \
           pid=$(cat {pid_file_q} 2>/dev/null || true); \
           if [ -n \"$pid\" ] && kill -0 \"$pid\" >/dev/null 2>&1; then \
             printf 'running %s\\n' \"$pid\"; \
             exit 0; \
           fi; \
         fi; \
         echo stopped",
        pid_file_q = sh_quote(&pid_file),
    );
    let output = run_ssh(remote, &script).context("query remote sparsync server status")?;
    let text = String::from_utf8(output).context("decode remote status output")?;
    let trimmed = text.trim();
    if let Some(pid) = trimmed.strip_prefix("running ") {
        let pid = pid
            .trim()
            .parse::<u32>()
            .with_context(|| format!("parse remote running pid '{}'", pid.trim()))?;
        return Ok(RemoteServerStatus {
            running: true,
            pid: Some(pid),
        });
    }
    Ok(RemoteServerStatus {
        running: false,
        pid: None,
    })
}

#[cfg(test)]
mod tests {
    use super::{BootstrapSession, RemoteBinaryLease};
    use crate::endpoint::{RemoteEndpoint, RemoteKind};
    use std::path::PathBuf;
    use std::process::Command;

    #[test]
    fn bootstrap_session_drop_terminates_child_process() {
        let child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep child");
        let pid = child.id();

        let session = BootstrapSession {
            server: "127.0.0.1:1".parse().expect("parse socket addr"),
            server_name: "test".to_string(),
            ca: PathBuf::new(),
            client_cert: PathBuf::new(),
            client_key: PathBuf::new(),
            _binary_lease: RemoteBinaryLease {
                remote: RemoteEndpoint {
                    user: None,
                    host: "localhost".to_string(),
                    port: None,
                    path: "/tmp".to_string(),
                    kind: RemoteKind::Ssh,
                },
                shell_prefix: "sparsync".to_string(),
                cleanup_path: None,
            },
            child: Some(child),
        };

        drop(session);

        let status = Command::new("sh")
            .arg("-c")
            .arg(format!("kill -0 {pid} >/dev/null 2>&1"))
            .status()
            .expect("check process liveness");
        assert!(!status.success(), "child process {pid} is still running");
    }
}
