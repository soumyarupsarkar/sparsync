use crate::endpoint::RemoteEndpoint;
use crate::profile;
use anyhow::{Context, Result, bail};
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

const DEFAULT_REMOTE_AUTH_DIR: &str = "$HOME/.config/sparsync/auth";
const DEFAULT_REMOTE_INSTALL_PATH: &str = "$HOME/.local/bin/sparsync";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InstallMode {
    Auto,
    Off,
    UploadLocalBinary,
}

impl InstallMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "auto" => Ok(Self::Auto),
            "off" => Ok(Self::Off),
            "upload-local-binary" => Ok(Self::UploadLocalBinary),
            other => bail!(
                "invalid install mode '{}' (expected auto|off|upload-local-binary)",
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
    child: Child,
}

impl BootstrapSession {
    pub fn wait(mut self) -> Result<()> {
        let mut waited = Duration::ZERO;
        let step = Duration::from_millis(100);
        let timeout = Duration::from_secs(5);
        let status = loop {
            if let Some(status) = self
                .child
                .try_wait()
                .context("poll remote serve-session ssh process")?
            {
                break status;
            }
            if waited >= timeout {
                let _ = self.child.kill();
                let _ = self.child.wait();
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

fn upload_file_via_ssh(remote: &RemoteEndpoint, local: &Path, remote_path: &str) -> Result<()> {
    let data =
        std::fs::read(local).with_context(|| format!("read local file {}", local.display()))?;
    let script = format!(
        "mkdir -p $(dirname {remote_path}) && cat > {remote_path} && chmod 0755 {remote_path}",
        remote_path = remote_path
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
    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create local dir {}", parent.display()))?;
    }
    std::fs::write(local_path, data).with_context(|| format!("write {}", local_path.display()))?;
    Ok(())
}

fn ensure_remote_binary(remote: &RemoteEndpoint, install_mode: InstallMode) -> Result<()> {
    if run_ssh_status(remote, "command -v sparsync >/dev/null 2>&1")? {
        return Ok(());
    }

    match install_mode {
        InstallMode::Off => bail!("remote sparsync is missing and install mode is off"),
        InstallMode::Auto | InstallMode::UploadLocalBinary => {
            let local =
                std::env::current_exe().context("resolve local sparsync executable path")?;
            upload_file_via_ssh(remote, &local, DEFAULT_REMOTE_INSTALL_PATH)
                .context("upload sparsync binary to remote")?;
            if !run_ssh_status(
                remote,
                "command -v sparsync >/dev/null 2>&1 || [ -x $HOME/.local/bin/sparsync ]",
            )? {
                bail!("remote sparsync install failed");
            }
            Ok(())
        }
    }
}

pub fn ensure_remote_binary_available(
    remote: &RemoteEndpoint,
    install_mode: InstallMode,
) -> Result<()> {
    ensure_remote_binary(remote, install_mode)
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

pub fn bootstrap_remote_push(options: &BootstrapOptions) -> Result<BootstrapSession> {
    ensure_remote_binary(&options.remote, options.install_mode)?;

    run_ssh(
        &options.remote,
        &format!(
            "sparsync auth init-server --dir {} --server-name {}",
            sh_quote(DEFAULT_REMOTE_AUTH_DIR),
            sh_quote(&options.server_name)
        ),
    )
    .context("initialize remote auth material")?;

    run_ssh(
        &options.remote,
        &format!(
            "sparsync auth issue-client --dir {} --client-id {} --allow-prefix /",
            sh_quote(DEFAULT_REMOTE_AUTH_DIR),
            sh_quote(&options.client_id)
        ),
    )
    .context("issue remote client certificate")?;

    let mut local_root = profile::ensure_config_root()?;
    local_root.push("bootstrap");
    local_root.push(&options.profile_name);
    std::fs::create_dir_all(&local_root)
        .with_context(|| format!("create local bootstrap dir {}", local_root.display()))?;

    let ca = local_root.join("server.cert.der");
    let client_cert = local_root.join("client.cert.der");
    let client_key = local_root.join("client.key.der");

    fetch_file_via_ssh(
        &options.remote,
        "$HOME/.config/sparsync/auth/server.cert.der",
        &ca,
    )?;
    fetch_file_via_ssh(
        &options.remote,
        &format!(
            "$HOME/.config/sparsync/auth/clients/{}.cert.der",
            options.client_id
        ),
        &client_cert,
    )?;
    fetch_file_via_ssh(
        &options.remote,
        &format!(
            "$HOME/.config/sparsync/auth/clients/{}.key.der",
            options.client_id
        ),
        &client_key,
    )?;

    let mut remote_cmd = format!(
        "sparsync serve --bind 0.0.0.0:{} --destination {} --cert $HOME/.config/sparsync/auth/server.cert.der --key $HOME/.config/sparsync/auth/server.key.der --client-ca $HOME/.config/sparsync/auth/client-ca.cert.der --authz $HOME/.config/sparsync/auth/authz.json --once",
        options.server_port,
        sh_quote(&options.destination)
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

    thread::sleep(Duration::from_millis(500));

    let server = resolve_server_addr(&options.remote, options.server_port)?;

    Ok(BootstrapSession {
        server,
        server_name: options.server_name.clone(),
        ca,
        client_cert,
        client_key,
        child,
    })
}
