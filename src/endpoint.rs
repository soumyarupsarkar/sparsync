use anyhow::{Context, Result, bail};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Local(PathBuf),
    Remote(RemoteEndpoint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteEndpoint {
    pub user: Option<String>,
    pub host: String,
    pub port: Option<u16>,
    pub path: String,
}

impl RemoteEndpoint {
    pub fn ssh_target(&self) -> String {
        match &self.user {
            Some(user) => format!("{user}@{}", self.host),
            None => self.host.clone(),
        }
    }
}

fn parse_user_host_port(input: &str) -> Result<(Option<String>, String, Option<u16>)> {
    let (user, host_port) = if let Some((left, right)) = input.split_once('@') {
        if left.is_empty() {
            bail!("remote user is empty");
        }
        (Some(left.to_string()), right)
    } else {
        (None, input)
    };

    if host_port.is_empty() {
        bail!("remote host is empty");
    }

    if let Some((host, port)) = host_port.rsplit_once(':') {
        if host.contains(':') {
            // likely an IPv6 literal without brackets; keep it simple for now.
            return Ok((user, host_port.to_string(), None));
        }
        let parsed = port
            .parse::<u16>()
            .with_context(|| format!("invalid remote port '{}'", port))?;
        return Ok((user, host.to_string(), Some(parsed)));
    }

    Ok((user, host_port.to_string(), None))
}

pub fn parse_endpoint(value: &str) -> Result<Endpoint> {
    if let Some(raw) = value.strip_prefix("ssh://") {
        let (host_part, path_part) = raw
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("ssh endpoint missing path: {}", value))?;
        let (user, host, port) = parse_user_host_port(host_part)?;
        let path = format!("/{}", path_part);
        if path == "/" {
            bail!("remote path is empty");
        }
        return Ok(Endpoint::Remote(RemoteEndpoint {
            user,
            host,
            port,
            path,
        }));
    }

    let is_abs = value.starts_with('/');
    let drive_like = value.len() >= 2
        && value.as_bytes()[1] == b':'
        && value.as_bytes()[0].is_ascii_alphabetic();
    if !is_abs && !drive_like {
        if let Some((host_part, path)) = value.split_once(':') {
            if !host_part.is_empty() && !host_part.contains('/') && !path.is_empty() {
                let (user, host, port) = parse_user_host_port(host_part)?;
                return Ok(Endpoint::Remote(RemoteEndpoint {
                    user,
                    host,
                    port,
                    path: path.to_string(),
                }));
            }
        }
    }

    Ok(Endpoint::Local(PathBuf::from(value)))
}

#[cfg(test)]
mod tests {
    use super::{Endpoint, parse_endpoint};

    #[test]
    fn parses_scp_style_remote() {
        let parsed = parse_endpoint("alice@example.com:/srv/data").expect("parse remote");
        match parsed {
            Endpoint::Remote(remote) => {
                assert_eq!(remote.user.as_deref(), Some("alice"));
                assert_eq!(remote.host, "example.com");
                assert_eq!(remote.port, None);
                assert_eq!(remote.path, "/srv/data");
            }
            other => panic!("expected remote endpoint, got {other:?}"),
        }
    }

    #[test]
    fn parses_ssh_url_remote() {
        let parsed = parse_endpoint("ssh://alice@example.com:2222/srv/data").expect("parse remote");
        match parsed {
            Endpoint::Remote(remote) => {
                assert_eq!(remote.user.as_deref(), Some("alice"));
                assert_eq!(remote.host, "example.com");
                assert_eq!(remote.port, Some(2222));
                assert_eq!(remote.path, "/srv/data");
            }
            other => panic!("expected remote endpoint, got {other:?}"),
        }
    }

    #[test]
    fn keeps_absolute_local_path_local() {
        let parsed = parse_endpoint("/tmp/source").expect("parse local");
        match parsed {
            Endpoint::Local(path) => assert_eq!(path.to_string_lossy(), "/tmp/source"),
            other => panic!("expected local endpoint, got {other:?}"),
        }
    }
}
