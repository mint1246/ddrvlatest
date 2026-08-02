use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub ddrv: DdrvConfig,
    #[serde(default)]
    pub dataprovider: DataproviderConfig,
    #[serde(default)]
    pub frontend: FrontendConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DdrvConfig {
    #[serde(default, deserialize_with = "deserialize_token_list")]
    pub token: Vec<String>,
    #[serde(default)]
    pub token_type: i32,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub chunk_size: usize,
    #[serde(default)]
    pub nitro: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TokenConfig {
    Single(String),
    Multiple(Vec<String>),
}

fn deserialize_token_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let token = Option::<TokenConfig>::deserialize(deserializer)?;
    Ok(match token {
        Some(TokenConfig::Single(v)) => vec![v],
        Some(TokenConfig::Multiple(v)) => v,
        None => Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn token_accepts_single_string() {
        let raw = r#"
ddrv:
  token: single-token
  channels: ["123"]
"#;
        let cfg: Config = serde_yaml::from_str(raw).expect("config should parse");
        assert_eq!(cfg.ddrv.token, vec!["single-token"]);
    }

    #[test]
    fn token_accepts_sequence() {
        let raw = r#"
ddrv:
  token: ["token-a", "token-b"]
  channels: ["123"]
"#;
        let cfg: Config = serde_yaml::from_str(raw).expect("config should parse");
        assert_eq!(cfg.ddrv.token, vec!["token-a", "token-b"]);
    }

    #[test]
    fn validate_collects_independent_errors() {
        let raw = r#"
ddrv:
  token: [same, same]
  channels: []
  chunk_size: 999999999
dataprovider:
  boltdb: { db_path: "" }
  postgres: { db_url: "" }
frontend:
  ftp:
    addr: not-an-address
    username: ""
    password: ""
    port_range: 50000-40000
  http:
    addr: ""
    cdn_proxy_base: file:///tmp/nope
    https_addr: ":443"
"#;
        let cfg: Config = serde_yaml::from_str(raw).unwrap();
        let report = cfg.validate().unwrap_err().to_string();
        for expected in [
            "exactly one metadata backend",
            "valid socket address",
            "valid start-end range",
            "FTP username and password",
            "must either all be set",
            "chunk_size",
            "duplicate value",
            "channels must not be empty",
            "valid HTTP or HTTPS URL",
        ] {
            assert!(report.contains(expected), "missing {expected:?}: {report}");
        }
    }

    #[test]
    fn redacted_config_hides_secrets() {
        let raw = r#"
ddrv: { token: secret, channels: ["123"] }
dataprovider:
  postgres: { db_url: "postgres://user:secret@localhost/db" }
frontend:
  ftp: { password: ftp-secret }
  http: { password: http-secret }
"#;
        let cfg: Config = serde_yaml::from_str(raw).unwrap();
        let rendered = serde_yaml::to_string(&cfg.redacted()).unwrap();
        assert!(!rendered.contains("secret"));
        assert!(rendered.contains("<redacted>"));
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct BoltConfig {
    #[serde(default)]
    pub db_path: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PostgresConfig {
    #[serde(default)]
    pub db_url: String,
    #[serde(default = "default_pg_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_pg_connect_timeout")]
    pub connect_timeout_seconds: u64,
    #[serde(default = "default_pg_idle_timeout")]
    pub idle_timeout_seconds: u64,
}

const fn default_pg_max_connections() -> u32 {
    10
}
const fn default_pg_connect_timeout() -> u64 {
    10
}
const fn default_pg_idle_timeout() -> u64 {
    600
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            db_url: String::new(),
            max_connections: default_pg_max_connections(),
            connect_timeout_seconds: default_pg_connect_timeout(),
            idle_timeout_seconds: default_pg_idle_timeout(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DataproviderConfig {
    #[serde(default)]
    pub boltdb: BoltConfig,
    #[serde(default)]
    pub postgres: PostgresConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct FtpConfig {
    #[serde(default)]
    pub addr: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    pub port_range: Option<String>,
    #[serde(default)]
    pub async_write: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct HttpConfig {
    #[serde(default)]
    pub addr: String,
    pub https_addr: Option<String>,
    pub https_keypath: Option<String>,
    pub https_crtpath: Option<String>,
    #[serde(default)]
    pub cdn_proxy_base: Option<String>,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub guest_mode: bool,
    #[serde(default)]
    pub async_write: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct FrontendConfig {
    #[serde(default)]
    pub ftp: FtpConfig,
    #[serde(default)]
    pub http: HttpConfig,
}

impl Config {
    /// Validate the complete configuration and report every problem at once.
    pub fn validate(&self) -> anyhow::Result<()> {
        let mut errors = Vec::new();
        let bolt = !self.dataprovider.boltdb.db_path.trim().is_empty();
        let postgres = !self.dataprovider.postgres.db_url.trim().is_empty();
        if bolt == postgres {
            errors.push(
                "exactly one metadata backend must be configured (boltdb or postgres)".into(),
            );
        }

        for (name, addr) in [
            ("frontend.ftp.addr", self.frontend.ftp.addr.as_str()),
            ("frontend.http.addr", self.frontend.http.addr.as_str()),
        ] {
            validate_addr(name, addr, &mut errors);
        }
        if let Some(addr) = self.frontend.http.https_addr.as_deref() {
            validate_addr("frontend.http.https_addr", addr, &mut errors);
        }

        if let Some(range) = self.frontend.ftp.port_range.as_deref() {
            let valid = range
                .split_once('-')
                .and_then(|(a, b)| Some((a.parse::<u16>().ok()?, b.parse::<u16>().ok()?)))
                .is_some_and(|(a, b)| a > 0 && a < b);
            if !valid {
                errors.push(
                    "frontend.ftp.port_range must be a valid start-end range with start < end"
                        .into(),
                );
            }
        }

        validate_credentials(
            "FTP",
            &self.frontend.ftp.addr,
            &self.frontend.ftp.username,
            &self.frontend.ftp.password,
            &mut errors,
        );
        validate_credentials(
            "HTTP",
            &self.frontend.http.addr,
            &self.frontend.http.username,
            &self.frontend.http.password,
            &mut errors,
        );

        let tls = [
            &self.frontend.http.https_addr,
            &self.frontend.http.https_keypath,
            &self.frontend.http.https_crtpath,
        ];
        let tls_count = tls
            .iter()
            .filter(|v| v.as_ref().is_some_and(|s| !s.trim().is_empty()))
            .count();
        if tls_count != 0 && tls_count != 3 {
            errors.push("HTTPS address, certificate, and private key must either all be set or all be omitted".into());
        }

        let max_chunk = match self.ddrv.token_type {
            0 | 1 => 25 * 1024 * 1024,
            2 => 500 * 1024 * 1024,
            3 => 50 * 1024 * 1024,
            _ => {
                errors.push("ddrv.token_type must be between 0 and 3".into());
                0
            }
        };
        if self.ddrv.chunk_size != 0 && (max_chunk == 0 || self.ddrv.chunk_size > max_chunk) {
            errors.push(format!(
                "ddrv.chunk_size must be 0 or no larger than {max_chunk} bytes for this token type"
            ));
        }
        validate_unique_nonempty("ddrv.token", &self.ddrv.token, &mut errors);
        validate_unique_nonempty("ddrv.channels", &self.ddrv.channels, &mut errors);

        if bolt {
            validate_redb_parent(&self.dataprovider.boltdb.db_path, &mut errors);
        }
        if let Some(proxy) = self
            .frontend
            .http
            .cdn_proxy_base
            .as_deref()
            .filter(|s| !s.trim().is_empty())
        {
            match url::Url::parse(proxy) {
                Ok(url) if matches!(url.scheme(), "http" | "https") && url.host().is_some() => {}
                _ => errors
                    .push("frontend.http.cdn_proxy_base must be a valid HTTP or HTTPS URL".into()),
            }
        }
        let pg = &self.dataprovider.postgres;
        if postgres
            && (pg.max_connections == 0
                || pg.connect_timeout_seconds == 0
                || pg.idle_timeout_seconds == 0)
        {
            errors.push("PostgreSQL pool size and timeout values must be greater than zero".into());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "configuration validation failed:\n  - {}",
                errors.join("\n  - ")
            )
        }
    }

    pub fn redacted(&self) -> Self {
        let mut cfg = self.clone();
        cfg.ddrv
            .token
            .iter_mut()
            .for_each(|v| *v = "<redacted>".into());
        if !cfg.frontend.ftp.password.is_empty() {
            cfg.frontend.ftp.password = "<redacted>".into();
        }
        if !cfg.frontend.http.password.is_empty() {
            cfg.frontend.http.password = "<redacted>".into();
        }
        if !cfg.dataprovider.postgres.db_url.is_empty() {
            cfg.dataprovider.postgres.db_url = "<redacted>".into();
        }
        cfg
    }
}

fn validate_addr(name: &str, addr: &str, errors: &mut Vec<String>) {
    if addr.is_empty() {
        return;
    }
    let normalized = if addr.starts_with(':') {
        format!("0.0.0.0{addr}")
    } else {
        addr.into()
    };
    if normalized.parse::<SocketAddr>().is_err() {
        let valid_hostname = normalized
            .rsplit_once(':')
            .map(|(host, port)| {
                let host = host.trim_start_matches('[').trim_end_matches(']');
                !host.trim().is_empty() && port.parse::<u16>().is_ok()
            })
            .unwrap_or(false);
        if !valid_hostname {
            errors.push(format!(
                "{name} must be a valid socket address or hostname:port"
            ));
        }
    }
}
fn validate_credentials(
    frontend: &str,
    addr: &str,
    username: &str,
    password: &str,
    errors: &mut Vec<String>,
) {
    if !addr.is_empty() && (username.trim().is_empty() || password.is_empty()) {
        errors.push(format!(
            "{frontend} username and password are required when the frontend is enabled"
        ));
    }
}
fn validate_unique_nonempty(name: &str, values: &[String], errors: &mut Vec<String>) {
    if values.is_empty() {
        errors.push(format!("{name} must not be empty"));
        return;
    }
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            errors.push(format!("{name} contains an empty value"));
        } else if !seen.insert(value) {
            errors.push(format!("{name} contains duplicate value {value:?}"));
        }
    }
}
fn validate_redb_parent(path: &str, errors: &mut Vec<String>) {
    let parent = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let probe = parent.join(format!(".ddrv-write-check-{}", uuid::Uuid::new_v4()));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(probe);
        }
        Err(e) => errors.push(format!(
            "redb parent directory {} is not writable: {e}",
            parent.display()
        )),
    }
}

pub fn load(config_path: Option<&str>) -> anyhow::Result<Config> {
    use config::{Config as Cfg, File};

    let home = std::env::var("HOME").unwrap_or_default();

    let mut builder = Cfg::builder()
        .add_source(File::with_name("config").required(false))
        .add_source(File::with_name(&format!("{}/.config/ddrv/config", home)).required(false));

    if let Some(path) = config_path {
        builder = builder.add_source(File::with_name(path).required(true));
    }

    // Env overrides
    if let Ok(v) = std::env::var("TOKEN") {
        builder = builder.set_override("ddrv.token", vec![v])?;
    }
    if let Ok(v) = std::env::var("TOKEN_TYPE") {
        builder = builder.set_override("ddrv.token_type", v)?;
    }
    if let Ok(v) = std::env::var("CHANNELS") {
        let channels: Vec<String> = v.split(',').map(|s| s.trim().to_string()).collect();
        builder = builder.set_override("ddrv.channels", channels)?;
    }
    if let Ok(v) = std::env::var("NITRO") {
        builder = builder.set_override("ddrv.nitro", v)?;
    }
    if let Ok(v) = std::env::var("CHUNK_SIZE") {
        builder = builder.set_override("ddrv.chunk_size", v)?;
    }
    if let Ok(v) = std::env::var("BOLTDB_DB_PATH") {
        builder = builder.set_override("dataprovider.boltdb.db_path", v)?;
    }
    if let Ok(v) = std::env::var("POSTGRES_DB_URL") {
        builder = builder.set_override("dataprovider.postgres.db_url", v)?;
    }
    if let Ok(v) = std::env::var("FTP_ADDR") {
        builder = builder.set_override("frontend.ftp.addr", v)?;
    }
    if let Ok(v) = std::env::var("FTP_USERNAME") {
        builder = builder.set_override("frontend.ftp.username", v)?;
    }
    if let Ok(v) = std::env::var("FTP_PASSWORD") {
        builder = builder.set_override("frontend.ftp.password", v)?;
    }
    if let Ok(v) = std::env::var("FTP_ASYNC_WRITE") {
        builder = builder.set_override("frontend.ftp.async_write", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_ADDR") {
        builder = builder.set_override("frontend.http.addr", v)?;
    }
    if let Ok(v) = std::env::var("CDN_PROXY_BASE") {
        builder = builder.set_override("frontend.http.cdn_proxy_base", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_USERNAME") {
        builder = builder.set_override("frontend.http.username", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_PASSWORD") {
        builder = builder.set_override("frontend.http.password", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_GUEST_MODE") {
        builder = builder.set_override("frontend.http.guest_mode", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_ASYNC_WRITE") {
        builder = builder.set_override("frontend.http.async_write", v)?;
    }
    if let Ok(v) = std::env::var("HTTPS_ADDR") {
        builder = builder.set_override("frontend.http.https_addr", v)?;
    }
    if let Ok(v) = std::env::var("HTTPS_CRTPATH") {
        builder = builder.set_override("frontend.http.https_crtpath", v)?;
    }
    if let Ok(v) = std::env::var("HTTPS_KEYPATH") {
        builder = builder.set_override("frontend.http.https_keypath", v)?;
    }

    let cfg: Config = builder.build()?.try_deserialize()?;
    Ok(cfg)
}
