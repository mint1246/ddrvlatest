use serde::de::Deserializer;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub ddrv: DdrvConfig,
    #[serde(default)]
    pub dataprovider: DataproviderConfig,
    #[serde(default)]
    pub frontend: FrontendConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
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
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct BoltConfig {
    #[serde(default)]
    pub db_path: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct PostgresConfig {
    #[serde(default)]
    pub db_url: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct DataproviderConfig {
    #[serde(default)]
    pub boltdb: BoltConfig,
    #[serde(default)]
    pub postgres: PostgresConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
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
#[derive(Debug, Deserialize, Clone, Default)]
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
    pub password_hash: String,
    /// Dedicated JWT signing secret. Prefer `jwt_secret_file` or HTTP_JWT_SECRET.
    #[serde(default)]
    pub jwt_secret: String,
    #[serde(default)]
    pub jwt_secret_file: Option<String>,
    #[serde(default = "default_access_token_ttl_seconds")]
    pub access_token_ttl_seconds: u64,
    #[serde(default)]
    pub guest_mode: bool,
    #[serde(default)]
    pub async_write: bool,
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfig")
            .field("addr", &self.addr)
            .field("https_addr", &self.https_addr)
            .field("https_keypath", &self.https_keypath)
            .field("https_crtpath", &self.https_crtpath)
            .field("cdn_proxy_base", &self.cdn_proxy_base)
            .field("username", &self.username)
            .field("password_hash", &"[REDACTED]")
            .field("jwt_secret", &"[REDACTED]")
            .field("jwt_secret_file", &self.jwt_secret_file)
            .field("access_token_ttl_seconds", &self.access_token_ttl_seconds)
            .field("guest_mode", &self.guest_mode)
            .field("async_write", &self.async_write)
            .finish()
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            https_addr: None,
            https_keypath: None,
            https_crtpath: None,
            cdn_proxy_base: None,
            username: String::new(),
            password_hash: String::new(),
            jwt_secret: String::new(),
            jwt_secret_file: None,
            access_token_ttl_seconds: default_access_token_ttl_seconds(),
            guest_mode: false,
            async_write: false,
        }
    }
}

fn default_access_token_ttl_seconds() -> u64 {
    900
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct FrontendConfig {
    #[serde(default)]
    pub ftp: FtpConfig,
    #[serde(default)]
    pub http: HttpConfig,
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
    if std::env::var("HTTP_PASSWORD").is_ok() {
        anyhow::bail!("HTTP_PASSWORD is no longer supported; configure HTTP_PASSWORD_HASH with an Argon2id PHC string");
    }
    if let Ok(v) = std::env::var("HTTP_PASSWORD_HASH") {
        builder = builder.set_override("frontend.http.password_hash", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_JWT_SECRET_FILE") {
        builder = builder.set_override("frontend.http.jwt_secret_file", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_JWT_SECRET") {
        builder = builder.set_override("frontend.http.jwt_secret", v)?;
    }
    if let Ok(v) = std::env::var("HTTP_ACCESS_TOKEN_TTL_SECONDS") {
        builder = builder.set_override("frontend.http.access_token_ttl_seconds", v)?;
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

    let mut cfg: Config = builder.build()?.try_deserialize()?;
    if let Some(path) = cfg.frontend.http.jwt_secret_file.as_deref() {
        cfg.frontend.http.jwt_secret = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read HTTP JWT secret file {path}: {e}"))?
            .trim_end_matches(['\r', '\n'])
            .to_owned();
    }
    Ok(cfg)
}
