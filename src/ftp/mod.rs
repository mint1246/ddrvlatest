use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use libunftp::auth::{AuthenticationError, Authenticator, Credentials, DefaultUser};
use libunftp::storage::{Error, ErrorKind, Fileinfo, Metadata, Result, StorageBackend};
use tokio::io::AsyncReadExt;
use tracing::info;

use crate::dataprovider::types::DataProviderError;

// ── Metadata ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct DdrvMetadata {
    pub inner: crate::dataprovider::types::File,
}

impl Metadata for DdrvMetadata {
    fn len(&self) -> u64 {
        self.inner.size as u64
    }

    fn is_dir(&self) -> bool {
        self.inner.dir
    }

    fn is_file(&self) -> bool {
        !self.inner.dir
    }

    fn is_symlink(&self) -> bool {
        false
    }

    fn modified(&self) -> Result<std::time::SystemTime> {
        Ok(self.inner.mtime.into())
    }

    fn gid(&self) -> u32 {
        0
    }

    fn uid(&self) -> u32 {
        0
    }
}

// ── Error conversion ──────────────────────────────────────────────────────────

fn dp_err(e: DataProviderError) -> Error {
    match e {
        DataProviderError::NotFound => {
            Error::new(ErrorKind::PermanentFileNotAvailable, e)
        }
        DataProviderError::AlreadyExists => {
            Error::new(ErrorKind::FileNameNotAllowed, e)
        }
        DataProviderError::PermissionDenied => {
            Error::new(ErrorKind::PermissionDenied, e)
        }
        _ => Error::new(ErrorKind::LocalError, e),
    }
}

// ── Storage backend ───────────────────────────────────────────────────────────

pub struct DdrvStorage {
    driver: Arc<crate::ddrv::Driver>,
    async_write: bool,
}

impl DdrvStorage {
    fn path_str<P: AsRef<Path>>(p: P) -> String {
        p.as_ref().to_string_lossy().into_owned()
    }
}

#[async_trait]
impl StorageBackend<DefaultUser> for DdrvStorage {
    type Metadata = DdrvMetadata;

    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        path: P,
    ) -> Result<Self::Metadata> {
        let dp = crate::dataprovider::get();
        let file = dp
            .stat(&Self::path_str(path))
            .await
            .map_err(dp_err)?;
        Ok(DdrvMetadata { inner: file })
    }

    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        path: P,
    ) -> Result<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
        let dp = crate::dataprovider::get();
        let files = dp
            .ls(&Self::path_str(path), 0, 0)
            .await
            .map_err(dp_err)?;

        let result = files
            .into_iter()
            .map(|f| {
                let name = f.name.clone();
                Fileinfo {
                    path: PathBuf::from(name),
                    metadata: DdrvMetadata { inner: f },
                }
            })
            .collect();
        Ok(result)
    }

    async fn get<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        path: P,
        start_pos: u64,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin + 'static>> {
        let dp = crate::dataprovider::get();
        let file = dp.stat(&Self::path_str(&path)).await.map_err(dp_err)?;

        let mut nodes = dp.get_nodes(&file.id).await.map_err(dp_err)?;

        // Refresh expired CDN URLs
        self.driver
            .update_nodes(&mut nodes)
            .await
            .map_err(|e| Error::new(ErrorKind::LocalError, e))?;

        let reader = self
            .driver
            .new_reader(nodes, start_pos as i64)
            .map_err(|e| Error::new(ErrorKind::LocalError, e))?;

        Ok(Box::new(reader))
    }

    async fn put<P: AsRef<Path> + Send + Debug, R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static>(
        &self,
        _user: &Option<DefaultUser>,
        mut input: R,
        path: P,
        _start_pos: u64,
    ) -> Result<u64> {
        let dp = crate::dataprovider::get();
        let path_s = Self::path_str(path);

        // Ensure the file record exists
        dp.touch(&path_s).await.map_err(dp_err)?;
        let file = dp.stat(&path_s).await.map_err(dp_err)?;
        dp.truncate(&file.id).await.map_err(dp_err)?;

        let mut nodes: Vec<crate::ddrv::types::Node> = Vec::new();
        let mut total: u64 = 0;

        if self.async_write {
            let mut writer = self.driver.new_nwriter(|node| nodes.push(node));
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = input
                    .read(&mut buf)
                    .await
                    .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
                if n == 0 {
                    break;
                }
                tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n])
                    .await
                    .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
                total += n as u64;
            }
            tokio::io::AsyncWriteExt::shutdown(&mut writer)
                .await
                .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
        } else {
            let mut writer = self.driver.new_writer(|node| nodes.push(node));
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = input
                    .read(&mut buf)
                    .await
                    .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
                if n == 0 {
                    break;
                }
                tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n])
                    .await
                    .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
                total += n as u64;
            }
            tokio::io::AsyncWriteExt::shutdown(&mut writer)
                .await
                .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
        }

        dp.create_nodes(&file.id, &nodes).await.map_err(dp_err)?;

        Ok(total)
    }

    async fn del<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.rm(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn mkd<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.mkdir(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        from: P,
        to: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.mv(&Self::path_str(from), &Self::path_str(to))
            .await
            .map_err(dp_err)
    }

    async fn rmd<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.rm(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn cwd<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<DefaultUser>,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.stat(&Self::path_str(path)).await.map_err(dp_err)?;
        Ok(())
    }
}

// ── Authenticator ─────────────────────────────────────────────────────────────

pub struct DdrvAuthenticator {
    pub username: String,
    pub password: String,
}

#[async_trait]
impl Authenticator<DefaultUser> for DdrvAuthenticator {
    async fn authenticate(
        &self,
        username: &str,
        creds: &Credentials,
    ) -> std::result::Result<DefaultUser, AuthenticationError> {
        // If both username and password are empty, allow all connections
        if self.username.is_empty() && self.password.is_empty() {
            return Ok(DefaultUser);
        }

        let password_match = match &creds.password {
            Some(p) => p.as_str() == self.password,
            None => self.password.is_empty(),
        };

        if username == self.username && password_match {
            Ok(DefaultUser)
        } else {
            Err(AuthenticationError::BadPassword)
        }
    }
}

// ── Server entry point ────────────────────────────────────────────────────────

pub async fn serve(
    driver: Arc<crate::ddrv::Driver>,
    config: &crate::config::FtpConfig,
) -> anyhow::Result<()> {
    if config.addr.is_empty() {
        return Ok(());
    }

    let storage = Arc::new(DdrvStorage {
        driver: driver.clone(),
        async_write: config.async_write,
    });

    let authenticator = DdrvAuthenticator {
        username: config.username.clone(),
        password: config.password.clone(),
    };

    let mut builder = libunftp::ServerBuilder::new(Box::new(move || {
        Arc::clone(&storage)
    }))
    .greeting("DDrv FTP Server")
    .idle_session_timeout(std::time::Duration::from_secs(86400))
    .authenticator(Arc::new(authenticator));

    if let Some(port_range) = &config.port_range {
        let parts: Vec<&str> = port_range.splitn(2, '-').collect();
        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                builder = builder.passive_ports(start..=end);
            }
        }
    }

    // Fetch public IP for passive mode
    let public_ip = fetch_public_ip().await;
    if let Some(ip) = public_ip {
        builder = builder.passive_host(libunftp::options::PassiveHost::Ip(ip));
    }

    let server = builder.build()?;

    info!("Starting FTP server on {}", config.addr);
    server.listen(&config.addr).await?;
    Ok(())
}

async fn fetch_public_ip() -> Option<std::net::Ipv4Addr> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;
    let resp = client
        .get("https://ipinfo.io/ip")
        .send()
        .await
        .ok()?;
    let text = resp.text().await.ok()?;
    text.trim().parse().ok()
}
