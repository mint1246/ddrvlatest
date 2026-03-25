use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use libunftp::auth::{AuthenticationError, Authenticator, Credentials, DefaultUser};
use libunftp::storage::{Error, ErrorKind, Fileinfo, Metadata, Result, StorageBackend};
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info};

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
            Error::new(ErrorKind::FileNameNotAllowedError, e)
        }
        DataProviderError::PermissionDenied => {
            Error::new(ErrorKind::PermissionDenied, e)
        }
        _ => Error::new(ErrorKind::LocalError, e),
    }
}

// ── Storage backend ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DdrvStorage {
    driver: Arc<crate::ddrv::Driver>,
    async_write: bool,
}

impl std::fmt::Debug for DdrvStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdrvStorage")
            .field("async_write", &self.async_write)
            .finish()
    }
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
        _user: &DefaultUser,
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
        _user: &DefaultUser,
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
        _user: &DefaultUser,
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

        let mut reader = self
            .driver
            .new_reader(nodes, start_pos as i64)
            .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
        let (mut tx, rx) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut reader, &mut tx).await;
        });
        Ok(Box::new(rx))
    }

    async fn put<P: AsRef<Path> + Send + Debug, R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static>(
        &self,
        _user: &DefaultUser,
        mut input: R,
        path: P,
        _start_pos: u64,
    ) -> Result<u64> {
        let dp = crate::dataprovider::get();
        let path_s = Self::path_str(path);
        info!(path = %path_s, async_write = self.async_write, "FTP upload started");

        // Ensure the file record exists
        dp.touch(&path_s).await.map_err(|e| {
            error!(path = %path_s, error = %e, "FTP upload touch failed");
            dp_err(e)
        })?;
        let file = dp.stat(&path_s).await.map_err(|e| {
            error!(path = %path_s, error = %e, "FTP upload stat failed");
            dp_err(e)
        })?;
        dp.truncate(&file.id).await.map_err(|e| {
            error!(path = %path_s, file_id = %file.id, error = %e, "FTP upload truncate failed");
            dp_err(e)
        })?;
        debug!(path = %path_s, file_id = %file.id, "FTP upload file prepared");

        let nodes = Arc::new(Mutex::new(Vec::<crate::ddrv::types::Node>::new()));
        let mut total: u64 = 0;

        if self.async_write {
            let nodes_cb = Arc::clone(&nodes);
            let mut writer = self.driver.new_nwriter(move |node| {
                nodes_cb.lock().expect("nodes mutex poisoned").push(node);
            });
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = input
                    .read(&mut buf)
                    .await
                    .map_err(|e| {
                        error!(path = %path_s, bytes_written = total, error = %e, "FTP upload read failed");
                        Error::new(ErrorKind::LocalError, e)
                    })?;
                if n == 0 {
                    break;
                }
                tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n])
                    .await
                    .map_err(|e| {
                        error!(path = %path_s, bytes_written = total, chunk_size = n, error = %e, "FTP upload write failed");
                        Error::new(ErrorKind::LocalError, e)
                    })?;
                total += n as u64;
            }
            tokio::io::AsyncWriteExt::shutdown(&mut writer)
                .await
                .map_err(|e| {
                    error!(path = %path_s, bytes_written = total, error = %e, "FTP upload writer shutdown failed");
                    Error::new(ErrorKind::LocalError, e)
                })?;
        } else {
            let nodes_cb = Arc::clone(&nodes);
            let mut writer = self.driver.new_writer(move |node| {
                nodes_cb.lock().expect("nodes mutex poisoned").push(node);
            });
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = input
                    .read(&mut buf)
                    .await
                    .map_err(|e| {
                        error!(path = %path_s, bytes_written = total, error = %e, "FTP upload read failed");
                        Error::new(ErrorKind::LocalError, e)
                    })?;
                if n == 0 {
                    break;
                }
                tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n])
                    .await
                    .map_err(|e| {
                        error!(path = %path_s, bytes_written = total, chunk_size = n, error = %e, "FTP upload write failed");
                        Error::new(ErrorKind::LocalError, e)
                    })?;
                total += n as u64;
            }
            tokio::io::AsyncWriteExt::shutdown(&mut writer)
                .await
                .map_err(|e| {
                    error!(path = %path_s, bytes_written = total, error = %e, "FTP upload writer shutdown failed");
                    Error::new(ErrorKind::LocalError, e)
                })?;
        }

        let final_nodes = nodes.lock().expect("nodes mutex poisoned").clone();
        dp.create_nodes(&file.id, &final_nodes).await.map_err(|e| {
            error!(
                path = %path_s,
                file_id = %file.id,
                bytes_written = total,
                node_count = final_nodes.len(),
                error = %e,
                "FTP upload node persistence failed"
            );
            dp_err(e)
        })?;

        info!(
            path = %path_s,
            file_id = %file.id,
            bytes_written = total,
            node_count = final_nodes.len(),
            "FTP upload completed"
        );

        Ok(total)
    }

    async fn del<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.rm(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn mkd<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.mkdir(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &DefaultUser,
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
        _user: &DefaultUser,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.rm(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn cwd<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.stat(&Self::path_str(path)).await.map_err(dp_err)?;
        Ok(())
    }
}

// ── Authenticator ─────────────────────────────────────────────────────────────

#[derive(Debug)]
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

    let storage = DdrvStorage {
        driver: driver.clone(),
        async_write: config.async_write,
    };

    let authenticator = DdrvAuthenticator {
        username: config.username.clone(),
        password: config.password.clone(),
    };

    let mut builder = libunftp::ServerBuilder::new(Box::new(move || {
        storage.clone()
    }))
    .greeting("DDrv FTP Server")
    .idle_session_timeout(86400)
    .authenticator(Arc::new(authenticator));

    if let Some(port_range) = &config.port_range {
        let parts: Vec<&str> = port_range.splitn(2, '-').collect();
        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                if start < end {
                    builder = builder.passive_ports(start..end);
                }
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
