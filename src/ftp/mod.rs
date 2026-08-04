use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use libunftp::auth::{AuthenticationError, Authenticator, Credentials, DefaultUser};
use libunftp::storage::{
    Error, ErrorKind, Fileinfo, Metadata, Result, StorageBackend, FEATURE_RESTART,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
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
        DataProviderError::NotFound => Error::new(ErrorKind::PermanentFileNotAvailable, e),
        DataProviderError::AlreadyExists => Error::new(ErrorKind::FileNameNotAllowedError, e),
        DataProviderError::PermissionDenied => Error::new(ErrorKind::PermissionDenied, e),
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
        let raw = p.as_ref().to_string_lossy().replace('\\', "/");
        if raw.is_empty() || raw == "." {
            "/".to_owned()
        } else if raw.starts_with('/') {
            raw
        } else {
            format!("/{raw}")
        }
    }

    fn synthetic_root() -> crate::dataprovider::types::File {
        crate::dataprovider::types::File {
            id: "root".into(),
            name: "/".into(),
            dir: true,
            size: 0,
            parent: None,
            mtime: chrono::Utc::now(),
        }
    }
}

async fn copy_to_writer<W, R>(writer: &mut W, reader: &mut R) -> Result<u64>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    tokio::io::copy(reader, writer)
        .await
        .map_err(|e| Error::new(ErrorKind::LocalError, e))
}

#[async_trait]
impl StorageBackend<DefaultUser> for DdrvStorage {
    type Metadata = DdrvMetadata;

    fn supported_features(&self) -> u32 {
        FEATURE_RESTART
    }

    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<Self::Metadata> {
        let dp = crate::dataprovider::get();
        let path = Self::path_str(path);
        let file = match dp.stat(&path).await {
            Ok(file) => file,
            Err(DataProviderError::NotFound) if path == "/" => Self::synthetic_root(),
            Err(e) => return Err(dp_err(e)),
        };
        Ok(DdrvMetadata { inner: file })
    }

    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
        let dp = crate::dataprovider::get();
        let path = Self::path_str(path);
        let files = match dp.ls(&path, 0, 0).await {
            Ok(files) => files,
            // Older migrated databases may not contain an explicit root row;
            // get_children still knows how to enumerate its direct children.
            Err(DataProviderError::NotFound) if path == "/" => {
                dp.get_children("root").await.map_err(dp_err)?
            }
            Err(e) => return Err(dp_err(e)),
        };

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
        let (mut tx, rx) = tokio::io::duplex(256 * 1024);
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut reader, &mut tx).await;
        });
        Ok(Box::new(rx))
    }

    async fn put<
        P: AsRef<Path> + Send + Debug,
        R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
    >(
        &self,
        _user: &DefaultUser,
        mut input: R,
        path: P,
        start_pos: u64,
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

        let file_size = u64::try_from(file.size).map_err(|_| {
            Error::new(
                ErrorKind::LocalError,
                std::io::Error::new(std::io::ErrorKind::InvalidData, "negative FTP file size"),
            )
        })?;
        if start_pos > file_size {
            return Err(Error::new(
                ErrorKind::LocalError,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "FTP REST offset is beyond the end of the file",
                ),
            ));
        }

        // REST + STOR rewrites the file from the requested byte offset. Rebuild
        // the prefix through the same writer before consuming the new suffix.
        let resume_nodes = if start_pos > 0 {
            let mut nodes = dp.get_nodes(&file.id).await.map_err(dp_err)?;
            self.driver
                .update_nodes(&mut nodes)
                .await
                .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
            Some(nodes)
        } else {
            None
        };

        dp.truncate(&file.id).await.map_err(|e| {
            error!(path = %path_s, file_id = %file.id, error = %e, "FTP upload truncate failed");
            dp_err(e)
        })?;
        debug!(path = %path_s, file_id = %file.id, "FTP upload file prepared");

        let nodes = Arc::new(Mutex::new(Vec::<crate::ddrv::types::Node>::new()));
        let total: u64;

        if self.async_write {
            let nodes_cb = Arc::clone(&nodes);
            let mut writer = self.driver.new_nwriter(move |node| {
                nodes_cb.lock().expect("nodes mutex poisoned").push(node);
            });
            if let Some(nodes) = resume_nodes {
                let prefix = self
                    .driver
                    .new_reader(nodes, 0)
                    .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
                let mut prefix = prefix.take(start_pos);
                copy_to_writer(&mut writer, &mut prefix)
                    .await
                    .map_err(|e| {
                        error!(path = %path_s, error = %e, "FTP REST prefix write failed");
                        e
                    })?;
            }
            total = copy_to_writer(&mut writer, &mut input).await.map_err(|e| {
                error!(path = %path_s, error = %e, "FTP upload write failed");
                e
            })?;
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
            if let Some(nodes) = resume_nodes {
                let prefix = self
                    .driver
                    .new_reader(nodes, 0)
                    .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
                let mut prefix = prefix.take(start_pos);
                copy_to_writer(&mut writer, &mut prefix)
                    .await
                    .map_err(|e| {
                        error!(path = %path_s, error = %e, "FTP REST prefix write failed");
                        e
                    })?;
            }
            total = copy_to_writer(&mut writer, &mut input).await.map_err(|e| {
                error!(path = %path_s, error = %e, "FTP upload write failed");
                e
            })?;
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

    async fn del<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, path: P) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.rm(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn mkd<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, path: P) -> Result<()> {
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

    async fn rmd<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, path: P) -> Result<()> {
        let dp = crate::dataprovider::get();
        dp.rm(&Self::path_str(path)).await.map_err(dp_err)
    }

    async fn cwd<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, path: P) -> Result<()> {
        let dp = crate::dataprovider::get();
        let path = Self::path_str(path);
        match dp.stat(&path).await {
            Ok(_) => {}
            Err(DataProviderError::NotFound) if path == "/" => {}
            Err(e) => return Err(dp_err(e)),
        }
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

    let mut builder = libunftp::ServerBuilder::new(Box::new(move || storage.clone()))
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

    // A loopback listener must advertise the loopback/peer address in PASV
    // responses. Advertising the machine's public IP here breaks local FTP
    // clients: the control connection succeeds, but LIST/RETR cannot open the
    // passive data connection. Public-IP advertisement remains useful for
    // wildcard/external listeners behind NAT.
    let bind_host = config
        .addr
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']))
        .unwrap_or(config.addr.as_str());
    let is_loopback = matches!(bind_host, "127.0.0.1" | "localhost" | "::1");
    if !is_loopback {
        if let Some(ip) = fetch_public_ip().await {
            builder = builder.passive_host(libunftp::options::PassiveHost::Ip(ip));
        }
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
    let resp = client.get("https://ipinfo.io/ip").send().await.ok()?;
    let text = resp.text().await.ok()?;
    text.trim().parse().ok()
}

#[cfg(test)]
mod protocol_tests {
    use super::*;
    use async_trait::async_trait;
    use libunftp::auth::{AuthenticationError, Authenticator, Credentials, DefaultUser};
    use libunftp::storage::{Fileinfo, StorageBackend, FEATURE_RESTART};
    use std::io::Cursor;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::SystemTime;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    #[derive(Clone, Debug)]
    struct MockStorage {
        state: Arc<Mutex<MockState>>,
    }

    #[derive(Debug)]
    struct MockState {
        data: Vec<u8>,
        last_start_pos: Option<u64>,
    }

    #[derive(Debug)]
    struct MockMetadata {
        len: u64,
    }

    impl Metadata for MockMetadata {
        fn len(&self) -> u64 {
            self.len
        }

        fn is_dir(&self) -> bool {
            false
        }

        fn is_file(&self) -> bool {
            true
        }

        fn is_symlink(&self) -> bool {
            false
        }

        fn modified(&self) -> Result<SystemTime> {
            Ok(SystemTime::UNIX_EPOCH)
        }

        fn gid(&self) -> u32 {
            0
        }

        fn uid(&self) -> u32 {
            0
        }
    }

    fn mock_error(message: &str) -> Error {
        Error::new(ErrorKind::LocalError, std::io::Error::other(message))
    }

    #[async_trait]
    impl StorageBackend<DefaultUser> for MockStorage {
        type Metadata = MockMetadata;

        fn supported_features(&self) -> u32 {
            FEATURE_RESTART
        }

        async fn metadata<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _path: P,
        ) -> Result<Self::Metadata> {
            let len = self.state.lock().unwrap().data.len() as u64;
            Ok(MockMetadata { len })
        }

        async fn list<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _path: P,
        ) -> Result<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
            Ok(Vec::new())
        }

        async fn get<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _path: P,
            start_pos: u64,
        ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>> {
            let data = self.state.lock().unwrap().data.clone();
            let start = usize::try_from(start_pos).map_err(|_| mock_error("offset overflow"))?;
            if start > data.len() {
                return Err(mock_error("offset beyond EOF"));
            }
            Ok(Box::new(Cursor::new(data[start..].to_vec())))
        }

        async fn put<
            P: AsRef<Path> + Send + Debug,
            R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
        >(
            &self,
            _user: &DefaultUser,
            mut input: R,
            _path: P,
            start_pos: u64,
        ) -> Result<u64> {
            let mut suffix = Vec::new();
            input
                .read_to_end(&mut suffix)
                .await
                .map_err(|e| Error::new(ErrorKind::LocalError, e))?;
            let mut state = self.state.lock().unwrap();
            let start = usize::try_from(start_pos).map_err(|_| mock_error("offset overflow"))?;
            if start > state.data.len() {
                return Err(mock_error("offset beyond EOF"));
            }
            state.data.truncate(start);
            state.data.extend_from_slice(&suffix);
            state.last_start_pos = Some(start_pos);
            Ok(suffix.len() as u64)
        }

        async fn del<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _path: P,
        ) -> Result<()> {
            Err(mock_error("unused"))
        }

        async fn mkd<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _path: P,
        ) -> Result<()> {
            Err(mock_error("unused"))
        }

        async fn rename<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _from: P,
            _to: P,
        ) -> Result<()> {
            Err(mock_error("unused"))
        }

        async fn rmd<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _path: P,
        ) -> Result<()> {
            Err(mock_error("unused"))
        }

        async fn cwd<P: AsRef<Path> + Send + Debug>(
            &self,
            _user: &DefaultUser,
            _path: P,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct MockAuthenticator;

    #[async_trait]
    impl Authenticator<DefaultUser> for MockAuthenticator {
        async fn authenticate(
            &self,
            _username: &str,
            _creds: &Credentials,
        ) -> std::result::Result<DefaultUser, AuthenticationError> {
            Ok(DefaultUser)
        }
    }

    async fn read_reply(control: &mut BufReader<TcpStream>) -> String {
        let mut line = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            control.read_line(&mut line),
        )
        .await
        .expect("FTP control reply timed out")
        .expect("FTP control reply failed");
        line
    }

    async fn command(control: &mut BufReader<TcpStream>, command: &str) -> String {
        control
            .get_mut()
            .write_all(format!("{command}\r\n").as_bytes())
            .await
            .unwrap();
        control.get_mut().flush().await.unwrap();
        read_reply(control).await
    }

    async fn multiline_command(control: &mut BufReader<TcpStream>, line: &str) -> String {
        let first = command(control, line).await;
        if first.len() >= 4 && first.as_bytes()[3] == b'-' {
            let code = first[..3].to_owned();
            let mut output = first;
            loop {
                let line = read_reply(control).await;
                let done = line.starts_with(&format!("{code} "));
                output.push_str(&line);
                if done {
                    return output;
                }
            }
        }
        first
    }

    fn passive_addr(reply: &str) -> SocketAddr {
        let values = reply
            .split_once('(')
            .and_then(|(_, rest)| rest.split_once(')'))
            .expect("invalid PASV response")
            .0
            .split(',')
            .map(|v| v.parse::<u16>().expect("invalid PASV value"))
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 6);
        SocketAddr::from((
            [
                values[0] as u8,
                values[1] as u8,
                values[2] as u8,
                values[3] as u8,
            ],
            values[4] * 256 + values[5],
        ))
    }

    async fn connect_control(addr: SocketAddr) -> BufReader<TcpStream> {
        for _ in 0..50 {
            if let Ok(stream) = TcpStream::connect(addr).await {
                return BufReader::new(stream);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("FTP server did not accept a connection")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ftp_protocol_supports_size_and_restart_for_retr_and_stor() {
        let state = Arc::new(Mutex::new(MockState {
            data: b"hello world".to_vec(),
            last_start_pos: None,
        }));
        let storage = MockStorage {
            state: Arc::clone(&state),
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let server = libunftp::ServerBuilder::new(Box::new(move || storage.clone()))
            .authenticator(Arc::new(MockAuthenticator))
            .greeting("FTP protocol test")
            .build()
            .unwrap();
        let server_task = tokio::spawn(server.listen(addr.to_string()));

        let mut control = connect_control(addr).await;
        assert!(read_reply(&mut control).await.starts_with("220"));
        assert!(command(&mut control, "USER test").await.starts_with("331"));
        assert!(command(&mut control, "PASS test").await.starts_with("230"));

        let feat = multiline_command(&mut control, "FEAT").await;
        assert!(feat.contains(" SIZE\r\n"));
        assert!(feat.contains(" REST STREAM\r\n"));
        assert_eq!(command(&mut control, "SIZE file").await, "213 11\r\n");

        assert!(command(&mut control, "REST 6").await.starts_with("350"));
        let pasv = command(&mut control, "PASV").await;
        let mut data = TcpStream::connect(passive_addr(&pasv)).await.unwrap();
        assert!(command(&mut control, "RETR file").await.starts_with("150"));
        let mut downloaded = Vec::new();
        data.read_to_end(&mut downloaded).await.unwrap();
        assert_eq!(downloaded, b"world");
        assert!(read_reply(&mut control).await.starts_with("226"));

        assert!(command(&mut control, "REST 6").await.starts_with("350"));
        let pasv = command(&mut control, "PASV").await;
        let mut data = TcpStream::connect(passive_addr(&pasv)).await.unwrap();
        assert!(command(&mut control, "STOR file").await.starts_with("150"));
        data.write_all(b"DDRV").await.unwrap();
        data.shutdown().await.unwrap();
        assert!(read_reply(&mut control).await.starts_with("226"));

        let (last_start_pos, final_data) = {
            let final_state = state.lock().unwrap();
            (final_state.last_start_pos, final_state.data.clone())
        };
        assert_eq!(last_start_pos, Some(6));
        assert_eq!(final_data, b"hello DDRV");

        let _ = command(&mut control, "QUIT").await;
        server_task.abort();
    }
}
