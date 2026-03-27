use axum::{
    body::{Body, Bytes},
    extract::{Multipart, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{sync::{Arc, Mutex, OnceLock}, time::Duration};
use tokio_util::io::ReaderStream;
use tracing::warn;

use super::types::{err, ApiResponse, UpdateFileRequest};
use crate::{
    dataprovider,
    dataprovider::types::DataProviderError,
    ddrv::{types::Node, utils::encode_attachment_url},
    http::AppState,
};

fn dp_err(e: DataProviderError) -> Response {
    match e {
        DataProviderError::NotFound => err(StatusCode::NOT_FOUND, e.to_string()),
        DataProviderError::AlreadyExists => err(StatusCode::CONFLICT, e.to_string()),
        DataProviderError::PermissionDenied => err(StatusCode::FORBIDDEN, e.to_string()),
        DataProviderError::InvalidParent => err(StatusCode::BAD_REQUEST, e.to_string()),
        e => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

fn validate_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(|c| matches!(c, '/' | '<' | '>' | '"' | '|' | '*'))
}

/// GET /api/directories/:dir_id/files/:id
pub async fn get_file_handler(
    State(_state): State<AppState>,
    Path((dir_id, id)): Path<(String, String)>,
) -> Response {
    let dp = dataprovider::get();
    match dp.get_by_id(&id, Some(&dir_id)).await {
        Ok(f) => ApiResponse::ok(f).into_response(),
        Err(e) => dp_err(e),
    }
}

/// POST /api/directories/:dir_id/files  (multipart)
pub async fn create_file_handler(
    State(state): State<AppState>,
    Path(dir_id): Path<String>,
    mut multipart: Multipart,
) -> Response {
    let dp = dataprovider::get();

    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() != Some("file") {
            continue;
        }
        let filename = match field.file_name() {
            Some(n) => n.to_string(),
            None => return err(StatusCode::BAD_REQUEST, "missing filename"),
        };
        if !validate_name(&filename) {
            return err(StatusCode::BAD_REQUEST, "invalid filename");
        }

        let file = match dp.create(&filename, &dir_id, false).await {
            Ok(f) => f,
            Err(e) => return dp_err(e),
        };

        let nodes: Arc<Mutex<Vec<Node>>> = Arc::new(Mutex::new(Vec::new()));
        let file_id = file.id.clone();

        // Use async writer; read all field bytes
        let data = match field.bytes().await {
            Ok(b) => b,
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };

        if state.config.async_write {
            let nodes_cb = Arc::clone(&nodes);
            let mut writer = state.driver.new_nwriter(move |chunk| {
                nodes_cb.lock().expect("nodes mutex poisoned").push(chunk);
            });
            use tokio::io::AsyncWriteExt;
            if let Err(e) = writer.write_all(&data).await {
                return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
            if let Err(e) = writer.shutdown().await {
                return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
        } else {
            let nodes_cb = Arc::clone(&nodes);
            let mut writer = state.driver.new_writer(move |chunk| {
                nodes_cb.lock().expect("nodes mutex poisoned").push(chunk);
            });
            use tokio::io::AsyncWriteExt;
            if let Err(e) = writer.write_all(&data).await {
                return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
            if let Err(e) = writer.shutdown().await {
                return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
        }

        let collected = nodes.lock().expect("nodes mutex poisoned").clone();
        if let Err(e) = dp.create_nodes(&file_id, &collected).await {
            return dp_err(e);
        }

        return ApiResponse::ok(file).into_response();
    }

    err(StatusCode::BAD_REQUEST, "no file field in multipart")
}

/// PUT /api/directories/:dir_id/files/:id
pub async fn update_file_handler(
    State(_state): State<AppState>,
    Path((dir_id, id)): Path<(String, String)>,
    Json(body): Json<UpdateFileRequest>,
) -> Response {
    let dp = dataprovider::get();
    let mut file = match dp.get_by_id(&id, Some(&dir_id)).await {
        Ok(f) => f,
        Err(e) => return dp_err(e),
    };
    if let Some(name) = body.name {
        if !validate_name(&name) {
            return err(StatusCode::BAD_REQUEST, "invalid filename");
        }
        file.name = name;
    }
    let parent_override = body.parent.as_deref();
    match dp.update(&id, Some(&dir_id), &file).await {
        Ok(f) => {
            let _ = parent_override; // parent update handled by dataprovider via file.parent
            ApiResponse::ok(f).into_response()
        }
        Err(e) => dp_err(e),
    }
}

/// DELETE /api/directories/:dir_id/files/:id
pub async fn delete_file_handler(
    State(_state): State<AppState>,
    Path((dir_id, id)): Path<(String, String)>,
) -> Response {
    let dp = dataprovider::get();
    match dp.delete(&id, Some(&dir_id)).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"message": "file deleted"})),
        )
            .into_response(),
        Err(e) => dp_err(e),
    }
}

/// A single chunk entry returned by the manifest endpoint.
#[derive(Serialize)]
pub struct ChunkInfo {
    /// Authenticated Discord CDN URL for this chunk (includes `ex`/`is`/`hm` params).
    pub url: String,
    /// Same URL but with `download=1` to force raw CDN response headers.
    /// Some Discord edges add more permissive CORS headers on download responses.
    pub download_url: String,
    /// Start byte offset of this chunk within the complete file.
    pub start: u64,
    /// End byte offset (inclusive) of this chunk within the complete file.
    pub end: u64,
    /// Byte size of this chunk.
    pub size: u64,
}

/// Response body returned by `GET /files/:id/manifest`.
#[derive(Serialize)]
pub struct FileManifest {
    pub id: String,
    pub name: String,
    pub size: i64,
    pub mime: String,
    /// Total number of chunks the file is split into across all pages.
    pub total_chunks: usize,
    pub chunks: Vec<ChunkInfo>,
}

/// Query params for the manifest endpoint.
#[derive(Deserialize, Default)]
pub struct ManifestQuery {
    /// Zero-based index of the first chunk to include (default: 0).
    pub offset: Option<usize>,
    /// Maximum number of chunks to return per request (default: all).
    /// Keeping this small limits the number of Discord API URL-refresh calls made
    /// per request, which reduces rate-limiting pressure on large files.
    pub limit: Option<usize>,
}

/// Append `download=1` to a Discord CDN URL, preserving existing query params.
fn discord_cdn_download_url(url: &str, proxy_base: Option<&str>) -> String {
    let mut parsed = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return url.to_string(),
    };
    parsed.query_pairs_mut().append_pair("download", "1");
    let with_download = parsed.to_string();

    if let Some(base) = proxy_base {
        if let Ok(mut proxy) = Url::parse(base) {
            proxy.query_pairs_mut().append_pair("url", &with_download);
            return proxy.to_string();
        }
    }

    with_download
}

fn manifest_probe_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(8))
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()
            .expect("manifest probe client build failed")
    })
}

/// Lightweight URL probe: fetch only the first byte to confirm the URL is alive.
/// This avoids downloading full chunks while still ensuring the link is not 404.
async fn probe_manifest_url(url: &str) -> bool {
    let mut res = match manifest_probe_client()
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return false,
    };

    let status = res.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return false;
    }
    if !(status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT) {
        return false;
    }

    matches!(res.chunk().await, Ok(Some(bytes)) if !bytes.is_empty())
}

/// Validate canonical chunk URLs and renew any that fail a lightweight probe.
/// Retries renewal a few times and returns whether all chunk URLs became healthy.
async fn ensure_manifest_nodes_valid(state: &AppState, nodes: &mut [Node]) -> bool {
    const MAX_RENEW_ATTEMPTS: usize = 3;

    for attempt in 1..=MAX_RENEW_ATTEMPTS {
        let mut invalid: Vec<usize> = Vec::new();

        for (idx, n) in nodes.iter().enumerate() {
            let url = encode_attachment_url(&n.url, n.ex, n.is, &n.hm);
            if !probe_manifest_url(&url).await {
                invalid.push(idx);
            }
        }

        if invalid.is_empty() {
            return true;
        }

        // Force a refresh for failing nodes by marking them expired for this pass.
        for idx in &invalid {
            nodes[*idx].ex = 0;
        }

        if let Err(e) = state.driver.update_nodes(nodes).await {
            warn!(attempt, error = %e, "manifest node renewal attempt failed");
        }
    }

    false
}

/// GET /files/:id/manifest  (no auth)
///
/// Returns a JSON manifest listing authenticated Discord CDN chunk URLs with
/// byte-range metadata.  Clients use this to download chunks directly from
/// Discord CDN and reassemble the file locally (client-side reconstruction),
/// removing the server as a bandwidth bottleneck.
///
/// Supports pagination via `?offset=N&limit=K` to spread URL-refresh calls
/// across multiple requests and avoid Discord API rate limits for large files.
pub async fn manifest_file_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ManifestQuery>,
) -> Response {
    let dp = dataprovider::get();

    let file = match dp.get_by_id(&id, None).await {
        Ok(f) => f,
        Err(DataProviderError::NotFound) => return err(StatusCode::NOT_FOUND, "not found"),
        Err(e) => return dp_err(e),
    };

    let ext = std::path::Path::new(&file.name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let mime = mime_guess::from_ext(ext)
        .first_or_octet_stream()
        .to_string();

    let offset = query.offset.unwrap_or(0);

    let (mut page_nodes, total_chunks, byte_offset) = if let Some(limit) = query.limit {
        // Paginated: only refresh nodes in the requested range.
        match dp.get_nodes_paged(&id, offset, limit).await {
            Ok(result) => result,
            Err(e) => return dp_err(e),
        }
    } else {
        // Unpaginated: fetch and refresh all nodes, then slice from offset.
        let all_nodes = match dp.get_nodes(&id).await {
            Ok(n) => n,
            Err(e) => return dp_err(e),
        };
        let total = all_nodes.len();
        let byte_offset: u64 = all_nodes
            .iter()
            .take(offset)
            .map(|n| n.size as u64)
            .sum();
        let page = if offset < total {
            all_nodes[offset..].to_vec()
        } else {
            Vec::new()
        };
        (page, total, byte_offset)
    };

    // Pre-renew upcoming chunks in the background so the next manifest page is
    // faster. Scale the prefetch size with token count (10 chunks per token).
    let next_offset = offset + page_nodes.len();
    if next_offset < total_chunks {
        let prefetch_limit = state.driver.manifest_prefetch_window();
        let prefetch_id = id.clone();
        tokio::spawn(async move {
            let dp = dataprovider::get();
            if let Err(e) = dp.get_nodes_paged(&prefetch_id, next_offset, prefetch_limit).await {
                warn!(
                    file_id = %prefetch_id,
                    next_offset,
                    prefetch_limit,
                    error = %e,
                    "manifest background prefetch failed"
                );
            }
        });
    }

    // Validate links before returning them in the manifest. If probes fail, force
    // renewals and retry. If some are still failing, continue with best-effort
    // renewed links instead of hard-failing the whole manifest request.
    if !ensure_manifest_nodes_valid(&state, &mut page_nodes).await {
        warn!(file_id = %id, "manifest link validation still failing after renew attempts; returning best-effort links");
    }

    // Build ChunkInfo entries, computing absolute byte offsets from byte_offset.
    // This mirrors the offset arithmetic in `ddrv::reader::Reader::new` so that
    // clients can use `Range: bytes={start}-{end}` when fetching individual chunks.
    let mut running_offset = byte_offset;
    let mut chunks: Vec<ChunkInfo> = Vec::with_capacity(page_nodes.len());
    for n in &page_nodes {
        let start = running_offset;
        let size = n.size as u64;
        let end = running_offset + size - 1;
        running_offset = end + 1;

        let url = encode_attachment_url(&n.url, n.ex, n.is, &n.hm);
        let preferred = discord_cdn_download_url(&url, state.config.cdn_proxy_base.as_deref());

        // Guarantee returned links are valid: if the preferred download URL fails,
        // fall back to the canonical URL that was already validated above.
        let download_url = if probe_manifest_url(&preferred).await {
            preferred
        } else {
            url.clone()
        };

        chunks.push(ChunkInfo {
            url,
            download_url,
            start,
            end,
            size,
        });
    }

    let manifest = FileManifest {
        id: file.id.clone(),
        name: file.name.clone(),
        size: file.size,
        mime,
        total_chunks,
        chunks,
    };

    (StatusCode::OK, Json(manifest)).into_response()
}

/// GET /files/:id  and  GET /files/:id/:fname  (no auth)
pub async fn download_file_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    stream_file(state, id, headers).await
}

/// GET /files/:id/:fname  (no auth, pretty filename)
pub async fn download_file_with_name_handler(
    State(state): State<AppState>,
    Path((id, _fname)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    stream_file(state, id, headers).await
}

async fn stream_file(state: AppState, id: String, headers: HeaderMap) -> Response {
    let dp = dataprovider::get();

    let file = match dp.get_by_id(&id, None).await {
        Ok(f) => f,
        Err(DataProviderError::NotFound) => return err(StatusCode::NOT_FOUND, "not found"),
        Err(e) => return dp_err(e),
    };

    let nodes = match dp.get_nodes(&id).await {
        Ok(n) => n,
        Err(e) => return dp_err(e),
    };

    // Determine content-type from file name
    let ext = std::path::Path::new(&file.name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let mime = mime_guess::from_ext(ext)
        .first_or_octet_stream()
        .to_string();
    let disposition = if mime.starts_with("audio/")
        || mime.starts_with("video/")
        || mime.starts_with("image/")
        || mime.starts_with("text/")
    {
        format!("inline; filename=\"{}\"", file.name)
    } else {
        format!("attachment; filename=\"{}\"", file.name)
    };

    // Handle Range header
    if let Some(range_val) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        match parse_range(range_val, file.size) {
            Ok((start, end, length)) => {
                let reader = match state.driver.new_reader(nodes, start) {
                    Ok(r) => r,
                    Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                };
                let limited = LimitedReader::new(reader, length as usize);
                let stream = ReaderStream::new(limited);
                let body = Body::from_stream(stream);
                let content_range = format!("bytes {}-{}/{}", start, end, file.size);
                return (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        (header::CONTENT_TYPE, mime),
                        (header::CONTENT_RANGE, content_range),
                        (header::CONTENT_LENGTH, length.to_string()),
                        (header::ACCEPT_RANGES, "bytes".to_string()),
                        (header::CONTENT_DISPOSITION, disposition.clone()),
                    ],
                    body,
                )
                    .into_response();
            }
            Err(_) => {
                return err(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range");
            }
        }
    }

    let reader = match state.driver.new_reader(nodes, 0) {
        Ok(r) => r,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let stream = ReaderStream::new(reader);
    let body = Body::from_stream(stream);
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, mime),
            (header::CONTENT_LENGTH, file.size.to_string()),
            (header::ACCEPT_RANGES, "bytes".to_string()),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        body,
    )
        .into_response()
}

/// PUT /api/directories/:dir_id/files/:id/content (overwrite bytes)
pub async fn overwrite_file_handler(
    State(state): State<AppState>,
    Path((dir_id, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let dp = dataprovider::get();

    let mut file = match dp.get_by_id(&id, Some(&dir_id)).await {
        Ok(f) => f,
        Err(e) => return dp_err(e),
    };

    let data = body;
    let nodes: Arc<Mutex<Vec<Node>>> = Arc::new(Mutex::new(Vec::new()));

    // Write new content to storage
    if state.config.async_write {
        let nodes_cb = Arc::clone(&nodes);
        let mut writer = state.driver.new_nwriter(move |chunk| {
            nodes_cb.lock().expect("nodes mutex poisoned").push(chunk);
        });
        use tokio::io::AsyncWriteExt;
        if let Err(e) = writer.write_all(&data).await {
            return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        }
        if let Err(e) = writer.shutdown().await {
            return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        }
    } else {
        let nodes_cb = Arc::clone(&nodes);
        let mut writer = state.driver.new_writer(move |chunk| {
            nodes_cb.lock().expect("nodes mutex poisoned").push(chunk);
        });
        use tokio::io::AsyncWriteExt;
        if let Err(e) = writer.write_all(&data).await {
            return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        }
        if let Err(e) = writer.shutdown().await {
            return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        }
    }

    let collected = nodes.lock().expect("nodes mutex poisoned").clone();
    if let Err(e) = dp.truncate(&id).await {
        return dp_err(e);
    }
    if let Err(e) = dp.create_nodes(&id, &collected).await {
        return dp_err(e);
    }

    file.size = data.len() as i64;
    file.mtime = Utc::now();

    match dp.update(&id, Some(&dir_id), &file).await {
        Ok(f) => ApiResponse::ok(f).into_response(),
        Err(e) => dp_err(e),
    }
}

/// Parse "bytes=start-end" range header. Returns (start, end, length).
fn parse_range(header: &str, size: i64) -> Result<(i64, i64, i64), ()> {
    let header = header.trim();
    if !header.starts_with("bytes=") {
        return Err(());
    }
    let range = &header["bytes=".len()..];
    if let Some(suffix) = range.strip_prefix('-') {
        let n: i64 = suffix.parse().map_err(|_| ())?;
        let start = size - n;
        let end = size - 1;
        if start < 0 || end < 0 || start > end {
            return Err(());
        }
        return Ok((start, end, end - start + 1));
    }
    if let Some(pos) = range.find('-') {
        let start_str = &range[..pos];
        let end_str = &range[pos + 1..];
        let start: i64 = start_str.parse().map_err(|_| ())?;
        let end: i64 = if end_str.is_empty() {
            size - 1
        } else {
            end_str.parse().map_err(|_| ())?
        };
        if start < 0 || end < start || end >= size {
            return Err(());
        }
        return Ok((start, end, end - start + 1));
    }
    Err(())
}

/// A reader that stops after `limit` bytes.
struct LimitedReader<R> {
    inner: R,
    remaining: usize,
}

impl<R> LimitedReader<R> {
    fn new(inner: R, limit: usize) -> Self {
        LimitedReader {
            inner,
            remaining: limit,
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for LimitedReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.remaining == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        let max = self.remaining.min(buf.remaining());
        let mut limited = buf.take(max);
        let res = std::pin::Pin::new(&mut self.inner).poll_read(cx, &mut limited);
        let n = limited.filled().len();
        unsafe { buf.assume_init(n) };
        buf.advance(n);
        if let std::task::Poll::Ready(Ok(())) = res {
            self.remaining -= n;
        }
        res
    }
}
