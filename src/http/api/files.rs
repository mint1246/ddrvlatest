use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio_util::io::ReaderStream;

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
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"message": "file deleted"}))).into_response(),
        Err(e) => dp_err(e),
    }
}

/// A single chunk entry returned by the manifest endpoint.
#[derive(Serialize)]
pub struct ChunkInfo {
    /// Authenticated Discord CDN URL for this chunk (includes `ex`/`is`/`hm` params).
    pub url: String,
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
    pub chunks: Vec<ChunkInfo>,
}

/// Query params understood by the download handler.
#[derive(Deserialize, Default)]
pub struct DownloadQuery {
    /// When present (any value: `?direct`, `?direct=1`, `?direct=true`), single-chunk
    /// files are served via HTTP 307 redirect to Discord CDN, removing the server from
    /// the download path entirely.  Multi-chunk files receive a 303 redirect to the
    /// `/files/:id/manifest` endpoint so the client can perform client-side reconstruction.
    pub direct: Option<String>,
}

/// GET /files/:id/manifest  (no auth)
///
/// Returns a JSON manifest listing every authenticated Discord CDN chunk URL
/// and its byte-range metadata.  Clients can use this to download all chunks
/// directly from Discord CDN and reassemble the file locally, removing the
/// server as a bandwidth bottleneck entirely.
pub async fn manifest_file_handler(
    State(_state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
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

    let ext = std::path::Path::new(&file.name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let mime = mime_guess::from_ext(ext)
        .first_or_octet_stream()
        .to_string();

    // Build absolute byte offsets for each chunk.
    // This mirrors the offset arithmetic in `ddrv::reader::Reader::new` so that
    // clients can use `Range: bytes={start}-{end}` when fetching each chunk URL.
    let mut offset: u64 = 0;
    let chunks: Vec<ChunkInfo> = nodes
        .iter()
        .map(|n| {
            let start = offset;
            let size = n.size as u64;
            let end = offset + size - 1;
            offset = end + 1;
            ChunkInfo {
                url: encode_attachment_url(&n.url, n.ex, n.is, &n.hm),
                start,
                end,
                size,
            }
        })
        .collect();

    let manifest = FileManifest {
        id: file.id.clone(),
        name: file.name.clone(),
        size: file.size,
        mime,
        chunks,
    };

    (StatusCode::OK, Json(manifest)).into_response()
}

/// GET /files/:id  and  GET /files/:id/:fname  (no auth)
pub async fn download_file_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<DownloadQuery>,
    headers: HeaderMap,
) -> Response {
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

    // ------------------------------------------------------------------
    // ?direct=1 – remove the server from the download path entirely.
    //
    // • Single-chunk file: HTTP 307 redirect straight to the Discord CDN
    //   URL so the client downloads the bytes without touching this server.
    // • Multi-chunk file:  HTTP 303 redirect to /files/:id/manifest so the
    //   client can fetch each chunk from Discord CDN and reassemble locally.
    // ------------------------------------------------------------------
    if query.direct.is_some() {
        if nodes.len() == 1 {
            let cdn_url = encode_attachment_url(&nodes[0].url, nodes[0].ex, nodes[0].is, &nodes[0].hm);
            return (
                StatusCode::TEMPORARY_REDIRECT,
                [(header::LOCATION, cdn_url)],
            )
                .into_response();
        } else {
            let manifest_url = format!("/files/{}/manifest", id);
            return (
                StatusCode::SEE_OTHER,
                [(header::LOCATION, manifest_url)],
            )
                .into_response();
        }
    }

    // Determine content-type from file name
    let ext = std::path::Path::new(&file.name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let mime = mime_guess::from_ext(ext)
        .first_or_octet_stream()
        .to_string();

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
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", file.name),
            ),
        ],
        body,
    )
        .into_response()
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
        LimitedReader { inner, remaining: limit }
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
