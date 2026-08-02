use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex, OnceLock,
    },
    time::{Duration, Instant},
};
use tokio::io::AsyncWriteExt;

use super::types::{err, limit_err, ApiResponse};
use crate::{
    dataprovider::{
        self,
        types::{UploadPart, UploadSession, UploadState},
        DataProviderError,
    },
    ddrv::types::Node,
    http::AppState,
};

static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static REQUESTS: OnceLock<Mutex<VecDeque<Instant>>> = OnceLock::new();
static SESSION_WRITE: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[derive(Deserialize)]
pub struct CreateUpload {
    pub name: String,
    pub parent: Option<String>,
    pub size: u64,
    pub part_size: Option<u64>,
}

#[derive(Serialize)]
pub struct UploadView {
    pub id: String,
    pub name: String,
    pub parent: String,
    pub size: u64,
    pub part_size: u64,
    pub state: UploadState,
    pub completed_parts: BTreeMap<u32, PartView>,
    pub file_id: Option<String>,
}
#[derive(Serialize)]
pub struct PartView {
    pub size: u64,
    pub sha256: String,
}

impl From<&UploadSession> for UploadView {
    fn from(s: &UploadSession) -> Self {
        Self {
            id: s.id.clone(),
            name: s.name.clone(),
            parent: s.parent.clone(),
            size: s.size,
            part_size: s.part_size,
            state: s.state.clone(),
            completed_parts: s
                .parts
                .iter()
                .map(|(i, p)| {
                    (
                        *i,
                        PartView {
                            size: p.size,
                            sha256: p.sha256.clone(),
                        },
                    )
                })
                .collect(),
            file_id: s.file_id.clone(),
        }
    }
}

fn rate_limit(state: &AppState) -> Option<Response> {
    let max = state.config.upload_requests_per_minute;
    if max == 0 {
        return None;
    }
    let now = Instant::now();
    let mut q = REQUESTS
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .unwrap();
    while q
        .front()
        .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
    {
        q.pop_front();
    }
    if q.len() >= max as usize {
        return Some(limit_err(
            StatusCode::TOO_MANY_REQUESTS,
            "request_rate",
            "upload request rate exceeded",
            max as u64,
        ));
    }
    q.push_back(now);
    None
}

fn dp_error(e: DataProviderError) -> Response {
    match e {
        DataProviderError::NotFound => err(StatusCode::NOT_FOUND, "upload session not found"),
        _ => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
fn valid_name(n: &str) -> bool {
    !n.trim().is_empty() && n.len() <= 255 && !n.contains(['/', '\\'])
}

pub async fn create(State(state): State<AppState>, Json(body): Json<CreateUpload>) -> Response {
    if let Some(r) = rate_limit(&state) {
        return r;
    }
    if !valid_name(&body.name) {
        return err(StatusCode::BAD_REQUEST, "invalid filename");
    }
    if body.size > state.config.upload_session_size_limit {
        return limit_err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "session_size",
            "file exceeds per-session size limit",
            state.config.upload_session_size_limit,
        );
    }
    let dp = dataprovider::get();
    let usage = match dp.storage_usage().await {
        Ok(v) => v,
        Err(e) => return dp_error(e),
    };
    if usage.saturating_add(body.size) > state.config.upload_user_quota {
        return limit_err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "user_quota",
            "user storage quota exceeded",
            state.config.upload_user_quota,
        );
    }
    let part_size = body
        .part_size
        .unwrap_or(state.config.upload_memory_limit as u64)
        .min(state.config.upload_memory_limit as u64);
    if part_size == 0 {
        return err(StatusCode::BAD_REQUEST, "part_size must be positive");
    }
    let now = Utc::now();
    let session = UploadSession {
        id: uuid::Uuid::new_v4().to_string(),
        owner: "default".into(),
        parent: body.parent.unwrap_or_else(|| "root".into()),
        name: body.name,
        size: body.size,
        part_size,
        state: UploadState::Open,
        parts: BTreeMap::new(),
        file_id: None,
        created_at: now,
        updated_at: now,
    };
    match dp.put_upload_session(&session).await {
        Ok(()) => ApiResponse::created(UploadView::from(&session)).into_response(),
        Err(e) => dp_error(e),
    }
}

pub async fn status(Path(id): Path<String>) -> Response {
    match dataprovider::get().get_upload_session(&id).await {
        Ok(s) => ApiResponse::ok(UploadView::from(&s)).into_response(),
        Err(e) => dp_error(e),
    }
}
pub async fn resume(Path(id): Path<String>) -> Response {
    status(Path(id)).await
}

struct ActiveGuard;
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}
pub async fn append(
    State(state): State<AppState>,
    Path((id, index)): Path<(String, u32)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(r) = rate_limit(&state) {
        return r;
    }
    if body.len() > state.config.upload_memory_limit {
        return limit_err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "memory",
            "part exceeds request memory limit",
            state.config.upload_memory_limit as u64,
        );
    }
    let current = ACTIVE.fetch_add(1, Ordering::SeqCst);
    if current >= state.config.upload_concurrent_transfers {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        return limit_err(
            StatusCode::TOO_MANY_REQUESTS,
            "concurrent_transfers",
            "too many concurrent part transfers",
            state.config.upload_concurrent_transfers as u64,
        );
    }
    let _guard = ActiveGuard;
    let supplied = headers
        .get("x-part-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let hash = format!("{:x}", Sha256::digest(&body));
    if supplied.len() != 64 || supplied != hash {
        return err(
            StatusCode::UNPROCESSABLE_ENTITY,
            "x-part-sha256 does not match request body",
        );
    }
    // Serialize the read/modify/write operation so parallel accepted parts cannot
    // overwrite one another in either durable provider.
    let _session_write = SESSION_WRITE
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let dp = dataprovider::get();
    let mut s = match dp.get_upload_session(&id).await {
        Ok(v) => v,
        Err(e) => return dp_error(e),
    };
    if s.state != UploadState::Open {
        return err(StatusCode::CONFLICT, "upload session is not open");
    }
    if let Some(p) = s.parts.get(&index) {
        return if p.sha256 == hash {
            ApiResponse::ok(UploadView::from(&s)).into_response()
        } else {
            err(
                StatusCode::CONFLICT,
                "part index already accepted with a different hash",
            )
        };
    }
    if body.len() as u64 > s.part_size
        || (index as u64)
            .saturating_mul(s.part_size)
            .saturating_add(body.len() as u64)
            > s.size
    {
        return err(
            StatusCode::BAD_REQUEST,
            "part is outside declared upload bounds",
        );
    }
    let nodes = std::sync::Arc::new(Mutex::new(Vec::<Node>::new()));
    let out = nodes.clone();
    let mut writer = state
        .driver
        .new_writer(move |n| out.lock().unwrap().push(n));
    if let Err(e) = writer.write_all(&body).await.and_then(|_| Ok(())) {
        return err(StatusCode::BAD_GATEWAY, e.to_string());
    }
    if let Err(e) = writer.shutdown().await {
        return err(StatusCode::BAD_GATEWAY, e.to_string());
    }
    let uploaded = nodes.lock().unwrap().clone();
    s.parts.insert(
        index,
        UploadPart {
            index,
            size: body.len() as u64,
            sha256: hash,
            nodes: uploaded,
        },
    );
    s.updated_at = Utc::now();
    match dp.put_upload_session(&s).await {
        Ok(()) => ApiResponse::ok(UploadView::from(&s)).into_response(),
        Err(e) => dp_error(e),
    }
}

pub async fn commit(Path(id): Path<String>) -> Response {
    let dp = dataprovider::get();
    let mut s = match dp.get_upload_session(&id).await {
        Ok(v) => v,
        Err(e) => return dp_error(e),
    };
    if s.state == UploadState::Committed {
        return ApiResponse::ok(UploadView::from(&s)).into_response();
    }
    if s.state != UploadState::Open {
        return err(StatusCode::CONFLICT, "upload session is cancelled");
    }
    let expected = if s.size == 0 {
        0
    } else {
        (s.size + s.part_size - 1) / s.part_size
    };
    let total: u64 = s.parts.values().map(|p| p.size).sum();
    if total != s.size || (0..expected).any(|i| !s.parts.contains_key(&(i as u32))) {
        return err(StatusCode::CONFLICT, "upload has missing parts");
    }
    let file = match dp.create(&s.name, &s.parent, false).await {
        Ok(v) => v,
        Err(e) => return dp_error(e),
    };
    let mut nodes = Vec::new();
    let mut offset = 0i64;
    for part in s.parts.values() {
        for node in &part.nodes {
            let mut n = node.clone();
            n.nid = nodes.len() as i64;
            n.start = offset;
            n.end = offset + n.size as i64 - 1;
            offset += n.size as i64;
            nodes.push(n)
        }
    }
    if let Err(e) = dp.create_nodes(&file.id, &nodes).await {
        let _ = dp.delete(&file.id, Some(&s.parent)).await;
        return dp_error(e);
    }
    s.state = UploadState::Committed;
    s.file_id = Some(file.id);
    s.updated_at = Utc::now();
    match dp.put_upload_session(&s).await {
        Ok(()) => ApiResponse::ok(UploadView::from(&s)).into_response(),
        Err(e) => dp_error(e),
    }
}

pub async fn cancel(Path(id): Path<String>) -> Response {
    let dp = dataprovider::get();
    let mut s = match dp.get_upload_session(&id).await {
        Ok(v) => v,
        Err(e) => return dp_error(e),
    };
    if s.state == UploadState::Committed {
        return err(StatusCode::CONFLICT, "committed upload cannot be cancelled");
    }
    s.state = UploadState::Cancelled;
    s.parts.clear();
    s.updated_at = Utc::now();
    match dp.put_upload_session(&s).await {
        Ok(()) => ApiResponse::ok(UploadView::from(&s)).into_response(),
        Err(e) => dp_error(e),
    }
}
