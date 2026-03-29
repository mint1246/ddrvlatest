use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use super::types::{err, ApiResponse, CreateDirRequest, Directory, UpdateDirRequest};
use crate::{dataprovider, dataprovider::types::DataProviderError, http::AppState};

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
    let trimmed = name.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 255
        && !trimmed.contains(|c| matches!(c, '/' | '<' | '>' | '"' | '|' | '*' | '\\'))
        && !trimmed.chars().any(|c| c.is_control())
}

/// GET /api/directories/:id   (or GET /api/directories with no id)
pub async fn get_dir_handler(State(_state): State<AppState>, id: Option<Path<String>>) -> Response {
    let dp = dataprovider::get();
    let dir_id = id.as_ref().map(|p| p.0.as_str()).unwrap_or("root");

    let dir = match dp.get_by_id(dir_id, None).await {
        Ok(f) => f,
        Err(DataProviderError::NotFound) if dir_id == "root" => {
            // Return a synthetic root if not found yet.
            crate::dataprovider::types::File {
                id: "root".into(),
                name: "/".into(),
                dir: true,
                size: 0,
                parent: None,
                mtime: chrono::Utc::now(),
            }
        }
        Err(e) => return dp_err(e),
    };

    let files = match dp.get_children(&dir.id).await {
        Ok(f) => f,
        Err(e) => return dp_err(e),
    };

    ApiResponse::ok(Directory { file: dir, files }).into_response()
}

/// POST /api/directories
pub async fn create_dir_handler(
    State(_state): State<AppState>,
    Json(body): Json<CreateDirRequest>,
) -> Response {
    if !validate_name(&body.name) {
        return err(StatusCode::BAD_REQUEST, "invalid directory name");
    }
    let parent = body.parent.as_deref().unwrap_or("root");
    let dp = dataprovider::get();
    match dp.create(&body.name, parent, true).await {
        Ok(f) => ApiResponse::created(f).into_response(),
        Err(e) => dp_err(e),
    }
}

/// PUT /api/directories/:id
pub async fn update_dir_handler(
    State(_state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateDirRequest>,
) -> Response {
    let dp = dataprovider::get();
    let mut dir = match dp.get_by_id(&id, None).await {
        Ok(f) => f,
        Err(e) => return dp_err(e),
    };
    if let Some(name) = body.name {
        if !validate_name(&name) {
            return err(StatusCode::BAD_REQUEST, "invalid directory name");
        }
        dir.name = name;
    }
    let parent = body.parent.as_deref();
    match dp.update(&id, parent, &dir).await {
        Ok(f) => ApiResponse::ok(f).into_response(),
        Err(e) => dp_err(e),
    }
}

/// DELETE /api/directories/:id
pub async fn delete_dir_handler(
    State(_state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    let dp = dataprovider::get();
    match dp.delete(&id, None).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"message":"deleted"})),
        )
            .into_response(),
        Err(e) => dp_err(e),
    }
}
