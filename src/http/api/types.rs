use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::dataprovider::types::File;

#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> (StatusCode, Json<Self>) {
        (
            StatusCode::OK,
            Json(Self {
                message: "ok".into(),
                data: Some(data),
            }),
        )
    }

    pub fn created(data: T) -> (StatusCode, Json<Self>) {
        (
            StatusCode::CREATED,
            Json(Self {
                message: "created".into(),
                data: Some(data),
            }),
        )
    }
}

pub fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    let body = ApiResponse::<()> {
        message: msg.into(),
        data: None,
    };
    (status, Json(body)).into_response()
}

pub fn limit_err(status: StatusCode, code: &str, message: &str, limit: u64) -> Response {
    (status, Json(serde_json::json!({"message":message,"error":{"type":"limit_exceeded","code":code,"limit":limit}}))).into_response()
}

/// Validate a single user-visible file or directory name before it is used as
/// a path component.  Keep this shared by all HTTP write endpoints so uploads
/// cannot create names that the regular file APIs would reject.
pub(crate) fn valid_name(name: &str) -> bool {
    let trimmed = name.trim();
    if trimmed != name
        || trimmed.is_empty()
        || trimmed.len() > 255
        || trimmed == "."
        || trimmed == ".."
        || trimmed.ends_with(['.', ' '])
        || trimmed
            .chars()
            .any(|c| c.is_control() || ['/', '\\', '<', '>', '"', '|', '*', '?', ':'].contains(&c))
    {
        return false;
    }

    let device = trimmed
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    !matches!(
        device.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct TokenResponse {
    pub csrf_token: String,
}

#[derive(Serialize)]
pub struct AuthConfigResponse {
    pub login: bool,
    pub anonymous: bool,
    pub upload: UploadCapabilities,
}

#[derive(Serialize)]
pub struct UploadCapabilities {
    pub resumable: bool,
    pub max_session_size: u64,
    pub user_quota: u64,
    pub max_concurrent_transfers: usize,
    pub requests_per_minute: u32,
    pub max_part_size: usize,
}

#[derive(Serialize)]
pub struct Directory {
    #[serde(flatten)]
    pub file: File,
    pub files: Vec<File>,
}

#[derive(Debug, Deserialize)]
pub struct CreateDirRequest {
    pub name: String,
    pub parent: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDirRequest {
    pub name: Option<String>,
    pub parent: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateFileRequest {
    pub name: Option<String>,
    pub parent: Option<String>,
}
