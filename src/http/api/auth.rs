use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use super::types::{
    err, ApiResponse, AuthConfigResponse, LoginRequest, TokenResponse, UploadCapabilities,
};
use crate::http::AppState;

#[derive(Serialize)]
struct CheckTokenResponse {
    valid: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iat: i64,
    exp: i64,
}

/// Identity attached to requests that passed authentication.  The HTTP
/// configuration currently defines one account, but carrying the identity
/// through request extensions keeps resource ownership explicit at handlers.
#[derive(Debug, Clone)]
pub struct AuthIdentity {
    pub username: String,
}

fn configured_identity(cfg: &crate::config::HttpConfig) -> AuthIdentity {
    AuthIdentity {
        username: if cfg.username.is_empty() {
            "anonymous".into()
        } else {
            cfg.username.clone()
        },
    }
}

fn attach_identity(
    mut request: axum::extract::Request,
    cfg: &crate::config::HttpConfig,
) -> axum::extract::Request {
    request.extensions_mut().insert(configured_identity(cfg));
    request
}

fn signing_key(cfg: &crate::config::HttpConfig) -> String {
    format!("{}:{}", cfg.username, cfg.password)
}

#[derive(Debug, Clone)]
enum TokenCandidate {
    None,
    Found(String),
    Invalid,
}

fn token_from_authorization(headers: &HeaderMap) -> TokenCandidate {
    let Some(raw) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return TokenCandidate::None;
    };

    let mut parts = raw.splitn(2, ' ');
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default().trim();

    if scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty() {
        return TokenCandidate::Found(token.to_string());
    }

    TokenCandidate::Invalid
}

fn token_from_cookie(headers: &HeaderMap) -> TokenCandidate {
    let Some(raw_cookie) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) else {
        return TokenCandidate::None;
    };

    for part in raw_cookie.split(';') {
        let entry = part.trim();
        if let Some(token) = entry.strip_prefix("ddrv_token=") {
            if token.is_empty() {
                return TokenCandidate::Invalid;
            }
            return TokenCandidate::Found(token.to_string());
        }
    }

    TokenCandidate::None
}

fn extract_token(headers: &HeaderMap) -> TokenCandidate {
    match token_from_authorization(headers) {
        TokenCandidate::None => token_from_cookie(headers),
        other => other,
    }
}

fn validate_token(cfg: &crate::config::HttpConfig, token: &str) -> bool {
    let key = signing_key(cfg);
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(key.as_bytes()),
        &Validation::default(),
    )
    .is_ok()
}

pub async fn login_handler(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let cfg = &state.config;
    if body.username != cfg.username || body.password != cfg.password {
        return err(StatusCode::UNAUTHORIZED, "invalid credentials");
    }

    let claims = Claims {
        iat: Utc::now().timestamp(),
        exp: (Utc::now() + chrono::Duration::days(30)).timestamp(),
    };
    let key = signing_key(cfg);
    match encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(key.as_bytes()),
    ) {
        Ok(token) => ApiResponse::ok(TokenResponse { token }).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn auth_config_handler(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = &state.config;
    let has_creds = !cfg.username.is_empty() && !cfg.password.is_empty();
    ApiResponse::ok(AuthConfigResponse {
        login: has_creds,
        anonymous: cfg.guest_mode,
        upload: UploadCapabilities {
            resumable: true,
            max_session_size: cfg.upload_session_size_limit,
            user_quota: cfg.upload_user_quota,
            max_concurrent_transfers: cfg.upload_concurrent_transfers,
            requests_per_minute: cfg.upload_requests_per_minute,
            max_part_size: cfg.upload_memory_limit,
        },
    })
}

pub async fn check_token_handler() -> impl IntoResponse {
    ApiResponse::ok(CheckTokenResponse { valid: true })
}

pub async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let cfg = &state.config;

    // No credentials configured: allow all.
    if cfg.username.is_empty() || cfg.password.is_empty() {
        return next.run(attach_identity(request, cfg)).await;
    }

    let token = extract_token(request.headers());
    let is_upload_route = request.uri().path().starts_with("/api/upload-sessions");
    let read_only = matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );

    // Guest mode + read-only requests can proceed without auth token.
    // If a token is supplied, it must still be valid.
    if cfg.guest_mode && read_only && !is_upload_route {
        match token {
            TokenCandidate::None => return next.run(request).await,
            TokenCandidate::Invalid => {
                return err(StatusCode::UNAUTHORIZED, "missing or invalid token")
            }
            TokenCandidate::Found(t) => {
                if validate_token(cfg, &t) {
                    return next.run(attach_identity(request, cfg)).await;
                }
                return err(StatusCode::UNAUTHORIZED, "invalid token");
            }
        }
    }

    // Non-guest or mutating requests require a valid token.
    match token {
        TokenCandidate::Found(t) if validate_token(cfg, &t) => {
            next.run(attach_identity(request, cfg)).await
        }
        TokenCandidate::Found(_) => err(StatusCode::UNAUTHORIZED, "invalid token"),
        TokenCandidate::Invalid | TokenCandidate::None => {
            err(StatusCode::UNAUTHORIZED, "missing or invalid token")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_token, signing_key, Claims, TokenCandidate};
    use crate::config::HttpConfig;
    use axum::http::{header, HeaderMap, HeaderValue};
    use chrono::Utc;
    use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};

    #[test]
    fn generated_claims_validate_with_default_validation() {
        let cfg = HttpConfig {
            username: "user".into(),
            password: "pass".into(),
            ..Default::default()
        };
        let key = signing_key(&cfg);
        let claims = Claims {
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::minutes(5)).timestamp(),
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(key.as_bytes()),
        )
        .expect("token should encode");

        decode::<Claims>(
            &token,
            &DecodingKey::from_secret(key.as_bytes()),
            &Validation::default(),
        )
        .expect("token should validate");
    }

    #[test]
    fn extract_token_uses_cookie_when_authorization_missing() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("theme=dark; ddrv_token=abc.def.ghi"),
        );

        match extract_token(&headers) {
            TokenCandidate::Found(token) => assert_eq!(token, "abc.def.ghi"),
            other => panic!("unexpected token candidate: {other:?}"),
        }
    }

    #[test]
    fn extract_token_marks_invalid_authorization() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Token abc123"),
        );

        match extract_token(&headers) {
            TokenCandidate::Invalid => {}
            other => panic!("unexpected token candidate: {other:?}"),
        }
    }
}
