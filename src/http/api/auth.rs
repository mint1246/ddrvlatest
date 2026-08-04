use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use argon2::{Argon2, PasswordHash, PasswordVerifier};
use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use super::types::{
    err, ApiResponse, AuthConfigResponse, LoginRequest, TokenResponse, UploadCapabilities,
};
use crate::http::AppState;

const ISSUER: &str = "ddrv";
const AUDIENCE: &str = "ddrv-web";
pub(crate) const MIN_SECRET_BYTES: usize = 32;
const MAX_LOGIN_ATTEMPTS: usize = 5;
const LOGIN_WINDOW: Duration = Duration::from_secs(60);

#[derive(Default)]
pub struct AuthState {
    revoked: Mutex<HashMap<String, i64>>,
    login_attempts: Mutex<HashMap<String, Vec<Instant>>>,
}

pub type SharedAuthState = Arc<AuthState>;

#[derive(Serialize)]
struct CheckTokenResponse {
    valid: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Claims {
    iss: String,
    aud: String,
    sub: String,
    jti: String,
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

#[derive(Debug, Clone, PartialEq)]
enum TokenCandidate {
    None,
    Bearer(String),
    Cookie(String),
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
    let (scheme, token) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default().trim(),
    );
    if scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty() {
        TokenCandidate::Bearer(token.into())
    } else {
        TokenCandidate::Invalid
    }
}

fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            (key == name && !value.is_empty()).then(|| value.to_owned())
        })
}

fn extract_token(headers: &HeaderMap) -> TokenCandidate {
    match token_from_authorization(headers) {
        TokenCandidate::None => cookie(headers, "ddrv_token")
            .map(TokenCandidate::Cookie)
            .unwrap_or(TokenCandidate::None),
        other => other,
    }
}

fn validation() -> Validation {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.algorithms = vec![Algorithm::HS256];
    validation.set_issuer(&[ISSUER]);
    validation.set_audience(&[AUDIENCE]);
    validation.required_spec_claims = ["exp", "iat", "iss", "aud", "sub", "jti"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    validation
}

fn validate_token(
    cfg: &crate::config::HttpConfig,
    auth: &AuthState,
    token: &str,
) -> Result<Claims, ()> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(cfg.jwt_secret.as_bytes()),
        &validation(),
    )
    .map_err(|_| ())?;
    if data.claims.sub != cfg.username {
        return Err(());
    }
    let now = Utc::now().timestamp();
    let mut revoked = auth.revoked.lock().map_err(|_| ())?;
    revoked.retain(|_, exp| *exp > now);
    if revoked.contains_key(&data.claims.jti) {
        return Err(());
    }
    Ok(data.claims)
}

fn random_token(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn login_limited(auth: &AuthState, username: &str) -> bool {
    let now = Instant::now();
    let mut attempts = auth
        .login_attempts
        .lock()
        .expect("login rate limiter poisoned");
    let entries = attempts.entry(username.to_owned()).or_default();
    entries.retain(|at| now.duration_since(*at) < LOGIN_WINDOW);
    entries.len() >= MAX_LOGIN_ATTEMPTS
}

fn record_failed_login(auth: &AuthState, username: &str) {
    auth.login_attempts
        .lock()
        .expect("login rate limiter poisoned")
        .entry(username.to_owned())
        .or_default()
        .push(Instant::now());
}

fn credentials_valid(cfg: &crate::config::HttpConfig, username: &str, password: &str) -> bool {
    let username_ok: bool = cfg.username.as_bytes().ct_eq(username.as_bytes()).into();
    let password_ok = PasswordHash::new(&cfg.password_hash)
        .ok()
        .and_then(|hash| {
            Argon2::default()
                .verify_password(password.as_bytes(), &hash)
                .ok()
        })
        .is_some();
    username_ok & password_ok
}

fn set_auth_cookies(response: &mut Response, token: &str, csrf: &str, ttl: u64) {
    let headers = response.headers_mut();
    headers.append(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "ddrv_token={token}; Max-Age={ttl}; Path=/; HttpOnly; Secure; SameSite=Strict"
        ))
        .unwrap(),
    );
    headers.append(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "ddrv_csrf={csrf}; Max-Age={ttl}; Path=/; Secure; SameSite=Strict"
        ))
        .unwrap(),
    );
}

pub async fn login_handler(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Response {
    // The service has one configured HTTP identity. Keying on that identity (rather
    // than attacker-controlled input) prevents username rotation from bypassing it.
    if login_limited(&state.auth, &state.config.username) {
        return err(StatusCode::TOO_MANY_REQUESTS, "too many login attempts");
    }
    if !credentials_valid(&state.config, &body.username, &body.password) {
        record_failed_login(&state.auth, &state.config.username);
        return err(StatusCode::UNAUTHORIZED, "invalid credentials");
    }
    state
        .auth
        .login_attempts
        .lock()
        .expect("login rate limiter poisoned")
        .remove(&state.config.username);

    let now = Utc::now();
    let claims = Claims {
        iss: ISSUER.into(),
        aud: AUDIENCE.into(),
        sub: state.config.username.clone(),
        jti: Uuid::new_v4().to_string(),
        iat: now.timestamp(),
        exp: (now + chrono::Duration::seconds(state.config.access_token_ttl_seconds as i64))
            .timestamp(),
    };
    let header = Header::new(Algorithm::HS256);
    match encode(
        &header,
        &claims,
        &EncodingKey::from_secret(state.config.jwt_secret.as_bytes()),
    ) {
        Ok(token) => {
            let csrf_token = random_token(32);
            let mut response = ApiResponse::ok(TokenResponse {
                csrf_token: csrf_token.clone(),
            })
            .into_response();
            set_auth_cookies(
                &mut response,
                &token,
                &csrf_token,
                state.config.access_token_ttl_seconds,
            );
            response
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn logout_handler(State(state): State<AppState>, request: Request) -> Response {
    if let TokenCandidate::Bearer(token) | TokenCandidate::Cookie(token) =
        extract_token(request.headers())
    {
        if let Ok(claims) = validate_token(&state.config, &state.auth, &token) {
            state
                .auth
                .revoked
                .lock()
                .expect("revocation store poisoned")
                .insert(claims.jti, claims.exp);
        }
    }
    let mut response = ApiResponse::ok(CheckTokenResponse { valid: false }).into_response();
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_static(
            "ddrv_token=; Max-Age=0; Path=/; HttpOnly; Secure; SameSite=Strict",
        ),
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_static("ddrv_csrf=; Max-Age=0; Path=/; Secure; SameSite=Strict"),
    );
    response
}

pub async fn auth_config_handler(State(state): State<AppState>) -> impl IntoResponse {
    ApiResponse::ok(AuthConfigResponse {
        login: !state.config.username.is_empty() && !state.config.password_hash.is_empty(),
        anonymous: state.config.guest_mode,
        upload: UploadCapabilities {
            resumable: true,
            max_session_size: state.config.upload_session_size_limit,
            user_quota: state.config.upload_user_quota,
            max_concurrent_transfers: state.config.upload_concurrent_transfers,
            requests_per_minute: state.config.upload_requests_per_minute,
            max_part_size: state.config.upload_memory_limit,
        },
    })
}
pub async fn check_token_handler() -> impl IntoResponse {
    ApiResponse::ok(CheckTokenResponse { valid: true })
}

fn csrf_valid(headers: &HeaderMap) -> bool {
    match (
        cookie(headers, "ddrv_csrf"),
        headers.get("x-csrf-token").and_then(|v| v.to_str().ok()),
    ) {
        (Some(cookie), Some(header)) => bool::from(cookie.as_bytes().ct_eq(header.as_bytes())),
        _ => false,
    }
}

fn clear_token_cookie(response: &mut Response) {
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_static(
            "ddrv_token=; Max-Age=0; Path=/; HttpOnly; Secure; SameSite=Strict",
        ),
    );
}

pub async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let cfg = &state.config;
    if cfg.username.is_empty() || cfg.password_hash.is_empty() {
        return next.run(attach_identity(request, cfg)).await;
    }
    let token = extract_token(request.headers());
    let is_upload_route = request.uri().path().starts_with("/api/upload-sessions");
    let read_only = matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    let is_check_token = request.uri().path().ends_with("/check_token");
    let cookie_token = matches!(&token, TokenCandidate::Cookie(_));
    // Guest mode + read-only requests can proceed without auth token.
    // If a token is supplied, it must still be valid.
    if cfg.guest_mode && read_only && !is_upload_route && !is_check_token {
        match token {
            TokenCandidate::None => return next.run(attach_identity(request, cfg)).await,
            TokenCandidate::Invalid => {
                return err(StatusCode::UNAUTHORIZED, "missing or invalid token")
            }
            TokenCandidate::Bearer(t) | TokenCandidate::Cookie(t) => {
                if validate_token(cfg, &state.auth, &t).is_ok() {
                    return next.run(attach_identity(request, cfg)).await;
                }
                let mut response = err(StatusCode::UNAUTHORIZED, "invalid token");
                if cookie_token {
                    clear_token_cookie(&mut response);
                }
                return response;
            }
        }
    }

    let valid = match &token {
        TokenCandidate::Bearer(t) | TokenCandidate::Cookie(t) => {
            validate_token(&state.config, &state.auth, t).is_ok()
        }
        _ => false,
    };
    if !valid {
        let mut response = err(
            StatusCode::UNAUTHORIZED,
            "missing, invalid, or revoked token",
        );
        if matches!(token, TokenCandidate::Cookie(_)) {
            clear_token_cookie(&mut response);
        }
        return response;
    }
    if !read_only && matches!(token, TokenCandidate::Cookie(_)) && !csrf_valid(request.headers()) {
        return err(StatusCode::FORBIDDEN, "missing or invalid CSRF token");
    }
    next.run(attach_identity(request, cfg)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HttpConfig;

    fn config() -> HttpConfig {
        HttpConfig { username: "user".into(), password_hash: "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$7/I8oeEyYR0hT9J3MRWmlZU6TPzF/iwJZPi3qadSlqU".into(), jwt_secret: "a-high-entropy-secret-with-at-least-32-bytes".into(), access_token_ttl_seconds: 300, ..Default::default() }
    }
    fn claims() -> Claims {
        let now = Utc::now().timestamp();
        Claims {
            iss: ISSUER.into(),
            aud: AUDIENCE.into(),
            sub: "user".into(),
            jti: Uuid::new_v4().to_string(),
            iat: now,
            exp: now + 300,
        }
    }
    fn encode_claims(c: &Claims, key: &[u8], algorithm: Algorithm) -> String {
        encode(&Header::new(algorithm), c, &EncodingKey::from_secret(key)).unwrap()
    }

    #[test]
    fn accepts_valid_and_rejects_forged_token() {
        let cfg = config();
        let token = encode_claims(&claims(), cfg.jwt_secret.as_bytes(), Algorithm::HS256);
        assert!(decode::<Claims>(
            &token,
            &DecodingKey::from_secret(cfg.jwt_secret.as_bytes()),
            &validation()
        )
        .is_ok());
        assert!(decode::<Claims>(
            &token,
            &DecodingKey::from_secret(b"wrong-secret"),
            &validation()
        )
        .is_err());
    }
    #[test]
    fn rejects_expired_token() {
        let cfg = config();
        let mut c = claims();
        c.exp = Utc::now().timestamp() - 120;
        let token = encode_claims(&c, cfg.jwt_secret.as_bytes(), Algorithm::HS256);
        assert!(decode::<Claims>(
            &token,
            &DecodingKey::from_secret(cfg.jwt_secret.as_bytes()),
            &validation()
        )
        .is_err());
    }
    #[test]
    fn rejects_wrong_audience() {
        let cfg = config();
        let mut c = claims();
        c.aud = "other".into();
        let token = encode_claims(&c, cfg.jwt_secret.as_bytes(), Algorithm::HS256);
        assert!(decode::<Claims>(
            &token,
            &DecodingKey::from_secret(cfg.jwt_secret.as_bytes()),
            &validation()
        )
        .is_err());
    }
    #[test]
    fn rejects_revoked_token() {
        let cfg = config();
        let c = claims();
        let token = encode_claims(&c, cfg.jwt_secret.as_bytes(), Algorithm::HS256);
        let auth = AuthState::default();
        auth.revoked.lock().unwrap().insert(c.jti, c.exp);
        assert!(validate_token(&cfg, &auth, &token).is_err());
    }
    #[test]
    fn rejects_algorithm_confusion() {
        let cfg = config();
        let token = encode_claims(&claims(), cfg.jwt_secret.as_bytes(), Algorithm::HS384);
        assert!(decode::<Claims>(
            &token,
            &DecodingKey::from_secret(cfg.jwt_secret.as_bytes()),
            &validation()
        )
        .is_err());
    }
}
