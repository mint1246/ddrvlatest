use axum::{
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use super::types::{err, ApiResponse, AuthConfigResponse, LoginRequest, TokenResponse};
use crate::http::AppState;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iat: i64,
    exp: i64,
}

fn signing_key(cfg: &crate::config::HttpConfig) -> String {
    format!("{}:{}", cfg.username, cfg.password)
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
    })
}

pub async fn check_token_handler() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let cfg = &state.config;

    // No credentials configured: allow all.
    if cfg.username.is_empty() || cfg.password.is_empty() {
        return next.run(request).await;
    }

    // Guest mode: allow read-only methods without auth.
    if cfg.guest_mode {
        let method = request.method();
        if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
            return next.run(request).await;
        }
    }

    // Require Bearer JWT.
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if !auth_header.starts_with("Bearer ") {
        return err(StatusCode::UNAUTHORIZED, "missing or invalid token");
    }

    let token = &auth_header["Bearer ".len()..];
    let key = signing_key(cfg);
    match decode::<Claims>(
        token,
        &DecodingKey::from_secret(key.as_bytes()),
        &Validation::default(),
    ) {
        Ok(_) => next.run(request).await,
        Err(_) => err(StatusCode::UNAUTHORIZED, "invalid token"),
    }
}

#[cfg(test)]
mod tests {
    use super::{signing_key, Claims};
    use crate::config::HttpConfig;
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
}
