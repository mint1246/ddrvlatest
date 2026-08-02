pub mod api;
pub mod web;

use std::sync::Arc;

use axum::{routing::get, Router};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::config::HttpConfig;

#[derive(Clone)]
pub struct AppState {
    pub driver: Arc<crate::ddrv::Driver>,
    pub config: Arc<HttpConfig>,
    pub auth: api::auth::SharedAuthState,
}

fn validate_auth_config(config: &HttpConfig) -> anyhow::Result<()> {
    let authenticated = !config.username.is_empty() || !config.password_hash.is_empty();
    if authenticated {
        anyhow::ensure!(
            !config.username.is_empty() && !config.password_hash.is_empty(),
            "HTTP authentication requires both username and password_hash"
        );
        anyhow::ensure!(
            config.password_hash.starts_with("$argon2id$"),
            "HTTP password_hash must be an Argon2id PHC string"
        );
        anyhow::ensure!(
            config.jwt_secret.as_bytes().len() >= api::auth::MIN_SECRET_BYTES,
            "HTTP JWT secret must contain at least 32 bytes"
        );
        anyhow::ensure!(
            (60..=3600).contains(&config.access_token_ttl_seconds),
            "HTTP access token lifetime must be between 60 and 3600 seconds"
        );
    }
    Ok(())
}

pub async fn serve(driver: Arc<crate::ddrv::Driver>, config: HttpConfig) -> anyhow::Result<()> {
    if config.addr.is_empty() {
        return Ok(());
    }

    validate_auth_config(&config)?;

    let state = AppState {
        driver: Arc::clone(&driver),
        config: Arc::new(config.clone()),
        auth: Arc::new(api::auth::AuthState::default()),
    };

    let app = Router::new()
        // Download route (no auth)
        .route("/files/:id", get(api::files::download_file_handler))
        .route(
            "/files/:id/:fname",
            get(api::files::download_file_with_name_handler),
        )
        // Manifest route – returns chunk URLs for client-side reconstruction (no auth)
        .route(
            "/files/:id/manifest",
            get(api::files::manifest_file_handler),
        )
        // API routes
        .nest("/api", api::router(state.clone()))
        // Static web UI
        .fallback(get(web::serve_static))
        .layer(CorsLayer::permissive())
        .with_state(state);

    info!("Starting HTTP server on {}", config.addr);
    let addr = config.addr.trim_start_matches(':');
    let bind = if config.addr.starts_with(':') {
        format!("0.0.0.0:{}", addr)
    } else {
        config.addr.clone()
    };

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_auth_config;
    use crate::config::HttpConfig;

    #[test]
    fn authenticated_http_requires_independent_high_entropy_secret() {
        let config = HttpConfig {
            username: "admin".into(),
            password_hash: "$argon2id$placeholder".into(),
            ..Default::default()
        };
        assert!(validate_auth_config(&config).is_err());
    }

    #[test]
    fn authenticated_http_accepts_valid_security_settings() {
        let config = HttpConfig {
            username: "admin".into(),
            password_hash: "$argon2id$placeholder".into(),
            jwt_secret: "0123456789abcdef0123456789abcdef".into(),
            ..Default::default()
        };
        assert!(validate_auth_config(&config).is_ok());
    }
}
