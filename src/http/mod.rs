pub mod api;
pub mod web;

use std::sync::Arc;

use argon2::PasswordHash;
use axum::{routing::get, Router};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

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
        let password_hash = PasswordHash::new(&config.password_hash).map_err(|_| {
            anyhow::anyhow!("HTTP password_hash must be a valid Argon2id PHC string")
        })?;
        anyhow::ensure!(
            password_hash.algorithm.as_str() == "argon2id",
            "HTTP password_hash must use the Argon2id algorithm"
        );
        anyhow::ensure!(
            config.jwt_secret.len() >= api::auth::MIN_SECRET_BYTES,
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

    let addr = config.addr.trim_start_matches(':');
    let bind = if config.addr.starts_with(':') {
        format!("0.0.0.0:{}", addr)
    } else {
        config.addr.clone()
    };

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    match listener.local_addr() {
        Ok(endpoint) => info!(%endpoint, "HTTP server listening"),
        Err(error) => {
            warn!(%error, bind = %bind, "HTTP server listening (bound address unavailable)")
        }
    }
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
            password_hash: "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$7/I8oeEyYR0hT9J3MRWmlZU6TPzF/iwJZPi3qadSlqU".into(),
            ..Default::default()
        };
        assert!(validate_auth_config(&config).is_err());
    }

    #[test]
    fn authenticated_http_accepts_valid_security_settings() {
        let config = HttpConfig {
            username: "admin".into(),
            password_hash: "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$7/I8oeEyYR0hT9J3MRWmlZU6TPzF/iwJZPi3qadSlqU".into(),
            jwt_secret: "0123456789abcdef0123456789abcdef".into(),
            ..Default::default()
        };
        assert!(validate_auth_config(&config).is_ok());
    }
}
