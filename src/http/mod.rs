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
}

pub async fn serve(driver: Arc<crate::ddrv::Driver>, config: HttpConfig) -> anyhow::Result<()> {
    if config.addr.is_empty() {
        return Ok(());
    }

    let state = AppState {
        driver: Arc::clone(&driver),
        config: Arc::new(config.clone()),
    };

    let app = Router::new()
        // Download route (no auth)
        .route("/files/:id", get(api::files::download_file_handler))
        .route("/files/:id/:fname", get(api::files::download_file_handler))
        // API routes
        .nest("/api", api::router(state.clone()))
        // Static web UI
        .fallback_service(web::static_handler())
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
