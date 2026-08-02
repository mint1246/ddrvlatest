pub mod auth;
pub mod dirs;
pub mod files;
pub mod types;
pub mod uploads;

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post, put},
    Router,
};

use crate::http::AppState;

pub fn router(state: AppState) -> Router<AppState> {
    // Protected API routes (auth middleware applied below)
    let protected = Router::new()
        .route("/check_token", get(auth::check_token_handler))
        .route("/upload-sessions", post(uploads::create))
        .route(
            "/upload-sessions/:id",
            get(uploads::status).delete(uploads::cancel),
        )
        .route("/upload-sessions/:id/resume", post(uploads::resume))
        .route("/upload-sessions/:id/commit", post(uploads::commit))
        .route("/upload-sessions/:id/parts/:index", put(uploads::append))
        // Directory routes
        .route("/directories/", post(dirs::create_dir_handler))
        .route("/directories/:id", get(dirs::get_dir_handler))
        .route("/directories/:id", put(dirs::update_dir_handler))
        .route("/directories/:id", delete(dirs::delete_dir_handler))
        // File routes
        .route(
            "/directories/:dir_id/files",
            post(files::create_file_handler),
        )
        .route(
            "/directories/:dir_id/files/:id",
            get(files::get_file_handler),
        )
        .route(
            "/directories/:dir_id/files/:id",
            put(files::update_file_handler),
        )
        .route(
            "/directories/:dir_id/files/:id",
            delete(files::delete_file_handler),
        )
        .route(
            "/directories/:dir_id/files/:id/content",
            put(files::overwrite_file_handler),
        )
        .layer(DefaultBodyLimit::max(state.config.upload_memory_limit))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::auth_middleware,
        ));

    Router::new()
        .route("/user/login", post(auth::login_handler))
        .route("/config", get(auth::auth_config_handler))
        .merge(protected)
}
