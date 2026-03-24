pub mod auth;
pub mod dirs;
pub mod files;
pub mod types;

use axum::{
    middleware,
    routing::{delete, get, post, put},
    Router,
};

use crate::http::AppState;

pub fn router(state: AppState) -> Router<AppState> {
    // Protected API routes (auth middleware applied below)
    let protected = Router::new()
        .route("/check_token", get(auth::check_token_handler))
        // Directory routes
        .route("/directories/", post(dirs::create_dir_handler))
        .route("/directories/:id", get(dirs::get_dir_handler))
        .route("/directories/:id", put(dirs::update_dir_handler))
        .route("/directories/:id", delete(dirs::delete_dir_handler))
        // File routes
        .route("/directories/:dir_id/files", post(files::create_file_handler))
        .route("/directories/:dir_id/files/:id", get(files::get_file_handler))
        .route("/directories/:dir_id/files/:id", put(files::update_file_handler))
        .route("/directories/:dir_id/files/:id", delete(files::delete_file_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::auth_middleware,
        ));

    Router::new()
        .route("/user/login", post(auth::login_handler))
        .route("/config", get(auth::auth_config_handler))
        .merge(protected)
}
