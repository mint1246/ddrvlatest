use axum::{
    body::Body,
    http::{Request, Response, StatusCode, Uri},
    response::IntoResponse,
};
use rust_embed::RustEmbed;
use tower::ServiceExt;
use tower_http::services::ServeDir;

#[derive(RustEmbed)]
#[folder = "internal/http/web/static/"]
struct Static;

/// Returns an Axum service that serves the embedded static files.
pub fn static_handler() -> axum::routing::MethodRouter<crate::http::AppState> {
    axum::routing::get(serve_static)
}

async fn serve_static(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Static::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", mime)
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => {
            // Fall back to index.html for SPA routing
            match Static::get("index.html") {
                Some(content) => Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "text/html")
                    .body(Body::from(content.data.into_owned()))
                    .unwrap(),
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("not found"))
                    .unwrap(),
            }
        }
    }
}
