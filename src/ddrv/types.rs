use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_CHUNK_SIZE: usize = 25 * 1024 * 1024;
pub const MAX_CHUNK_SIZE_NITRO: usize = 500 * 1024 * 1024;
pub const MAX_CHUNK_SIZE_NITRO_BASIC: usize = 50 * 1024 * 1024;

pub const TOKEN_BOT: i32 = 0;
pub const TOKEN_USER: i32 = 1;
pub const TOKEN_USER_NITRO: i32 = 2;
pub const TOKEN_USER_NITRO_BASIC: i32 = 3;

/// A Discord attachment chunk – stores the URL and metadata for one piece of a file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Node {
    /// Internal ID used by data providers (not used in ddrv itself)
    pub nid: i64,
    /// URL where the data is stored (without query params)
    pub url: String,
    /// Size of this chunk in bytes
    pub size: usize,
    /// Start byte position in the overall file
    pub start: i64,
    /// End byte position in the overall file
    pub end: i64,
    /// Discord message ID
    pub mid: i64,
    /// Link expiry timestamp (unix, hex in URL)
    pub ex: i64,
    /// Link issued timestamp (unix, hex in URL)
    pub is: i64,
    /// Link HMAC signature
    pub hm: String,
}

/// A Discord message containing attachments.
#[derive(Debug, Deserialize)]
pub struct Message {
    pub id: String,
    pub attachments: Vec<NodeAttachment>,
}

/// Raw attachment as returned by Discord API.
#[derive(Debug, Deserialize)]
pub struct NodeAttachment {
    pub url: String,
    pub size: usize,
}

#[derive(Debug, Error)]
pub enum DdrvError {
    #[error("writer/reader is closed")]
    Closed,
    #[error("writer/reader is already closed")]
    AlreadyClosed,
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Discord API error: expected {expected}, got {got}: {body}")]
    DiscordApi {
        expected: u16,
        got: u16,
        body: String,
    },
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, DdrvError>;
