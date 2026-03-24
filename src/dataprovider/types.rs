use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// File/directory metadata stored in the data provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub id: String,
    pub name: String,
    pub dir: bool,
    pub size: i64,
    pub parent: Option<String>,
    pub mtime: DateTime<Utc>,
}

impl Default for File {
    fn default() -> Self {
        File {
            id: String::new(),
            name: String::new(),
            dir: false,
            size: 0,
            parent: None,
            mtime: Utc::now(),
        }
    }
}

#[derive(Debug, Error)]
pub enum DataProviderError {
    #[error("file already exists")]
    AlreadyExists,
    #[error("file or directory not found")]
    NotFound,
    #[error("permission denied")]
    PermissionDenied,
    #[error("invalid parent: parent does not exist or is not a directory")]
    InvalidParent,
    #[error("database error: {0}")]
    Database(String),
    #[error("other error: {0}")]
    Other(String),
}

impl From<redb::Error> for DataProviderError {
    fn from(e: redb::Error) -> Self {
        DataProviderError::Database(e.to_string())
    }
}

impl From<redb::DatabaseError> for DataProviderError {
    fn from(e: redb::DatabaseError) -> Self {
        DataProviderError::Database(e.to_string())
    }
}

impl From<redb::TransactionError> for DataProviderError {
    fn from(e: redb::TransactionError) -> Self {
        DataProviderError::Database(e.to_string())
    }
}

impl From<redb::TableError> for DataProviderError {
    fn from(e: redb::TableError) -> Self {
        DataProviderError::Database(e.to_string())
    }
}

impl From<redb::StorageError> for DataProviderError {
    fn from(e: redb::StorageError) -> Self {
        DataProviderError::Database(e.to_string())
    }
}

impl From<redb::CommitError> for DataProviderError {
    fn from(e: redb::CommitError) -> Self {
        DataProviderError::Database(e.to_string())
    }
}

impl From<bincode::Error> for DataProviderError {
    fn from(e: bincode::Error) -> Self {
        DataProviderError::Database(format!("serialization: {}", e))
    }
}

pub type Result<T> = std::result::Result<T, DataProviderError>;
