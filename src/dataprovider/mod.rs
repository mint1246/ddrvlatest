use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod boltdb;
pub mod postgres;
pub mod types;

pub use types::{DataProviderError, File, Result};

use crate::ddrv::types::Node;

/// Global data provider instance
static PROVIDER: tokio::sync::OnceCell<Arc<dyn DataProvider>> = tokio::sync::OnceCell::const_new();

/// Load the global data provider
pub fn load(provider: Arc<dyn DataProvider>) {
    PROVIDER
        .set(provider)
        .map_err(|_| ())
        .expect("provider already initialized");
}

/// Get the global data provider
pub fn get() -> Arc<dyn DataProvider> {
    PROVIDER.get().expect("data provider not initialized").clone()
}

/// The DataProvider trait abstracts over different storage backends.
#[async_trait]
pub trait DataProvider: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// Get a file/dir by id (and optionally verify its parent)
    async fn get_by_id(&self, id: &str, parent: Option<&str>) -> Result<File>;

    /// Get direct children of a directory
    async fn get_children(&self, id: &str) -> Result<Vec<File>>;

    /// Create a new file or directory
    async fn create(&self, name: &str, parent: &str, is_dir: bool) -> Result<File>;

    /// Update file/directory metadata (rename/move)
    async fn update(&self, id: &str, parent: Option<&str>, file: &File) -> Result<File>;

    /// Delete a file or directory by id
    async fn delete(&self, id: &str, parent: Option<&str>) -> Result<()>;

    /// Get Discord attachment nodes for a file
    async fn get_nodes(&self, id: &str) -> Result<Vec<Node>>;

    /// Persist Discord attachment nodes for a file
    async fn create_nodes(&self, id: &str, nodes: &[Node]) -> Result<()>;

    /// Remove all nodes for a file (truncate)
    async fn truncate(&self, id: &str) -> Result<()>;

    /// Stat a file/directory by path
    async fn stat(&self, path: &str) -> Result<File>;

    /// List directory contents by path
    async fn ls(&self, path: &str, limit: i64, offset: i64) -> Result<Vec<File>>;

    /// Create a file at path if it doesn't exist (touch)
    async fn touch(&self, path: &str) -> Result<()>;

    /// Create directory (and parents) at path
    async fn mkdir(&self, path: &str) -> Result<()>;

    /// Remove file/directory at path
    async fn rm(&self, path: &str) -> Result<()>;

    /// Rename/move from old path to new path
    async fn mv(&self, old_path: &str, new_path: &str) -> Result<()>;

    /// Update the modification time of a file/directory
    async fn chtime(&self, path: &str, time: DateTime<Utc>) -> Result<()>;

    /// Close/cleanup the data provider
    async fn close(&self) -> Result<()>;
}
