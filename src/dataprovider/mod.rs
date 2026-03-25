use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;

pub mod boltdb;
pub mod postgres;
pub mod types;

pub use types::{DataProviderError, File, Result};

use crate::ddrv::types::Node;

/// How far ahead of `ex` we refresh Discord URLs (seconds).
pub const NODE_RENEWAL_HEADROOM_SECS: i64 = 10 * 60;

/// True if any node is expired or close enough to expiry that we should refresh it now.
pub fn nodes_need_refresh(nodes: &[Node]) -> bool {
    let now = Utc::now().timestamp();
    nodes
        .iter()
        .any(|n| n.ex > 0 && now + NODE_RENEWAL_HEADROOM_SECS >= n.ex)
}

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
    PROVIDER
        .get()
        .expect("data provider not initialized")
        .clone()
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

    /// Get a paginated slice of nodes for a file, refreshing only the URLs in the
    /// requested range to avoid hammering the Discord API with large files.
    ///
    /// Returns `(page_nodes, total_chunk_count, byte_offset_of_first_page_node)`.
    /// - `offset` is the zero-based index of the first chunk in the page.
    /// - `limit` is the maximum number of chunks to return.
    async fn get_nodes_paged(
        &self,
        id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<Node>, usize, u64)>;

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
