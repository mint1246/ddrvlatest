use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::dataprovider::{DataProvider, DataProviderError, File, Result};
use crate::ddrv::{Driver, Node};

// ── table definitions ────────────────────────────────────────────────────────

const FS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("fs");
const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const ROOT: &str = "/";

// ── serialisation types ──────────────────────────────────────────────────────

/// What we actually persist in the FS table (path is the key, not stored here).
#[derive(Serialize, Deserialize, Clone)]
struct StoredFile {
    name: String,
    dir: bool,
    size: i64,
    mtime: DateTime<Utc>,
}

/// What we persist in the NODES table for a single chunk.
#[derive(Serialize, Deserialize, Clone)]
struct StoredNode {
    nid: i64,
    url: String,
    size: usize,
    start: i64,
    end: i64,
    mid: i64,
    ex: i64,
    is: i64,
    hm: String,
}

// ── path helpers ─────────────────────────────────────────────────────────────

fn encode_path(p: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(p.as_bytes())
}

fn decode_path(id: &str) -> Result<String> {
    if id == "root" {
        return Ok(ROOT.to_string());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(id)
        .map_err(|e| DataProviderError::Other(format!("base64 decode: {e}")))?;
    String::from_utf8(bytes).map_err(|e| DataProviderError::Other(format!("utf8: {e}")))
}

fn clean_path(p: &str) -> String {
    path_clean::clean(p).to_string_lossy().into_owned()
}

/// Return the parent path component (never empty; root's parent is ROOT).
fn parent_of(path: &str) -> &str {
    if path == ROOT {
        return ROOT;
    }
    match path.rfind('/') {
        Some(0) | None => ROOT,
        Some(pos) => &path[..pos],
    }
}

/// True if `child` is a **direct** child of `parent` (not a deeper descendant).
fn is_direct_child(parent: &str, child: &str) -> bool {
    // Normalise root to empty string for comparison
    let norm = if parent == ROOT {
        ""
    } else {
        parent.trim_end_matches('/')
    };
    match child.rfind('/') {
        Some(pos) => {
            let cp = &child[..pos];
            let cp = if cp.is_empty() { "" } else { cp };
            cp == norm
        }
        None => false,
    }
}

fn stored_to_file(path: &str, sf: &StoredFile) -> File {
    let pp = parent_of(path);
    let parent_id = if path == ROOT {
        None
    } else if pp == ROOT {
        Some("root".into())
    } else {
        Some(encode_path(pp))
    };
    let id = if path == ROOT { "root".into() } else { encode_path(path) };
    File {
        id,
        name: sf.name.clone(),
        dir: sf.dir,
        size: sf.size,
        parent: parent_id,
        mtime: sf.mtime,
    }
}

// ── node key helpers ─────────────────────────────────────────────────────────

/// Composite key: `<path>\x00<nid:020>` — null byte guarantees no collision
/// with any valid file path, while zero-padding makes lexicographic order
/// match numeric order.
fn node_key(path: &str, nid: i64) -> String {
    format!("{}\x00{:020}", path, nid)
}

fn node_range_start(path: &str) -> String {
    format!("{}\x00", path)
}

/// One past the last possible key for this path: `\x01` > `\x00`.
fn node_range_end(path: &str) -> String {
    format!("{}\x01", path)
}

// ── provider ─────────────────────────────────────────────────────────────────

pub struct BoltDbProvider {
    db: Arc<RwLock<Database>>,
    driver: Arc<Driver>,
}

impl BoltDbProvider {
    pub fn new(path: &str, driver: Arc<Driver>) -> Result<Self> {
        let db = Database::create(path).map_err(|e| DataProviderError::Database(e.to_string()))?;

        // Initialise tables and ensure the root directory exists.
        let write_txn = db.begin_write()?;
        {
            write_txn.open_table(NODES_TABLE)?;
            let mut fs_table = write_txn.open_table(FS_TABLE)?;
            if fs_table.get(ROOT)?.is_none() {
                let root = StoredFile {
                    name: ROOT.to_string(),
                    dir: true,
                    size: 0,
                    mtime: Utc::now(),
                };
                let data = bincode::serialize(&root)?;
                fs_table.insert(ROOT, data.as_slice())?;
            }
        }
        write_txn.commit()?;

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            driver,
        })
    }
}

// ── trait implementation ──────────────────────────────────────────────────────

#[async_trait]
impl DataProvider for BoltDbProvider {
    fn name(&self) -> &str {
        "redb"
    }

    // ── id-based ops ──────────────────────────────────────────────────────────

    async fn get_by_id(&self, id: &str, parent: Option<&str>) -> Result<File> {
        let path = decode_path(id)?;
        let expected_parent = parent.map(decode_path).transpose()?;
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<File> {
            let db = db.read().unwrap();
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(FS_TABLE)?;
            let sf = get_stored_file(&table, &path)?;
            let file = stored_to_file(&path, &sf);
            if let Some(exp_parent) = expected_parent {
                let actual_parent =
                    decode_path(file.parent.as_deref().unwrap_or(&encode_path(ROOT)))?;
                if actual_parent != exp_parent {
                    return Err(DataProviderError::InvalidParent);
                }
            }
            Ok(file)
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn get_children(&self, id: &str) -> Result<Vec<File>> {
        let path = decode_path(id)?;
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<Vec<File>> {
            let db = db.read().unwrap();
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(FS_TABLE)?;

            let entries = collect_direct_children(&table, &path)?;
            Ok(entries
                .into_iter()
                .map(|(p, sf)| stored_to_file(&p, &sf))
                .collect())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn create(&self, name: &str, parent: &str, is_dir: bool) -> Result<File> {
        let parent_path = decode_path(parent)?;
        let name = name.to_owned();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<File> {
            let db = db.write().unwrap();
            let new_path = clean_path(&format!("{}/{}", parent_path, name));
            let write_txn = db.begin_write()?;
            let file = {
                let mut fs_table = write_txn.open_table(FS_TABLE)?;

                // Verify parent exists and is a directory
                let parent_sf = get_stored_file(&fs_table, &parent_path)?;
                if !parent_sf.dir {
                    return Err(DataProviderError::InvalidParent);
                }

                // Reject duplicates
                if fs_table.get(new_path.as_str())?.is_some() {
                    return Err(DataProviderError::AlreadyExists);
                }

                let sf = StoredFile {
                    name: new_path.rsplit('/').next().unwrap_or(&name).to_owned(),
                    dir: is_dir,
                    size: 0,
                    mtime: Utc::now(),
                };
                let data = bincode::serialize(&sf)?;
                fs_table.insert(new_path.as_str(), data.as_slice())?;
                stored_to_file(&new_path, &sf)
            };
            write_txn.commit()?;
            Ok(file)
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn update(&self, id: &str, _parent: Option<&str>, file: &File) -> Result<File> {
        let old_path = decode_path(id)?;
        // Derive new path from new parent + new name
        let new_path = if let Some(ref pid) = file.parent {
            let pp = decode_path(pid)?;
            clean_path(&format!("{}/{}", pp, file.name))
        } else {
            clean_path(&format!("{}/{}", parent_of(&old_path), file.name))
        };

        if old_path != new_path {
            self.mv(&old_path, &new_path).await?;
        }
        self.stat(&new_path).await
    }

    async fn delete(&self, id: &str, parent: Option<&str>) -> Result<()> {
        let path = decode_path(id)?;
        if let Some(pid) = parent {
            let parent_path = decode_path(pid)?;
            let actual = parent_of(&path).to_owned();
            if actual != parent_path {
                return Err(DataProviderError::InvalidParent);
            }
        }
        self.rm(&path).await
    }

    // ── node ops ──────────────────────────────────────────────────────────────

    async fn get_nodes(&self, id: &str) -> Result<Vec<Node>> {
        let path = decode_path(id)?;
        let db = Arc::clone(&self.db);

        // Step 1: read persisted nodes
        let path_clone = path.clone();
        let mut nodes: Vec<Node> = tokio::task::spawn_blocking(move || -> Result<Vec<Node>> {
            let db = db.read().unwrap();
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(NODES_TABLE)?;
            collect_nodes(&table, &path_clone)
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))??;

        // Step 2: refresh any expired Discord URLs
        if crate::dataprovider::nodes_need_refresh(&nodes) {
            self.driver
                .update_nodes(&mut nodes)
                .await
                .map_err(|e| DataProviderError::Other(e.to_string()))?;

            // Step 3: persist refreshed nodes
            let db2 = Arc::clone(&self.db);
            let updated = nodes.clone();
            let path2 = path.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let db = db2.write().unwrap();
                let write_txn = db.begin_write()?;
                {
                    let mut table = write_txn.open_table(NODES_TABLE)?;
                    for n in &updated {
                        let key = node_key(&path2, n.nid);
                        let sn = StoredNode {
                            nid: n.nid,
                            url: n.url.clone(),
                            size: n.size,
                            start: n.start,
                            end: n.end,
                            mid: n.mid,
                            ex: n.ex,
                            is: n.is,
                            hm: n.hm.clone(),
                        };
                        let data = bincode::serialize(&sn)?;
                        table.insert(key.as_str(), data.as_slice())?;
                    }
                }
                write_txn.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| DataProviderError::Other(e.to_string()))??;
        }

        Ok(nodes)
    }

    async fn get_nodes_paged(
        &self,
        id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<Node>, usize, u64)> {
        let path = decode_path(id)?;
        let db = Arc::clone(&self.db);

        // Read all persisted nodes to determine the total count and compute the
        // cumulative byte offset at the start of the requested page.  Only the
        // node metadata (URL, size, timestamps) is read – actual file bytes are
        // never loaded here.  Node counts in practice are small (typically < 100
        // even for multi-GB files at 25 MB/chunk), so this is not a concern.
        let path_clone = path.clone();
        let all_nodes: Vec<Node> = tokio::task::spawn_blocking(move || -> Result<Vec<Node>> {
            let db = db.read().unwrap();
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(NODES_TABLE)?;
            collect_nodes(&table, &path_clone)
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))??;

        let total = all_nodes.len();

        // Compute the cumulative byte offset at the start of the requested page.
        let byte_offset: u64 = all_nodes
            .iter()
            .take(offset)
            .map(|n| n.size as u64)
            .sum();

        if offset >= total || limit == 0 {
            return Ok((Vec::new(), total, byte_offset));
        }

        let end = (offset + limit).min(total);
        let mut page: Vec<Node> = all_nodes[offset..end].to_vec();

        // Refresh only the nodes in the requested page to limit Discord API calls.
        if crate::dataprovider::nodes_need_refresh(&page) {
            self.driver
                .update_nodes(&mut page)
                .await
                .map_err(|e| DataProviderError::Other(e.to_string()))?;

            // Persist the refreshed page nodes back to the database.
            let db2 = Arc::clone(&self.db);
            let updated = page.clone();
            let path2 = path.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let db = db2.write().unwrap();
                let write_txn = db.begin_write()?;
                {
                    let mut table = write_txn.open_table(NODES_TABLE)?;
                    for n in &updated {
                        let key = node_key(&path2, n.nid);
                        let sn = StoredNode {
                            nid: n.nid,
                            url: n.url.clone(),
                            size: n.size,
                            start: n.start,
                            end: n.end,
                            mid: n.mid,
                            ex: n.ex,
                            is: n.is,
                            hm: n.hm.clone(),
                        };
                        let data = bincode::serialize(&sn)?;
                        table.insert(key.as_str(), data.as_slice())?;
                    }
                }
                write_txn.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| DataProviderError::Other(e.to_string()))??;
        }

        Ok((page, total, byte_offset))
    }

    async fn create_nodes(&self, id: &str, nodes: &[Node]) -> Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let path = decode_path(id)?;
        let nodes: Vec<Node> = nodes.to_vec();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = db.write().unwrap();

            // Count existing nodes to determine starting nid
            let existing_count: i64 = {
                let read_txn = db.begin_read()?;
                let table = read_txn.open_table(NODES_TABLE)?;
                let start = node_range_start(&path);
                let end = node_range_end(&path);
                table.range(start.as_str()..end.as_str())?.count() as i64
            };

            let write_txn = db.begin_write()?;
            {
                let mut fs_table = write_txn.open_table(FS_TABLE)?;
                let mut nodes_table = write_txn.open_table(NODES_TABLE)?;

                for (i, n) in nodes.iter().enumerate() {
                    let nid = if n.nid > 0 {
                        n.nid
                    } else {
                        existing_count + i as i64
                    };
                    let key = node_key(&path, nid);
                    let sn = StoredNode {
                        nid,
                        url: n.url.clone(),
                        size: n.size,
                        start: n.start,
                        end: n.end,
                        mid: n.mid,
                        ex: n.ex,
                        is: n.is,
                        hm: n.hm.clone(),
                    };
                    let data = bincode::serialize(&sn)?;
                    nodes_table.insert(key.as_str(), data.as_slice())?;
                }

                // Update file size: total of all node sizes
                let start = node_range_start(&path);
                let end_range = node_range_end(&path);
                let all_nodes = collect_nodes_from_table(&nodes_table, &path, &start, &end_range)?;
                let new_size: i64 = all_nodes.iter().map(|n| n.size as i64).sum();

                let existing = fs_table.get(path.as_str())?.map(|g| g.value().to_vec());
                if let Some(raw) = existing {
                    let mut sf: StoredFile = bincode::deserialize(&raw)?;
                    sf.size = new_size;
                    let data = bincode::serialize(&sf)?;
                    fs_table.insert(path.as_str(), data.as_slice())?;
                }
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn truncate(&self, id: &str) -> Result<()> {
        let path = decode_path(id)?;
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = db.write().unwrap();
            let write_txn = db.begin_write()?;
            {
                let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
                let mut fs_table = write_txn.open_table(FS_TABLE)?;

                let start = node_range_start(&path);
                let end = node_range_end(&path);
                let keys: Vec<String> = {
                    nodes_table
                        .range(start.as_str()..end.as_str())?
                        .map(|r| r.map(|(k, _v)| k.value().to_owned()))
                        .collect::<std::result::Result<Vec<_>, _>>()?
                };
                for key in keys {
                    nodes_table.remove(key.as_str())?;
                }

                // Reset file size
                let existing = fs_table.get(path.as_str())?.map(|g| g.value().to_vec());
                if let Some(raw) = existing {
                    let mut sf: StoredFile = bincode::deserialize(&raw)?;
                    sf.size = 0;
                    let data = bincode::serialize(&sf)?;
                    fs_table.insert(path.as_str(), data.as_slice())?;
                }
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    // ── path-based ops ────────────────────────────────────────────────────────

    async fn stat(&self, path: &str) -> Result<File> {
        let path = clean_path(path);
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<File> {
            let db = db.read().unwrap();
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(FS_TABLE)?;
            let sf = get_stored_file(&table, &path)?;
            Ok(stored_to_file(&path, &sf))
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn ls(&self, path: &str, limit: i64, offset: i64) -> Result<Vec<File>> {
        let path = clean_path(path);
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<Vec<File>> {
            let db = db.read().unwrap();
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(FS_TABLE)?;

            // Verify the directory exists
            if table.get(path.as_str())?.is_none() {
                return Err(DataProviderError::NotFound);
            }

            let children_entries = collect_direct_children(&table, &path)?;
            let mut children: Vec<File> = children_entries
                .iter()
                .map(|(p, sf)| stored_to_file(p, sf))
                .collect();
            // Stable sort by name
            children.sort_by(|a, b| a.name.cmp(&b.name));

            let start = offset.max(0) as usize;
            let limit = if limit <= 0 {
                children.len()
            } else {
                limit as usize
            };
            Ok(children.into_iter().skip(start).take(limit).collect())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn touch(&self, path: &str) -> Result<()> {
        let path = clean_path(path);
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = db.write().unwrap();
            let write_txn = db.begin_write()?;
            {
                let mut fs_table = write_txn.open_table(FS_TABLE)?;
                if fs_table.get(path.as_str())?.is_none() {
                    let name = path.rsplit('/').next().unwrap_or(&path).to_owned();
                    let sf = StoredFile {
                        name,
                        dir: false,
                        size: 0,
                        mtime: Utc::now(),
                    };
                    let data = bincode::serialize(&sf)?;
                    fs_table.insert(path.as_str(), data.as_slice())?;
                }
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn mkdir(&self, path: &str) -> Result<()> {
        let path = clean_path(path);
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = db.write().unwrap();

            // Collect all path components bottom-up (root first).
            let mut components: Vec<String> = Vec::new();
            let mut cur = path.as_str();
            loop {
                components.push(cur.to_owned());
                if cur == ROOT {
                    break;
                }
                cur = parent_of(cur);
            }
            components.reverse();

            let write_txn = db.begin_write()?;
            {
                let mut fs_table = write_txn.open_table(FS_TABLE)?;
                for component in &components {
                    if fs_table.get(component.as_str())?.is_none() {
                        let name = component.rsplit('/').next().unwrap_or(component).to_owned();
                        let sf = StoredFile {
                            name,
                            dir: true,
                            size: 0,
                            mtime: Utc::now(),
                        };
                        let data = bincode::serialize(&sf)?;
                        fs_table.insert(component.as_str(), data.as_slice())?;
                    }
                }
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn rm(&self, path: &str) -> Result<()> {
        let path = clean_path(path);
        if path == ROOT {
            return Err(DataProviderError::PermissionDenied);
        }
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = db.write().unwrap();

            // Collect FS paths to delete (this path + all descendants)
            let fs_paths: Vec<String> = {
                let read_txn = db.begin_read()?;
                let table = read_txn.open_table(FS_TABLE)?;
                if table.get(path.as_str())?.is_none() {
                    return Err(DataProviderError::NotFound);
                }
                collect_descendants(&table, &path)?
            };

            // Collect node keys to delete
            let node_keys: Vec<String> = {
                let read_txn = db.begin_read()?;
                let table = read_txn.open_table(NODES_TABLE)?;
                let mut keys = Vec::new();
                for p in &fs_paths {
                    let start = node_range_start(p);
                    let end = node_range_end(p);
                    let partial: Vec<String> = table
                        .range(start.as_str()..end.as_str())?
                        .map(|r| r.map(|(k, _v)| k.value().to_owned()))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    keys.extend(partial);
                }
                keys
            };

            let write_txn = db.begin_write()?;
            {
                let mut fs_table = write_txn.open_table(FS_TABLE)?;
                let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
                for p in &fs_paths {
                    fs_table.remove(p.as_str())?;
                }
                for k in &node_keys {
                    nodes_table.remove(k.as_str())?;
                }
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn mv(&self, old_path: &str, new_path: &str) -> Result<()> {
        let old_path = clean_path(old_path);
        let new_path = clean_path(new_path);
        if old_path == new_path {
            return Ok(());
        }
        if new_path == ROOT {
            return Err(DataProviderError::PermissionDenied);
        }
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = db.write().unwrap();

            // Read all FS entries that will move: (old_path, new_path, raw_bytes)
            let fs_moves: Vec<(String, String, Vec<u8>)> = {
                let read_txn = db.begin_read()?;
                let table = read_txn.open_table(FS_TABLE)?;

                let guard = table
                    .get(old_path.as_str())?
                    .ok_or(DataProviderError::NotFound)?;
                let mut moves = vec![(old_path.clone(), new_path.clone(), guard.value().to_vec())];

                // Collect descendants
                let descendants = collect_descendants(&table, &old_path)?;
                for desc in descendants {
                    if desc == old_path {
                        continue; // already handled
                    }
                    let suffix = &desc[old_path.len()..]; // starts with '/'
                    let new_desc = format!("{}{}", new_path, suffix);
                    let guard = table
                        .get(desc.as_str())?
                        .ok_or(DataProviderError::NotFound)?;
                    moves.push((desc, new_desc, guard.value().to_vec()));
                }
                moves
            };

            // Read node entries for all paths that are moving
            let node_moves: Vec<(String, String, Vec<u8>)> = {
                let read_txn = db.begin_read()?;
                let table = read_txn.open_table(NODES_TABLE)?;
                let mut moves = Vec::new();
                for (old_p, new_p, _) in &fs_moves {
                    let start = node_range_start(old_p);
                    let end = node_range_end(old_p);
                    for entry in table.range(start.as_str()..end.as_str())? {
                        let (k, v) = entry?;
                        let old_key = k.value().to_owned();
                        // Replace old_path prefix with new_path in the key
                        let suffix = &old_key[old_p.len()..]; // "\x00nnn..."
                        let new_key = format!("{}{}", new_p, suffix);
                        moves.push((old_key, new_key, v.value().to_vec()));
                    }
                }
                moves
            };

            // Apply all moves in a single write transaction
            let write_txn = db.begin_write()?;
            {
                let mut fs_table = write_txn.open_table(FS_TABLE)?;
                let mut nodes_table = write_txn.open_table(NODES_TABLE)?;

                for (old_p, new_p, data) in &fs_moves {
                    // Update StoredFile.name to reflect the new basename
                    let mut sf: StoredFile = bincode::deserialize(data)?;
                    sf.name = new_p.rsplit('/').next().unwrap_or(new_p).to_owned();
                    let new_data = bincode::serialize(&sf)?;
                    fs_table.insert(new_p.as_str(), new_data.as_slice())?;
                    fs_table.remove(old_p.as_str())?;
                }
                for (old_k, new_k, data) in &node_moves {
                    nodes_table.insert(new_k.as_str(), data.as_slice())?;
                    nodes_table.remove(old_k.as_str())?;
                }
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn chtime(&self, path: &str, time: DateTime<Utc>) -> Result<()> {
        let path = clean_path(path);
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = db.write().unwrap();
            let write_txn = db.begin_write()?;
            {
                let mut fs_table = write_txn.open_table(FS_TABLE)?;
                let guard = fs_table
                    .get(path.as_str())?
                    .ok_or(DataProviderError::NotFound)?;
                let mut sf: StoredFile = bincode::deserialize(guard.value())?;
                drop(guard);
                sf.mtime = time;
                let data = bincode::serialize(&sf)?;
                fs_table.insert(path.as_str(), data.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| DataProviderError::Other(e.to_string()))?
    }

    async fn close(&self) -> Result<()> {
        // redb flushes on every commit; nothing extra needed here.
        Ok(())
    }
}

// ── private helpers ───────────────────────────────────────────────────────────

/// Look up a single file, returning NotFound if absent.
fn get_stored_file(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    path: &str,
) -> Result<StoredFile> {
    let guard = table.get(path)?.ok_or(DataProviderError::NotFound)?;
    let sf: StoredFile = bincode::deserialize(guard.value())?;
    Ok(sf)
}

/// Collect direct children of `parent` using a prefix range scan.
fn collect_direct_children(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    parent: &str,
) -> Result<Vec<(String, StoredFile)>> {
    let prefix = if parent == ROOT {
        "/".to_string()
    } else {
        format!("{}/", parent.trim_end_matches('/'))
    };

    let mut out = Vec::new();
    for entry in table.range(prefix.as_str()..)? {
        let (k, v) = entry?;
        let p = k.value();
        if !p.starts_with(prefix.as_str()) {
            break;
        }
        if p != ROOT && is_direct_child(parent, p) {
            let sf: StoredFile = bincode::deserialize(v.value())?;
            out.push((p.to_owned(), sf));
        }
    }
    Ok(out)
}

/// Return `path` itself plus all its descendant FS paths.
fn collect_descendants(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    path: &str,
) -> Result<Vec<String>> {
    let prefix = if path == ROOT {
        String::new() // every path starts with '/'
    } else {
        format!("{}/", path)
    };
    let mut paths = vec![path.to_owned()];
    for entry in table.iter()? {
        let (k, _) = entry?;
        let p = k.value();
        if p == path {
            continue;
        }
        if path == ROOT || p.starts_with(prefix.as_str()) {
            paths.push(p.to_owned());
        }
    }
    Ok(paths)
}

/// Collect all Node values for a given file path from the NODES table.
fn collect_nodes(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    path: &str,
) -> Result<Vec<Node>> {
    let start = node_range_start(path);
    let end = node_range_end(path);
    collect_nodes_from_table(table, path, &start, &end)
}

fn collect_nodes_from_table(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    _path: &str,
    start: &str,
    end: &str,
) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();
    for entry in table.range(start..end)? {
        let (_, v) = entry?;
        let sn: StoredNode = bincode::deserialize(v.value())?;
        nodes.push(Node {
            nid: sn.nid,
            url: sn.url,
            size: sn.size,
            start: sn.start,
            end: sn.end,
            mid: sn.mid,
            ex: sn.ex,
            is: sn.is,
            hm: sn.hm,
        });
    }
    Ok(nodes)
}
