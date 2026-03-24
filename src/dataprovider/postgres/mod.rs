use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row as _;
use std::sync::Arc;

use crate::dataprovider::{DataProvider, DataProviderError, File, Result};
use crate::ddrv::{Driver, Node};

// ── error mapping ─────────────────────────────────────────────────────────────

fn map_sqlx_err(e: sqlx::Error) -> DataProviderError {
    match e {
        sqlx::Error::RowNotFound => DataProviderError::NotFound,
        e => DataProviderError::Database(e.to_string()),
    }
}

// ── row converters ────────────────────────────────────────────────────────────

fn row_to_file(row: &sqlx::postgres::PgRow) -> Result<File> {
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx_err)?;
    let name: String = row.try_get("name").map_err(map_sqlx_err)?;
    let dir: bool = row.try_get("dir").map_err(map_sqlx_err)?;
    let size: i64 = row.try_get("size").map_err(map_sqlx_err)?;
    let parent: Option<uuid::Uuid> = row.try_get("parent").map_err(map_sqlx_err)?;
    let mtime: DateTime<Utc> = row.try_get("mtime").map_err(map_sqlx_err)?;
    Ok(File {
        id: id.to_string(),
        name,
        dir,
        size,
        parent: parent.map(|u| u.to_string()),
        mtime,
    })
}

fn row_to_node(row: &sqlx::postgres::PgRow) -> Result<Node> {
    let nid: i64 = row.try_get("id").map_err(map_sqlx_err)?;
    let url: String = row.try_get("url").map_err(map_sqlx_err)?;
    let size: i64 = row.try_get("size").map_err(map_sqlx_err)?;
    let start: i64 = row.try_get("start").map_err(map_sqlx_err)?;
    let end: i64 = row.try_get("end").map_err(map_sqlx_err)?;
    let mid: i64 = row.try_get("mid").map_err(map_sqlx_err)?;
    let ex: i64 = row.try_get("ex").map_err(map_sqlx_err)?;
    let is: i64 = row.try_get("is").map_err(map_sqlx_err)?;
    let hm: String = row.try_get("hm").map_err(map_sqlx_err)?;
    Ok(Node {
        nid,
        url,
        size: size as usize,
        start,
        end,
        mid,
        ex,
        is,
        hm,
    })
}

/// Parse a string that could be either a UUID (postgres backend) or a plain
/// path string, returning a `uuid::Uuid`. For non-UUID IDs (e.g. from
/// high-level callers), returns an error.
fn parse_uuid(id: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(id).map_err(|e| DataProviderError::Other(format!("invalid uuid: {e}")))
}

// ── provider ──────────────────────────────────────────────────────────────────

pub struct PgProvider {
    pool: sqlx::PgPool,
    driver: Arc<Driver>,
}

/// Configuration for the PostgreSQL data provider.
pub struct PostgresConfig {
    pub db_url: String,
}

impl PgProvider {
    pub async fn new(config: &PostgresConfig, driver: Arc<Driver>) -> Self {
        let pool = sqlx::PgPool::connect(&config.db_url)
            .await
            .expect("postgres connect failed");
        PgProvider { pool, driver }
    }
}

// ── trait implementation ──────────────────────────────────────────────────────

#[async_trait]
impl DataProvider for PgProvider {
    fn name(&self) -> &str {
        "postgres"
    }

    // ── id-based ops ──────────────────────────────────────────────────────────

    async fn get_by_id(&self, id: &str, parent: Option<&str>) -> Result<File> {
        let uuid = parse_uuid(id)?;
        let parent_uuid = parent.map(parse_uuid).transpose()?;

        let row = sqlx::query(
            "SELECT id, name, dir, size, parent, mtime FROM fs WHERE id = $1",
        )
        .bind(uuid)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let file = row_to_file(&row)?;

        if let Some(expected_parent) = parent_uuid {
            let actual = file
                .parent
                .as_deref()
                .and_then(|p| uuid::Uuid::parse_str(p).ok());
            if actual != Some(expected_parent) {
                return Err(DataProviderError::InvalidParent);
            }
        }
        Ok(file)
    }

    async fn get_children(&self, id: &str) -> Result<Vec<File>> {
        let uuid = parse_uuid(id)?;

        let rows = sqlx::query(
            "SELECT id, name, dir, size, parent, mtime FROM fs WHERE parent = $1 ORDER BY name",
        )
        .bind(uuid)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        rows.iter().map(row_to_file).collect()
    }

    async fn create(&self, name: &str, parent: &str, is_dir: bool) -> Result<File> {
        let parent_uuid = parse_uuid(parent)?;
        let new_id = uuid::Uuid::new_v4();

        let row = sqlx::query(
            "INSERT INTO fs (id, name, dir, size, parent, mtime)
             VALUES ($1, $2, $3, 0, $4, NOW())
             RETURNING id, name, dir, size, parent, mtime",
        )
        .bind(new_id)
        .bind(name)
        .bind(is_dir)
        .bind(parent_uuid)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(ref dbe) if dbe.code().as_deref() == Some("23505") => {
                DataProviderError::AlreadyExists
            }
            sqlx::Error::Database(ref dbe) if dbe.code().as_deref() == Some("23503") => {
                DataProviderError::InvalidParent
            }
            e => DataProviderError::Database(e.to_string()),
        })?;

        row_to_file(&row)
    }

    async fn update(&self, id: &str, _parent: Option<&str>, file: &File) -> Result<File> {
        let uuid = parse_uuid(id)?;
        let new_parent = file.parent.as_deref().map(parse_uuid).transpose()?;

        let row = sqlx::query(
            "UPDATE fs SET name = $1, parent = $2, mtime = NOW()
             WHERE id = $3
             RETURNING id, name, dir, size, parent, mtime",
        )
        .bind(&file.name)
        .bind(new_parent)
        .bind(uuid)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        row_to_file(&row)
    }

    async fn delete(&self, id: &str, parent: Option<&str>) -> Result<()> {
        let uuid = parse_uuid(id)?;
        if let Some(pid) = parent {
            // Verify parent before deleting
            let expected = parse_uuid(pid)?;
            let row = sqlx::query("SELECT parent FROM fs WHERE id = $1")
                .bind(uuid)
                .fetch_one(&self.pool)
                .await
                .map_err(map_sqlx_err)?;
            let actual: Option<uuid::Uuid> = row.try_get("parent").map_err(map_sqlx_err)?;
            if actual != Some(expected) {
                return Err(DataProviderError::InvalidParent);
            }
        }

        let affected = sqlx::query("DELETE FROM fs WHERE id = $1")
            .bind(uuid)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?
            .rows_affected();

        if affected == 0 {
            return Err(DataProviderError::NotFound);
        }
        Ok(())
    }

    // ── node ops ──────────────────────────────────────────────────────────────

    async fn get_nodes(&self, id: &str) -> Result<Vec<Node>> {
        let uuid = parse_uuid(id)?;

        let rows = sqlx::query(
            r#"SELECT id, url, size, "start", "end", mid, ex, "is", hm
               FROM node WHERE file = $1 ORDER BY id"#,
        )
        .bind(uuid)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let mut nodes: Vec<Node> = rows.iter().map(row_to_node).collect::<Result<Vec<_>>>()?;

        // Refresh any expired Discord CDN URLs
        let now = Utc::now().timestamp();
        let has_expired = nodes.iter().any(|n| n.ex > 0 && now > n.ex);
        if has_expired {
            self.driver
                .update_nodes(&mut nodes)
                .await
                .map_err(|e| DataProviderError::Other(e.to_string()))?;

            // Persist refreshed URL/expiry data back to the database
            for n in &nodes {
                sqlx::query(
                    r#"UPDATE node SET url = $1, ex = $2, "is" = $3, hm = $4
                       WHERE id = $5"#,
                )
                .bind(&n.url)
                .bind(n.ex)
                .bind(n.is)
                .bind(&n.hm)
                .bind(n.nid)
                .execute(&self.pool)
                .await
                .map_err(map_sqlx_err)?;
            }
        }
        Ok(nodes)
    }

    async fn create_nodes(&self, id: &str, nodes: &[Node]) -> Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let uuid = parse_uuid(id)?;

        for n in nodes {
            sqlx::query(
                r#"INSERT INTO node (file, url, size, "start", "end", mid, ex, "is", hm)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
            )
            .bind(uuid)
            .bind(&n.url)
            .bind(n.size as i64)
            .bind(n.start)
            .bind(n.end)
            .bind(n.mid)
            .bind(n.ex)
            .bind(n.is)
            .bind(&n.hm)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        }

        // Update the file's aggregate size
        sqlx::query(
            "UPDATE fs SET size = (SELECT COALESCE(SUM(size), 0) FROM node WHERE file = $1)
             WHERE id = $1",
        )
        .bind(uuid)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        Ok(())
    }

    async fn truncate(&self, id: &str) -> Result<()> {
        let uuid = parse_uuid(id)?;

        sqlx::query("DELETE FROM node WHERE file = $1")
            .bind(uuid)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;

        sqlx::query("UPDATE fs SET size = 0 WHERE id = $1")
            .bind(uuid)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;

        Ok(())
    }

    // ── path-based ops ────────────────────────────────────────────────────────

    async fn stat(&self, path: &str) -> Result<File> {
        let rows = sqlx::query(
            "SELECT id, name, dir, size, parent, mtime FROM stat($1)",
        )
        .bind(path)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        rows.into_iter()
            .next()
            .ok_or(DataProviderError::NotFound)
            .and_then(|row| row_to_file(&row))
    }

    async fn ls(&self, path: &str, limit: i64, offset: i64) -> Result<Vec<File>> {
        let rows = sqlx::query(
            "SELECT id, name, dir, size, parent, mtime
             FROM ls($1) ORDER BY name LIMIT $2 OFFSET $3",
        )
        .bind(path)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        rows.iter().map(row_to_file).collect()
    }

    async fn touch(&self, path: &str) -> Result<()> {
        sqlx::query("SELECT touch($1)")
            .bind(path)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn mkdir(&self, path: &str) -> Result<()> {
        sqlx::query("SELECT mkdir($1)")
            .bind(path)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn rm(&self, path: &str) -> Result<()> {
        sqlx::query("SELECT rm($1)")
            .bind(path)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn mv(&self, old_path: &str, new_path: &str) -> Result<()> {
        sqlx::query("SELECT mv($1, $2)")
            .bind(old_path)
            .bind(new_path)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn chtime(&self, path: &str, time: DateTime<Utc>) -> Result<()> {
        // Resolve path to file id via the stat stored function, then update mtime.
        let affected = sqlx::query(
            "UPDATE fs SET mtime = $1
             WHERE id = (SELECT id FROM stat($2) LIMIT 1)",
        )
        .bind(time)
        .bind(path)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?
        .rows_affected();

        if affected == 0 {
            return Err(DataProviderError::NotFound);
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.pool.close().await;
        Ok(())
    }
}
