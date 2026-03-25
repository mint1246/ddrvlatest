use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

const FS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("fs");
const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const ROOT: &str = "/";

#[derive(Debug, Deserialize)]
struct LegacyExport {
    files: Vec<LegacyFile>,
    nodes: Vec<LegacyNode>,
}

#[derive(Debug, Deserialize)]
struct LegacyFile {
    path: String,
    name: String,
    dir: bool,
    size: i64,
    mtime_unix: i64,
}

#[derive(Debug, Deserialize)]
struct LegacyNode {
    path: String,
    nid: i64,
    url: String,
    size: i64,
    start: i64,
    end: i64,
    mid: i64,
    ex: i64,
    is: i64,
    hm: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct StoredFile {
    name: String,
    dir: bool,
    size: i64,
    mtime: DateTime<Utc>,
}

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

fn node_key(path: &str, nid: i64) -> String {
    format!("{}\x00{:020}", path, nid)
}

fn to_utc(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0)
        .single()
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn write_redb_from_export(output: &Path, export: LegacyExport) -> Result<()> {
    let db = Database::create(output).context("create redb output")?;
    let write_txn = db.begin_write().context("begin redb write txn")?;

    {
        let mut fs_table = write_txn.open_table(FS_TABLE).context("open fs table")?;
        for file in export.files {
            let path = if file.path.is_empty() {
                ROOT.to_owned()
            } else {
                file.path
            };
            let sf = StoredFile {
                name: if path == ROOT {
                    ROOT.to_owned()
                } else if file.name.is_empty() {
                    path.rsplit('/').next().unwrap_or(&path).to_owned()
                } else {
                    file.name
                },
                dir: file.dir,
                size: file.size,
                mtime: to_utc(file.mtime_unix),
            };
            let encoded = bincode::serialize(&sf).context("serialize file record")?;
            fs_table
                .insert(path.as_str(), encoded.as_slice())
                .context("insert file record")?;
        }

        if fs_table
            .get(ROOT)
            .context("read root after file import")?
            .is_none()
        {
            let root = StoredFile {
                name: ROOT.to_owned(),
                dir: true,
                size: 0,
                mtime: Utc::now(),
            };
            let encoded = bincode::serialize(&root).context("serialize root")?;
            fs_table
                .insert(ROOT, encoded.as_slice())
                .context("insert synthesized root")?;
        }
    }

    {
        let mut nodes_table = write_txn
            .open_table(NODES_TABLE)
            .context("open nodes table")?;
        for node in export.nodes {
            let size: usize = usize::try_from(node.size)
                .map_err(|_| anyhow::anyhow!("invalid negative node size for {}", node.path))?;
            let sn = StoredNode {
                nid: node.nid,
                url: node.url,
                size,
                start: node.start,
                end: node.end,
                mid: node.mid,
                ex: node.ex,
                is: node.is,
                hm: node.hm,
            };
            let encoded = bincode::serialize(&sn).context("serialize node record")?;
            let key = node_key(&node.path, node.nid);
            nodes_table
                .insert(key.as_str(), encoded.as_slice())
                .context("insert node record")?;
        }
    }

    write_txn.commit().context("commit redb migration")?;
    Ok(())
}

fn run_go_exporter(input: &Path) -> Result<LegacyExport> {
    let workdir = std::env::temp_dir().join(format!("ddrv-migrator-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&workdir).context("create temporary go exporter dir")?;

    let go_mod = r#"module ddrvlegacyexport

go 1.24

require go.etcd.io/bbolt v1.3.8
"#;

    let go_src = r#"package main

import (
"bytes"
"encoding/gob"
"encoding/json"
"fmt"
"os"
"path"
"time"

"go.etcd.io/bbolt"
)

type LegacyFile struct {
Path      string `json:"path"`
Name      string `json:"name"`
Dir       bool   `json:"dir"`
Size      int64  `json:"size"`
MTimeUnix int64  `json:"mtime_unix"`
}

type LegacyNode struct {
Path  string `json:"path"`
NId   int64  `json:"nid"`
URL   string `json:"url"`
Size  int    `json:"size"`
Start int64  `json:"start"`
End   int64  `json:"end"`
MId   int64  `json:"mid"`
Ex    int    `json:"ex"`
Is    int    `json:"is"`
Hm    string `json:"hm"`
}

type Export struct {
Files []LegacyFile `json:"files"`
Nodes []LegacyNode `json:"nodes"`
}

type FileRecord struct {
Id     string
Name   string
Dir    bool
Size   int64
Parent string
MTime  time.Time
}

type NodeRecord struct {
NId   int64
URL   string
Size  int
Start int64
End   int64
MId   int64
Ex    int
Is    int
Hm    string
}

func decodeFile(v []byte) (FileRecord, error) {
var out FileRecord
dec := gob.NewDecoder(bytes.NewReader(v))
if err := dec.Decode(&out); err != nil {
return out, err
}
return out, nil
}

func decodeNode(v []byte) (NodeRecord, error) {
var out NodeRecord
dec := gob.NewDecoder(bytes.NewReader(v))
if err := dec.Decode(&out); err != nil {
return out, err
}
return out, nil
}

func basename(p string) string {
if p == "/" {
return "/"
}
name := path.Base(path.Clean(p))
if name == "." || name == "" {
return "/"
}
return name
}

func main() {
if len(os.Args) != 2 {
fmt.Fprintln(os.Stderr, "usage: exporter <legacy-db-path>")
os.Exit(2)
}

db, err := bbolt.Open(os.Args[1], 0444, &bbolt.Options{ReadOnly: true})
if err != nil {
fmt.Fprintf(os.Stderr, "open legacy db: %v\n", err)
os.Exit(1)
}
defer db.Close()

export := Export{
Files: make([]LegacyFile, 0),
Nodes: make([]LegacyNode, 0),
}

err = db.View(func(tx *bbolt.Tx) error {
fsBucket := tx.Bucket([]byte("fs"))
if fsBucket != nil {
err := fsBucket.ForEach(func(k, v []byte) error {
record, err := decodeFile(v)
if err != nil {
return fmt.Errorf("decode fs[%s]: %w", string(k), err)
}
p := string(k)
export.Files = append(export.Files, LegacyFile{
Path:      p,
Name:      basename(record.Name),
Dir:       record.Dir,
Size:      record.Size,
MTimeUnix: record.MTime.Unix(),
})
return nil
})
if err != nil {
return err
}
}

nodesBucket := tx.Bucket([]byte("nodes"))
if nodesBucket != nil {
err := nodesBucket.ForEach(func(k, v []byte) error {
if v != nil {
return nil
}
filePath := string(k)
nb := nodesBucket.Bucket(k)
if nb == nil {
return nil
}
return nb.ForEach(func(_nk, nv []byte) error {
node, err := decodeNode(nv)
if err != nil {
return fmt.Errorf("decode node[%s]: %w", filePath, err)
}
export.Nodes = append(export.Nodes, LegacyNode{
Path:  filePath,
NId:   node.NId,
URL:   node.URL,
Size:  node.Size,
Start: node.Start,
End:   node.End,
MId:   node.MId,
Ex:    node.Ex,
Is:    node.Is,
Hm:    node.Hm,
})
return nil
})
})
if err != nil {
return err
}
}

return nil
})
if err != nil {
fmt.Fprintf(os.Stderr, "export legacy db: %v\n", err)
os.Exit(1)
}

enc := json.NewEncoder(os.Stdout)
if err := enc.Encode(export); err != nil {
fmt.Fprintf(os.Stderr, "encode export: %v\n", err)
os.Exit(1)
}
}
"#;

    fs::write(workdir.join("go.mod"), go_mod).context("write exporter go.mod")?;
    fs::write(workdir.join("main.go"), go_src).context("write exporter main.go")?;

    let mod_tidy = Command::new("go")
        .arg("mod")
        .arg("tidy")
        .current_dir(&workdir)
        .output()
        .context("run go mod tidy for temporary exporter")?;
    if !mod_tidy.status.success() {
        let _ = fs::remove_dir_all(&workdir);
        bail!(
            "temporary exporter dependency resolution failed: {}",
            String::from_utf8_lossy(&mod_tidy.stderr)
        );
    }

    let output = Command::new("go")
        .arg("run")
        .arg(".")
        .arg(input)
        .current_dir(&workdir)
        .output()
        .context("run temporary Go exporter")?;

    let cleanup_result = fs::remove_dir_all(&workdir);
    if !output.status.success() {
        bail!(
            "legacy exporter failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    if let Err(e) = cleanup_result {
        eprintln!("warning: failed to clean temporary exporter dir: {e}");
    }

    let export: LegacyExport =
        serde_json::from_slice(&output.stdout).context("parse exported JSON")?;
    Ok(export)
}

pub fn migrate_legacy_boltdb(input: &Path, output: &Path, force: bool) -> Result<()> {
    if !input.exists() {
        bail!("input file does not exist: {}", input.display());
    }
    if output.exists() {
        if force {
            fs::remove_file(output)
                .with_context(|| format!("remove existing output file {}", output.display()))?;
        } else {
            bail!(
                "output file already exists: {} (use --force to overwrite)",
                output.display()
            );
        }
    }

    let input_meta =
        fs::metadata(input).with_context(|| format!("read input file metadata {}", input.display()))?;
    if input_meta.len() == 0 {
        return write_redb_from_export(
            output,
            LegacyExport {
                files: Vec::new(),
                nodes: Vec::new(),
            },
        );
    }

    let export = run_go_exporter(input)?;
    write_redb_from_export(output, export)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{}-{}.db", name, uuid::Uuid::new_v4()))
    }

    #[test]
    fn writes_redb_records_in_expected_shape() {
        let out = temp_file("ddrv-migrate-test");

        let export = LegacyExport {
            files: vec![
                LegacyFile {
                    path: "/".to_owned(),
                    name: "/".to_owned(),
                    dir: true,
                    size: 0,
                    mtime_unix: 0,
                },
                LegacyFile {
                    path: "/a.txt".to_owned(),
                    name: "a.txt".to_owned(),
                    dir: false,
                    size: 10,
                    mtime_unix: 1,
                },
            ],
            nodes: vec![LegacyNode {
                path: "/a.txt".to_owned(),
                nid: 42,
                url: "https://cdn.example/a".to_owned(),
                size: 10,
                start: 0,
                end: 9,
                mid: 100,
                ex: 200,
                is: 150,
                hm: "hm".to_owned(),
            }],
        };

        write_redb_from_export(&out, export).expect("migration should write redb");

        let db = Database::open(&out).expect("open output redb");
        let tx = db.begin_read().expect("begin read");
        let fs_table = tx.open_table(FS_TABLE).expect("open fs table");
        let nodes_table = tx.open_table(NODES_TABLE).expect("open nodes table");

        let root = fs_table
            .get("/")
            .expect("read root")
            .expect("root should exist");
        let root_file: StoredFile = bincode::deserialize(root.value()).expect("decode root");
        assert!(root_file.dir);

        let file = fs_table
            .get("/a.txt")
            .expect("read file")
            .expect("file should exist");
        let file_data: StoredFile = bincode::deserialize(file.value()).expect("decode file");
        assert_eq!(file_data.name, "a.txt");
        assert_eq!(file_data.size, 10);

        let nkey = node_key("/a.txt", 42);
        let node = nodes_table
            .get(nkey.as_str())
            .expect("read node")
            .expect("node should exist");
        let node_data: StoredNode = bincode::deserialize(node.value()).expect("decode node");
        assert_eq!(node_data.mid, 100);
        assert_eq!(node_data.size, 10);

        fs::remove_file(out).ok();
    }

    #[test]
    fn migrates_zero_length_legacy_db_without_exporter() {
        let input = temp_file("ddrv-migrate-empty-in");
        let output = temp_file("ddrv-migrate-empty-out");
        fs::write(&input, []).expect("create zero-length legacy db file");

        migrate_legacy_boltdb(&input, &output, true).expect("zero-length migration should succeed");

        let db = Database::open(&output).expect("open output redb");
        let tx = db.begin_read().expect("begin read");
        let fs_table = tx.open_table(FS_TABLE).expect("open fs table");

        let root = fs_table
            .get("/")
            .expect("read root")
            .expect("root should exist");
        let root_file: StoredFile = bincode::deserialize(root.value()).expect("decode root");
        assert!(root_file.dir);

        fs::remove_file(input).ok();
        fs::remove_file(output).ok();
    }
}
