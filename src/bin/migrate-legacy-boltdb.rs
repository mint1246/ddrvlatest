#[path = "../migration.rs"]
mod migration;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "migrate-legacy-boltdb",
    version,
    about = "Migrate legacy (Go/bbolt+gob) ddrv DB into rewrite redb format"
)]
struct Args {
    /// Legacy ddrv DB path from old master branch
    #[arg(long)]
    input: PathBuf,

    /// Output path for rewrite redb DB
    #[arg(long)]
    output: PathBuf,

    /// Overwrite output DB if it already exists
    #[arg(long, default_value_t = false)]
    force: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    migration::migrate_legacy_boltdb(&args.input, &args.output, args.force)?;
    println!("migration completed: {}", args.output.display());
    Ok(())
}
