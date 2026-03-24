mod config;
mod dataprovider;
mod ddrv;
mod ftp;
mod http;

use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "ddrv", version = "2.3.0", about = "Discord-backed cloud storage")]
struct Args {
    /// Path to config file
    #[arg(long, default_value = "")]
    config: String,
    /// Enable debug logging
    #[arg(long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Setup logging
    let filter = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .init();

    // Load config
    let cfg = config::load(if args.config.is_empty() { None } else { Some(&args.config) })?;

    // Build driver
    let ddrv_cfg = ddrv::Config {
        tokens: cfg.ddrv.token.clone(),
        token_type: cfg.ddrv.token_type,
        channels: cfg.ddrv.channels.clone(),
        chunk_size: cfg.ddrv.chunk_size,
        nitro: cfg.ddrv.nitro,
    };
    let driver = Arc::new(ddrv::Driver::new(ddrv_cfg)?);

    // Build and load data provider
    let bolt_path = &cfg.dataprovider.boltdb.db_path;
    let pg_url = &cfg.dataprovider.postgres.db_url;

    if !bolt_path.is_empty() {
        info!("Using BoltDB provider at {}", bolt_path);
        let provider = dataprovider::boltdb::BoltDbProvider::new(bolt_path, Arc::clone(&driver))?;
        dataprovider::load(Arc::new(provider));
    } else if !pg_url.is_empty() {
        info!("Using PostgreSQL provider");
        let pg_cfg = dataprovider::postgres::PostgresConfig { db_url: pg_url.clone() };
        let provider = dataprovider::postgres::PgProvider::new(&pg_cfg, Arc::clone(&driver)).await;
        dataprovider::load(Arc::new(provider));
    } else {
        anyhow::bail!("No data provider configured. Set boltdb.db_path or postgres.db_url.");
    }

    // Spawn FTP + HTTP servers
    let ftp_driver = Arc::clone(&driver);
    let ftp_cfg = cfg.frontend.ftp.clone();
    let http_driver = Arc::clone(&driver);
    let http_cfg = cfg.frontend.http.clone();

    let ftp_task = tokio::spawn(async move {
        if let Err(e) = ftp::serve(ftp_driver, &ftp_cfg).await {
            error!("FTP server error: {}", e);
        }
    });

    let http_task = tokio::spawn(async move {
        if let Err(e) = http::serve(http_driver, http_cfg).await {
            error!("HTTP server error: {}", e);
        }
    });

    tokio::select! {
        _ = ftp_task => {},
        _ = http_task => {},
    }

    Ok(())
}
