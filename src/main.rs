mod config;
mod dataprovider;
mod ddrv;
mod ftp;
mod http;
mod migration;
mod tracker;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "ddrv",
    version = "2.3.0",
    about = "Discord-backed cloud storage"
)]
struct Args {
    /// Path to config file
    #[arg(long, default_value = "")]
    config: String,
    /// Enable debug logging
    #[arg(long)]
    debug: bool,
    /// Migrate a legacy master-branch BoltDB file in place and exit
    #[arg(long)]
    migrate: Option<PathBuf>,
    /// Destination for migrated DB (defaults to <input>.migrated.redb)
    #[arg(long)]
    migrate_output: Option<PathBuf>,
    /// Overwrite migration output if it already exists
    #[arg(long, default_value_t = false)]
    migrate_force: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Configuration utilities
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCommand {
    /// Validate and print the redacted effective configuration
    Check {
        /// Path to config file
        #[arg(long)]
        config: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if let Some(input) = args.migrate.as_deref() {
        let output = args
            .migrate_output
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("{}.migrated.redb", input.to_string_lossy())));
        migration::migrate_legacy_boltdb(input, &output, args.migrate_force)?;
        println!("migration completed: {}", output.display());
        return Ok(());
    }

    let check_path = match &args.command {
        Some(Command::Config {
            command: ConfigCommand::Check { config },
        }) => Some(config.as_deref()),
        None => None,
    };

    // Setup logging
    let filter = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .init();

    // Load config
    let requested_path = check_path
        .flatten()
        .or_else(|| (!args.config.is_empty()).then_some(args.config.as_str()));
    let cfg = config::load(if requested_path.is_none() {
        None
    } else {
        requested_path
    })?;
    cfg.validate()?;

    if check_path.is_some() {
        println!("{}", serde_yaml::to_string(&cfg.redacted())?);
        return Ok(());
    }

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
        let pg_cfg = dataprovider::postgres::PostgresConfig {
            db_url: pg_url.clone(),
            max_connections: cfg.dataprovider.postgres.max_connections,
            connect_timeout_seconds: cfg.dataprovider.postgres.connect_timeout_seconds,
            idle_timeout_seconds: cfg.dataprovider.postgres.idle_timeout_seconds,
        };
        let provider =
            dataprovider::postgres::PgProvider::new(&pg_cfg, Arc::clone(&driver)).await?;
        dataprovider::load(Arc::new(provider));
    } else {
        anyhow::bail!("No data provider configured. Set boltdb.db_path or postgres.db_url.");
    }

    // Keep Discord CDN URLs fresh in the background so download requests stay fast.
    tracker::spawn_auto_renewal_task();

    // Spawn FTP + HTTP servers
    let ftp_driver = Arc::clone(&driver);
    let ftp_cfg = cfg.frontend.ftp.clone();
    let http_driver = Arc::clone(&driver);
    let http_cfg = cfg.frontend.http.clone();

    let http_task = tokio::spawn(async move {
        if let Err(e) = http::serve(http_driver, http_cfg).await {
            error!("HTTP server error: {}", e);
        }
    });

    if ftp_cfg.addr.is_empty() {
        http_task.await?;
    } else {
        let ftp_task = tokio::spawn(async move {
            if let Err(e) = ftp::serve(ftp_driver, &ftp_cfg).await {
                error!("FTP server error: {}", e);
            }
        });

        tokio::select! {
            _ = ftp_task => {},
            _ = http_task => {},
        }
    }

    Ok(())
}
