use std::{sync::Arc, time::Duration};

use tracing::{debug, warn};

use crate::dataprovider::{self, nodes_need_refresh, DataProvider, DataProviderError, File};

const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(300);

/// Spawn a background task that walks all files and refreshes expiring Discord URLs.
pub fn spawn_auto_renewal_task() {
    // Clone the provider once; it lives for the lifetime of the process.
    let provider = dataprovider::get();

    tokio::spawn(async move {
        loop {
            if let Err(e) = refresh_all_files(&provider).await {
                warn!(error = %e, "file tracker refresh pass failed");
            }
            tokio::time::sleep(DEFAULT_SCAN_INTERVAL).await;
        }
    });
}

async fn refresh_all_files(provider: &Arc<dyn DataProvider>) -> Result<(), DataProviderError> {
    let mut stack: Vec<File> = Vec::new();

    // Start from root and walk depth-first.
    let root = provider.stat("/").await?;
    stack.push(root);

    while let Some(entry) = stack.pop() {
        if entry.dir {
            match provider.get_children(&entry.id).await {
                Ok(children) => stack.extend(children),
                Err(e) => warn!(dir_id = %entry.id, error = %e, "failed to list directory"),
            }
            continue;
        }

        match provider.get_nodes(&entry.id).await {
            Ok(nodes) => {
                if nodes_need_refresh(&nodes) {
                    debug!(
                        file_id = %entry.id,
                        "refreshed nodes; next expiry in ~{}s",
                        nodes
                            .iter()
                            .filter(|n| n.ex > 0)
                            .map(|n| n.ex - chrono::Utc::now().timestamp())
                            .min()
                            .unwrap_or(0)
                    );
                }
            }
            Err(e) => warn!(file_id = %entry.id, error = %e, "failed to refresh nodes"),
        }
    }

    Ok(())
}
