use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use rand::Rng;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::dataprovider::{self, DataProvider, NODE_RENEWAL_HEADROOM_SECS};

const BATCH_SIZE: usize = 64;
const MAX_CONCURRENCY: usize = 8;
const POLL_INTERVAL: Duration = Duration::from_secs(30);
const MAX_JITTER: Duration = Duration::from_secs(10);
const MAX_RETRY: Duration = Duration::from_secs(15 * 60);
const RETRY_STATE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Copy)]
struct RetryState {
    attempts: u32,
    retry_at: tokio::time::Instant,
    last_seen: tokio::time::Instant,
}

/// Spawn the indexed renewal worker. Cancelling `shutdown` stops polling and
/// aborts outstanding renewals at their next cancellation point.
pub fn spawn_auto_renewal_task(shutdown: CancellationToken) -> JoinHandle<()> {
    let provider = dataprovider::get();
    tokio::spawn(run(provider, shutdown))
}

async fn run(provider: Arc<dyn DataProvider>, shutdown: CancellationToken) {
    let mut retries = HashMap::<String, RetryState>::new();
    loop {
        let jitter =
            Duration::from_millis(rand::thread_rng().gen_range(0..=MAX_JITTER.as_millis() as u64));
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(POLL_INTERVAL + jitter) => {}
        }

        let cutoff = chrono::Utc::now().timestamp() + NODE_RENEWAL_HEADROOM_SECS;
        let candidates = match provider.due_node_groups(cutoff, BATCH_SIZE * 4).await {
            Ok(candidates) => candidates,
            Err(error) => {
                metrics::counter!("ddrv_renewal_failures_total", "stage" => "query").increment(1);
                warn!(%error, "renewal queue query failed");
                continue;
            }
        };
        let sample_size = candidates.len();
        let oldest_due = candidates.first().map(|item| item.min_expiry).unwrap_or(0);
        metrics::gauge!("ddrv_renewal_sample_size").set(sample_size as f64);
        metrics::gauge!("ddrv_renewal_oldest_due_timestamp_seconds").set(oldest_due as f64);
        info!(
            sample_size,
            oldest_due,
            provider = provider.name(),
            "renewal queue sample collected"
        );

        let now = tokio::time::Instant::now();
        retries.retain(|_, state| now.duration_since(state.last_seen) <= RETRY_STATE_TTL);
        for item in &candidates {
            if let Some(state) = retries.get_mut(&item.file_id) {
                state.last_seen = now;
            }
        }
        let batch = candidates
            .into_iter()
            .filter(|item| {
                retries
                    .get(&item.file_id)
                    .is_none_or(|retry| retry.retry_at <= now)
            })
            .take(BATCH_SIZE)
            .collect::<Vec<_>>();
        let mut pending = batch.into_iter();
        let mut active = FuturesUnordered::new();

        loop {
            while active.len() < MAX_CONCURRENCY {
                let Some(item) = pending.next() else { break };
                let provider = Arc::clone(&provider);
                let shutdown = shutdown.clone();
                active.push(async move {
                    let started = std::time::Instant::now();
                    let result = tokio::select! {
                        _ = shutdown.cancelled() => None,
                        result = provider.renew_node_group(&item.file_id, cutoff) => Some(result),
                    };
                    (item, started.elapsed(), result)
                });
            }
            let Some((item, latency, result)) = active.next().await else {
                break;
            };
            metrics::histogram!("ddrv_renewal_latency_seconds").record(latency.as_secs_f64());
            match result {
                None => return,
                Some(Ok(renewed)) => {
                    retries.remove(&item.file_id);
                    debug!(file_id = %item.file_id, renewed, latency_ms = latency.as_millis(), "renewal finished");
                }
                Some(Err(error)) => {
                    metrics::counter!("ddrv_renewal_failures_total", "stage" => "renew")
                        .increment(1);
                    let attempts = retries
                        .get(&item.file_id)
                        .map_or(1, |state| state.attempts.saturating_add(1));
                    let delay = Duration::from_secs(
                        (5_u64.saturating_mul(1_u64 << attempts.min(8))).min(MAX_RETRY.as_secs()),
                    );
                    retries.insert(
                        item.file_id.clone(),
                        RetryState {
                            attempts,
                            retry_at: tokio::time::Instant::now() + delay,
                            last_seen: tokio::time::Instant::now(),
                        },
                    );
                    warn!(file_id = %item.file_id, %error, attempts, retry_in_secs = delay.as_secs(), latency_ms = latency.as_millis(), "node renewal failed; retry scheduled");
                }
            }
        }
    }
    info!("renewal tracker stopped");
}
