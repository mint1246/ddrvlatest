use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

const EXTRA_DELAY: Duration = Duration::from_millis(250);

struct BucketState {
    reset: Option<Instant>,
    remaining: u64,
}

struct LimiterState {
    global_reset: Option<Instant>,
    buckets: HashMap<String, BucketState>,
}

pub struct Limiter {
    state: Mutex<LimiterState>,
}

impl Limiter {
    pub fn new() -> Self {
        Limiter {
            state: Mutex::new(LimiterState {
                global_reset: None,
                buckets: HashMap::new(),
            }),
        }
    }

    /// Acquire a rate-limit slot for `path`, sleeping if necessary.
    pub async fn acquire(&self, path: &str) {
        loop {
            let wait_dur = {
                let state = self.state.lock().await;
                let now = Instant::now();

                // Global rate limit takes priority.
                if let Some(gr) = state.global_reset {
                    if gr > now {
                        Some(gr - now + EXTRA_DELAY)
                    } else {
                        Self::bucket_wait(&state, path, now)
                    }
                } else {
                    Self::bucket_wait(&state, path, now)
                }
            };

            match wait_dur {
                Some(dur) => tokio::time::sleep(dur).await,
                None => {
                    let mut state = self.state.lock().await;
                    let bucket = state
                        .buckets
                        .entry(path.to_string())
                        .or_insert(BucketState {
                            reset: None,
                            remaining: 1,
                        });
                    if bucket.remaining > 0 {
                        bucket.remaining -= 1;
                    }
                    break;
                }
            }
        }
    }

    fn bucket_wait(state: &LimiterState, path: &str, now: Instant) -> Option<Duration> {
        if let Some(b) = state.buckets.get(path) {
            if b.remaining == 0 {
                if let Some(reset) = b.reset {
                    if reset > now {
                        return Some(reset - now + EXTRA_DELAY);
                    }
                }
            }
        }
        None
    }

    /// Update the bucket state from Discord response headers.
    pub async fn release(&self, path: &str, headers: Option<&reqwest::header::HeaderMap>) {
        let mut state = self.state.lock().await;
        let now = Instant::now();

        let headers = match headers {
            Some(h) => h,
            None => return,
        };

        let global = headers
            .get("X-RateLimit-Global")
            .and_then(|v| v.to_str().ok());
        let remaining = headers
            .get("X-RateLimit-Remaining")
            .and_then(|v| v.to_str().ok());
        let reset = headers
            .get("X-RateLimit-Reset")
            .and_then(|v| v.to_str().ok());
        let retry_after = headers
            .get("Retry-After")
            .and_then(|v| v.to_str().ok());

        if let Some(ra) = retry_after {
            if let Ok(secs) = ra.parse::<u64>() {
                let at = now + Duration::from_secs(secs);
                if global.is_some() {
                    state.global_reset = Some(at);
                } else if let Some(b) = state.buckets.get_mut(path) {
                    b.reset = Some(at);
                }
            }
        } else if let Some(reset_str) = reset {
            if let Ok(unix_f) = reset_str.parse::<f64>() {
                // Convert absolute Unix timestamp to a relative Instant offset.
                let now_unix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                let delta_secs = (unix_f - now_unix).max(0.0);
                let delta = Duration::from_secs_f64(delta_secs);
                if let Some(b) = state.buckets.get_mut(path) {
                    b.reset = Some(now + delta + EXTRA_DELAY);
                }
            }
        }

        if let Some(rem_str) = remaining {
            if let Ok(rem) = rem_str.parse::<u64>() {
                if let Some(b) = state.buckets.get_mut(path) {
                    b.remaining = rem;
                }
            }
        }
    }
}

impl Default for Limiter {
    fn default() -> Self {
        Self::new()
    }
}
