pub mod limiter;
pub mod nwriter;
pub mod reader;
pub mod rest;
pub mod types;
pub mod utils;
pub mod writer;

pub use types::*;

use std::sync::Arc;

use rest::Rest;

/// Configuration for the DDRV driver.
pub struct Config {
    pub tokens: Vec<String>,
    pub token_type: i32,
    pub channels: Vec<String>,
    pub chunk_size: usize,
    pub nitro: bool,
}

/// High-level driver that owns the REST client and exposes read/write helpers.
#[derive(Clone)]
pub struct Driver {
    pub rest: Arc<Rest>,
    pub chunk_size: usize,
}

impl Driver {
    pub fn new(mut cfg: Config) -> Result<Self> {
        if cfg.tokens.is_empty() || cfg.channels.is_empty() {
            return Err(DdrvError::Other(format!(
                "not enough tokens or channels: tokens {} channels {}",
                cfg.tokens.len(),
                cfg.channels.len()
            )));
        }

        let chunk_size = parse_chunk_size(cfg.chunk_size, cfg.token_type)?;

        // Cloudflare rejects payloads > 100 MB; use the nitro upload path instead.
        if chunk_size > 100 * 1024 * 1024 {
            cfg.nitro = true;
        }

        for t in &mut cfg.tokens {
            if cfg.token_type == TOKEN_BOT {
                *t = format!("Bot {}", t);
            }
        }

        Ok(Driver {
            rest: Arc::new(Rest::new(cfg.tokens, cfg.channels, chunk_size, cfg.nitro)),
            chunk_size,
        })
    }

    /// Create a sequential chunk writer.
    pub fn new_writer(&self, on_chunk: impl FnMut(Node) + Send + 'static) -> writer::Writer {
        writer::Writer::new(Arc::clone(&self.rest), self.chunk_size, on_chunk)
    }

    /// Create a parallel chunk writer (one uploader task per Discord channel).
    pub fn new_nwriter(&self, on_chunk: impl FnMut(Node) + Send + 'static) -> nwriter::NWriter {
        let num_channels = self.rest.num_channels();
        nwriter::NWriter::new(
            Arc::clone(&self.rest),
            self.chunk_size,
            num_channels,
            on_chunk,
        )
    }

    /// Create an async reader that reassembles `chunks` starting at byte `pos`.
    pub fn new_reader(&self, chunks: Vec<Node>, pos: i64) -> Result<reader::Reader> {
        reader::Reader::new(chunks, pos, Arc::clone(&self.rest))
    }

    /// Refresh any chunks whose CDN URL has expired.
    /// Uses all available bot tokens in rotation to distribute rate limit load.
    pub async fn update_nodes(&self, chunks: &mut [Node]) -> Result<()> {
        use crate::ddrv::utils::extract_channel_id;
        use futures::stream::{self, StreamExt};
        use std::collections::HashMap;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // Collect expired message IDs (deduplicated) and their channel IDs.
        let mut expired: HashMap<i64, String> = HashMap::new();
        for chunk in chunks.iter() {
            if now > chunk.ex {
                expired
                    .entry(chunk.mid)
                    .or_insert_with(|| extract_channel_id(&chunk.url));
            }
        }

        if expired.is_empty() {
            return Ok(());
        }

        let mut chunks_by_mid: HashMap<i64, Vec<usize>> = HashMap::new();
        for (idx, chunk) in chunks.iter().enumerate() {
            chunks_by_mid.entry(chunk.mid).or_default().push(idx);
        }

        let rest = Arc::clone(&self.rest);
        // Increase concurrency to leverage multiple tokens - this will automatically
        // rotate through all available tokens, spreading the rate limit load.
        let concurrency = (expired.len().min(16)).max(8);
        let mut fetches = stream::iter(expired.into_iter().map(move |(mid, channel_id)| {
            let rest = Arc::clone(&rest);
            async move {
                let mut messages = Vec::new();
                rest.get_messages(&channel_id, mid - 1, "after", &mut messages)
                    .await?;
                Ok::<Vec<Message>, DdrvError>(messages)
            }
        }))
        .buffer_unordered(concurrency);

        let mut messages_by_id: HashMap<i64, Message> = HashMap::new();
        while let Some(messages) = fetches.next().await {
            for msg in messages? {
                if let Ok(mid) = msg.id.parse::<i64>() {
                    messages_by_id.insert(mid, msg);
                }
            }
        }

        for (mid, indexes) in chunks_by_mid {
            if let Some(msg) = messages_by_id.get(&mid) {
                if let Some(att) = msg.attachments.first() {
                    let (url, ex, is, hm) = utils::decode_attachment_url(&att.url);
                    for idx in indexes {
                        let chunk = &mut chunks[idx];
                        chunk.url = url.clone();
                        chunk.ex = ex;
                        chunk.is = is;
                        chunk.hm = hm.clone();
                    }
                }
            }
        }

        Ok(())
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn parse_chunk_size(chunk_size: usize, token_type: i32) -> Result<usize> {
    if token_type > TOKEN_USER_NITRO_BASIC || token_type < 0 {
        return Err(DdrvError::Other(format!(
            "invalid token type {}",
            token_type
        )));
    }

    let size = if (token_type == TOKEN_BOT || token_type == TOKEN_USER)
        && (chunk_size > MAX_CHUNK_SIZE || chunk_size == 0)
    {
        MAX_CHUNK_SIZE
    } else if token_type == TOKEN_USER_NITRO_BASIC
        && (chunk_size > MAX_CHUNK_SIZE_NITRO_BASIC || chunk_size == 0)
    {
        MAX_CHUNK_SIZE_NITRO_BASIC
    } else if token_type == TOKEN_USER_NITRO
        && (chunk_size > MAX_CHUNK_SIZE_NITRO || chunk_size == 0)
    {
        MAX_CHUNK_SIZE_NITRO
    } else {
        chunk_size
    };

    Ok(size)
}
