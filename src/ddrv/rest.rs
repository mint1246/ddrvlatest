use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::limiter::Limiter;
use super::types::{DdrvError, Message, Node, NodeAttachment, Result};
use super::utils::{decode_attachment_url, encode_attachment_url};

const BASE_URL: &str = "https://discord.com/api/v10";
const USER_AGENT: &str = "PostmanRuntime/7.35.0";
const REQ_TIMEOUT: Duration = Duration::from_secs(60);
const MESSAGE_FILE_FORM_FIELD: &str = "files[0]";

struct RestState {
    last_token_idx: usize,
    last_ch_idx: usize,
}

pub struct Rest {
    channels: Vec<String>,
    nitro: bool,
    limiter: Limiter,
    client: reqwest::Client,
    cdn_client: reqwest::Client,
    tokens: Vec<String>,
    state: Arc<Mutex<RestState>>,
    pub chunk_size: usize,
}

impl Rest {
    pub fn new(
        tokens: Vec<String>,
        channels: Vec<String>,
        chunk_size: usize,
        nitro: bool,
    ) -> Self {
        Rest {
            client: reqwest::Client::builder()
                .timeout(REQ_TIMEOUT)
                .build()
                .expect("failed to build reqwest client"),
            cdn_client: reqwest::Client::builder()
                .timeout(REQ_TIMEOUT)
                .build()
                .expect("failed to build cdn reqwest client"),
            channels,
            nitro,
            limiter: Limiter::new(),
            tokens,
            state: Arc::new(Mutex::new(RestState {
                last_token_idx: 0,
                last_ch_idx: 0,
            })),
            chunk_size,
        }
    }

    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }

    async fn token(&self) -> String {
        let mut s = self.state.lock().await;
        let t = self.tokens[s.last_token_idx].clone();
        s.last_token_idx = (s.last_token_idx + 1) % self.tokens.len();
        t
    }

    async fn channel(&self) -> String {
        let mut s = self.state.lock().await;
        let c = self.channels[s.last_ch_idx].clone();
        s.last_ch_idx = (s.last_ch_idx + 1) % self.channels.len();
        c
    }

    /// Execute an API request with rate-limiting and optional retry on 429 / 5xx.
    /// `build` is called freshly on every attempt so the request body can be resent.
    async fn do_req<F>(&self, path_suffix: &str, build: F, retry: bool) -> Result<reqwest::Response>
    where
        F: Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder,
    {
        let mut attempt: u32 = 1;
        loop {
            let token = self.token().await;
            let bucket_id = format!("{}{}", token, path_suffix);
            debug!(
                path_suffix,
                retry,
                attempt,
                "Discord API request attempt"
            );

            self.limiter.acquire(&bucket_id).await;

            let req = build(&self.client, &token)
                .header("User-Agent", USER_AGENT)
                .header("Authorization", &token);

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    self.limiter.release(&bucket_id, Some(resp.headers())).await;
                    debug!(
                        path_suffix,
                        retry,
                        attempt,
                        status,
                        "Discord API response received"
                    );
                    if retry && (status == 429 || status > 500) {
                        warn!(
                            path_suffix,
                            attempt,
                            status,
                            "Discord API request will be retried"
                        );
                        attempt += 1;
                        continue;
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    self.limiter.release(&bucket_id, None).await;
                    error!(
                        path_suffix,
                        retry,
                        attempt,
                        error = %e,
                        "Discord API request failed"
                    );
                    return Err(DdrvError::Http(e));
                }
            }
        }
    }

    /// Fetch up to 100 messages from a channel, optionally anchored at `message_id`.
    pub async fn get_messages(
        &self,
        channel_id: &str,
        message_id: i64,
        query: &str,
        messages: &mut Vec<Message>,
    ) -> Result<()> {
        let path = if message_id != 0 && !query.is_empty() {
            format!(
                "/channels/{}/messages?limit=100&{}={}",
                channel_id, query, message_id
            )
        } else {
            format!("/channels/{}/messages?limit=100", channel_id)
        };

        let path_suffix = format!("/{}/messages", channel_id);
        let url = format!("{}{}", BASE_URL, path);

        let resp = self
            .do_req(&path_suffix, |c, _t| c.get(&url), true)
            .await?;

        let status = resp.status().as_u16();
        if status != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(DdrvError::DiscordApi {
                expected: 200,
                got: status,
                body,
            });
        }

        let result: Vec<Message> = resp.json().await?;
        *messages = result;
        Ok(())
    }

    /// Upload `data` as a Discord message attachment, returning the resulting Node.
    pub async fn create_attachment(&self, data: Bytes) -> Result<Node> {
        if self.nitro {
            self.create_attachment_nitro(data).await
        } else {
            self.create_attachment_regular(data).await
        }
    }

    async fn create_attachment_regular(&self, data: Bytes) -> Result<Node> {
        let channel_id = self.channel().await;
        let path_suffix = format!("/{}/messages", channel_id);
        let url = format!("{}/channels/{}/messages", BASE_URL, channel_id);
        let upload_size = data.len();
        debug!(
            channel_id,
            upload_size,
            "Starting regular Discord attachment upload"
        );

        let fname = Uuid::new_v4().to_string();
        let resp = self
            .do_req(&path_suffix, move |c, _t| {
                let part = reqwest::multipart::Part::bytes(data.to_vec())
                    .file_name(fname.clone())
                    .mime_str("application/octet-stream")
                    .expect("invalid mime type");
                let form = reqwest::multipart::Form::new()
                    .part(MESSAGE_FILE_FORM_FIELD.to_string(), part);
                c.post(&url).multipart(form)
            }, false)
            .await?;

        let status = resp.status().as_u16();
        if !is_message_create_success(status) {
            let body = resp.text().await.unwrap_or_default();
            error!(
                channel_id,
                upload_size,
                status,
                body,
                "Regular Discord attachment upload failed"
            );
            return Err(DdrvError::Other(format!(
                "Discord API error: expected 200 or 201, got {}: {}",
                status, body
            )));
        }

        let msg: Message = resp.json().await?;
        debug!(
            channel_id,
            upload_size,
            message_id = %msg.id,
            "Regular Discord attachment upload succeeded"
        );
        node_from_message(msg)
    }

    async fn create_attachment_nitro(&self, data: Bytes) -> Result<Node> {
        let fname = Uuid::new_v4().to_string();
        let channel_id = self.channel().await;
        let path_suffix = format!("/{}/messages", channel_id);
        let upload_size = data.len();
        debug!(
            channel_id,
            upload_size,
            "Starting nitro Discord attachment upload"
        );

        // Step 1 – request a pre-signed upload URL.
        let req_url = format!("{}/channels/{}/attachments", BASE_URL, channel_id);
        let body1 = format!(
            r#"{{"files":[{{"filename":"{}","file_size":{}}}]}}"#,
            fname, upload_size
        );

        let resp = self
            .do_req(&path_suffix, move |c, _t| {
                c.post(&req_url)
                    .header("Content-Type", "application/json")
                    .body(body1.clone())
            }, true)
            .await?;

        let status = resp.status().as_u16();
        if !is_message_create_success(status) {
            let body = resp.text().await.unwrap_or_default();
            error!(
                channel_id,
                upload_size,
                status,
                body,
                "Nitro upload pre-sign request failed"
            );
            return Err(DdrvError::Other(format!(
                "Discord API error: expected 200 or 201, got {}: {}",
                status, body
            )));
        }

        #[derive(Deserialize)]
        struct AttachmentEntry {
            upload_url: String,
            upload_filename: String,
        }
        #[derive(Deserialize)]
        struct AttachmentResp {
            attachments: Vec<AttachmentEntry>,
        }

        let ar: AttachmentResp = resp.json().await?;
        let entry = ar.attachments.into_iter().next().ok_or_else(|| {
            DdrvError::Other("nitro: no attachment entry in response".into())
        })?;

        // Step 2 – PUT the raw bytes to the pre-signed URL (no auth header).
        let put_resp = self
            .cdn_client
            .put(&entry.upload_url)
            .body(data)
            .send()
            .await
            .map_err(DdrvError::Http)?;

        if put_resp.status().as_u16() != 200 {
            let body = put_resp.text().await.unwrap_or_default();
            error!(
                channel_id,
                upload_size,
                body,
                "Nitro upload PUT failed"
            );
            return Err(DdrvError::Other(format!(
                "nitro upload PUT failed: {}",
                body
            )));
        }

        // Step 3 – confirm the upload by creating a message.
        let msg_url = format!("{}/channels/{}/messages", BASE_URL, channel_id);
        let body3 = format!(
            r#"{{"attachments":[{{"id":"0","filename":"{}","uploaded_filename":"{}"}}]}}"#,
            fname, entry.upload_filename
        );

        let resp = self
            .do_req(&path_suffix, move |c, _t| {
                c.post(&msg_url)
                    .header("Content-Type", "application/json")
                    .body(body3.clone())
            }, true)
            .await?;

        let status = resp.status().as_u16();
        if !is_message_create_success(status) {
            let body = resp.text().await.unwrap_or_default();
            error!(
                channel_id,
                upload_size,
                status,
                body,
                "Nitro upload final message create failed"
            );
            return Err(DdrvError::Other(format!(
                "Discord API error: expected 200 or 201, got {}: {}",
                status, body
            )));
        }

        let msg: Message = resp.json().await?;
        debug!(
            channel_id,
            upload_size,
            message_id = %msg.id,
            "Nitro Discord attachment upload succeeded"
        );
        node_from_message(msg)
    }

    /// Fetch a byte range from a Discord CDN attachment URL, returning the body as Bytes.
    pub async fn read_attachment(&self, node: &Node, start: usize, end: usize) -> Result<Bytes> {
        let url = encode_attachment_url(&node.url, node.ex, node.is, &node.hm);
        let mut attempt: u32 = 1;
        loop {
            debug!(
                url = %node.url,
                start,
                end,
                attempt,
                "Reading Discord attachment range"
            );
            let resp = self
                .cdn_client
                .get(&url)
                .header("Range", format!("bytes={}-{}", start, end))
                .send()
                .await
                .map_err(DdrvError::Http)?;

            let status = resp.status().as_u16();
            if status > 500 {
                // Retry on Discord / Cloudflare 5xx errors.
                warn!(
                    url = %node.url,
                    start,
                    end,
                    attempt,
                    status,
                    "Discord CDN request returned 5xx, retrying"
                );
                attempt += 1;
                continue;
            }
            if status != 206 {
                let body = resp.text().await.unwrap_or_default();
                error!(
                    url = %node.url,
                    start,
                    end,
                    attempt,
                    status,
                    body,
                    "Discord CDN request failed"
                );
                return Err(DdrvError::DiscordApi {
                    expected: 206,
                    got: status,
                    body,
                });
            }

            return Ok(resp.bytes().await?);
        }
    }
}

/// Convert a raw Discord Message (as returned by the API) into a Node.
fn node_from_message(msg: Message) -> Result<Node> {
    let att: &NodeAttachment = msg.attachments.first().ok_or_else(|| {
        DdrvError::Other("create_attachment: message has no attachments".into())
    })?;

    let (clean_url, ex, is, hm) = decode_attachment_url(&att.url);
    let mid: i64 = msg.id.parse().unwrap_or(0);

    Ok(Node {
        url: clean_url,
        size: att.size,
        mid,
        ex,
        is,
        hm,
        ..Default::default()
    })
}

fn is_message_create_success(status: u16) -> bool {
    matches!(status, 200 | 201)
}

#[cfg(test)]
mod tests {
    use super::is_message_create_success;

    #[test]
    fn message_create_success_statuses_are_accepted() {
        assert!(is_message_create_success(200));
        assert!(is_message_create_success(201));
        assert!(!is_message_create_success(204));
        assert!(!is_message_create_success(400));
    }
}
