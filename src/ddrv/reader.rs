use std::io;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::io::{AsyncRead, ReadBuf};

use super::rest::Rest;
use super::types::{DdrvError, Node, Result};

/// Async reader that reassembles a file from its ordered Discord attachment chunks.
///
/// Supports seeking via the `pos` parameter: chunks that end before `pos` are
/// skipped and the first matching chunk is fetched with a `Range` header that
/// starts at `pos - chunk.start`.
pub struct Reader {
    chunks: Vec<Node>,
    cur_idx: usize,
    closed: bool,
    rest: Arc<Rest>,
    /// Buffered bytes of the current chunk with a read cursor.
    current: Option<Cursor<Bytes>>,
    /// In-flight future fetching the next chunk body.
    fetch: Option<BoxFuture<'static, Result<Bytes>>>,
    /// Absolute byte position within the whole file where reading begins.
    pos: i64,
}

impl Reader {
    /// Create a new Reader starting at byte offset `pos`.
    ///
    /// Returns `Err(DdrvError::Other)` wrapping `io::Error` for `UnexpectedEof`
    /// when `pos` is beyond the end of the file.
    pub fn new(mut chunks: Vec<Node>, pos: i64, rest: Arc<Rest>) -> Result<Self> {
        // Compute absolute start/end offsets for every chunk from their sizes.
        let mut offset: i64 = 0;
        for c in &mut chunks {
            c.start = offset;
            c.end = offset + c.size as i64 - 1;
            offset = c.end + 1;
        }

        // `pos` beyond EOF.
        if pos > offset {
            return Err(DdrvError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "seek position beyond end of file",
            )));
        }

        // Drop chunks that are entirely before `pos`.
        let start_idx = if pos == 0 {
            0
        } else {
            let mut idx = 0;
            for (i, c) in chunks.iter().enumerate() {
                // c.end + 1 == first byte of the next chunk
                if c.end + 1 > pos {
                    idx = i;
                    break;
                }
                idx = i + 1;
            }
            idx
        };

        let chunks = chunks.into_iter().skip(start_idx).collect();

        Ok(Reader {
            chunks,
            cur_idx: 0,
            closed: false,
            rest,
            current: None,
            fetch: None,
            pos,
        })
    }

    /// Start fetching the chunk at `cur_idx` in the background.
    fn start_fetch(&mut self) {
        let chunk = self.chunks[self.cur_idx].clone();
        let rest = Arc::clone(&self.rest);
        let pos = self.pos;

        // For the very first chunk we might need to skip some bytes if pos falls
        // inside it; all subsequent chunks start from byte 0.
        let byte_start = if pos > chunk.start {
            (pos - chunk.start) as usize
        } else {
            0
        };
        let byte_end = chunk.size - 1;

        self.fetch = Some(Box::pin(async move {
            rest.read_attachment(&chunk, byte_start, byte_end).await
        }));
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();

        if this.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                DdrvError::Closed.to_string(),
            )));
        }

        loop {
            // ── no more chunks: EOF ──────────────────────────────────────────
            if this.cur_idx >= this.chunks.len() {
                return Poll::Ready(Ok(()));
            }

            // ── poll in-flight fetch ─────────────────────────────────────────
            if let Some(fut) = this.fetch.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(bytes)) => {
                        this.fetch = None;
                        this.current = Some(Cursor::new(bytes));
                    }
                    Poll::Ready(Err(e)) => {
                        this.fetch = None;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            e.to_string(),
                        )));
                    }
                }
            }

            // ── start a fetch if we have no buffered data ────────────────────
            if this.current.is_none() {
                this.start_fetch();
                continue; // immediately poll the new future
            }

            // ── read from the buffered chunk ─────────────────────────────────
            let cursor = this.current.as_mut().unwrap();
            let inner_pos = cursor.position() as usize;
            let data = cursor.get_ref();
            let remaining = data.len() - inner_pos;

            if remaining == 0 {
                // Exhausted this chunk; move to the next.
                this.current = None;
                this.cur_idx += 1;
                // After the first chunk pos-offset is no longer relevant.
                this.pos = 0;
                continue;
            }

            let to_copy = remaining.min(buf.remaining());
            buf.put_slice(&data[inner_pos..inner_pos + to_copy]);
            cursor.set_position((inner_pos + to_copy) as u64);

            return Poll::Ready(Ok(()));
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.closed = true;
    }
}
