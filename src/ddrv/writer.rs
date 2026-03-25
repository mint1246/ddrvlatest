use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Future;
use tokio::io::AsyncWrite;
use tokio::task::JoinHandle;

use super::rest::Rest;
use super::types::{DdrvError, Node, Result};

/// Sequential chunk writer.
///
/// Buffers incoming bytes up to `chunk_size`, then uploads each full chunk to
/// Discord before accepting more data.  When closed the remaining partial chunk
/// (if any) is uploaded.  `on_chunk` is called once per uploaded chunk, in
/// order.
pub struct Writer {
    rest: Arc<Rest>,
    chunk_size: usize,
    on_chunk: Box<dyn FnMut(Node) + Send + 'static>,
    buf: Vec<u8>,
    /// In-flight upload task for the current chunk.
    pending: Option<JoinHandle<Result<Node>>>,
    closed: bool,
}

impl Writer {
    pub fn new(
        rest: Arc<Rest>,
        chunk_size: usize,
        on_chunk: impl FnMut(Node) + Send + 'static,
    ) -> Self {
        Writer {
            rest,
            chunk_size,
            on_chunk: Box::new(on_chunk),
            buf: Vec::new(),
            pending: None,
            closed: false,
        }
    }

    /// Poll any in-flight upload to completion, calling `on_chunk` on success.
    /// Returns `Poll::Pending` if still uploading, `Poll::Ready(Err)` on failure,
    /// or `Poll::Ready(Ok(()))` when done.
    fn poll_pending(this: &mut Self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(handle) = this.pending.as_mut() {
            match Pin::new(handle).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(res) => {
                    this.pending = None;
                    let node = flatten_join(res)?;
                    (this.on_chunk)(node);
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Spawn an upload task for the current buffer contents, clearing the buffer.
    fn start_upload(&mut self) {
        let data = Bytes::from(std::mem::take(&mut self.buf));
        let rest = Arc::clone(&self.rest);
        self.pending = Some(tokio::spawn(
            async move { rest.create_attachment(data).await },
        ));
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();

        if this.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                DdrvError::Closed.to_string(),
            )));
        }

        // Drain any in-flight upload before accepting new bytes.
        if this.pending.is_some() {
            match Self::poll_pending(this, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
        }

        // Write bytes, flushing to Discord whenever a full chunk accumulates.
        let mut written = 0;
        while written < buf.len() {
            let space = this.chunk_size - this.buf.len();
            let take = (buf.len() - written).min(space);
            this.buf.extend_from_slice(&buf[written..written + take]);
            written += take;

            if this.buf.len() == this.chunk_size {
                this.start_upload();
                // Wait for the upload before we can accept more bytes.
                match Self::poll_pending(this, cx) {
                    Poll::Pending => {
                        // We already consumed `written` bytes; report that.
                        return Poll::Ready(Ok(written));
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {}
                }
            }
        }

        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();

        if this.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                DdrvError::AlreadyClosed.to_string(),
            )));
        }

        loop {
            // Wait for any in-flight upload.
            if this.pending.is_some() {
                match Self::poll_pending(this, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {}
                }
            }

            // Upload remaining buffered bytes.
            if !this.buf.is_empty() {
                this.start_upload();
                continue; // loop back to poll the new upload
            }

            this.closed = true;
            return Poll::Ready(Ok(()));
        }
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn flatten_join(
    res: std::result::Result<Result<Node>, tokio::task::JoinError>,
) -> io::Result<Node> {
    match res {
        Ok(Ok(node)) => Ok(node),
        Ok(Err(e)) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
    }
}
