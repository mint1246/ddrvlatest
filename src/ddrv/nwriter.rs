use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Future;
use tokio::io::AsyncWrite;
use tokio::task::JoinHandle;

use super::rest::Rest;
use super::types::{DdrvError, Node, Result};

/// Parallel chunk writer.
///
/// Writes are piped through a `tokio::io::duplex` channel to a pool of
/// `num_channels` uploader tasks that each consume `chunk_size` bytes and
/// upload them concurrently.  On shutdown the chunks are sorted by their
/// original write order and `on_chunk` is called once per chunk in order.
pub struct NWriter {
    /// Write end of the internal pipe; `None` after shutdown begins.
    pipe: Option<tokio::io::DuplexStream>,
    /// Background task that drives all uploaders and returns sorted nodes.
    handle: JoinHandle<Result<Vec<Node>>>,
    on_chunk: Box<dyn FnMut(Node) + Send + 'static>,
    closed: bool,
}

impl NWriter {
    pub fn new(
        rest: Arc<Rest>,
        chunk_size: usize,
        num_channels: usize,
        on_chunk: impl FnMut(Node) + Send + 'static,
    ) -> Self {
        // The duplex buffer is large enough to keep all workers busy.
        let buf_cap = chunk_size * num_channels.max(1) * 2;
        let (pipe_write, pipe_read) = tokio::io::duplex(buf_cap);

        let handle = tokio::spawn(run_workers(pipe_read, rest, chunk_size, num_channels));

        NWriter {
            pipe: Some(pipe_write),
            handle,
            on_chunk: Box::new(on_chunk),
            closed: false,
        }
    }
}

impl AsyncWrite for NWriter {
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

        if let Some(pipe) = this.pipe.as_mut() {
            Pin::new(pipe).poll_write(cx, buf)
        } else {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pipe already closed",
            )))
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if let Some(pipe) = this.pipe.as_mut() {
            Pin::new(pipe).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();

        if this.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                DdrvError::AlreadyClosed.to_string(),
            )));
        }

        // Drop the write end to signal EOF to the workers.
        this.pipe.take();

        // Wait for the worker pool to finish.
        match Pin::new(&mut this.handle).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                this.closed = true;
                let mut nodes: Vec<Node> = match res {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e.to_string())))
                    }
                    Err(e) => {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e.to_string())))
                    }
                };

                // Restore original write order (Start was set to the chunk sequence number).
                nodes.sort_by_key(|n| n.start);
                for node in nodes {
                    (this.on_chunk)(node);
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

// ── worker pool ──────────────────────────────────────────────────────────────

async fn run_workers(
    pipe_read: tokio::io::DuplexStream,
    rest: Arc<Rest>,
    chunk_size: usize,
    num_channels: usize,
) -> Result<Vec<Node>> {
    use tokio::sync::Mutex;

    let reader = Arc::new(Mutex::new(pipe_read));
    let counter = Arc::new(AtomicI64::new(0));
    let results: Arc<Mutex<Vec<Node>>> = Arc::new(Mutex::new(Vec::new()));
    let error: Arc<Mutex<Option<DdrvError>>> = Arc::new(Mutex::new(None));

    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_channels);

    for _ in 0..num_channels.max(1) {
        let reader = Arc::clone(&reader);
        let rest = Arc::clone(&rest);
        let counter = Arc::clone(&counter);
        let results = Arc::clone(&results);
        let error = Arc::clone(&error);

        handles.push(tokio::spawn(async move {
            loop {
                // Check if another worker already encountered an error.
                if error.lock().await.is_some() {
                    return;
                }

                // Read exactly `chunk_size` bytes, or fewer at EOF.
                // The sequence number is assigned inside the same critical section
                // so that read order == sequence order even under concurrent workers.
                let (data, seq) = {
                    let mut r = reader.lock().await;
                    let chunk = match read_chunk(&mut *r, chunk_size).await {
                        Ok(Some(d)) => d,
                        Ok(None) => return, // clean EOF
                        Err(e) => {
                            let mut err = error.lock().await;
                            if err.is_none() {
                                *err = Some(DdrvError::Io(e));
                            }
                            return;
                        }
                    };
                    let seq = counter.fetch_add(1, Ordering::SeqCst);
                    (chunk, seq)
                };

                match rest.create_attachment(data).await {
                    Ok(mut node) => {
                        node.start = seq;
                        results.lock().await.push(node);
                    }
                    Err(e) => {
                        let mut err = error.lock().await;
                        if err.is_none() {
                            *err = Some(e);
                        }
                        return;
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    // Surface the first error, if any.
    if let Some(e) = error.lock().await.take() {
        return Err(e);
    }

    let nodes = Arc::try_unwrap(results)
        .expect("results Arc still shared after all workers finished")
        .into_inner();

    Ok(nodes)
}

/// Read up to `max_bytes` from `reader`, blocking until `max_bytes` are
/// available or EOF is reached.  Returns `None` on a clean zero-byte EOF.
async fn read_chunk<R>(reader: &mut R, max_bytes: usize) -> io::Result<Option<Bytes>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut buf = vec![0u8; max_bytes];
    let mut total = 0usize;

    loop {
        match reader.read(&mut buf[total..]).await? {
            0 => break, // EOF
            n => {
                total += n;
                if total == max_bytes {
                    break; // full chunk
                }
            }
        }
    }

    if total == 0 {
        Ok(None)
    } else {
        buf.truncate(total);
        Ok(Some(Bytes::from(buf)))
    }
}
