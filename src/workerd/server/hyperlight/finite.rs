// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

/// One finite HTTP exchange, not a device or a general duplex transport.
pub struct Exchange {
    input: io::Cursor<Vec<u8>>,
    output: Arc<Mutex<Vec<u8>>>,
    limit: usize,
}

impl Exchange {
    pub fn new(input: Vec<u8>, limit: usize) -> (Self, Arc<Mutex<Vec<u8>>>) {
        let output = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                input: io::Cursor::new(input),
                output: Arc::clone(&output),
                limit,
            },
            output,
        )
    }
}

impl AsyncRead for Exchange {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let position = usize::try_from(self.input.position()).map_err(io::Error::other)?;
        let remaining = &self.input.get_ref()[position..];
        let length = remaining.len().min(output.remaining());
        output.put_slice(&remaining[..length]);
        self.input.set_position((position + length) as u64);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Exchange {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        input: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut output = self
            .output
            .lock()
            .map_err(|_| io::Error::other("HTTP reply buffer lock poisoned"))?;
        if output.len() > self.limit || input.len() > self.limit - output.len() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP response exceeds host-supplied reply limit",
            )));
        }
        output
            .try_reserve_exact(input.len())
            .map_err(io::Error::other)?;
        output.extend_from_slice(input);
        drop(output);
        Poll::Ready(Ok(input.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
#[path = "finite-test.rs"]
mod tests;
