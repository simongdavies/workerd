// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Host-to-guest named calls, distinct from guest-to-host HOSTCALL ioctls.
#![allow(
    unsafe_code,
    reason = "the kernel MAXLEN ioctl writes the negotiated input capacity"
)]

use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::os::fd::AsRawFd;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;

pub struct Queue {
    file: File,
    buffer: Vec<u8>,
}

impl Queue {
    pub fn open() -> Result<Self> {
        let file = File::options().read(true).write(true).open("/dev/hlcall")?;
        let mut capacity = 0_u64;
        // SAFETY: the owned descriptor stays open and MAXLEN writes exactly one
        // aligned u64 to this live local for the duration of the synchronous ioctl.
        if unsafe { libc::ioctl(file.as_raw_fd(), 0x8008_4801, &raw mut capacity) } < 0 {
            return Err(io::Error::last_os_error().into());
        }

        let capacity = usize::try_from(capacity)?;
        if capacity == 0 || isize::try_from(capacity).is_err() {
            bail!("invalid Hyperlight input capacity");
        }
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(capacity)?;
        buffer.resize(capacity, 0);
        Ok(Self { file, buffer })
    }

    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Reading again acknowledges the previous call; never read while its work is active.
    pub fn next(&mut self) -> Result<&[u8]> {
        loop {
            match self.file.read(&mut self.buffer) {
                Ok(0) => bail!("Hyperlight call device returned EOF"),
                Ok(amount) => return Ok(&self.buffer[..amount]),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// The device consumes one status/result message per write, not a byte stream.
    pub fn complete(&mut self, status: i32, result: &[u8]) -> Result<()> {
        let mut message = Vec::new();
        message.try_reserve_exact(
            result
                .len()
                .checked_add(size_of::<i32>())
                .context("call-result length overflow")?,
        )?;
        message.extend_from_slice(&status.to_ne_bytes());
        message.extend_from_slice(result);
        let amount = self
            .file
            .write(&message)
            .context("writing Hyperlight call result")?;
        if amount != message.len() {
            bail!("short Hyperlight call-result write");
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "queue-test.rs"]
mod tests;
