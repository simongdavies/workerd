// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Standard configuration in the admitted guest image, never a caller-selected host path.

use std::fs::File;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use nix::fcntl::OFlag;
use nix::fcntl::openat;
use nix::sys::stat::Mode;
use ring::digest;

use crate::protocol::validate_artifact_bounds;

const CONFIG_DIRECTORY: &str = "/workerd";
const CONFIG_FILE: &str = "config.bin";

fn read_owned(
    input: &mut impl Read,
    length: usize,
    budget: usize,
    expected_digest: &[u8; 32],
) -> Result<Vec<u8>> {
    validate_artifact_bounds(length, budget)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length)?;
    bytes.resize(length, 0);
    input
        .read_exact(&mut bytes)
        .context("truncated config artifact")?;
    let mut extra = [0];
    if input.read(&mut extra)? != 0 {
        bail!("extra bytes after declared config artifact");
    }
    if digest::digest(&digest::SHA256, &bytes).as_ref() != expected_digest {
        bail!("config artifact digest mismatch");
    }
    Ok(bytes)
}

fn from_directory(
    directory: &Path,
    length: usize,
    budget: usize,
    expected_digest: &[u8; 32],
) -> Result<Vec<u8>> {
    validate_artifact_bounds(length, budget)?;
    let directory = File::options()
        .read(true)
        .custom_flags((OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
        .open(directory)
        .context("opening admitted config directory")?;
    let fd = openat(
        &directory,
        CONFIG_FILE,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
        Mode::empty(),
    )
    .context("opening admitted config artifact")?;
    let mut file = File::from(fd);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != u64::try_from(length)? {
        bail!("config artifact is not a regular file of the declared length");
    }
    read_owned(&mut file, length, budget, expected_digest)
}

pub fn load(length: usize, budget: usize, expected_digest: &[u8; 32]) -> Result<Vec<u8>> {
    from_directory(Path::new(CONFIG_DIRECTORY), length, budget, expected_digest)
}

#[cfg(test)]
#[path = "artifact-test.rs"]
mod tests;
