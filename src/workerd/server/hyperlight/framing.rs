// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! The boundary of one complete request, checked before any worker dispatch.
//! Headers and chunk sizes use httparse; chunk framing also enforces Hyper's
//! CRLF and resource rules before the worker can observe the request.

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use httparse::Status;

// Match Hyper 1.12.0's private chunk extension/trailer limits and kj-hyper's max_headers.
const CHUNK_EXTENSIONS_LIMIT: usize = 16 * 1024;
const TRAILER_BYTES_LIMIT: usize = 16 * 1024;
const MAX_TRAILERS: usize = 16 * 1024;

fn crlf_only(input: &[u8]) -> Result<()> {
    for (index, &byte) in input.iter().enumerate() {
        if (byte == b'\n' && (index == 0 || input[index - 1] != b'\r'))
            || (byte == b'\r' && input.get(index + 1) != Some(&b'\n'))
        {
            bail!("invalid HTTP chunk or trailer newline");
        }
    }
    Ok(())
}

fn headers(input: &[u8]) -> Result<Vec<httparse::Header<'_>>> {
    let count = input
        .split(|&byte| byte == b'\n')
        .skip_while(|line| line.is_empty() || *line == b"\r")
        .take_while(|line| !line.is_empty() && *line != b"\r")
        .count();
    let mut headers = Vec::new();
    headers.try_reserve_exact(count)?;
    headers.resize(count, httparse::EMPTY_HEADER);
    Ok(headers)
}

fn chunks(input: &[u8], mut position: usize) -> Result<usize> {
    let mut extensions = 0_usize;
    loop {
        if !input.get(position).is_some_and(u8::is_ascii_hexdigit) {
            bail!("missing HTTP chunk size");
        }
        let Status::Complete((prefix, size)) = httparse::parse_chunk_size(&input[position..])
            .map_err(|_| anyhow::anyhow!("invalid HTTP chunk size"))?
        else {
            bail!("truncated HTTP chunk size");
        };
        let next_line = position
            .checked_add(prefix)
            .context("chunk offset overflow")?;
        let line = &input[position..next_line];
        crlf_only(line)?;
        if let Some(separator) = line.iter().position(|&byte| byte == b';') {
            extensions = extensions
                .checked_add(line.len() - separator - 3)
                .context("HTTP chunk extension budget overflow")?;
            if extensions >= CHUNK_EXTENSIONS_LIMIT {
                bail!("HTTP chunk extensions exceed Hyper's limit");
            }
        }
        position = next_line;
        if size == 0 {
            let mut trailers = headers(&input[position..])?;
            let Status::Complete((length, fields)) =
                httparse::parse_headers(&input[position..], &mut trailers)?
            else {
                bail!("truncated HTTP trailers");
            };
            if fields.len() > MAX_TRAILERS {
                bail!("HTTP trailers exceed Hyper's header count limit");
            }
            let end = position
                .checked_add(length)
                .context("trailer offset overflow")?;
            let block = &input[position..end];
            if block.len() >= TRAILER_BYTES_LIMIT {
                bail!("HTTP trailers exceed Hyper's byte limit");
            }
            crlf_only(block)?;
            if !block.ends_with(b"\r\n") {
                bail!("HTTP trailers require CRLF termination");
            }
            return Ok(end);
        }
        let end = position
            .checked_add(usize::try_from(size)?)
            .context("chunk size overflow")?;
        let next = end.checked_add(2).context("chunk terminator overflow")?;
        if input.get(end..next) != Some(b"\r\n") {
            bail!("truncated HTTP chunk body or invalid terminator");
        }
        position = next;
    }
}

/// Refuses ambiguous, incomplete or trailing framing independently of keep-alive and read-ahead.
pub fn validate(input: &[u8]) -> Result<()> {
    let mut fields = headers(input)?;
    let mut request = httparse::Request::new(&mut fields);
    let Status::Complete(head) = request.parse(input)? else {
        bail!("truncated HTTP request head");
    };
    let mut length = None;
    let mut transfer = false;
    let mut chunked = false;
    for header in request.headers.iter() {
        if header.name.eq_ignore_ascii_case("content-length") {
            for value in header.value.split(|&byte| byte == b',') {
                let value = value.trim_ascii();
                if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
                    bail!("invalid HTTP Content-Length");
                }
                let parsed = std::str::from_utf8(value)?.parse::<usize>()?;
                if length.is_some_and(|length| length != parsed) {
                    bail!("conflicting HTTP Content-Length fields");
                }
                length = Some(parsed);
            }
        } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
            for encoding in header.value.split(|&byte| byte == b',') {
                let encoding = encoding.trim_ascii();
                if encoding.is_empty() || chunked {
                    bail!("invalid HTTP Transfer-Encoding order");
                }
                transfer = true;
                chunked = encoding.eq_ignore_ascii_case(b"chunked");
            }
        }
    }
    let end = if transfer {
        if !chunked || request.version != Some(1) || length.is_some() {
            bail!("finite HTTP requests require unambiguous chunked framing");
        }
        chunks(input, head)?
    } else {
        head.checked_add(length.unwrap_or(0))
            .context("HTTP body length overflow")?
    };
    if end != input.len() {
        bail!("incomplete HTTP body or trailing bytes after one request");
    }
    Ok(())
}

#[cfg(test)]
#[path = "framing-test.rs"]
mod tests;
