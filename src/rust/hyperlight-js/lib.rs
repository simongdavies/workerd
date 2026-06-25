// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Hyperlight JS worker interface crate.
//!
//! Runs a Worker's HTTP request inside a Hyperlight + QuickJS micro-VM and returns the response.
//! A request to this backend never enters V8 or JSG: it is marshalled to a JSON event, handed to a
//! warm guest (via `hyperlight-js`), which runs the Worker's handler and returns a JSON result that
//! becomes the HTTP response.
//!
//! The execution model is a warm pool of micro-VMs, each built once on its own dedicated thread
//! (the QuickJS sandbox is `!Send` and its calls block) and kept warm. Requests are handed out
//! round-robin; every `handle_event` rewinds the guest to its clean post-load snapshot, so requests
//! stay isolated from one another regardless of which VM serves them.
//!
//! The C++ `bridge.h` wraps a per-request handle as a `workerd::WorkerInterface`; only the
//! `request` (fetch) event is meaningful here, so this crate implements just `kj::http::Service`.

// The cxx FFI bridge (C++ <-> Rust) is defined in ffi.rs; declaring the module here compiles it
// into the crate so its generated cxxbridge symbols are emitted for the C++ side to link against.
mod ffi;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread::JoinHandle;

use cxx::KjError;
use cxx::KjExceptionType;
use futures::channel::oneshot;
use hyperlight_js::LoadedJSSandbox;
use hyperlight_js::SandboxBuilder;
use hyperlight_js::Script;
use kj::http::Method;

use crate::ffi::bridge::GuestResponse;
use crate::ffi::bridge::HttpHeaderEntry;

/// The name under which the Worker's handler is registered in the guest. The guest runtime invokes
/// the JS function exported under this name for every event.
const HANDLER_NAME: &str = "fetch";

/// Configuration for a Hyperlight JS worker: the JavaScript handler module source.
#[derive(Debug, Clone)]
pub struct HyperlightJsConfig {
    /// The Worker's JavaScript source. It must export a handler under [`HANDLER_NAME`], e.g.
    /// `export function fetch(event) { ... }`, returning a JSON-serializable value.
    pub handler_code: String,
}

/// A unit of work handed to the VM thread: the request marshalled as a JSON event, plus a one-shot
/// channel for the handler's JSON result.
struct VmJob {
    event_json: String,
    reply: oneshot::Sender<anyhow::Result<String>>,
}

/// Owns the dedicated OS thread that holds the warm, `!Send` [`LoadedJSSandbox`].
///
/// The guest sandbox is thread-bound and its calls block, so it lives on its own thread for its
/// entire lifetime. Work is submitted over an `mpsc` channel and each result is returned via a
/// one-shot channel that the async caller awaits — the cross-thread wake of that await is what keeps
/// the KJ event loop free while the guest executes.
struct VmThread {
    jobs: Option<mpsc::Sender<VmJob>>,
    handle: Option<JoinHandle<()>>,
}

impl VmThread {
    fn spawn(config: HyperlightJsConfig) -> Self {
        let (jobs, rx) = mpsc::channel::<VmJob>();
        match std::thread::Builder::new()
            .name("hyperlight-js-vm".to_owned())
            .spawn(move || vm_thread_main(&config, &rx))
        {
            Ok(handle) => Self {
                jobs: Some(jobs),
                handle: Some(handle),
            },
            Err(_) => {
                // The OS refused to create the VM thread (extremely rare). Drop the job sender so
                // `submit()` reports the worker as shut down and requests fail cleanly rather than
                // hanging forever waiting on a reply that will never come.
                Self {
                    jobs: None,
                    handle: None,
                }
            }
        }
    }

    /// Clone the job-submission channel for a new per-request handle. Returns `None` if the VM
    /// thread never started.
    fn sender(&self) -> Option<mpsc::Sender<VmJob>> {
        self.jobs.clone()
    }
}

impl Drop for VmThread {
    fn drop(&mut self) {
        // Close the channel so the VM thread's receive loop ends, then wait for it to unwind
        // (tearing down the guest VM cleanly).
        drop(self.jobs.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Body of the dedicated VM thread: build + load the sandbox once (registering the Worker's
/// handler), then serve every job by dispatching its event to the warm guest until the channel
/// closes.
fn vm_thread_main(config: &HyperlightJsConfig, jobs: &mpsc::Receiver<VmJob>) {
    match build_loaded_sandbox(config) {
        Ok(mut loaded) => {
            for job in jobs {
                // Each call rewinds the guest to its clean post-load snapshot before running, so
                // requests are isolated from one another despite sharing one warm VM.
                let result = loaded
                    .handle_event(HANDLER_NAME, job.event_json, None)
                    .map_err(|err| anyhow::anyhow!(err.to_string()));
                let _ = job.reply.send(result);
            }
        }
        Err(err) => {
            // The VM failed to build/load; fail every queued and future job with the error so
            // callers get a clear failure instead of hanging.
            let message = err.to_string();
            for job in jobs {
                let _ = job.reply.send(Err(anyhow::anyhow!(message.clone())));
            }
        }
    }
}

/// Build a warm [`LoadedJSSandbox`] with the Worker's handler registered. This performs the one-time
/// QuickJS runtime load + handler compile; subsequent `handle_event` calls reuse the warm guest.
fn build_loaded_sandbox(config: &HyperlightJsConfig) -> anyhow::Result<LoadedJSSandbox> {
    let proto = SandboxBuilder::new()
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build hyperlight-js sandbox: {e}"))?;
    let mut sandbox = proto
        .load_runtime()
        .map_err(|e| anyhow::anyhow!("failed to load hyperlight-js runtime: {e}"))?;
    sandbox
        .add_handler(
            HANDLER_NAME,
            Script::from_content(config.handler_code.clone()),
        )
        .map_err(|e| anyhow::anyhow!("failed to register hyperlight-js handler: {e}"))?;
    sandbox
        .get_loaded_sandbox()
        .map_err(|e| anyhow::anyhow!("failed to load hyperlight-js sandbox: {e}"))
}

fn internal_error(message: &str) -> KjError {
    KjError::new(
        KjExceptionType::Failed,
        format!("hyperlight-js worker: {message}"),
    )
}

/// Marshal an HTTP request into the JSON event passed to the guest handler.
///
/// The event carries the method, URL, request headers (as `[name, value]` pairs, preserving
/// duplicates) and the whole request body. A UTF-8 body is passed through as a string
/// (`bodyEncoding: "utf-8"`); a binary body is base64-encoded (`bodyEncoding: "base64"`, decodable
/// in-guest via `atob`); an empty body is `null` (`bodyEncoding: "none"`).
fn marshal_event(
    method: Method,
    url: &[u8],
    req_headers: &[HttpHeaderEntry],
    body: &[u8],
) -> String {
    let url = String::from_utf8_lossy(url);
    let headers: Vec<[&str; 2]> = req_headers
        .iter()
        .map(|h| [h.name.as_str(), h.value.as_str()])
        .collect();
    let (body_value, body_encoding) = if body.is_empty() {
        (serde_json::Value::Null, "none")
    } else {
        match std::str::from_utf8(body) {
            Ok(text) => (serde_json::Value::String(text.to_owned()), "utf-8"),
            Err(_) => (serde_json::Value::String(base64_encode(body)), "base64"),
        }
    };
    serde_json::json!({
        "method": format!("{method:?}"),
        "url": url,
        "headers": headers,
        "body": body_value,
        "bodyEncoding": body_encoding,
    })
    .to_string()
}

/// Standard base64 alphabet (RFC 4648).
const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode bytes as standard base64 with `=` padding (used to carry a binary request body into the
/// guest, where it is decoded with `atob`).
fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(BASE64_ALPHABET[(b0 >> 2) as usize] as char);
        out.push(BASE64_ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            BASE64_ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64_ALPHABET[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Decode standard base64, ignoring padding and ASCII whitespace. Returns `None` on an invalid
/// character. Used to recover a binary response body the guest produced with `btoa`.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let mut reverse = [255u8; 256];
    for (i, &c) in BASE64_ALPHABET.iter().enumerate() {
        reverse[c as usize] = i as u8;
    }
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    let mut buffer = 0u32;
    let mut bits = 0u32;
    for &c in input.as_bytes() {
        if c == b'=' || c == b'\n' || c == b'\r' || c == b' ' {
            continue;
        }
        let value = reverse[c as usize];
        if value == 255 {
            return None;
        }
        buffer = (buffer << 6) | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
}

/// JSON key the guest's `Response` serializes under (`Response.prototype.toJSON`), letting the host
/// distinguish an explicit `Response` from a plain handler return value.
const HL_RESPONSE_MARKER: &str = "$hlResponse";

/// Map the guest handler's JSON result into a [`GuestResponse`].
///
/// A result tagged with [`HL_RESPONSE_MARKER`] (the handler returned a `Response`) is mapped to its
/// status/headers/body. Any other value is served as-is with `200 OK` and `application/json` (the
/// legacy behaviour).
fn parse_guest_response(result: &str) -> GuestResponse {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(result) {
        if value.get(HL_RESPONSE_MARKER).is_some() {
            let status = value
                .get("status")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(200)
                .clamp(200, 599) as u16;
            let status_text = match value.get("statusText").and_then(serde_json::Value::as_str) {
                Some(text) if !text.is_empty() => text.to_owned(),
                _ => reason_phrase(status).to_owned(),
            };
            let headers = value
                .get("headers")
                .and_then(serde_json::Value::as_array)
                .map(|pairs| {
                    pairs
                        .iter()
                        .filter_map(|pair| {
                            let pair = pair.as_array()?;
                            Some(HttpHeaderEntry {
                                name: pair.first()?.as_str()?.to_owned(),
                                value: pair.get(1)?.as_str()?.to_owned(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let body_text = value
                .get("body")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let body = if value
                .get("bodyBase64")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                base64_decode(body_text).unwrap_or_default()
            } else {
                body_text.as_bytes().to_vec()
            };
            return GuestResponse {
                status,
                status_text,
                headers,
                body,
            };
        }
    }

    GuestResponse {
        status: 200,
        status_text: "OK".to_owned(),
        headers: vec![HttpHeaderEntry {
            name: "content-type".to_owned(),
            value: "application/json; charset=utf-8".to_owned(),
        }],
        body: result.as_bytes().to_vec(),
    }
}

/// A reason phrase for common status codes, used when a guest `Response` did not set an explicit
/// `statusText`. Falls back to `"Unknown"` so the kj response always has a non-empty reason phrase.
fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        418 => "I'm a Teapot",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

/// Environment variable selecting how many warm micro-VMs to keep in the pool.
///
/// Each VM serves requests serially (rewinding between them), so the pool size is the effective
/// concurrency ceiling. Defaults to [`DEFAULT_POOL_SIZE`], clamped to `1..=MAX_POOL_SIZE`.
const POOL_SIZE_ENV: &str = "WORKERD_HYPERLIGHT_POOL_SIZE";
const DEFAULT_POOL_SIZE: usize = 1;
const MAX_POOL_SIZE: usize = 64;

/// Read the configured pool size from [`POOL_SIZE_ENV`], clamped to `1..=MAX_POOL_SIZE`. An unset,
/// empty, or unparseable value falls back to [`DEFAULT_POOL_SIZE`].
fn configured_pool_size() -> usize {
    std::env::var(POOL_SIZE_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .map(|n| n.clamp(1, MAX_POOL_SIZE))
        .unwrap_or(DEFAULT_POOL_SIZE)
}

/// A warm, long-lived pool of Hyperlight JS micro-VMs shared across all requests.
pub struct SharedVm {
    vms: Vec<VmThread>,
    next: AtomicUsize,
}

impl SharedVm {
    /// Boot the pool of warm micro-VMs for the given Worker source. The number of VMs comes from the
    /// `WORKERD_HYPERLIGHT_POOL_SIZE` environment variable (default 1). Each VM builds and warms up
    /// asynchronously on its own thread.
    pub fn new(config: HyperlightJsConfig) -> Self {
        let size = configured_pool_size();
        let vms = (0..size).map(|_| VmThread::spawn(config.clone())).collect();
        Self {
            vms,
            next: AtomicUsize::new(0),
        }
    }

    /// Create a lightweight per-request handle bound to the next VM in the pool (round-robin).
    pub fn new_request(&self) -> RequestHandle {
        // The pool always has at least one VM, so the modulo is safe.
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.vms.len();
        RequestHandle {
            jobs: self.vms[index].sender(),
        }
    }
}

/// A per-request handle onto the shared warm VM pool. Holds a clone of one VM's job channel; serving
/// a request marshals it to an event, runs the Worker's handler on the warm guest, and returns the
/// handler's result as the response body.
pub struct RequestHandle {
    jobs: Option<mpsc::Sender<VmJob>>,
}

impl RequestHandle {
    /// Submit a request event to the shared VM thread and obtain a one-shot receiver for its result.
    fn submit(
        &self,
        event_json: String,
    ) -> Result<oneshot::Receiver<anyhow::Result<String>>, KjError> {
        let jobs = self
            .jobs
            .as_ref()
            .ok_or_else(|| internal_error("the micro-VM thread is not running"))?;
        let (reply, rx) = oneshot::channel();
        jobs.send(VmJob { event_json, reply })
            .map_err(|_| internal_error("the micro-VM thread is no longer running"))?;
        Ok(rx)
    }

    /// Marshal the request into the guest event, run the Worker's handler on the warm guest, and map
    /// the handler's JSON result into a [`GuestResponse`]. The await is woken cross-thread when the
    /// VM thread replies, so the KJ event loop stays free while the guest runs.
    async fn run_request(
        &self,
        method: Method,
        url: &[u8],
        req_headers: &[HttpHeaderEntry],
        body: &[u8],
    ) -> worker::Result<GuestResponse> {
        let event_json = marshal_event(method, url, req_headers, body);
        let rx = self.submit(event_json)?;
        let result = rx
            .await
            .map_err(|_| internal_error("the micro-VM thread dropped the request"))?
            .map_err(|err| internal_error(&format!("guest execution failed: {err}")))?;
        Ok(parse_guest_response(&result))
    }
}
