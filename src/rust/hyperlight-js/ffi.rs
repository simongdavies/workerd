// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! C++ <-> Rust bridge for the Hyperlight JS worker.
//!
//! Exposes two opaque handles to C++: a `SharedWorker` (the warm, long-lived pool of micro-VMs,
//! built once per service) and a per-request `RequestWorker` minted from it via `new_request`. The
//! bridge is self-contained (cxx opaque types are per-bridge) but borrows the shared kj cxx types
//! for the request/response plumbing.

use std::pin::Pin;

use kj::http::Service as _;
use worker::Result;

use crate::HyperlightJsConfig;
use crate::RequestHandle;
use crate::SharedVm;

#[cxx::bridge(namespace = "workerd::rust::hyperlight_js")]
pub mod bridge {
    unsafe extern "C++" {
        // Brings the `kj::rust` C++ type aliases (HttpMethod, HttpHeaders, ...) into the generated
        // header/source so the shared kj cxx types below resolve.
        include!("workerd/rust/kj/ffi.h");
    }

    #[namespace = "kj::rust"]
    unsafe extern "C++" {
        type HttpMethod = kj::http::ffi::HttpMethod;
        type HttpHeaders = kj::http::ffi::HttpHeaders;
        type HttpServiceResponse = kj::http::ffi::HttpServiceResponse;
        type AsyncInputStream = kj::io::ffi::AsyncInputStream;
    }

    extern "Rust" {
        type SharedWorker;
        type RequestWorker;

        /// Boot the shared, warm pool of micro-VMs running the given Worker JS source, spawning each
        /// VM's dedicated thread. Called once per service. Infallible at the boundary: a failure to
        /// build/load a guest surfaces as an error on the first request rather than here.
        fn new_hyperlight_js_worker(handler_code: &str) -> Box<SharedWorker>;

        /// Mint a lightweight per-request handle onto the shared warm VM pool.
        fn new_request(self: &SharedWorker) -> Box<RequestWorker>;

        async unsafe fn request<'a>(
            self: &'a mut RequestWorker,
            method: HttpMethod,
            url: &'a [u8],
            headers: &'a HttpHeaders,
            request_body: Pin<&'a mut AsyncInputStream>,
            response: Pin<&'a mut HttpServiceResponse>,
        ) -> Result<()>;
    }

    impl Box<SharedWorker> {}
    impl Box<RequestWorker> {}
}

/// The warm, long-lived pool of micro-VMs behind the cxx opaque `SharedWorker` handle.
pub struct SharedWorker {
    inner: SharedVm,
}

/// A per-request handle behind the cxx opaque `RequestWorker` handle.
pub struct RequestWorker {
    inner: RequestHandle,
}

fn new_hyperlight_js_worker(handler_code: &str) -> Box<SharedWorker> {
    let config = HyperlightJsConfig {
        handler_code: handler_code.into(),
    };
    Box::new(SharedWorker {
        inner: SharedVm::new(config),
    })
}

impl SharedWorker {
    fn new_request(&self) -> Box<RequestWorker> {
        Box::new(RequestWorker {
            inner: self.inner.new_request(),
        })
    }
}

impl RequestWorker {
    async fn request<'a>(
        &'a mut self,
        method: bridge::HttpMethod,
        url: &'a [u8],
        headers: &'a bridge::HttpHeaders,
        request_body: Pin<&'a mut bridge::AsyncInputStream>,
        response: Pin<&'a mut bridge::HttpServiceResponse>,
    ) -> Result<()> {
        let response = kj::http::ServiceResponse::from(response);
        self.inner
            .request(method, url, headers.into(), request_body, response)
            .await?;
        Ok(())
    }
}
