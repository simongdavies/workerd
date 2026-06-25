// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/worker-interface.h>
#include <workerd/rust/hyperlight-js/ffi.rs.h>

#include <kj-rs/kj-rs.h>

#include <kj/vector.h>

#include <cstdint>

namespace workerd::rust::hyperlight_js {

// Cap on the request body materialized for the guest. This backend has no streaming, so the entire
// body is read into memory before the guest runs; this bounds that allocation.
constexpr uint64_t MAX_REQUEST_BODY_BYTES = 100ull * 1024 * 1024;

// Adapts a Rust per-request `RequestWorker` (a handle onto the shared warm Hyperlight + QuickJS
// micro-VM pool) to workerd's `WorkerInterface`. Only `request` (i.e. `fetch`) is delegated to the
// guest; the other event types are not meaningful for a Hyperlight JS worker and are unsupported.
class RustHyperlightJsWorkerInterface final: public workerd::WorkerInterface {
 public:
  using Impl = workerd::rust::hyperlight_js::RequestWorker;
  explicit RustHyperlightJsWorkerInterface(::rust::Box<Impl> impl): impl(kj::mv(impl)) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override {
    // Marshal the request headers into a flat list for the guest event.
    kj::Vector<HttpHeaderEntry> reqHeaders;
    headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
      reqHeaders.add(HttpHeaderEntry{
        ::rust::String(name.begin(), name.size()),
        ::rust::String(value.begin(), value.size()),
      });
    });

    // Whole-body read: this backend has no streaming, so the entire request body is materialized
    // before the guest runs (the Tier 2 blocking model).
    auto body = co_await requestBody.readAllBytes(MAX_REQUEST_BODY_BYTES);

    auto guestResponse = co_await impl->run_request(method, url.asBytes().as<kj_rs::Rust>(),
        ::rust::Slice<const HttpHeaderEntry>(reqHeaders.begin(), reqHeaders.size()),
        ::rust::Slice<const ::std::uint8_t>(body.begin(), body.size()));

    // Build the response headers on the request's header table (a fresh table would mismatch the
    // header IDs the runtime registered), then add the guest's headers by name.
    auto responseHeaders = headers.cloneShallow();
    responseHeaders.clear();
    for (const auto& header: guestResponse.headers) {
      responseHeaders.add(kj::heapString(header.name.data(), header.name.size()),
          kj::heapString(header.value.data(), header.value.size()));
    }

    auto statusText =
        kj::heapString(guestResponse.status_text.data(), guestResponse.status_text.size());
    auto out = response.send(guestResponse.status, statusText, responseHeaders,
        static_cast<uint64_t>(guestResponse.body.size()));
    co_await out->write(kj::arrayPtr(guestResponse.body.data(), guestResponse.body.size()));
    co_return;
  }

  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& tunnel,
      kj::HttpConnectSettings settings) override {
    throwUnsupported();
  }

  kj::Promise<void> prewarm(kj::StringPtr url) override {
    return kj::READY_NOW;
  }

  kj::Promise<ScheduledResult> runScheduled(kj::Date scheduledTime, kj::StringPtr cron) override {
    throwUnsupported();
  }

  kj::Promise<AlarmResult> runAlarm(kj::Date scheduledTime, uint32_t retryCount) override {
    throwUnsupported();
  }

  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    throwUnsupported();
  }

 private:
  ::rust::Box<Impl> impl;

  [[noreturn]] static void throwUnsupported() {
    KJ_FAIL_REQUIRE("Hyperlight JS workers only support fetch (request) events.");
  }
};

// Boot the shared, warm micro-VM pool once (at service construction) for the given Worker JS source.
inline ::rust::Box<SharedWorker> newHyperlightJsSharedWorker(kj::StringPtr handlerCode) {
  return new_hyperlight_js_worker(::rust::Str(handlerCode.begin(), handlerCode.size()));
}

// Mint a per-request `WorkerInterface` onto the shared warm micro-VM pool.
inline kj::Own<workerd::WorkerInterface> newHyperlightJsWorkerInterface(SharedWorker& shared) {
  return kj::heap<RustHyperlightJsWorkerInterface>(shared.new_request());
}

}  // namespace workerd::rust::hyperlight_js
