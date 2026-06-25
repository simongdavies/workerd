// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/worker-interface.h>
#include <workerd/rust/hyperlight-js/ffi.rs.h>

#include <kj-rs/kj-rs.h>

namespace workerd::rust::hyperlight_js {

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
    return impl->request(method, url.asBytes().as<kj_rs::Rust>(), headers, requestBody, response);
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
