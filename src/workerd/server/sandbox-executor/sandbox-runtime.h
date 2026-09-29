// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/worker.h>

#include <kj/async.h>
#include <kj/compat/http.h>

namespace workerd::server::sandbox_executor {

struct Header {
  kj::String name;
  kj::String value;
};

struct Response {
  uint statusCode;
  kj::Array<Header> headers;
  kj::String body;
};

enum class ModuleType {
  ES_MODULE,
  TEXT,
  JSON,
};

struct Module {
  kj::String name;
  ModuleType type;
  kj::String source;
};

struct WorkerBundle {
  kj::String workerVersion;
  kj::String compatibilityDate;
  kj::Array<kj::String> compatibilityFlags;
  kj::String mainModule;
  kj::Array<Module> modules;
};

class SandboxRuntime {
 public:
  explicit SandboxRuntime(const WorkerBundle& bundle);
  ~SandboxRuntime() noexcept(false);

  Response runRequest(kj::HttpMethod method,
      kj::StringPtr url,
      kj::ArrayPtr<const Header> headers,
      kj::StringPtr body);

 private:
  struct Impl;
  kj::Own<Impl> impl;
};

}  // namespace workerd::server::sandbox_executor
