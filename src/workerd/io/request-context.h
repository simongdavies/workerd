// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/refcount.h>

namespace workerd {

// Immutable, embedder-owned request provenance. This is never serialized or exposed to
// application JavaScript. An embedder interprets its own subtype and validates authority at
// its effect boundary; merely having a context is not an authorization decision.
class RequestContext: public kj::Refcounted {
 public:
  virtual ~RequestContext() noexcept(false) = default;
};

}  // namespace workerd
