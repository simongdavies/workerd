// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/actor-storage.capnp.h>

#include <kj/string.h>

#include <cstdint>

namespace workerd::server {

// An explicitly supplied namespace backend. Implementations retain their own trusted
// authority and return Workerd's existing Stage capability, not an HTTP facade.
class ActorStorageBackend {
 public:
  virtual ~ActorStorageBackend() noexcept(false) = default;
  virtual rpc::ActorStorage::Stage::Client getStage(kj::StringPtr actorId, uint32_t facetId) = 0;
};

}  // namespace workerd::server
