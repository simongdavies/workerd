// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/worker-interface.capnp.h>

#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/compat/http.h>
#include <kj/memory.h>
#include <kj/string.h>

namespace workerd::server {

class WorkerVersionKey {
 public:
  WorkerVersionKey(kj::String workerId, kj::String version);

  kj::StringPtr getWorkerId() const {
    return workerId;
  }

  kj::StringPtr getVersion() const {
    return version;
  }

 private:
  kj::String workerId;
  kj::String version;
};

// Supplies a single-use event dispatcher for one immutable Worker version. Implementations own
// transport and sandbox lifecycle. A future Hyperlight implementation can provide the same
// capability over an embedded transport without changing Server request routing.
class SandboxSupervisor: public kj::Refcounted {
 public:
  virtual const WorkerVersionKey& getWorkerVersion() const = 0;

  virtual rpc::EventDispatcher::Client startEvent(kj::Maybe<kj::StringPtr> cfBlobJson) = 0;
};

kj::Own<SandboxSupervisor> newNetworkSandboxSupervisor(WorkerVersionKey workerVersion,
    kj::Own<kj::NetworkAddress> address,
    kj::String capnpConnectHost,
    kj::HttpHeaderTable& headerTable,
    kj::Timer& timer,
    kj::EntropySource& entropySource);

}  // namespace workerd::server
