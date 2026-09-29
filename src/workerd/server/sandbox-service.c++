// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-service.h"

#include <capnp/message.h>

namespace workerd::server {

WorkerVersionKey::WorkerVersionKey(kj::String workerId, kj::String version)
    : workerId(kj::mv(workerId)),
      version(kj::mv(version)) {}

namespace {

class NetworkSandboxSupervisor final: public SandboxSupervisor {
 public:
  NetworkSandboxSupervisor(WorkerVersionKey workerVersion,
      kj::Own<kj::NetworkAddress> address,
      kj::String capnpConnectHost,
      kj::HttpHeaderTable& headerTable,
      kj::Timer& timer,
      kj::EntropySource& entropySource)
      : workerVersion(kj::mv(workerVersion)),
        address(kj::mv(address)),
        capnpConnectHost(kj::mv(capnpConnectHost)),
        headerTable(headerTable),
        httpClient(kj::newHttpClient(
            timer, headerTable, *this->address, {.entropySource = entropySource})) {}

  const WorkerVersionKey& getWorkerVersion() const override {
    return workerVersion;
  }

  rpc::EventDispatcher::Client startEvent(kj::Maybe<kj::StringPtr> cfBlobJson) override {
    auto request = getBootstrap().startEventRequest(capnp::MessageSize{4, 0});
    KJ_IF_SOME(cf, cfBlobJson) {
      request.setCfBlobJson(cf);
    }
    return request.send().getDispatcher();
  }

 private:
  struct Connection {
    kj::Own<kj::AsyncIoStream> stream;
    capnp::TwoPartyClient rpcSystem;

    Connection(kj::Own<kj::AsyncIoStream> stream)
        : stream(kj::mv(stream)),
          rpcSystem(*this->stream) {}
  };

  WorkerVersionKey workerVersion;
  kj::Own<kj::NetworkAddress> address;
  kj::String capnpConnectHost;
  kj::HttpHeaderTable& headerTable;
  kj::Own<kj::HttpClient> httpClient;
  kj::Maybe<Connection> connection;
  kj::Promise<void> clearConnectionTask = nullptr;

  rpc::WorkerdBootstrap::Client getBootstrap() {
    KJ_IF_SOME(c, connection) {
      return c.rpcSystem.bootstrap().castAs<rpc::WorkerdBootstrap>();
    }

    auto connectRequest = httpClient->connect(capnpConnectHost, kj::HttpHeaders(headerTable), {});
    auto& c = connection.emplace(kj::mv(connectRequest.connection));
    clearConnectionTask =
        c.rpcSystem.onDisconnect().attach(kj::defer([this]() {
      connection = kj::none;
    })).eagerlyEvaluate(nullptr);
    return c.rpcSystem.bootstrap().castAs<rpc::WorkerdBootstrap>();
  }
};

}  // namespace

kj::Own<SandboxSupervisor> newNetworkSandboxSupervisor(WorkerVersionKey workerVersion,
    kj::Own<kj::NetworkAddress> address,
    kj::String capnpConnectHost,
    kj::HttpHeaderTable& headerTable,
    kj::Timer& timer,
    kj::EntropySource& entropySource) {
  return kj::refcounted<NetworkSandboxSupervisor>(kj::mv(workerVersion), kj::mv(address),
      kj::mv(capnpConnectHost), headerTable, timer, entropySource);
}

}  // namespace workerd::server
