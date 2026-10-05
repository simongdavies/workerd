// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-logical-service.h"

#include "hyperlight-driver.h"

#include <workerd/server/logical-service-broker/logical-service-broker.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/async-io.h>

#include <cmath>

namespace workerd::server::sandbox_executor {
namespace {

namespace composite = logical_service_broker::composite;

enum class LogicalServiceProtocol {
  SIGNED_V1,
  COMPOSITE_V2,
};

LogicalServiceProtocol classifyRequest(kj::ArrayPtr<const char> envelope) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(envelope, root);
  KJ_REQUIRE(root.isObject(), "logical service request must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(fields.size() > 0, "logical service request must not be empty");
  auto first = fields[0];
  KJ_REQUIRE(first.getValue().isNumber(), "invalid logical service protocol version");
  auto version = first.getValue().getNumber();
  KJ_REQUIRE(std::floor(version) == version, "invalid logical service protocol version");
  if (first.getName() == "protocol_version"_kj && version == 1) {
    logical_service_broker::parseCanonicalRequest(envelope);
    return LogicalServiceProtocol::SIGNED_V1;
  }
  if (first.getName() == "version"_kj && version == composite::PROTOCOL_VERSION) {
    composite::parseRequestEnvelope(envelope);
    return LogicalServiceProtocol::COMPOSITE_V2;
  }
  KJ_FAIL_REQUIRE("unsupported logical service protocol version");
}

class HyperlightLogicalServiceHostChannel final: public LogicalServiceHostChannel {
 public:
  kj::String invoke(kj::ArrayPtr<const char> canonicalEnvelope) override {
    KJ_REQUIRE(canonicalEnvelope.size() <= logical_service_broker::MAX_ENVELOPE_BYTES,
        "logical service request exceeds limit");
    auto protocol = classifyRequest(canonicalEnvelope);
    auto argument =
        hostCallStringArg(kj::StringPtr(canonicalEnvelope.begin(), canonicalEnvelope.size()));
    auto response =
        hostCallString(logical_service_broker::HOST_CALL_NAME, kj::arrayPtr(&argument, 1));
    KJ_REQUIRE(response.size() <= logical_service_broker::MAX_ENVELOPE_BYTES,
        "logical service response exceeds limit");
    switch (protocol) {
      case LogicalServiceProtocol::SIGNED_V1:
        logical_service_broker::parseCanonicalResponse(response);
        break;
      case LogicalServiceProtocol::COMPOSITE_V2:
        composite::parseResponseEnvelope(response);
        break;
    }
    return response;
  }
};

class CompositeServiceWorker final: public WorkerInterface {
 public:
  CompositeServiceWorker(kj::Rc<LogicalServiceHostChannel> host,
      kj::HttpHeaderTable& headerTable,
      kj::String binding,
      composite::BindingKind kind)
      : host(kj::mv(host)),
        headerTable(headerTable),
        binding(kj::mv(binding)),
        kind(kind) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& response) override {
    KJ_REQUIRE(method == kj::HttpMethod::POST, "logical service bindings accept only POST");
    auto envelope = co_await requestBody.readAllText(logical_service_broker::MAX_ENVELOPE_BYTES);
    auto parsed = composite::parseRequestEnvelope(envelope);
    KJ_REQUIRE(parsed.binding == binding, "logical service request binding mismatch");
    KJ_REQUIRE(composite::bindingKindFor(parsed.operation) == kind,
        "logical service operation does not match binding kind");

    auto hostResponse = host->invoke(envelope);
    auto parsedResponse = composite::parseResponseEnvelope(hostResponse);
    KJ_REQUIRE(parsedResponse.requestId == parsed.requestId,
        "logical service response request ID mismatch");

    kj::HttpHeaders responseHeaders(headerTable);
    responseHeaders.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json"_kj);
    auto output = response.send(200, "OK"_kj, responseHeaders, hostResponse.size());
    co_await output->write(hostResponse.asBytes()).attach(kj::mv(output), kj::mv(hostResponse));
  }

  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncIoStream&,
      ConnectResponse&,
      kj::HttpConnectSettings) override {
    KJ_FAIL_REQUIRE("logical service bindings do not support connect");
  }
  kj::Promise<void> prewarm(kj::StringPtr) override {
    return kj::READY_NOW;
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date, kj::StringPtr) override {
    KJ_FAIL_REQUIRE("logical service bindings do not support scheduled events");
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date, uint32_t) override {
    KJ_FAIL_REQUIRE("logical service bindings do not support alarms");
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return event->notSupported();
  }

 private:
  kj::Rc<LogicalServiceHostChannel> host;
  kj::HttpHeaderTable& headerTable;
  kj::String binding;
  composite::BindingKind kind;
};

class CompositeServiceChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  CompositeServiceChannel(kj::Rc<LogicalServiceHostChannel> host,
      kj::HttpHeaderTable& headerTable,
      kj::String binding,
      composite::BindingKind kind)
      : host(kj::mv(host)),
        headerTable(headerTable),
        binding(kj::mv(binding)),
        kind(kind) {}

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata) override {
    return kj::heap<CompositeServiceWorker>(host.addRef(), headerTable, kj::str(binding), kind);
  }

  void requireAllowsTransfer() override {
    KJ_FAIL_REQUIRE("logical service bindings cannot be transferred");
  }

  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    KJ_FAIL_REQUIRE("logical service bindings cannot be tokenized");
  }

 private:
  kj::Rc<LogicalServiceHostChannel> host;
  kj::HttpHeaderTable& headerTable;
  kj::String binding;
  composite::BindingKind kind;
};

}  // namespace

kj::Rc<LogicalServiceHostChannel> newHyperlightLogicalServiceHostChannel() {
  return kj::rc<HyperlightLogicalServiceHostChannel>();
}

kj::Own<IoChannelFactory::SubrequestChannel> newCompositeServiceChannel(
    kj::Rc<LogicalServiceHostChannel> host,
    kj::HttpHeaderTable& headerTable,
    kj::String binding,
    composite::BindingKind kind) {
  KJ_REQUIRE(composite::isBindingName(binding), "invalid composite binding name");
  return kj::refcounted<CompositeServiceChannel>(kj::mv(host), headerTable, kj::mv(binding), kind);
}

}  // namespace workerd::server::sandbox_executor
