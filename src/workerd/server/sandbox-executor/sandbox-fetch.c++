// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-fetch.h"

#include <workerd/jsg/exception.h>
#include <workerd/util/stream-utils.h>

namespace workerd::server::sandbox_executor {
namespace {

bool validHeaderName(kj::StringPtr name) {
  if (name.size() == 0 || name.size() > 256) return false;
  for (char c: name) {
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) &&
        strchr("!#$%&'*+-.^_`|~", c) == nullptr) {
      return false;
    }
  }
  return true;
}

bool validHeaderValue(kj::StringPtr value) {
  for (char c: value) {
    auto byte = static_cast<unsigned char>(c);
    if ((byte < 0x20 || byte == 0x7f) && c != '\t') return false;
  }
  return true;
}

class OutboundFetchWorker final: public WorkerInterface {
 public:
  OutboundFetchWorker(kj::Rc<FetchBroker> broker, TimerChannel& timer, kj::String requestId)
      : broker(kj::mv(broker)),
        timer(timer),
        requestId(kj::mv(requestId)) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& response) override {
    try {
      KJ_REQUIRE(url.size() <= MAX_OUTBOUND_FETCH_URL_BYTES &&
              (url.startsWith("http://") || url.startsWith("https://")),
          "invalid outbound fetch URL");

      kj::Vector<Header> requestHeaders;
      size_t aggregateHeaderBytes = 0;
      headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
        KJ_REQUIRE(
            requestHeaders.size() < MAX_OUTBOUND_FETCH_HEADERS, "too many outbound fetch headers");
        KJ_REQUIRE(
            validHeaderName(name) && validHeaderValue(value), "invalid outbound fetch header");
        aggregateHeaderBytes += name.size() + value.size();
        KJ_REQUIRE(aggregateHeaderBytes <= MAX_OUTBOUND_FETCH_HEADER_BYTES,
            "outbound fetch headers exceed limit");
        requestHeaders.add(Header{kj::str(name), kj::str(value)});
      });

      co_await broker->request(
          FetchRequest{
            .requestId = kj::str(requestId),
            .method = method,
            .url = kj::str(url),
            .headers = requestHeaders.releaseAsArray(),
            .bodyLength = requestBody.tryGetLength(),
          },
          headers, requestBody, response, timer);
    } catch (kj::Exception& exception) {
      if (jsg::isTunneledException(exception.getDescription())) {
        throw kj::mv(exception);
      }
      throw kj::Exception(exception.getType(), exception.getFile(), exception.getLine(),
          kj::str(JSG_EXCEPTION(Error), ": outbound fetch failed: ", exception.getDescription()));
    }
  }

  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncIoStream&,
      ConnectResponse&,
      kj::HttpConnectSettings) override {
    KJ_FAIL_REQUIRE("raw outbound connections are unavailable");
  }
  kj::Promise<void> prewarm(kj::StringPtr) override {
    return kj::READY_NOW;
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date, kj::StringPtr) override {
    KJ_FAIL_REQUIRE("scheduled events are unavailable");
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date, uint32_t) override {
    KJ_FAIL_REQUIRE("alarms are unavailable");
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return event->notSupported();
  }

 private:
  kj::Rc<FetchBroker> broker;
  TimerChannel& timer;
  kj::String requestId;
};

class OutboundFetchChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  OutboundFetchChannel(
      kj::Rc<FetchBroker> broker, TimerChannel& timer, kj::Rc<uint64_t> nextRequestId)
      : broker(kj::mv(broker)),
        timer(timer),
        nextRequestId(kj::mv(nextRequestId)) {}

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata) override {
    KJ_REQUIRE(*nextRequestId != kj::maxValue, "outbound fetch request ID space exhausted");
    return newOutboundFetchWorker(broker.addRef(), timer, kj::str((*nextRequestId)++));
  }

  void requireAllowsTransfer() override {
    KJ_FAIL_REQUIRE("outbound fetch channels cannot be transferred");
  }

  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    KJ_FAIL_REQUIRE("outbound fetch channels cannot be tokenized");
  }

 private:
  kj::Rc<FetchBroker> broker;
  TimerChannel& timer;
  kj::Rc<uint64_t> nextRequestId;
};

}  // namespace

kj::Own<WorkerInterface> newOutboundFetchWorker(
    kj::Rc<FetchBroker> broker, TimerChannel& timer, kj::String requestId) {
  KJ_REQUIRE(requestId.size() > 0 && requestId.size() <= MAX_OUTBOUND_FETCH_REQUEST_ID_BYTES,
      "invalid outbound fetch request ID");
  return kj::heap<OutboundFetchWorker>(kj::mv(broker), timer, kj::mv(requestId));
}

kj::Own<IoChannelFactory::SubrequestChannel> newOutboundFetchChannel(
    kj::Rc<FetchBroker> broker, TimerChannel& timer, kj::Rc<uint64_t> nextRequestId) {
  return kj::refcounted<OutboundFetchChannel>(kj::mv(broker), timer, kj::mv(nextRequestId));
}

}  // namespace workerd::server::sandbox_executor
