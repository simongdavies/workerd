// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/io-channels.h>
#include <workerd/io/worker-interface.h>

#include <kj/async.h>
#include <kj/compat/http.h>

namespace workerd::server::sandbox_executor {

inline constexpr size_t MAX_OUTBOUND_FETCH_REQUEST_BODY_BYTES = 1024 * 1024;
inline constexpr size_t MAX_OUTBOUND_FETCH_RESPONSE_BODY_BYTES = 4 * 1024 * 1024;
inline constexpr size_t MAX_OUTBOUND_FETCH_HEADERS = 128;
inline constexpr size_t MAX_OUTBOUND_FETCH_HEADER_BYTES = 64 * 1024;
inline constexpr size_t MAX_OUTBOUND_FETCH_URL_BYTES = 16 * 1024;
inline constexpr size_t MAX_OUTBOUND_FETCH_REQUEST_ID_BYTES = 256;
inline constexpr uint32_t OUTBOUND_FETCH_DEADLINE_MS = 10'000;
inline constexpr size_t MAX_CONCURRENT_OUTBOUND_FETCHES = 16;

struct Header {
  kj::String name;
  kj::String value;
};

enum class FetchError {
  DENIED,
  TIMEOUT,
  CANCELED,
  MALFORMED_RESPONSE,
  HOST_FAILURE,
  OVERLOADED,
  SIZE_LIMIT,
};

struct FetchFailure {
  FetchError error;
  kj::String message;
};

struct FetchRequest {
  kj::String requestId;
  kj::HttpMethod method;
  kj::String url;
  kj::Array<Header> headers;
  kj::Maybe<uint64_t> bodyLength;
};

struct FetchResponse {
  uint statusCode;
  kj::Array<Header> headers;
  kj::String body;
};

class FetchBroker: public kj::Refcounted {
 public:
  virtual bool isQuiescent() const {
    return false;
  }

  virtual kj::Promise<void> request(FetchRequest request,
      const kj::HttpHeaders& requestHeaders,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& response,
      TimerChannel& timer) = 0;
};

kj::Own<WorkerInterface> newOutboundFetchWorker(
    kj::Rc<FetchBroker> broker, TimerChannel& timer, kj::String requestId);

kj::Own<IoChannelFactory::SubrequestChannel> newOutboundFetchChannel(
    kj::Rc<FetchBroker> broker, TimerChannel& timer, kj::Rc<uint64_t> nextRequestId);

}  // namespace workerd::server::sandbox_executor
