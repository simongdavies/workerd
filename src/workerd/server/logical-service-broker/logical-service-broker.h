// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/array.h>
#include <kj/common.h>
#include <kj/string.h>

#include <cstdint>

namespace workerd::server::logical_service_broker {

inline constexpr uint32_t PROTOCOL_VERSION = 1;
inline constexpr kj::StringPtr HOST_CALL_NAME = "WorkerdLogicalServiceV1Invoke"_kj;

inline constexpr size_t MAX_ENVELOPE_BYTES = 2 * 1024 * 1024;
inline constexpr size_t MAX_IDENTIFIER_BYTES = 128;
inline constexpr size_t MAX_TARGET_BYTES = 8 * 1024;
inline constexpr size_t MAX_VALUE_BYTES = 1024 * 1024;
inline constexpr size_t MAX_ATTRIBUTES = 64;
inline constexpr size_t MAX_ATTRIBUTE_BYTES = 16 * 1024;
inline constexpr uint32_t MAX_PAGE_LIMIT = 1000;

enum class Service {
  KV,
  CACHE,
  POLICY,
  IDENTITY,
};

enum class Operation {
  KV_GET,
  KV_PUT,
  KV_DELETE,
  KV_LIST,
  CACHE_MATCH,
  CACHE_PUT,
  CACHE_DELETE,
  POLICY_CHECK,
  IDENTITY_GET,
};

enum class ResponseStatus {
  OK,
  NOT_FOUND,
  DENIED,
  QUOTA_EXCEEDED,
  INVALID_REQUEST,
  HOST_ERROR,
};

enum class Authorization {
  ALLOW,
  IDENTITY_MISMATCH,
  BINDING_NOT_GRANTED,
  OPERATION_NOT_GRANTED,
  REQUEST_TOO_LARGE,
};

struct RequestIdentity {
  kj::String workloadId;
  kj::String snapshotId;
  uint32_t attempt;
};

struct Attribute {
  kj::String name;
  kj::String value;
};

struct Request {
  kj::String requestId;
  kj::String binding;
  Operation operation;
  kj::String target;
  kj::Maybe<kj::Array<kj::byte>> value;
  kj::Array<Attribute> attributes;
  kj::Maybe<uint32_t> limit;
  kj::Maybe<kj::String> cursor;
  kj::Maybe<uint64_t> expirationUnixMs;
};

struct Response {
  kj::String requestId;
  ResponseStatus status;
  kj::Maybe<kj::Array<kj::byte>> value;
  kj::Array<Attribute> attributes;
  kj::Maybe<kj::String> cursor;
};

struct Grant {
  kj::String binding;
  Service service;
  kj::Array<Operation> operations;
  size_t maxValueBytes;
  size_t maxAttributeBytes;
  uint32_t maxPageLimit;
};

class Policy {
 public:
  Policy(RequestIdentity identity, kj::Array<Grant> grants);

  Authorization authorize(const RequestIdentity& identity, const Request& request) const;

 private:
  RequestIdentity identity;
  kj::Array<Grant> grants;
};

Service serviceFor(Operation operation);
kj::StringPtr operationName(Operation operation);
kj::StringPtr responseStatusName(ResponseStatus status);
void validateRequestIdentity(const RequestIdentity& identity);

Request parseRequest(kj::ArrayPtr<const char> input);
Response parseResponse(kj::ArrayPtr<const char> input);
Request parseCanonicalRequest(kj::ArrayPtr<const char> input);
Response parseCanonicalResponse(kj::ArrayPtr<const char> input);
kj::String serializeRequest(const Request& request);
kj::String serializeResponse(const Response& response);

}  // namespace workerd::server::logical_service_broker
