// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/array.h>
#include <kj/common.h>
#include <kj/string.h>

#include <cstdint>

namespace workerd::server::logical_service_broker::composite {

inline constexpr uint32_t PROTOCOL_VERSION = 2;
inline constexpr size_t MAX_BINDING_NAME_BYTES = 64;
inline constexpr size_t MAX_REQUEST_ID_BYTES = 128;

enum class BindingKind {
  KV,
  CACHE,
  D1,
  DURABLE_OBJECT,
};

enum class OperationKind {
  KV_GET,
  KV_PUT,
  KV_DELETE,
  KV_LIST,
  CACHE_MATCH,
  CACHE_PUT,
  CACHE_DELETE,
  D1_BATCH,
  DO_DELIVER,
  DO_ALARM,
  DO_STORAGE_GET,
  DO_STORAGE_PUT,
  DO_STORAGE_DELETE,
  DO_STORAGE_LIST,
  DO_SET_ALARM,
  DO_DELETE_ALARM,
  DO_PASSIVATE,
};

enum class ResponseStatus {
  OK,
  DENIED,
  QUOTA_EXCEEDED,
  INVALID_REQUEST,
  HOST_ERROR,
};

struct RequestEnvelope {
  kj::String requestId;
  kj::String binding;
  OperationKind operation;
};

struct ResponseEnvelope {
  kj::String requestId;
  ResponseStatus status;
  kj::String code;
};

bool isBindingName(kj::StringPtr value);
BindingKind parseBindingKind(kj::StringPtr value);
kj::StringPtr bindingKindName(BindingKind kind);
OperationKind parseOperationKind(kj::StringPtr value);
kj::StringPtr operationKindName(OperationKind operation);
BindingKind bindingKindFor(OperationKind operation);
ResponseStatus parseResponseStatus(kj::StringPtr value);
kj::StringPtr responseStatusName(ResponseStatus status);

RequestEnvelope parseRequestEnvelope(kj::ArrayPtr<const char> input);
ResponseEnvelope parseResponseEnvelope(kj::ArrayPtr<const char> input);
kj::String serializeRejection(kj::StringPtr requestId, ResponseStatus status, kj::StringPtr code);

}  // namespace workerd::server::logical_service_broker::composite
