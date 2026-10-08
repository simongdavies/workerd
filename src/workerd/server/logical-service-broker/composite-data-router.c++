// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "composite-data-router.h"

#include "logical-service-broker.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <kj/encoding.h>
#include <kj/vector.h>

#include <cmath>

namespace workerd::server::logical_service_broker::composite {
namespace {

using JsonReader = capnp::JsonValue::Reader;

bool isPrintableCorrelation(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > MAX_REQUEST_ID_BYTES) return false;
  for (auto c: value.asBytes()) {
    if (c < 0x20 || c > 0x7e) return false;
  }
  return true;
}

bool isCode(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > MAX_IDENTIFIER_BYTES) return false;
  for (auto c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) return false;
  }
  return true;
}

uint32_t parseVersion(JsonReader value) {
  KJ_REQUIRE(value.isNumber(), "invalid composite router version");
  auto number = value.getNumber();
  KJ_REQUIRE(number == PROTOCOL_VERSION && std::floor(number) == number,
      "unsupported composite router version");
  return static_cast<uint32_t>(number);
}

void appendJsonString(kj::Vector<char>& output, kj::StringPtr value) {
  static constexpr char HEX[] = "0123456789abcdef";
  output.add('"');
  for (auto c: value) {
    switch (c) {
      case '"':
        output.addAll("\\\""_kj);
        break;
      case '\\':
        output.addAll("\\\\"_kj);
        break;
      case '\b':
        output.addAll("\\b"_kj);
        break;
      case '\f':
        output.addAll("\\f"_kj);
        break;
      case '\n':
        output.addAll("\\n"_kj);
        break;
      case '\r':
        output.addAll("\\r"_kj);
        break;
      case '\t':
        output.addAll("\\t"_kj);
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          output.addAll("\\u00"_kj);
          output.add(HEX[(static_cast<unsigned char>(c) >> 4) & 0x0f]);
          output.add(HEX[static_cast<unsigned char>(c) & 0x0f]);
        } else {
          output.add(c);
        }
        break;
    }
  }
  output.add('"');
}

}  // namespace

bool isBindingName(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > MAX_BINDING_NAME_BYTES || value[0] < 'a' ||
      value[0] > 'z') {
    return false;
  }
  for (auto c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_')) {
      return false;
    }
  }
  return true;
}

BindingKind parseBindingKind(kj::StringPtr value) {
  if (value == "kv"_kj) return BindingKind::KV;
  if (value == "cache"_kj) return BindingKind::CACHE;
  if (value == "d1"_kj) return BindingKind::D1;
  if (value == "durable_object"_kj) return BindingKind::DURABLE_OBJECT;
  if (value == "webhook"_kj) return BindingKind::WEBHOOK;
  if (value == "provider_websocket"_kj) return BindingKind::PROVIDER_WEBSOCKET;
  KJ_FAIL_REQUIRE("unknown composite binding kind", value);
}

kj::StringPtr bindingKindName(BindingKind kind) {
  switch (kind) {
    case BindingKind::KV:
      return "kv"_kj;
    case BindingKind::CACHE:
      return "cache"_kj;
    case BindingKind::D1:
      return "d1"_kj;
    case BindingKind::DURABLE_OBJECT:
      return "durable_object"_kj;
    case BindingKind::WEBHOOK:
      return "webhook"_kj;
    case BindingKind::PROVIDER_WEBSOCKET:
      return "provider_websocket"_kj;
  }
  KJ_UNREACHABLE;
}

OperationKind parseOperationKind(kj::StringPtr value) {
  if (value == "kv_get"_kj) return OperationKind::KV_GET;
  if (value == "kv_put"_kj) return OperationKind::KV_PUT;
  if (value == "kv_delete"_kj) return OperationKind::KV_DELETE;
  if (value == "kv_list"_kj) return OperationKind::KV_LIST;
  if (value == "cache_match"_kj) return OperationKind::CACHE_MATCH;
  if (value == "cache_put"_kj) return OperationKind::CACHE_PUT;
  if (value == "cache_delete"_kj) return OperationKind::CACHE_DELETE;
  if (value == "d1_batch"_kj) return OperationKind::D1_BATCH;
  if (value == "do_deliver"_kj) return OperationKind::DO_DELIVER;
  if (value == "do_alarm"_kj) return OperationKind::DO_ALARM;
  if (value == "do_storage_get"_kj) return OperationKind::DO_STORAGE_GET;
  if (value == "do_storage_put"_kj) return OperationKind::DO_STORAGE_PUT;
  if (value == "do_storage_delete"_kj) return OperationKind::DO_STORAGE_DELETE;
  if (value == "do_storage_list"_kj) return OperationKind::DO_STORAGE_LIST;
  if (value == "do_set_alarm"_kj) return OperationKind::DO_SET_ALARM;
  if (value == "do_delete_alarm"_kj) return OperationKind::DO_DELETE_ALARM;
  if (value == "do_passivate"_kj) return OperationKind::DO_PASSIVATE;
  if (value == "webhook_verify"_kj) return OperationKind::WEBHOOK_VERIFY;
  KJ_FAIL_REQUIRE("unknown composite operation kind", value);
}

kj::StringPtr operationKindName(OperationKind operation) {
  switch (operation) {
    case OperationKind::KV_GET:
      return "kv_get"_kj;
    case OperationKind::KV_PUT:
      return "kv_put"_kj;
    case OperationKind::KV_DELETE:
      return "kv_delete"_kj;
    case OperationKind::KV_LIST:
      return "kv_list"_kj;
    case OperationKind::CACHE_MATCH:
      return "cache_match"_kj;
    case OperationKind::CACHE_PUT:
      return "cache_put"_kj;
    case OperationKind::CACHE_DELETE:
      return "cache_delete"_kj;
    case OperationKind::D1_BATCH:
      return "d1_batch"_kj;
    case OperationKind::DO_DELIVER:
      return "do_deliver"_kj;
    case OperationKind::DO_ALARM:
      return "do_alarm"_kj;
    case OperationKind::DO_STORAGE_GET:
      return "do_storage_get"_kj;
    case OperationKind::DO_STORAGE_PUT:
      return "do_storage_put"_kj;
    case OperationKind::DO_STORAGE_DELETE:
      return "do_storage_delete"_kj;
    case OperationKind::DO_STORAGE_LIST:
      return "do_storage_list"_kj;
    case OperationKind::DO_SET_ALARM:
      return "do_set_alarm"_kj;
    case OperationKind::DO_DELETE_ALARM:
      return "do_delete_alarm"_kj;
    case OperationKind::DO_PASSIVATE:
      return "do_passivate"_kj;
    case OperationKind::WEBHOOK_VERIFY:
      return "webhook_verify"_kj;
  }
  KJ_UNREACHABLE;
}

BindingKind bindingKindFor(OperationKind operation) {
  switch (operation) {
    case OperationKind::KV_GET:
    case OperationKind::KV_PUT:
    case OperationKind::KV_DELETE:
    case OperationKind::KV_LIST:
      return BindingKind::KV;
    case OperationKind::CACHE_MATCH:
    case OperationKind::CACHE_PUT:
    case OperationKind::CACHE_DELETE:
      return BindingKind::CACHE;
    case OperationKind::D1_BATCH:
      return BindingKind::D1;
    case OperationKind::DO_DELIVER:
    case OperationKind::DO_ALARM:
    case OperationKind::DO_STORAGE_GET:
    case OperationKind::DO_STORAGE_PUT:
    case OperationKind::DO_STORAGE_DELETE:
    case OperationKind::DO_STORAGE_LIST:
    case OperationKind::DO_SET_ALARM:
    case OperationKind::DO_DELETE_ALARM:
    case OperationKind::DO_PASSIVATE:
      return BindingKind::DURABLE_OBJECT;
    case OperationKind::WEBHOOK_VERIFY:
      return BindingKind::WEBHOOK;
  }
  KJ_UNREACHABLE;
}

ResponseStatus parseResponseStatus(kj::StringPtr value) {
  if (value == "ok"_kj) return ResponseStatus::OK;
  if (value == "denied"_kj) return ResponseStatus::DENIED;
  if (value == "quota_exceeded"_kj) return ResponseStatus::QUOTA_EXCEEDED;
  if (value == "invalid_request"_kj) return ResponseStatus::INVALID_REQUEST;
  if (value == "host_error"_kj) return ResponseStatus::HOST_ERROR;
  KJ_FAIL_REQUIRE("unknown composite response status", value);
}

kj::StringPtr responseStatusName(ResponseStatus status) {
  switch (status) {
    case ResponseStatus::OK:
      return "ok"_kj;
    case ResponseStatus::DENIED:
      return "denied"_kj;
    case ResponseStatus::QUOTA_EXCEEDED:
      return "quota_exceeded"_kj;
    case ResponseStatus::INVALID_REQUEST:
      return "invalid_request"_kj;
    case ResponseStatus::HOST_ERROR:
      return "host_error"_kj;
  }
  KJ_UNREACHABLE;
}

RequestEnvelope parseRequestEnvelope(kj::ArrayPtr<const char> input) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "composite router envelope exceeds limit");
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "composite router request must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(fields.size() == 4, "composite router request must contain exactly four fields");
  KJ_REQUIRE(fields[0].getName() == "version"_kj && fields[1].getName() == "request_id"_kj &&
          fields[2].getName() == "binding"_kj && fields[3].getName() == "operation"_kj,
      "noncanonical composite router request field order");
  parseVersion(fields[0].getValue());
  KJ_REQUIRE(fields[1].getValue().isString(), "invalid composite router request ID");
  auto requestId = kj::str(fields[1].getValue().getString());
  KJ_REQUIRE(isPrintableCorrelation(requestId), "invalid composite router request ID");
  KJ_REQUIRE(fields[2].getValue().isString(), "invalid composite router binding");
  auto binding = kj::str(fields[2].getValue().getString());
  KJ_REQUIRE(isBindingName(binding), "invalid composite router binding");
  KJ_REQUIRE(fields[3].getValue().isObject(), "invalid composite router operation");
  auto operationFields = fields[3].getValue().getObject();
  KJ_REQUIRE(operationFields.size() > 0 && operationFields[0].getName() == "kind"_kj &&
          operationFields[0].getValue().isString(),
      "composite router operation kind must be the first field");
  auto operation = parseOperationKind(operationFields[0].getValue().getString());
  if (operation == OperationKind::WEBHOOK_VERIFY) {
    KJ_REQUIRE(operationFields.size() == 3 && operationFields[1].getName() == "body_base64"_kj &&
            operationFields[1].getValue().isString() &&
            operationFields[2].getName() == "signature"_kj &&
            operationFields[2].getValue().isString(),
        "invalid webhook verification operation");
    auto encoded = operationFields[1].getValue().getString();
    auto signature = operationFields[2].getValue().getString();
    KJ_REQUIRE(encoded.size() <= ((MAX_WEBHOOK_BODY_BYTES + 2) / 3) * 4 && signature.size() > 0 &&
            signature.size() <= MAX_WEBHOOK_SIGNATURE_BYTES,
        "webhook verification input exceeds limit");
    auto body = kj::decodeBase64(encoded);
    KJ_REQUIRE(!body.hadErrors && body.size() <= MAX_WEBHOOK_BODY_BYTES &&
            kj::encodeBase64(body) == encoded,
        "invalid webhook verification body");
  }
  return RequestEnvelope{
    .requestId = kj::mv(requestId),
    .binding = kj::mv(binding),
    .operation = operation,
  };
}

ResponseEnvelope parseResponseEnvelope(kj::ArrayPtr<const char> input) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "composite router envelope exceeds limit");
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "composite router response must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(fields.size() >= 4, "composite router response must contain common fields");
  KJ_REQUIRE(fields[0].getName() == "version"_kj && fields[1].getName() == "request_id"_kj &&
          fields[2].getName() == "status"_kj && fields[3].getName() == "code"_kj,
      "noncanonical composite router response field order");
  parseVersion(fields[0].getValue());
  KJ_REQUIRE(fields[1].getValue().isString(), "invalid composite router response ID");
  auto requestId = kj::str(fields[1].getValue().getString());
  KJ_REQUIRE(isPrintableCorrelation(requestId), "invalid composite router response ID");
  KJ_REQUIRE(fields[2].getValue().isString(), "invalid composite router response status");
  auto status = parseResponseStatus(fields[2].getValue().getString());
  KJ_REQUIRE(fields[3].getValue().isString(), "invalid composite router response code");
  auto code = kj::str(fields[3].getValue().getString());
  KJ_REQUIRE(isCode(code), "invalid composite router response code");
  return ResponseEnvelope{
    .requestId = kj::mv(requestId),
    .status = status,
    .code = kj::mv(code),
  };
}

kj::String serializeRejection(kj::StringPtr requestId, ResponseStatus status, kj::StringPtr code) {
  KJ_REQUIRE(status != ResponseStatus::OK, "composite router rejection cannot have ok status");
  KJ_REQUIRE(requestId.size() == 0 || isPrintableCorrelation(requestId),
      "invalid composite router rejection ID");
  KJ_REQUIRE(isCode(code), "invalid composite router rejection code");
  kj::Vector<char> output;
  output.addAll("{\"version\":2,\"request_id\":"_kj);
  appendJsonString(output, requestId);
  output.addAll(",\"status\":"_kj);
  appendJsonString(output, responseStatusName(status));
  output.addAll(",\"code\":"_kj);
  appendJsonString(output, code);
  output.addAll("}\0"_kj);
  return kj::String(output.releaseAsArray());
}

}  // namespace workerd::server::logical_service_broker::composite
