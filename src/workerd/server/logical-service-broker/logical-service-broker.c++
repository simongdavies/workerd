// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "logical-service-broker.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <kj/encoding.h>
#include <kj/vector.h>

#include <cmath>

namespace workerd::server::logical_service_broker {
namespace {

using JsonReader = capnp::JsonValue::Reader;

bool isIdentifier(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > MAX_IDENTIFIER_BYTES) return false;
  for (auto c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' ||
            c == '_' || c == ':' || c == '-')) {
      return false;
    }
  }
  return true;
}

bool identitiesMatch(const RequestIdentity& expected, const RequestIdentity& actual) {
  return expected.workloadId == actual.workloadId && expected.snapshotId == actual.snapshotId &&
      expected.attempt == actual.attempt;
}

size_t attributeBytes(kj::ArrayPtr<const Attribute> attributes) {
  size_t total = 0;
  for (const auto& attribute: attributes) {
    KJ_REQUIRE(attribute.name.size() <= MAX_ATTRIBUTE_BYTES - total,
        "logical service attribute block exceeds limit");
    total += attribute.name.size();
    KJ_REQUIRE(attribute.value.size() <= MAX_ATTRIBUTE_BYTES - total,
        "logical service attribute block exceeds limit");
    total += attribute.value.size();
  }
  return total;
}

void validateAttributes(kj::ArrayPtr<const Attribute> attributes) {
  KJ_REQUIRE(attributes.size() <= MAX_ATTRIBUTES, "too many logical service attributes");
  for (const auto& attribute: attributes) {
    KJ_REQUIRE(isIdentifier(attribute.name), "invalid logical service attribute name");
    KJ_REQUIRE(attribute.value.size() <= MAX_ATTRIBUTE_BYTES,
        "logical service attribute value exceeds limit");
  }
  attributeBytes(attributes);
}

void validateRequestShape(const Request& request) {
  KJ_REQUIRE(isIdentifier(request.requestId), "invalid logical service request ID");
  KJ_REQUIRE(isIdentifier(request.binding), "invalid logical service binding");
  KJ_REQUIRE(request.target.size() <= MAX_TARGET_BYTES, "logical service target exceeds limit");
  KJ_IF_SOME(value, request.value) {
    KJ_REQUIRE(value.size() <= MAX_VALUE_BYTES, "logical service value exceeds limit");
  }
  validateAttributes(request.attributes);
  KJ_IF_SOME(limit, request.limit) {
    KJ_REQUIRE(limit > 0 && limit <= MAX_PAGE_LIMIT, "invalid logical service page limit");
  }
  KJ_IF_SOME(cursor, request.cursor) {
    KJ_REQUIRE(isIdentifier(cursor), "invalid logical service cursor");
  }

  auto hasValue = request.value != kj::none;
  auto hasLimit = request.limit != kj::none;
  auto hasCursor = request.cursor != kj::none;
  auto hasExpiration = request.expirationUnixMs != kj::none;
  auto hasAttributes = request.attributes.size() > 0;

  switch (request.operation) {
    case Operation::KV_GET:
    case Operation::KV_DELETE:
      KJ_REQUIRE(request.target.size() > 0 && !hasValue && !hasAttributes && !hasLimit &&
              !hasCursor && !hasExpiration,
          "invalid logical KV request shape");
      return;
    case Operation::KV_PUT:
      KJ_REQUIRE(request.target.size() > 0 && hasValue && !hasAttributes && !hasLimit && !hasCursor,
          "invalid logical KV put request shape");
      return;
    case Operation::KV_LIST:
      KJ_REQUIRE(
          !hasValue && !hasAttributes && !hasExpiration, "invalid logical KV list request shape");
      return;
    case Operation::CACHE_MATCH:
    case Operation::CACHE_DELETE:
      KJ_REQUIRE(
          request.target.size() > 0 && !hasValue && !hasLimit && !hasCursor && !hasExpiration,
          "invalid logical cache request shape");
      return;
    case Operation::CACHE_PUT:
      KJ_REQUIRE(request.target.size() > 0 && hasValue && !hasLimit && !hasCursor,
          "invalid logical cache put request shape");
      return;
    case Operation::POLICY_CHECK:
      KJ_REQUIRE(
          request.target.size() > 0 && !hasValue && !hasLimit && !hasCursor && !hasExpiration,
          "invalid logical policy request shape");
      return;
    case Operation::IDENTITY_GET:
      KJ_REQUIRE(request.target.size() == 0 && !hasValue && !hasAttributes && !hasLimit &&
              !hasCursor && !hasExpiration,
          "invalid logical identity request shape");
      return;
  }
  KJ_UNREACHABLE;
}

void validateResponseShape(const Response& response) {
  KJ_REQUIRE(isIdentifier(response.requestId), "invalid logical service response ID");
  KJ_IF_SOME(value, response.value) {
    KJ_REQUIRE(value.size() <= MAX_VALUE_BYTES, "logical service response value exceeds limit");
  }
  validateAttributes(response.attributes);
  KJ_IF_SOME(cursor, response.cursor) {
    KJ_REQUIRE(isIdentifier(cursor), "invalid logical service response cursor");
  }
  if (response.status != ResponseStatus::OK) {
    KJ_REQUIRE(response.value == kj::none && response.attributes.size() == 0 &&
            response.cursor == kj::none,
        "failed logical service response must not expose result data");
  }
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

void appendNullableString(
    kj::Vector<char>& output, const kj::Maybe<kj::String>& value, kj::StringPtr field) {
  output.addAll(field);
  KJ_IF_SOME(actual, value) {
    appendJsonString(output, actual);
  } else {
    output.addAll("null"_kj);
  }
}

void appendNullableNumber(
    kj::Vector<char>& output, const kj::Maybe<uint64_t>& value, kj::StringPtr field) {
  output.addAll(field);
  KJ_IF_SOME(actual, value) {
    output.addAll(kj::str(actual));
  } else {
    output.addAll("null"_kj);
  }
}

void appendAttributes(kj::Vector<char>& output, kj::ArrayPtr<const Attribute> attributes) {
  output.add('[');
  bool first = true;
  for (const auto& attribute: attributes) {
    if (!first) output.add(',');
    first = false;
    output.addAll("{\"name\":"_kj);
    appendJsonString(output, attribute.name);
    output.addAll(",\"value\":"_kj);
    appendJsonString(output, attribute.value);
    output.add('}');
  }
  output.add(']');
}

kj::String finishJson(kj::Vector<char>& output) {
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

Operation parseOperation(kj::StringPtr value) {
  if (value == "kv.get"_kj) return Operation::KV_GET;
  if (value == "kv.put"_kj) return Operation::KV_PUT;
  if (value == "kv.delete"_kj) return Operation::KV_DELETE;
  if (value == "kv.list"_kj) return Operation::KV_LIST;
  if (value == "cache.match"_kj) return Operation::CACHE_MATCH;
  if (value == "cache.put"_kj) return Operation::CACHE_PUT;
  if (value == "cache.delete"_kj) return Operation::CACHE_DELETE;
  if (value == "policy.check"_kj) return Operation::POLICY_CHECK;
  if (value == "identity.get"_kj) return Operation::IDENTITY_GET;
  KJ_FAIL_REQUIRE("unknown logical service operation", value);
}

ResponseStatus parseResponseStatus(kj::StringPtr value) {
  if (value == "ok"_kj) return ResponseStatus::OK;
  if (value == "not_found"_kj) return ResponseStatus::NOT_FOUND;
  if (value == "denied"_kj) return ResponseStatus::DENIED;
  if (value == "quota_exceeded"_kj) return ResponseStatus::QUOTA_EXCEEDED;
  if (value == "invalid_request"_kj) return ResponseStatus::INVALID_REQUEST;
  if (value == "host_error"_kj) return ResponseStatus::HOST_ERROR;
  KJ_FAIL_REQUIRE("unknown logical service response status", value);
}

uint64_t parseUnsigned(JsonReader value, uint64_t maximum, kj::StringPtr description) {
  KJ_REQUIRE(value.isNumber(), "invalid numeric logical service field", description);
  auto number = value.getNumber();
  KJ_REQUIRE(number >= 0 && number <= static_cast<double>(maximum) && std::floor(number) == number,
      "invalid numeric logical service field", description);
  return static_cast<uint64_t>(number);
}

kj::Maybe<kj::String> parseNullableString(JsonReader value, kj::StringPtr description) {
  if (value.isNull()) return kj::none;
  KJ_REQUIRE(value.isString(), "invalid logical service string field", description);
  return kj::str(value.getString());
}

kj::Maybe<kj::Array<kj::byte>> parseNullableBytes(JsonReader value) {
  if (value.isNull()) return kj::none;
  KJ_REQUIRE(value.isString(), "invalid logical service value");
  auto decoded = kj::decodeBase64(kj::str(value.getString()));
  KJ_REQUIRE(!decoded.hadErrors && decoded.size() <= MAX_VALUE_BYTES,
      "invalid logical service base64 value");
  return kj::heapArray<kj::byte>(decoded.asBytes());
}

kj::Array<Attribute> parseAttributes(JsonReader value) {
  KJ_REQUIRE(value.isArray(), "logical service attributes must be an array");
  auto input = value.getArray();
  KJ_REQUIRE(input.size() <= MAX_ATTRIBUTES, "too many logical service attributes");
  auto result = kj::heapArrayBuilder<Attribute>(input.size());
  for (auto item: input) {
    KJ_REQUIRE(item.isObject(), "logical service attribute must be an object");
    kj::Maybe<kj::String> name;
    kj::Maybe<kj::String> attributeValue;
    for (auto field: item.getObject()) {
      if (field.getName() == "name"_kj) {
        KJ_REQUIRE(name == kj::none && field.getValue().isString(),
            "invalid logical service attribute name");
        name = kj::str(field.getValue().getString());
      } else if (field.getName() == "value"_kj) {
        KJ_REQUIRE(attributeValue == kj::none && field.getValue().isString(),
            "invalid logical service attribute value");
        attributeValue = kj::str(field.getValue().getString());
      } else {
        KJ_FAIL_REQUIRE("unknown logical service attribute field", field.getName());
      }
    }
    result.add(Attribute{KJ_REQUIRE_NONNULL(kj::mv(name), "missing logical service attribute name"),
      KJ_REQUIRE_NONNULL(kj::mv(attributeValue), "missing logical service attribute value")});
  }
  auto attributes = result.finish();
  validateAttributes(attributes);
  return attributes;
}

JsonReader parseRoot(kj::ArrayPtr<const char> input,
    capnp::MallocMessageBuilder& message,
    capnp::JsonCodec& codec,
    kj::StringPtr description) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "logical service envelope exceeds limit");
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  auto reader = root.asReader();
  KJ_REQUIRE(reader.isObject(), description, " must be an object");
  return reader;
}

}  // namespace

Policy::Policy(RequestIdentity identity, kj::Array<Grant> grants)
    : identity(kj::mv(identity)),
      grants(kj::mv(grants)) {
  validateRequestIdentity(this->identity);
  for (const auto& grant: this->grants) {
    KJ_REQUIRE(isIdentifier(grant.binding), "invalid policy binding");
    KJ_REQUIRE(grant.operations.size() > 0, "policy grant must contain an operation");
    KJ_REQUIRE(grant.maxValueBytes <= MAX_VALUE_BYTES, "invalid policy value quota");
    KJ_REQUIRE(grant.maxAttributeBytes <= MAX_ATTRIBUTE_BYTES, "invalid policy attribute quota");
    KJ_REQUIRE(grant.maxPageLimit > 0 && grant.maxPageLimit <= MAX_PAGE_LIMIT,
        "invalid policy page quota");
    for (auto operation: grant.operations) {
      KJ_REQUIRE(serviceFor(operation) == grant.service,
          "policy grant operation does not match its service");
    }
  }
}

void validateRequestIdentity(const RequestIdentity& identity) {
  KJ_REQUIRE(isIdentifier(identity.workloadId), "invalid logical service workload identity");
  KJ_REQUIRE(isIdentifier(identity.snapshotId), "invalid logical service snapshot identity");
}

Authorization Policy::authorize(
    const RequestIdentity& actualIdentity, const Request& request) const {
  if (!identitiesMatch(identity, actualIdentity)) return Authorization::IDENTITY_MISMATCH;
  validateRequestShape(request);

  bool sawBinding = false;
  for (const auto& grant: grants) {
    if (grant.binding != request.binding) continue;
    sawBinding = true;
    if (grant.service != serviceFor(request.operation)) continue;
    bool operationGranted = false;
    for (auto operation: grant.operations) {
      if (operation == request.operation) {
        operationGranted = true;
        break;
      }
    }
    if (!operationGranted) continue;
    KJ_IF_SOME(value, request.value) {
      if (value.size() > grant.maxValueBytes) return Authorization::REQUEST_TOO_LARGE;
    }
    if (attributeBytes(request.attributes) > grant.maxAttributeBytes) {
      return Authorization::REQUEST_TOO_LARGE;
    }
    KJ_IF_SOME(limit, request.limit) {
      if (limit > grant.maxPageLimit) return Authorization::REQUEST_TOO_LARGE;
    }
    return Authorization::ALLOW;
  }
  return sawBinding ? Authorization::OPERATION_NOT_GRANTED : Authorization::BINDING_NOT_GRANTED;
}

Service serviceFor(Operation operation) {
  switch (operation) {
    case Operation::KV_GET:
    case Operation::KV_PUT:
    case Operation::KV_DELETE:
    case Operation::KV_LIST:
      return Service::KV;
    case Operation::CACHE_MATCH:
    case Operation::CACHE_PUT:
    case Operation::CACHE_DELETE:
      return Service::CACHE;
    case Operation::POLICY_CHECK:
      return Service::POLICY;
    case Operation::IDENTITY_GET:
      return Service::IDENTITY;
  }
  KJ_UNREACHABLE;
}

kj::StringPtr operationName(Operation operation) {
  switch (operation) {
    case Operation::KV_GET:
      return "kv.get"_kj;
    case Operation::KV_PUT:
      return "kv.put"_kj;
    case Operation::KV_DELETE:
      return "kv.delete"_kj;
    case Operation::KV_LIST:
      return "kv.list"_kj;
    case Operation::CACHE_MATCH:
      return "cache.match"_kj;
    case Operation::CACHE_PUT:
      return "cache.put"_kj;
    case Operation::CACHE_DELETE:
      return "cache.delete"_kj;
    case Operation::POLICY_CHECK:
      return "policy.check"_kj;
    case Operation::IDENTITY_GET:
      return "identity.get"_kj;
  }
  KJ_UNREACHABLE;
}

kj::StringPtr responseStatusName(ResponseStatus status) {
  switch (status) {
    case ResponseStatus::OK:
      return "ok"_kj;
    case ResponseStatus::NOT_FOUND:
      return "not_found"_kj;
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

Request parseRequest(kj::ArrayPtr<const char> input) {
  capnp::MallocMessageBuilder message;
  capnp::JsonCodec codec;
  auto root = parseRoot(input, message, codec, "logical service request"_kj);

  bool sawVersion = false;
  kj::Maybe<kj::String> requestId;
  kj::Maybe<kj::String> binding;
  kj::Maybe<Operation> operation;
  kj::Maybe<kj::String> target;
  kj::Maybe<kj::Array<kj::byte>> value;
  bool sawValue = false;
  kj::Maybe<kj::Array<Attribute>> attributes;
  kj::Maybe<uint32_t> limit;
  bool sawLimit = false;
  kj::Maybe<kj::String> cursor;
  bool sawCursor = false;
  kj::Maybe<uint64_t> expiration;
  bool sawExpiration = false;

  for (auto field: root.getObject()) {
    auto name = field.getName();
    auto fieldValue = field.getValue();
    if (name == "protocol_version"_kj) {
      KJ_REQUIRE(!sawVersion &&
              parseUnsigned(fieldValue, PROTOCOL_VERSION, "protocol_version"_kj) ==
                  PROTOCOL_VERSION,
          "invalid logical service protocol version");
      sawVersion = true;
    } else if (name == "request_id"_kj) {
      KJ_REQUIRE(
          requestId == kj::none && fieldValue.isString(), "invalid logical service request ID");
      requestId = kj::str(fieldValue.getString());
    } else if (name == "binding"_kj) {
      KJ_REQUIRE(binding == kj::none && fieldValue.isString(), "invalid logical service binding");
      binding = kj::str(fieldValue.getString());
    } else if (name == "operation"_kj) {
      KJ_REQUIRE(
          operation == kj::none && fieldValue.isString(), "invalid logical service operation");
      operation = parseOperation(fieldValue.getString());
    } else if (name == "target"_kj) {
      KJ_REQUIRE(target == kj::none && fieldValue.isString(), "invalid logical service target");
      target = kj::str(fieldValue.getString());
    } else if (name == "value_base64"_kj) {
      KJ_REQUIRE(!sawValue, "duplicate logical service value");
      sawValue = true;
      value = parseNullableBytes(fieldValue);
    } else if (name == "attributes"_kj) {
      KJ_REQUIRE(attributes == kj::none, "duplicate logical service attributes");
      attributes = parseAttributes(fieldValue);
    } else if (name == "limit"_kj) {
      KJ_REQUIRE(!sawLimit, "duplicate logical service page limit");
      sawLimit = true;
      if (!fieldValue.isNull()) {
        limit = static_cast<uint32_t>(
            parseUnsigned(fieldValue, MAX_PAGE_LIMIT, "logical service page limit"_kj));
      }
    } else if (name == "cursor"_kj) {
      KJ_REQUIRE(!sawCursor, "duplicate logical service cursor");
      sawCursor = true;
      cursor = parseNullableString(fieldValue, "logical service cursor"_kj);
    } else if (name == "expiration_unix_ms"_kj) {
      KJ_REQUIRE(!sawExpiration, "duplicate logical service expiration");
      sawExpiration = true;
      if (!fieldValue.isNull()) {
        expiration =
            parseUnsigned(fieldValue, 9'007'199'254'740'991, "logical service expiration"_kj);
      }
    } else {
      KJ_FAIL_REQUIRE("unknown logical service request field", name);
    }
  }

  KJ_REQUIRE(sawVersion && sawValue && sawLimit && sawCursor && sawExpiration,
      "incomplete logical service request");
  Request request{
    KJ_REQUIRE_NONNULL(kj::mv(requestId), "missing logical service request ID"),
    KJ_REQUIRE_NONNULL(kj::mv(binding), "missing logical service binding"),
    KJ_REQUIRE_NONNULL(operation, "missing logical service operation"),
    KJ_REQUIRE_NONNULL(kj::mv(target), "missing logical service target"),
    kj::mv(value),
    KJ_REQUIRE_NONNULL(kj::mv(attributes), "missing logical service attributes"),
    limit,
    kj::mv(cursor),
    expiration,
  };
  validateRequestShape(request);
  return request;
}

Response parseResponse(kj::ArrayPtr<const char> input) {
  capnp::MallocMessageBuilder message;
  capnp::JsonCodec codec;
  auto root = parseRoot(input, message, codec, "logical service response"_kj);

  bool sawVersion = false;
  kj::Maybe<kj::String> requestId;
  kj::Maybe<ResponseStatus> status;
  kj::Maybe<kj::Array<kj::byte>> value;
  bool sawValue = false;
  kj::Maybe<kj::Array<Attribute>> attributes;
  kj::Maybe<kj::String> cursor;
  bool sawCursor = false;

  for (auto field: root.getObject()) {
    auto name = field.getName();
    auto fieldValue = field.getValue();
    if (name == "protocol_version"_kj) {
      KJ_REQUIRE(!sawVersion &&
              parseUnsigned(fieldValue, PROTOCOL_VERSION, "protocol_version"_kj) ==
                  PROTOCOL_VERSION,
          "invalid logical service protocol version");
      sawVersion = true;
    } else if (name == "request_id"_kj) {
      KJ_REQUIRE(
          requestId == kj::none && fieldValue.isString(), "invalid logical service response ID");
      requestId = kj::str(fieldValue.getString());
    } else if (name == "status"_kj) {
      KJ_REQUIRE(
          status == kj::none && fieldValue.isString(), "invalid logical service response status");
      status = parseResponseStatus(fieldValue.getString());
    } else if (name == "value_base64"_kj) {
      KJ_REQUIRE(!sawValue, "duplicate logical service response value");
      sawValue = true;
      value = parseNullableBytes(fieldValue);
    } else if (name == "attributes"_kj) {
      KJ_REQUIRE(attributes == kj::none, "duplicate logical service response attributes");
      attributes = parseAttributes(fieldValue);
    } else if (name == "cursor"_kj) {
      KJ_REQUIRE(!sawCursor, "duplicate logical service response cursor");
      sawCursor = true;
      cursor = parseNullableString(fieldValue, "logical service response cursor"_kj);
    } else {
      KJ_FAIL_REQUIRE("unknown logical service response field", name);
    }
  }

  KJ_REQUIRE(sawVersion && sawValue && sawCursor, "incomplete logical service response");
  Response response{
    KJ_REQUIRE_NONNULL(kj::mv(requestId), "missing logical service response ID"),
    KJ_REQUIRE_NONNULL(status, "missing logical service response status"),
    kj::mv(value),
    KJ_REQUIRE_NONNULL(kj::mv(attributes), "missing logical service response attributes"),
    kj::mv(cursor),
  };
  validateResponseShape(response);
  return response;
}

Request parseCanonicalRequest(kj::ArrayPtr<const char> input) {
  auto request = parseRequest(input);
  KJ_REQUIRE(serializeRequest(request) == kj::StringPtr(input.begin(), input.size()),
      "logical service request is not canonical");
  return request;
}

Response parseCanonicalResponse(kj::ArrayPtr<const char> input) {
  auto response = parseResponse(input);
  KJ_REQUIRE(serializeResponse(response) == kj::StringPtr(input.begin(), input.size()),
      "logical service response is not canonical");
  return response;
}

kj::String serializeRequest(const Request& request) {
  validateRequestShape(request);
  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
  appendJsonString(output, request.requestId);
  output.addAll(",\"binding\":"_kj);
  appendJsonString(output, request.binding);
  output.addAll(",\"operation\":"_kj);
  appendJsonString(output, operationName(request.operation));
  output.addAll(",\"target\":"_kj);
  appendJsonString(output, request.target);
  output.addAll(",\"value_base64\":"_kj);
  KJ_IF_SOME(value, request.value) {
    auto encoded = kj::encodeBase64(value);
    appendJsonString(output, encoded);
  } else {
    output.addAll("null"_kj);
  }
  output.addAll(",\"attributes\":"_kj);
  appendAttributes(output, request.attributes);
  output.addAll(",\"limit\":"_kj);
  KJ_IF_SOME(limit, request.limit) {
    output.addAll(kj::str(limit));
  } else {
    output.addAll("null"_kj);
  }
  appendNullableString(output, request.cursor, ",\"cursor\":"_kj);
  appendNullableNumber(output, request.expirationUnixMs, ",\"expiration_unix_ms\":"_kj);
  output.add('}');
  return finishJson(output);
}

kj::String serializeResponse(const Response& response) {
  validateResponseShape(response);
  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
  appendJsonString(output, response.requestId);
  output.addAll(",\"status\":"_kj);
  appendJsonString(output, responseStatusName(response.status));
  output.addAll(",\"value_base64\":"_kj);
  KJ_IF_SOME(value, response.value) {
    auto encoded = kj::encodeBase64(value);
    appendJsonString(output, encoded);
  } else {
    output.addAll("null"_kj);
  }
  output.addAll(",\"attributes\":"_kj);
  appendAttributes(output, response.attributes);
  appendNullableString(output, response.cursor, ",\"cursor\":"_kj);
  output.add('}');
  return finishJson(output);
}

}  // namespace workerd::server::logical_service_broker
