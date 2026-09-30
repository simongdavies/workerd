// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "hyperlight-driver.h"
#include "sandbox-runtime.h"
#include "src/workerd/server/helloworld_worker.embed.h"
#include "src/workerd/server/streams_util.embed.h"
#include "src/workerd/server/web_streams_worker.embed.h"
#include "src/workerd/server/wintertc_smoke.embed.h"

#include <workerd/io/compatibility-date.h>

#include <sys/wait.h>
#include <unistd.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/encoding.h>

namespace workerd::server::sandbox_executor {
namespace {

constexpr size_t MAX_ENVELOPE_BYTES = 60 * 1024;
constexpr size_t MAX_BODY_BYTES = 32 * 1024;
constexpr size_t MAX_HEADERS = 64;
constexpr size_t MAX_HEADER_BYTES = 8 * 1024;
constexpr size_t MAX_URL_BYTES = 8 * 1024;
constexpr size_t MAX_METHOD_BYTES = 32;
constexpr size_t MAX_REQUEST_ID_BYTES = 64;
constexpr size_t MAX_WORKER_VERSION_BYTES = 256;
constexpr size_t MAX_COMPATIBILITY_FLAGS = 32;
constexpr size_t MAX_COMPATIBILITY_FLAG_BYTES = 64;
constexpr size_t MAX_COMPATIBILITY_FLAGS_BYTES = 2 * 1024;
constexpr size_t MAX_MODULES = 32;
constexpr size_t MAX_MODULE_NAME_BYTES = 256;
constexpr size_t MAX_MODULE_SOURCE_BYTES = 32 * 1024;
constexpr size_t MAX_MODULE_SOURCES_BYTES = 48 * 1024;
constexpr size_t MAX_HOST_CALL_CHUNK_BYTES = 60 * 1024;
constexpr uint64_t MAX_HOST_OPERATION_ID = 9'007'199'254'740'991;
constexpr kj::Duration FETCH_POLL_INTERVAL = 1 * kj::MILLISECONDS;

struct Request {
  kj::String requestId;
  kj::HttpMethod method;
  kj::String url;
  kj::Array<Header> headers;
  kj::String body;
};

bool validIdentifier(kj::StringPtr value, size_t limit) {
  if (value.size() == 0 || value.size() > limit) return false;
  for (char c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) && c != '-' &&
        c != '_' && c != '.' && c != ':') {
      return false;
    }
  }
  return true;
}

bool validCompatibilityFlag(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > MAX_COMPATIBILITY_FLAG_BYTES) return false;
  for (char c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) return false;
  }
  return true;
}

bool lexicographicallyBefore(kj::StringPtr left, kj::StringPtr right) {
  auto commonSize = kj::min(left.size(), right.size());
  auto comparison = memcmp(left.begin(), right.begin(), commonSize);
  return comparison < 0 || (comparison == 0 && left.size() < right.size());
}

bool validModuleName(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > MAX_MODULE_NAME_BYTES || value[0] == '/' ||
      value[value.size() - 1] == '/') {
    return false;
  }

  size_t segmentStart = 0;
  for (size_t i = 0; i <= value.size(); ++i) {
    if (i < value.size()) {
      auto c = value[i];
      if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              c == '.' || c == '_' || c == '-' || c == '/')) {
        return false;
      }
      if (c != '/') continue;
    }

    auto segment = value.slice(segmentStart, i);
    if (segment.size() == 0 || segment == "."_kj || segment == ".."_kj) return false;
    segmentStart = i + 1;
  }
  return true;
}

ModuleType parseModuleType(kj::StringPtr value) {
  if (value == "esModule"_kj) return ModuleType::ES_MODULE;
  if (value == "text"_kj) return ModuleType::TEXT;
  if (value == "json"_kj) return ModuleType::JSON;
  KJ_FAIL_REQUIRE("unsupported module type");
}

kj::StringPtr moduleTypeName(ModuleType type) {
  switch (type) {
    case ModuleType::ES_MODULE:
      return "esModule"_kj;
    case ModuleType::TEXT:
      return "text"_kj;
    case ModuleType::JSON:
      return "json"_kj;
  }
  KJ_UNREACHABLE;
}

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

kj::HttpMethod parseMethod(kj::StringPtr method) {
  KJ_REQUIRE(method.size() > 0 && method.size() <= MAX_METHOD_BYTES, "invalid method");
  for (char c: method) {
    KJ_REQUIRE((c >= 'A' && c <= 'Z') || c == '-', "invalid method");
  }
#define METHOD(name)                                                                               \
  if (method == #name##_kj) return kj::HttpMethod::name;
  METHOD(GET)
  METHOD(HEAD)
  METHOD(POST)
  METHOD(PUT)
  METHOD(DELETE)
  METHOD(PATCH)
  METHOD(OPTIONS)
  METHOD(TRACE)
  METHOD(PURGE)
#undef METHOD
  KJ_FAIL_REQUIRE("unsupported method");
}

kj::String copyBytes(kj::ArrayPtr<const byte> bytes) {
  auto result = kj::heapArray<char>(bytes.size() + 1);
  memcpy(result.begin(), bytes.begin(), bytes.size());
  result[bytes.size()] = '\0';
  return kj::String(kj::mv(result));
}

Request parseRequest(kj::ArrayPtr<const char> input) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "request envelope exceeds limit");
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "request must be an object");

  kj::Maybe<kj::String> requestId;
  kj::Maybe<kj::String> method;
  kj::Maybe<kj::String> url;
  kj::Maybe<kj::String> bodyBase64;
  kj::Maybe<kj::Array<Header>> headers;
  bool sawProtocolVersion = false;

  for (auto field: root.getObject()) {
    auto name = field.getName();
    auto value = field.getValue();
    if (name == "protocol_version") {
      KJ_REQUIRE(!sawProtocolVersion && value.isNumber() && value.getNumber() == 1,
          "invalid protocol version");
      sawProtocolVersion = true;
    } else if (name == "request_id") {
      KJ_REQUIRE(requestId == kj::none && value.isString(), "invalid request ID");
      requestId = kj::str(value.getString());
    } else if (name == "method") {
      KJ_REQUIRE(method == kj::none && value.isString(), "invalid method");
      method = kj::str(value.getString());
    } else if (name == "url") {
      KJ_REQUIRE(url == kj::none && value.isString(), "invalid URL");
      url = kj::str(value.getString());
    } else if (name == "body_base64") {
      KJ_REQUIRE(bodyBase64 == kj::none && value.isString(), "invalid body");
      bodyBase64 = kj::str(value.getString());
    } else if (name == "headers") {
      KJ_REQUIRE(headers == kj::none && value.isArray(), "invalid headers");
      auto values = value.getArray();
      KJ_REQUIRE(values.size() <= MAX_HEADERS, "too many headers");
      kj::Vector<Header> parsed(values.size());
      size_t aggregateSize = 0;
      for (auto headerValue: values) {
        KJ_REQUIRE(headerValue.isObject(), "invalid header");
        kj::Maybe<kj::String> headerName;
        kj::Maybe<kj::String> headerContent;
        for (auto headerField: headerValue.getObject()) {
          if (headerField.getName() == "name") {
            KJ_REQUIRE(
                headerName == kj::none && headerField.getValue().isString(), "invalid header");
            headerName = kj::str(headerField.getValue().getString());
          } else if (headerField.getName() == "value") {
            KJ_REQUIRE(
                headerContent == kj::none && headerField.getValue().isString(), "invalid header");
            headerContent = kj::str(headerField.getValue().getString());
          } else {
            KJ_FAIL_REQUIRE("unknown header field");
          }
        }
        auto ownedName = KJ_REQUIRE_NONNULL(kj::mv(headerName), "missing header name");
        auto ownedValue = KJ_REQUIRE_NONNULL(kj::mv(headerContent), "missing header value");
        KJ_REQUIRE(validHeaderName(ownedName) && validHeaderValue(ownedValue), "invalid header");
        aggregateSize += ownedName.size() + ownedValue.size();
        KJ_REQUIRE(aggregateSize <= MAX_HEADER_BYTES, "headers exceed size limit");
        parsed.add(Header{kj::mv(ownedName), kj::mv(ownedValue)});
      }
      headers = parsed.releaseAsArray();
    } else {
      KJ_FAIL_REQUIRE("unknown request field");
    }
  }

  KJ_REQUIRE(sawProtocolVersion, "missing protocol version");
  auto ownedRequestId = KJ_REQUIRE_NONNULL(kj::mv(requestId), "missing request ID");
  KJ_REQUIRE(validIdentifier(ownedRequestId, MAX_REQUEST_ID_BYTES), "invalid request ID");
  auto ownedUrl = KJ_REQUIRE_NONNULL(kj::mv(url), "missing URL");
  KJ_REQUIRE(ownedUrl.size() <= MAX_URL_BYTES &&
          (ownedUrl.startsWith("http://") || ownedUrl.startsWith("https://")),
      "invalid URL");
  for (char c: ownedUrl) {
    KJ_REQUIRE(static_cast<unsigned char>(c) > 0x20 && c != 0x7f, "invalid URL");
  }
  auto encodedBody = KJ_REQUIRE_NONNULL(kj::mv(bodyBase64), "missing body");
  KJ_REQUIRE(encodedBody.size() <= ((MAX_BODY_BYTES + 2) / 3) * 4, "body exceeds size limit");
  auto decodedBody = kj::decodeBase64(encodedBody);
  KJ_REQUIRE(!decodedBody.hadErrors && decodedBody.size() <= MAX_BODY_BYTES, "invalid body");

  return {
    kj::mv(ownedRequestId),
    parseMethod(KJ_REQUIRE_NONNULL(method, "missing method")),
    kj::mv(ownedUrl),
    KJ_REQUIRE_NONNULL(kj::mv(headers), "missing headers"),
    copyBytes(decodedBody),
  };
}

WorkerBundle parseWorkerBundle(kj::ArrayPtr<const char> input) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "init envelope exceeds limit");
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "init envelope must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(fields.size() == 6, "init envelope must contain exactly six fields");
  static constexpr kj::StringPtr FIELD_NAMES[] = {
    "protocol_version"_kj,
    "worker_version"_kj,
    "compatibility_date"_kj,
    "compatibility_flags"_kj,
    "main_module"_kj,
    "modules"_kj,
  };
  for (auto i: kj::indices(fields)) {
    KJ_REQUIRE(fields[i].getName() == FIELD_NAMES[i], "noncanonical init field order");
  }

  KJ_REQUIRE(fields[0].getValue().isNumber() && fields[0].getValue().getNumber() == 1,
      "invalid init protocol version");
  KJ_REQUIRE(fields[1].getValue().isString(), "invalid Worker version");
  auto workerVersion = kj::str(fields[1].getValue().getString());
  KJ_REQUIRE(validIdentifier(workerVersion, MAX_WORKER_VERSION_BYTES), "invalid Worker version");

  KJ_REQUIRE(fields[2].getValue().isString(), "invalid compatibility date");
  auto compatibilityDate = kj::str(fields[2].getValue().getString());
  KJ_REQUIRE(compatibilityDate.size() == 10, "invalid compatibility date");
  auto normalizedDate =
      KJ_REQUIRE_NONNULL(normalizeCompatDate(compatibilityDate), "invalid compatibility date");
  KJ_REQUIRE(normalizedDate == compatibilityDate, "noncanonical compatibility date");

  KJ_REQUIRE(fields[3].getValue().isArray(), "invalid compatibility flags");
  auto flagValues = fields[3].getValue().getArray();
  KJ_REQUIRE(flagValues.size() <= MAX_COMPATIBILITY_FLAGS, "too many compatibility flags");
  auto flagBuilder = kj::heapArrayBuilder<kj::String>(flagValues.size());
  size_t aggregateFlagBytes = 0;
  kj::Maybe<kj::StringPtr> previousFlag;
  for (auto value: flagValues) {
    KJ_REQUIRE(value.isString(), "invalid compatibility flag");
    auto flag = kj::str(value.getString());
    KJ_REQUIRE(validCompatibilityFlag(flag), "invalid compatibility flag");
    aggregateFlagBytes += flag.size();
    KJ_REQUIRE(aggregateFlagBytes <= MAX_COMPATIBILITY_FLAGS_BYTES,
        "compatibility flags exceed size limit");
    KJ_IF_SOME(previous, previousFlag) {
      KJ_REQUIRE(
          lexicographicallyBefore(previous, flag), "compatibility flags must be sorted and unique");
    }
    previousFlag = flag;
    flagBuilder.add(kj::mv(flag));
  }

  KJ_REQUIRE(fields[4].getValue().isString(), "invalid main module");
  auto mainModule = kj::str(fields[4].getValue().getString());
  KJ_REQUIRE(validModuleName(mainModule), "invalid main module");

  KJ_REQUIRE(fields[5].getValue().isArray(), "invalid modules");
  auto moduleValues = fields[5].getValue().getArray();
  KJ_REQUIRE(moduleValues.size() > 0 && moduleValues.size() <= MAX_MODULES, "invalid module count");
  auto moduleBuilder = kj::heapArrayBuilder<Module>(moduleValues.size());
  size_t aggregateSourceBytes = 0;
  kj::Maybe<kj::StringPtr> previousModule;
  for (auto i: kj::indices(moduleValues)) {
    auto value = moduleValues[i];
    KJ_REQUIRE(value.isObject(), "invalid module");
    auto moduleFields = value.getObject();
    KJ_REQUIRE(moduleFields.size() == 3, "module must contain exactly three fields");
    KJ_REQUIRE(moduleFields[0].getName() == "name"_kj && moduleFields[1].getName() == "type"_kj &&
            moduleFields[2].getName() == "source"_kj,
        "noncanonical module field order");
    KJ_REQUIRE(moduleFields[0].getValue().isString() && moduleFields[1].getValue().isString() &&
            moduleFields[2].getValue().isString(),
        "invalid module field type");
    auto name = kj::str(moduleFields[0].getValue().getString());
    auto type = parseModuleType(moduleFields[1].getValue().getString());
    auto source = kj::str(moduleFields[2].getValue().getString());
    KJ_REQUIRE(validModuleName(name), "invalid module name");
    KJ_REQUIRE(source.size() <= MAX_MODULE_SOURCE_BYTES, "module source exceeds size limit");
    aggregateSourceBytes += source.size();
    KJ_REQUIRE(
        aggregateSourceBytes <= MAX_MODULE_SOURCES_BYTES, "module sources exceed size limit");
    if (i == 0) {
      KJ_REQUIRE(name == mainModule && type == ModuleType::ES_MODULE,
          "main module must be the first module and an ES module");
    } else {
      KJ_REQUIRE(name != mainModule, "duplicate main module");
      KJ_IF_SOME(previous, previousModule) {
        KJ_REQUIRE(
            lexicographicallyBefore(previous, name), "non-main modules must be sorted and unique");
      }
      previousModule = name;
    }
    moduleBuilder.add(Module{kj::mv(name), type, kj::mv(source)});
  }

  return WorkerBundle{
    kj::mv(workerVersion),
    kj::mv(compatibilityDate),
    flagBuilder.finish(),
    kj::mv(mainModule),
    moduleBuilder.finish(),
  };
}

void appendJsonString(kj::Vector<char>& output, kj::StringPtr value) {
  static constexpr char HEX[] = "0123456789abcdef";
  output.add('"');
  for (byte raw: value.asBytes()) {
    auto c = static_cast<unsigned char>(raw);
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
        if (c < 0x20) {
          output.addAll("\\u00"_kj);
          output.add(HEX[c >> 4]);
          output.add(HEX[c & 0xf]);
        } else {
          output.add(static_cast<char>(c));
        }
        break;
    }
  }
  output.add('"');
}

FetchFailure parseFetchFailure(capnp::JsonValue::Reader value) {
  KJ_REQUIRE(value.isObject(), "host fetch error must be an object");
  kj::Maybe<kj::String> code;
  kj::Maybe<kj::String> message;
  for (auto field: value.getObject()) {
    if (field.getName() == "code"_kj) {
      KJ_REQUIRE(code == kj::none && field.getValue().isString(), "invalid host fetch error code");
      code = kj::str(field.getValue().getString());
    } else if (field.getName() == "message"_kj) {
      KJ_REQUIRE(
          message == kj::none && field.getValue().isString(), "invalid host fetch error message");
      message = kj::str(field.getValue().getString());
    } else {
      KJ_FAIL_REQUIRE("unknown host fetch error field");
    }
  }

  auto ownedCode = KJ_REQUIRE_NONNULL(kj::mv(code), "missing host fetch error code");
  auto ownedMessage = KJ_REQUIRE_NONNULL(kj::mv(message), "missing host fetch error message");
  FetchError error;
  if (ownedCode == "policy_denied"_kj) {
    error = FetchError::DENIED;
  } else if (ownedCode == "timeout"_kj) {
    error = FetchError::TIMEOUT;
  } else if (ownedCode == "cancelled"_kj) {
    error = FetchError::CANCELED;
  } else if (ownedCode == "response_too_large"_kj) {
    error = FetchError::SIZE_LIMIT;
  } else if (ownedCode == "invalid_request"_kj) {
    error = FetchError::MALFORMED_RESPONSE;
  } else if (ownedCode == "overloaded"_kj) {
    error = FetchError::OVERLOADED;
  } else if (ownedCode == "dns_failed"_kj || ownedCode == "connect_failed"_kj ||
      ownedCode == "redirect_limit"_kj) {
    error = FetchError::HOST_FAILURE;
  } else {
    KJ_FAIL_REQUIRE("unknown host fetch error code", ownedCode);
  }
  return FetchFailure{
    .error = error,
    .message = kj::str(ownedCode, ": ", ownedMessage),
  };
}

kj::String serializeFetchHeaderBlock(kj::ArrayPtr<const Header> headers) {
  KJ_REQUIRE(headers.size() <= MAX_OUTBOUND_FETCH_HEADERS, "too many outbound fetch headers");
  kj::Vector<char> output;
  output.add('[');
  bool first = true;
  size_t aggregateHeaderBytes = 0;
  for (const auto& header: headers) {
    KJ_REQUIRE(validHeaderName(header.name) && validHeaderValue(header.value),
        "invalid outbound fetch header");
    aggregateHeaderBytes += header.name.size() + header.value.size();
    KJ_REQUIRE(aggregateHeaderBytes <= MAX_OUTBOUND_FETCH_HEADER_BYTES,
        "outbound fetch headers exceed limit");
    if (!first) output.add(',');
    first = false;
    output.addAll("{\"name\":"_kj);
    appendJsonString(output, header.name);
    output.addAll(",\"value\":"_kj);
    appendJsonString(output, header.value);
    output.add('}');
  }
  output.add(']');
  KJ_REQUIRE(
      output.size() <= MAX_OUTBOUND_FETCH_HEADER_BYTES, "encoded outbound headers exceed limit");
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

kj::String serializeFetchMetadata(
    const FetchRequest& request, size_t headerBlockLength, size_t bodyLength) {
  KJ_REQUIRE(request.requestId.size() > 0 &&
          request.requestId.size() <= MAX_OUTBOUND_FETCH_REQUEST_ID_BYTES,
      "invalid outbound fetch request ID");
  KJ_REQUIRE(
      request.url.size() <= MAX_OUTBOUND_FETCH_URL_BYTES, "outbound fetch URL exceeds limit");
  KJ_REQUIRE(headerBlockLength <= MAX_OUTBOUND_FETCH_HEADER_BYTES,
      "encoded outbound headers exceed limit");
  KJ_REQUIRE(
      bodyLength <= MAX_OUTBOUND_FETCH_REQUEST_BODY_BYTES, "outbound fetch body exceeds limit");

  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
  appendJsonString(output, request.requestId);
  output.addAll(",\"method\":"_kj);
  appendJsonString(output, kj::str(request.method));
  output.addAll(",\"url\":"_kj);
  appendJsonString(output, request.url);
  output.addAll(",\"header_block_length\":"_kj);
  output.addAll(kj::str(headerBlockLength));
  output.addAll(",\"body_length\":"_kj);
  output.addAll(kj::str(bodyLength));
  output.add('}');
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

struct FetchStart {
  uint64_t operationId;
  kj::Maybe<FetchFailure> error;
};

FetchStart parseFetchStart(kj::StringPtr input) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "host fetch start response must be an object");

  bool sawVersion = false;
  kj::Maybe<uint64_t> operationId;
  kj::Maybe<FetchFailure> error;
  bool sawError = false;
  for (auto field: root.getObject()) {
    if (field.getName() == "protocol_version"_kj) {
      KJ_REQUIRE(!sawVersion && field.getValue().isNumber() && field.getValue().getNumber() == 1,
          "invalid host fetch start protocol version");
      sawVersion = true;
    } else if (field.getName() == "operation_id"_kj) {
      KJ_REQUIRE(operationId == kj::none && field.getValue().isNumber(),
          "invalid host fetch operation ID");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 0 && number <= MAX_HOST_OPERATION_ID &&
              number == static_cast<double>(static_cast<uint64_t>(number)),
          "invalid host fetch operation ID");
      operationId = static_cast<uint64_t>(number);
    } else if (field.getName() == "error"_kj) {
      KJ_REQUIRE(!sawError, "duplicate host fetch start error");
      sawError = true;
      if (!field.getValue().isNull()) {
        error = parseFetchFailure(field.getValue());
      }
    } else {
      KJ_FAIL_REQUIRE("unknown host fetch start field");
    }
  }
  KJ_REQUIRE(sawVersion && sawError, "incomplete host fetch start response");
  auto ownedOperationId = KJ_REQUIRE_NONNULL(operationId, "missing host fetch operation ID");
  if (error == kj::none) {
    KJ_REQUIRE(ownedOperationId > 0, "successful host fetch start returned no operation");
  } else {
    KJ_REQUIRE(ownedOperationId == 0, "failed host fetch start returned an operation");
  }
  return FetchStart{ownedOperationId, kj::mv(error)};
}

struct FetchPoll {
  bool complete;
  kj::Maybe<uint> status;
  kj::Maybe<FetchFailure> error;
  size_t headerBlockLength;
  size_t bodyLength;
};

FetchPoll parseFetchPoll(
    kj::StringPtr input, uint64_t expectedOperationId, kj::StringPtr expectedRequestId) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "host fetch poll response must be an object");

  bool sawVersion = false;
  kj::Maybe<uint64_t> operationId;
  kj::Maybe<kj::String> state;
  kj::Maybe<capnp::JsonValue::Reader> responseValue;
  bool sawResponse = false;
  kj::Maybe<FetchFailure> error;
  bool sawError = false;
  for (auto field: root.getObject()) {
    if (field.getName() == "protocol_version"_kj) {
      KJ_REQUIRE(!sawVersion && field.getValue().isNumber() && field.getValue().getNumber() == 1,
          "invalid host fetch poll protocol version");
      sawVersion = true;
    } else if (field.getName() == "operation_id"_kj) {
      KJ_REQUIRE(operationId == kj::none && field.getValue().isNumber(),
          "invalid host fetch poll operation ID");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number > 0 && number <= MAX_HOST_OPERATION_ID &&
              number == static_cast<double>(static_cast<uint64_t>(number)),
          "invalid host fetch poll operation ID");
      operationId = static_cast<uint64_t>(number);
    } else if (field.getName() == "state"_kj) {
      KJ_REQUIRE(state == kj::none && field.getValue().isString(), "invalid host fetch poll state");
      state = kj::str(field.getValue().getString());
    } else if (field.getName() == "response"_kj) {
      KJ_REQUIRE(!sawResponse, "duplicate host fetch poll response");
      sawResponse = true;
      if (!field.getValue().isNull()) {
        responseValue = field.getValue();
      }
    } else if (field.getName() == "error"_kj) {
      KJ_REQUIRE(!sawError, "duplicate host fetch poll error");
      sawError = true;
      if (!field.getValue().isNull()) {
        error = parseFetchFailure(field.getValue());
      }
    } else {
      KJ_FAIL_REQUIRE("unknown host fetch poll field");
    }
  }
  KJ_REQUIRE(sawVersion && sawResponse && sawError, "incomplete host fetch poll response");
  KJ_REQUIRE(KJ_REQUIRE_NONNULL(operationId, "missing host fetch poll operation ID") ==
          expectedOperationId,
      "host fetch poll operation ID mismatch");
  auto ownedState = KJ_REQUIRE_NONNULL(kj::mv(state), "missing host fetch poll state");
  if (ownedState == "receiving"_kj || ownedState == "pending"_kj) {
    KJ_REQUIRE(responseValue == kj::none && error == kj::none,
        "incomplete host fetch unexpectedly included a result");
    return FetchPoll{false, kj::none, kj::none, 0, 0};
  }
  KJ_REQUIRE(ownedState == "complete"_kj, "invalid host fetch poll state");
  if (error != kj::none) {
    KJ_REQUIRE(responseValue == kj::none, "failed host fetch included response metadata");
    return FetchPoll{true, kj::none, kj::mv(error), 0, 0};
  }
  auto responseObject =
      KJ_REQUIRE_NONNULL(responseValue, "successful host fetch is missing response metadata");
  KJ_REQUIRE(responseObject.isObject(), "host fetch response metadata must be an object");

  bool sawResponseVersion = false;
  kj::Maybe<kj::String> requestId;
  kj::Maybe<uint> status;
  kj::Maybe<size_t> headerBlockLength;
  kj::Maybe<size_t> bodyLength;
  for (auto field: responseObject.getObject()) {
    if (field.getName() == "protocol_version"_kj) {
      KJ_REQUIRE(
          !sawResponseVersion && field.getValue().isNumber() && field.getValue().getNumber() == 1,
          "invalid host fetch response protocol version");
      sawResponseVersion = true;
    } else if (field.getName() == "request_id"_kj) {
      KJ_REQUIRE(requestId == kj::none && field.getValue().isString(),
          "invalid host fetch response request ID");
      requestId = kj::str(field.getValue().getString());
    } else if (field.getName() == "status"_kj) {
      KJ_REQUIRE(
          status == kj::none && field.getValue().isNumber(), "invalid host fetch response status");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(
          number >= 0 && number <= 599 && number == static_cast<double>(static_cast<uint>(number)),
          "invalid host fetch response status");
      status = static_cast<uint>(number);
    } else if (field.getName() == "header_block_length"_kj) {
      KJ_REQUIRE(headerBlockLength == kj::none && field.getValue().isNumber(),
          "invalid host fetch response header block length");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 0 && number <= MAX_OUTBOUND_FETCH_HEADER_BYTES &&
              number == static_cast<double>(static_cast<size_t>(number)),
          "invalid host fetch response header block length");
      headerBlockLength = static_cast<size_t>(number);
    } else if (field.getName() == "body_length"_kj) {
      KJ_REQUIRE(bodyLength == kj::none && field.getValue().isNumber(),
          "invalid host fetch response body length");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 0 && number <= MAX_OUTBOUND_FETCH_RESPONSE_BODY_BYTES &&
              number == static_cast<double>(static_cast<size_t>(number)),
          "invalid host fetch response body length");
      bodyLength = static_cast<size_t>(number);
    } else {
      KJ_FAIL_REQUIRE("unknown host fetch response field");
    }
  }

  KJ_REQUIRE(sawResponseVersion, "missing host fetch response protocol version");
  auto ownedRequestId = KJ_REQUIRE_NONNULL(kj::mv(requestId), "missing host fetch response ID");
  KJ_REQUIRE(ownedRequestId == expectedRequestId, "host fetch response ID mismatch");
  auto ownedStatus = KJ_REQUIRE_NONNULL(status, "missing host fetch response status");
  auto ownedHeaderBlockLength =
      KJ_REQUIRE_NONNULL(headerBlockLength, "missing host fetch response header block length");
  auto ownedBodyLength = KJ_REQUIRE_NONNULL(bodyLength, "missing host fetch response body length");
  KJ_REQUIRE(ownedStatus >= 100, "successful host fetch returned invalid status");
  return FetchPoll{true, ownedStatus, kj::none, ownedHeaderBlockLength, ownedBodyLength};
}

kj::Array<Header> parseFetchHeaderBlock(kj::ArrayPtr<const char> input) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isArray(), "host fetch header block must be an array");
  auto values = root.getArray();
  KJ_REQUIRE(values.size() <= MAX_OUTBOUND_FETCH_HEADERS, "too many host fetch headers");
  auto parsed = kj::heapArrayBuilder<Header>(values.size());
  size_t aggregateHeaderBytes = 0;
  for (auto value: values) {
    KJ_REQUIRE(value.isObject(), "invalid host fetch response header");
    kj::Maybe<kj::String> name;
    kj::Maybe<kj::String> content;
    for (auto field: value.getObject()) {
      if (field.getName() == "name"_kj) {
        KJ_REQUIRE(
            name == kj::none && field.getValue().isString(), "invalid host fetch header name");
        name = kj::str(field.getValue().getString());
      } else if (field.getName() == "value"_kj) {
        KJ_REQUIRE(
            content == kj::none && field.getValue().isString(), "invalid host fetch header value");
        content = kj::str(field.getValue().getString());
      } else {
        KJ_FAIL_REQUIRE("unknown host fetch header field");
      }
    }
    auto ownedName = KJ_REQUIRE_NONNULL(kj::mv(name), "missing host fetch header name");
    auto ownedContent = KJ_REQUIRE_NONNULL(kj::mv(content), "missing host fetch header value");
    KJ_REQUIRE(validHeaderName(ownedName) && validHeaderValue(ownedContent),
        "invalid host fetch response header");
    aggregateHeaderBytes += ownedName.size() + ownedContent.size();
    KJ_REQUIRE(aggregateHeaderBytes <= MAX_OUTBOUND_FETCH_HEADER_BYTES,
        "host fetch response headers exceed limit");
    parsed.add(Header{kj::mv(ownedName), kj::mv(ownedContent)});
  }
  return parsed.finish();
}

class FetchHostChannel {
 public:
  virtual kj::String start(kj::StringPtr metadata) = 0;
  virtual int32_t write(uint64_t operationId, kj::ArrayPtr<const byte> bytes) = 0;
  virtual int32_t finish(uint64_t operationId) = 0;
  virtual kj::String poll(uint64_t operationId) = 0;
  virtual kj::Array<byte> read(uint64_t operationId, size_t maxBytes) = 0;
  virtual int32_t cancel(uint64_t operationId) = 0;
  virtual ~FetchHostChannel() noexcept(false) = default;
};

class HyperlightFetchHostChannel final: public FetchHostChannel {
 public:
  kj::String start(kj::StringPtr metadata) override {
    auto arg = hostCallStringArg(metadata);
    return hostCallString("WorkerdFetchV1Start"_kj, kj::arrayPtr(&arg, 1));
  }

  int32_t write(uint64_t operationId, kj::ArrayPtr<const byte> bytes) override {
    hlcall_host_arg args[] = {hostCallU64(operationId), hostCallBytesArg(bytes)};
    return hostCallI32("WorkerdFetchV1Write"_kj, kj::arrayPtr(args));
  }

  int32_t finish(uint64_t operationId) override {
    auto arg = hostCallU64(operationId);
    return hostCallI32("WorkerdFetchV1Finish"_kj, kj::arrayPtr(&arg, 1));
  }

  kj::String poll(uint64_t operationId) override {
    auto arg = hostCallU64(operationId);
    return hostCallString("WorkerdFetchV1Poll"_kj, kj::arrayPtr(&arg, 1));
  }

  kj::Array<byte> read(uint64_t operationId, size_t maxBytes) override {
    hlcall_host_arg args[] = {hostCallU64(operationId), hostCallU64(maxBytes)};
    return hostCallBytes("WorkerdFetchV1Read"_kj, kj::arrayPtr(args), maxBytes);
  }

  int32_t cancel(uint64_t operationId) override {
    auto arg = hostCallU64(operationId);
    return hostCallI32("WorkerdFetchV1Cancel"_kj, kj::arrayPtr(&arg, 1));
  }
};

kj::Exception fetchFailureToException(FetchFailure failure) {
  switch (failure.error) {
    case FetchError::DENIED:
      return KJ_EXCEPTION(FAILED, "outbound fetch denied by host", failure.message);
    case FetchError::TIMEOUT:
      return KJ_EXCEPTION(OVERLOADED, "outbound fetch timed out", failure.message);
    case FetchError::CANCELED:
      return KJ_EXCEPTION(DISCONNECTED, "outbound fetch canceled", failure.message);
    case FetchError::MALFORMED_RESPONSE:
      return KJ_EXCEPTION(
          FAILED, "host returned a malformed outbound fetch response", failure.message);
    case FetchError::HOST_FAILURE:
      return KJ_EXCEPTION(FAILED, "outbound fetch host call failed", failure.message);
    case FetchError::OVERLOADED:
      return KJ_EXCEPTION(OVERLOADED, "outbound fetch host is overloaded", failure.message);
    case FetchError::SIZE_LIMIT:
      return KJ_EXCEPTION(FAILED, "outbound fetch exceeded host size limit", failure.message);
  }
  KJ_UNREACHABLE;
}

class BufferedV1FetchBroker final: public FetchBroker {
 public:
  BufferedV1FetchBroker(): channel(kj::heap<HyperlightFetchHostChannel>()) {}
  explicit BufferedV1FetchBroker(kj::Own<FetchHostChannel> channel): channel(kj::mv(channel)) {}

  kj::Promise<void> request(FetchRequest request,
      const kj::HttpHeaders& requestHeaders,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& httpResponse,
      TimerChannel& timer) override {
    return requestImpl(kj::mv(request), requestHeaders, requestBody, httpResponse, timer)
        .exclusiveJoin(timer.afterLimitTimeout(OUTBOUND_FETCH_DEADLINE_MS * kj::MILLISECONDS)
                           .then([]() -> kj::Promise<void> {
      return KJ_EXCEPTION(OVERLOADED, "outbound fetch v1 deadline exceeded");
    }));
  }

 private:
  kj::Promise<void> requestImpl(FetchRequest request,
      const kj::HttpHeaders& requestHeaders,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& httpResponse,
      TimerChannel& timer) {
    KJ_REQUIRE(activeOperations < MAX_CONCURRENT_OUTBOUND_FETCHES,
        "too many concurrent outbound fetch v1 operations");
    ++activeOperations;
    KJ_DEFER(--activeOperations);
    KJ_IF_SOME(length, request.bodyLength) {
      KJ_REQUIRE(
          length <= MAX_OUTBOUND_FETCH_REQUEST_BODY_BYTES, "outbound fetch body exceeds v1 limit");
    }
    auto requestBodyBytes =
        co_await requestBody.readAllBytes(MAX_OUTBOUND_FETCH_REQUEST_BODY_BYTES + 1);
    KJ_REQUIRE(requestBodyBytes.size() <= MAX_OUTBOUND_FETCH_REQUEST_BODY_BYTES,
        "outbound fetch body exceeds v1 limit");
    auto headerBlock = serializeFetchHeaderBlock(request.headers);
    auto metadata = serializeFetchMetadata(request, headerBlock.size(), requestBodyBytes.size());
    auto start = parseFetchStart(channel->start(metadata));
    KJ_IF_SOME(error, start.error) {
      kj::throwRecoverableException(fetchFailureToException(kj::mv(error)));
      KJ_UNREACHABLE;
    }

    struct Operation {
      Operation(FetchHostChannel& channel, uint64_t id): channel(channel), id(id) {}
      ~Operation() noexcept {
        if (!collected) {
          try {
            auto result = channel.cancel(id);
            if (result != 0 && result != -ENOENT) {
              KJ_LOG(ERROR, "Hyperlight outbound fetch cancellation failed", id, result);
            }
          } catch (const kj::Exception& exception) {
            KJ_LOG(ERROR, "Hyperlight outbound fetch cancellation host call failed", id, exception);
          }
        }
      }
      FetchHostChannel& channel;
      uint64_t id;
      bool collected = false;
    };
    auto operation = kj::heap<Operation>(*channel, start.operationId);

    auto writeAll = [&](kj::ArrayPtr<const byte> bytes) {
      while (bytes.size() > 0) {
        auto amount = kj::min(bytes.size(), MAX_HOST_CALL_CHUNK_BYTES);
        auto result = channel->write(operation->id, bytes.first(amount));
        KJ_REQUIRE(
            result == static_cast<int32_t>(amount), "Hyperlight fetch input write failed", result);
        bytes = bytes.slice(amount);
      }
    };
    writeAll(headerBlock.asBytes());
    writeAll(requestBodyBytes);
    KJ_REQUIRE(channel->finish(operation->id) == 0, "Hyperlight fetch finish failed");

    FetchPoll poll;
    for (;;) {
      poll = parseFetchPoll(channel->poll(operation->id), operation->id, request.requestId);
      if (poll.complete) break;
      co_await timer.afterLimitTimeout(FETCH_POLL_INTERVAL);
    }

    KJ_IF_SOME(error, poll.error) {
      operation->collected = true;
      kj::throwRecoverableException(fetchFailureToException(kj::mv(error)));
      KJ_UNREACHABLE;
    }
    auto status = KJ_REQUIRE_NONNULL(poll.status, "complete host fetch has no status");
    KJ_REQUIRE(poll.headerBlockLength <= MAX_OUTBOUND_FETCH_HEADER_BYTES &&
            poll.bodyLength <= MAX_OUTBOUND_FETCH_RESPONSE_BODY_BYTES &&
            poll.headerBlockLength <= SIZE_MAX - poll.bodyLength,
        "host fetch response stream exceeds limit");
    auto totalLength = poll.headerBlockLength + poll.bodyLength;
    auto responseBytes = kj::heapArray<char>(totalLength + 1);
    size_t offset = 0;
    while (offset < totalLength) {
      auto amount = kj::min(totalLength - offset, MAX_HOST_CALL_CHUNK_BYTES);
      auto chunk = channel->read(operation->id, amount);
      KJ_REQUIRE(chunk.size() > 0 && chunk.size() <= amount,
          "Hyperlight fetch response read returned invalid chunk");
      memcpy(responseBytes.begin() + offset, chunk.begin(), chunk.size());
      offset += chunk.size();
    }
    responseBytes[totalLength] = '\0';
    auto headers = parseFetchHeaderBlock(responseBytes.first(poll.headerBlockLength));
    auto body = kj::heapArray<char>(poll.bodyLength + 1);
    memcpy(body.begin(), responseBytes.begin() + poll.headerBlockLength, poll.bodyLength);
    body[poll.bodyLength] = '\0';
    operation->collected = true;
    auto responseHeaders = requestHeaders.cloneShallow();
    responseHeaders.clear();
    for (const auto& header: headers) {
      responseHeaders.addPtrPtr(header.name, header.value);
    }
    auto output =
        httpResponse.send(status, ""_kj, responseHeaders, static_cast<uint64_t>(poll.bodyLength));
    co_await output->write(kj::arrayPtr(body.begin(), poll.bodyLength).asBytes());
  }

  kj::Own<FetchHostChannel> channel;
  size_t activeOperations = 0;
};

constexpr size_t PREFERRED_STREAM_CHUNK_BYTES = 32 * 1024;
constexpr size_t MAX_V2_WRITE_CHUNK_BYTES = 60 * 1024;
constexpr size_t MAX_V2_READ_CHUNK_BYTES = 60 * 1024 - 1;

struct V2Start {
  uint64_t operationId;
  size_t maxWriteChunk;
  size_t maxReadChunk;
  kj::Maybe<FetchFailure> error;
};

V2Start parseV2Start(kj::StringPtr input) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "v2 fetch start response must be an object");

  bool sawVersion = false;
  kj::Maybe<uint64_t> operationId;
  kj::Maybe<size_t> maxWriteChunk;
  kj::Maybe<size_t> maxReadChunk;
  kj::Maybe<FetchFailure> error;
  bool sawError = false;
  for (auto field: root.getObject()) {
    if (field.getName() == "protocol_version"_kj) {
      KJ_REQUIRE(!sawVersion && field.getValue().isNumber() && field.getValue().getNumber() == 2,
          "invalid v2 fetch start protocol version");
      sawVersion = true;
    } else if (field.getName() == "operation_id"_kj) {
      KJ_REQUIRE(
          operationId == kj::none && field.getValue().isNumber(), "invalid v2 fetch operation ID");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 0 && number <= MAX_HOST_OPERATION_ID &&
              number == static_cast<double>(static_cast<uint64_t>(number)),
          "invalid v2 fetch operation ID");
      operationId = static_cast<uint64_t>(number);
    } else if (field.getName() == "max_write_chunk"_kj) {
      KJ_REQUIRE(
          maxWriteChunk == kj::none && field.getValue().isNumber(), "invalid v2 fetch write chunk");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 0 && number <= MAX_V2_WRITE_CHUNK_BYTES &&
              number == static_cast<double>(static_cast<size_t>(number)),
          "invalid v2 fetch write chunk");
      maxWriteChunk = static_cast<size_t>(number);
    } else if (field.getName() == "max_read_chunk"_kj) {
      KJ_REQUIRE(
          maxReadChunk == kj::none && field.getValue().isNumber(), "invalid v2 fetch read chunk");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 0 && number <= MAX_V2_READ_CHUNK_BYTES &&
              number == static_cast<double>(static_cast<size_t>(number)),
          "invalid v2 fetch read chunk");
      maxReadChunk = static_cast<size_t>(number);
    } else if (field.getName() == "error"_kj) {
      KJ_REQUIRE(!sawError, "duplicate v2 fetch start error");
      sawError = true;
      if (!field.getValue().isNull()) {
        error = parseFetchFailure(field.getValue());
      }
    } else {
      KJ_FAIL_REQUIRE("unknown v2 fetch start field");
    }
  }
  KJ_REQUIRE(sawVersion && sawError, "incomplete v2 fetch start response");
  auto ownedOperationId = KJ_REQUIRE_NONNULL(operationId, "missing v2 fetch operation ID");
  auto ownedMaxWrite = KJ_REQUIRE_NONNULL(maxWriteChunk, "missing v2 fetch write chunk");
  auto ownedMaxRead = KJ_REQUIRE_NONNULL(maxReadChunk, "missing v2 fetch read chunk");
  if (error == kj::none) {
    KJ_REQUIRE(ownedOperationId > 0 && ownedMaxWrite > 0 && ownedMaxRead > 0,
        "successful v2 fetch start returned invalid limits");
  } else {
    KJ_REQUIRE(ownedOperationId == 0 && ownedMaxWrite == 0 && ownedMaxRead == 0,
        "failed v2 fetch start returned an operation");
  }
  return {ownedOperationId, ownedMaxWrite, ownedMaxRead, kj::mv(error)};
}

kj::String serializeV2Metadata(const FetchRequest& request, size_t headerBlockLength) {
  KJ_REQUIRE(request.requestId.size() > 0 &&
          request.requestId.size() <= MAX_OUTBOUND_FETCH_REQUEST_ID_BYTES,
      "invalid outbound fetch request ID");
  KJ_REQUIRE(
      request.url.size() <= MAX_OUTBOUND_FETCH_URL_BYTES, "outbound fetch URL exceeds limit");
  KJ_REQUIRE(headerBlockLength <= MAX_OUTBOUND_FETCH_HEADER_BYTES,
      "encoded outbound headers exceed limit");

  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":2,\"request_id\":"_kj);
  appendJsonString(output, request.requestId);
  output.addAll(",\"method\":"_kj);
  appendJsonString(output, kj::str(request.method));
  output.addAll(",\"url\":"_kj);
  appendJsonString(output, request.url);
  output.addAll(",\"header_block_length\":"_kj);
  output.addAll(kj::str(headerBlockLength));
  output.addAll(",\"body_length\":"_kj);
  KJ_IF_SOME(length, request.bodyLength) {
    output.addAll(kj::str(length));
  } else {
    output.addAll("null"_kj);
  }
  output.addAll(",\"preferred_write_chunk\":"_kj);
  output.addAll(kj::str(PREFERRED_STREAM_CHUNK_BYTES));
  output.addAll(",\"preferred_read_chunk\":"_kj);
  output.addAll(kj::str(PREFERRED_STREAM_CHUNK_BYTES));
  output.add('}');
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

enum class V2PollState {
  RECEIVING_HEADERS,
  UPLOADING,
  RESPONSE,
  COMPLETE,
};

struct V2ResponseMetadata {
  uint status;
  size_t headerBlockLength;
  kj::Maybe<uint64_t> bodyLength;
};

struct V2Poll {
  V2PollState state;
  kj::Maybe<V2ResponseMetadata> response;
  kj::Maybe<FetchFailure> error;
};

V2Poll parseV2Poll(
    kj::StringPtr input, uint64_t expectedOperationId, kj::StringPtr expectedRequestId) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "v2 fetch poll response must be an object");

  bool sawVersion = false;
  kj::Maybe<uint64_t> operationId;
  kj::Maybe<kj::String> state;
  kj::Maybe<capnp::JsonValue::Reader> responseValue;
  bool sawResponse = false;
  kj::Maybe<FetchFailure> error;
  bool sawError = false;
  for (auto field: root.getObject()) {
    if (field.getName() == "protocol_version"_kj) {
      KJ_REQUIRE(!sawVersion && field.getValue().isNumber() && field.getValue().getNumber() == 2,
          "invalid v2 fetch poll protocol version");
      sawVersion = true;
    } else if (field.getName() == "operation_id"_kj) {
      KJ_REQUIRE(operationId == kj::none && field.getValue().isNumber(),
          "invalid v2 fetch poll operation ID");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number > 0 && number <= MAX_HOST_OPERATION_ID &&
              number == static_cast<double>(static_cast<uint64_t>(number)),
          "invalid v2 fetch poll operation ID");
      operationId = static_cast<uint64_t>(number);
    } else if (field.getName() == "state"_kj) {
      KJ_REQUIRE(state == kj::none && field.getValue().isString(), "invalid v2 fetch poll state");
      state = kj::str(field.getValue().getString());
    } else if (field.getName() == "response"_kj) {
      KJ_REQUIRE(!sawResponse, "duplicate v2 fetch poll response");
      sawResponse = true;
      if (!field.getValue().isNull()) responseValue = field.getValue();
    } else if (field.getName() == "error"_kj) {
      KJ_REQUIRE(!sawError, "duplicate v2 fetch poll error");
      sawError = true;
      if (!field.getValue().isNull()) error = parseFetchFailure(field.getValue());
    } else {
      KJ_FAIL_REQUIRE("unknown v2 fetch poll field");
    }
  }
  KJ_REQUIRE(sawVersion && sawResponse && sawError, "incomplete v2 fetch poll response");
  KJ_REQUIRE(
      KJ_REQUIRE_NONNULL(operationId, "missing v2 fetch poll operation ID") == expectedOperationId,
      "v2 fetch poll operation ID mismatch");
  auto ownedState = KJ_REQUIRE_NONNULL(kj::mv(state), "missing v2 fetch poll state");

  V2PollState parsedState;
  if (ownedState == "receiving_headers"_kj) {
    parsedState = V2PollState::RECEIVING_HEADERS;
  } else if (ownedState == "uploading"_kj) {
    parsedState = V2PollState::UPLOADING;
  } else if (ownedState == "response"_kj) {
    parsedState = V2PollState::RESPONSE;
  } else if (ownedState == "complete"_kj) {
    parsedState = V2PollState::COMPLETE;
  } else {
    KJ_FAIL_REQUIRE("invalid v2 fetch poll state");
  }

  if (error != kj::none) {
    KJ_REQUIRE(parsedState == V2PollState::COMPLETE && responseValue == kj::none,
        "v2 fetch failure has invalid state");
    return {parsedState, kj::none, kj::mv(error)};
  }
  if (responseValue == kj::none) {
    KJ_REQUIRE(
        parsedState == V2PollState::RECEIVING_HEADERS || parsedState == V2PollState::UPLOADING,
        "v2 fetch response metadata is missing");
    return {parsedState, kj::none, kj::none};
  }
  KJ_REQUIRE(parsedState == V2PollState::RESPONSE || parsedState == V2PollState::COMPLETE,
      "v2 fetch response metadata arrived in invalid state");

  auto responseObject = KJ_REQUIRE_NONNULL(responseValue);
  KJ_REQUIRE(responseObject.isObject(), "v2 fetch response metadata must be an object");
  bool sawResponseVersion = false;
  kj::Maybe<kj::String> requestId;
  kj::Maybe<uint> status;
  kj::Maybe<size_t> headerBlockLength;
  kj::Maybe<kj::Maybe<uint64_t>> bodyLength;
  for (auto field: responseObject.getObject()) {
    if (field.getName() == "protocol_version"_kj) {
      KJ_REQUIRE(
          !sawResponseVersion && field.getValue().isNumber() && field.getValue().getNumber() == 2,
          "invalid v2 fetch response protocol version");
      sawResponseVersion = true;
    } else if (field.getName() == "request_id"_kj) {
      KJ_REQUIRE(requestId == kj::none && field.getValue().isString(),
          "invalid v2 fetch response request ID");
      requestId = kj::str(field.getValue().getString());
    } else if (field.getName() == "status"_kj) {
      KJ_REQUIRE(
          status == kj::none && field.getValue().isNumber(), "invalid v2 fetch response status");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 100 && number <= 599 &&
              number == static_cast<double>(static_cast<uint>(number)),
          "invalid v2 fetch response status");
      status = static_cast<uint>(number);
    } else if (field.getName() == "header_block_length"_kj) {
      KJ_REQUIRE(headerBlockLength == kj::none && field.getValue().isNumber(),
          "invalid v2 fetch response header block length");
      auto number = field.getValue().getNumber();
      KJ_REQUIRE(number >= 0 && number <= MAX_OUTBOUND_FETCH_HEADER_BYTES &&
              number == static_cast<double>(static_cast<size_t>(number)),
          "invalid v2 fetch response header block length");
      headerBlockLength = static_cast<size_t>(number);
    } else if (field.getName() == "body_length"_kj) {
      KJ_REQUIRE(bodyLength == kj::none, "duplicate v2 fetch response body length");
      if (field.getValue().isNull()) {
        bodyLength = kj::Maybe<uint64_t>(kj::none);
      } else {
        KJ_REQUIRE(field.getValue().isNumber(), "invalid v2 fetch response body length");
        auto number = field.getValue().getNumber();
        KJ_REQUIRE(number >= 0 && number <= MAX_HOST_OPERATION_ID &&
                number == static_cast<double>(static_cast<uint64_t>(number)),
            "invalid v2 fetch response body length");
        bodyLength = kj::Maybe<uint64_t>(static_cast<uint64_t>(number));
      }
    } else {
      KJ_FAIL_REQUIRE("unknown v2 fetch response field");
    }
  }
  KJ_REQUIRE(sawResponseVersion, "missing v2 fetch response protocol version");
  KJ_REQUIRE(
      KJ_REQUIRE_NONNULL(requestId, "missing v2 fetch response request ID") == expectedRequestId,
      "v2 fetch response request ID mismatch");
  return {parsedState,
    V2ResponseMetadata{KJ_REQUIRE_NONNULL(status, "missing v2 fetch response status"),
      KJ_REQUIRE_NONNULL(headerBlockLength, "missing v2 fetch response header block length"),
      KJ_REQUIRE_NONNULL(kj::mv(bodyLength), "missing v2 fetch response body length")},
    kj::none};
}

class V2FetchHostChannel {
 public:
  virtual kj::String start(kj::StringPtr metadata) = 0;
  virtual int32_t write(uint64_t operationId, kj::ArrayPtr<const byte> bytes) = 0;
  virtual int32_t finish(uint64_t operationId) = 0;
  virtual kj::String poll(uint64_t operationId) = 0;
  virtual kj::Array<byte> read(uint64_t operationId, size_t maxBytes) = 0;
  virtual int32_t cancel(uint64_t operationId) = 0;
  virtual ~V2FetchHostChannel() noexcept(false) = default;
};

class HyperlightV2FetchHostChannel final: public V2FetchHostChannel {
 public:
  kj::String start(kj::StringPtr metadata) override {
    auto arg = hostCallStringArg(metadata);
    return hostCallString("WorkerdFetchV2Start"_kj, kj::arrayPtr(&arg, 1));
  }
  int32_t write(uint64_t id, kj::ArrayPtr<const byte> bytes) override {
    hlcall_host_arg args[] = {hostCallU64(id), hostCallBytesArg(bytes)};
    return hostCallI32("WorkerdFetchV2Write"_kj, kj::arrayPtr(args));
  }
  int32_t finish(uint64_t id) override {
    auto arg = hostCallU64(id);
    return hostCallI32("WorkerdFetchV2Finish"_kj, kj::arrayPtr(&arg, 1));
  }
  kj::String poll(uint64_t id) override {
    auto arg = hostCallU64(id);
    return hostCallString("WorkerdFetchV2Poll"_kj, kj::arrayPtr(&arg, 1));
  }
  kj::Array<byte> read(uint64_t id, size_t maxBytes) override {
    hlcall_host_arg args[] = {hostCallU64(id), hostCallU64(maxBytes)};
    return hostCallBytes("WorkerdFetchV2Read"_kj, kj::arrayPtr(args), maxBytes + 1);
  }
  int32_t cancel(uint64_t id) override {
    auto arg = hostCallU64(id);
    return hostCallI32("WorkerdFetchV2Cancel"_kj, kj::arrayPtr(&arg, 1));
  }
};

class StreamingV2FetchBroker final: public FetchBroker {
 public:
  StreamingV2FetchBroker(): channel(kj::heap<HyperlightV2FetchHostChannel>()) {}
  explicit StreamingV2FetchBroker(kj::Own<V2FetchHostChannel> channel): channel(kj::mv(channel)) {}

  kj::Promise<void> request(FetchRequest request,
      const kj::HttpHeaders& requestHeaders,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& httpResponse,
      TimerChannel& timer) override {
    KJ_REQUIRE(activeOperations < MAX_CONCURRENT_OUTBOUND_FETCHES,
        "too many concurrent outbound fetch v2 operations");
    ++activeOperations;
    KJ_DEFER(--activeOperations);
    auto headerBlock = serializeFetchHeaderBlock(request.headers);
    auto start = parseV2Start(channel->start(serializeV2Metadata(request, headerBlock.size())));
    KJ_IF_SOME(error, start.error) {
      kj::throwRecoverableException(fetchFailureToException(kj::mv(error)));
      KJ_UNREACHABLE;
    }

    struct Operation: public kj::Refcounted {
      Operation(V2FetchHostChannel& channel, uint64_t id, size_t writeChunk, size_t readChunk)
          : channel(channel),
            id(id),
            writeChunk(writeChunk),
            readChunk(readChunk) {}
      ~Operation() noexcept {
        if (!collected) {
          try {
            auto result = channel.cancel(id);
            if (result != 0 && result != -ENOENT) {
              KJ_LOG(ERROR, "v2 outbound fetch cancellation failed", id, result);
            }
          } catch (const kj::Exception& exception) {
            KJ_LOG(ERROR, "v2 outbound fetch cancellation host call failed", id, exception);
          }
        }
      }
      V2FetchHostChannel& channel;
      uint64_t id;
      size_t writeChunk;
      size_t readChunk;
      bool collected = false;
    };
    auto operation =
        kj::rc<Operation>(*channel, start.operationId, start.maxWriteChunk, start.maxReadChunk);

    auto writeChunk = [&](kj::ArrayPtr<const byte> bytes) -> kj::Promise<kj::Maybe<size_t>> {
      for (;;) {
        auto result = channel->write(operation->id, bytes);
        if (result > 0) {
          KJ_REQUIRE(static_cast<size_t>(result) <= bytes.size(),
              "v2 outbound fetch host accepted too many bytes", result, bytes.size());
          co_return static_cast<size_t>(result);
        }
        if (result == -EPIPE) co_return kj::none;
        KJ_REQUIRE(result == -EAGAIN, "v2 outbound fetch write failed", result);
        co_await timer.afterLimitTimeout(FETCH_POLL_INTERVAL);
      }
    };
    auto writeAll = [&](kj::ArrayPtr<const byte> bytes) -> kj::Promise<bool> {
      while (bytes.size() > 0) {
        auto amount = kj::min(bytes.size(), operation->writeChunk);
        auto accepted = co_await writeChunk(bytes.first(amount));
        KJ_IF_SOME(count, accepted) {
          bytes = bytes.slice(count);
        } else {
          co_return false;
        }
      }
      co_return true;
    };

    if (!co_await writeAll(headerBlock.asBytes())) {
      KJ_FAIL_REQUIRE("v2 host returned a response before request headers completed");
    }

    auto uploadFn = [&]() -> kj::Promise<void> {
      auto buffer = kj::heapArray<byte>(operation->writeChunk);
      for (;;) {
        auto amount = co_await requestBody.tryRead(buffer.begin(), 1, buffer.size());
        if (amount == 0) break;
        if (!co_await writeAll(buffer.first(amount))) co_return;
      }
      auto result = channel->finish(operation->id);
      KJ_REQUIRE(result == 0 || result == -EPIPE, "v2 outbound fetch finish failed", result);
    };
    auto upload = uploadFn().eagerlyEvaluate(nullptr);

    V2ResponseMetadata responseMetadata;
    for (;;) {
      auto poll = parseV2Poll(channel->poll(operation->id), operation->id, request.requestId);
      KJ_IF_SOME(error, poll.error) {
        operation->collected = true;
        kj::throwRecoverableException(fetchFailureToException(kj::mv(error)));
        KJ_UNREACHABLE;
      }
      KJ_IF_SOME(response, poll.response) {
        responseMetadata = kj::mv(response);
        break;
      }
      co_await timer.afterLimitTimeout(FETCH_POLL_INTERVAL);
    }

    auto readChunk = [&]() -> kj::Promise<kj::Maybe<kj::Array<byte>>> {
      for (;;) {
        auto encoded = channel->read(operation->id, operation->readChunk);
        KJ_REQUIRE(encoded.size() > 0, "v2 outbound fetch read returned no tag");
        switch (encoded[0]) {
          case 0:
            KJ_REQUIRE(encoded.size() == 1, "invalid v2 pending read");
            co_await timer.afterLimitTimeout(FETCH_POLL_INTERVAL);
            break;
          case 1: {
            KJ_REQUIRE(encoded.size() > 1 && encoded.size() - 1 <= operation->readChunk,
                "invalid v2 data read");
            auto data = kj::heapArray<byte>(encoded.size() - 1);
            memcpy(data.begin(), encoded.begin() + 1, data.size());
            co_return kj::Maybe<kj::Array<byte>>(kj::mv(data));
          }
          case 2:
            KJ_REQUIRE(encoded.size() == 1, "invalid v2 eof read");
            operation->collected = true;
            co_return kj::Maybe<kj::Array<byte>>(kj::none);
          default:
            KJ_FAIL_REQUIRE("invalid v2 fetch read tag");
        }
      }
    };

    kj::Vector<byte> headerBytes;
    kj::Maybe<kj::Array<byte>> firstBodyChunk;
    while (headerBytes.size() < responseMetadata.headerBlockLength) {
      auto chunk = KJ_REQUIRE_NONNULL(co_await readChunk(), "v2 response ended during headers");
      auto remaining = responseMetadata.headerBlockLength - headerBytes.size();
      auto headerAmount = kj::min(remaining, chunk.size());
      headerBytes.addAll(chunk.first(headerAmount));
      if (headerAmount < chunk.size()) {
        auto body = kj::heapArray<byte>(chunk.size() - headerAmount);
        memcpy(body.begin(), chunk.begin() + headerAmount, body.size());
        firstBodyChunk = kj::mv(body);
      }
    }
    auto headers = parseFetchHeaderBlock(headerBytes.asPtr().asChars());
    auto responseHeaders = requestHeaders.cloneShallow();
    responseHeaders.clear();
    for (const auto& header: headers) {
      responseHeaders.addPtrPtr(header.name, header.value);
    }
    auto output = httpResponse.send(
        responseMetadata.status, ""_kj, responseHeaders, responseMetadata.bodyLength);

    uint64_t bodyBytes = 0;
    KJ_IF_SOME(data, firstBodyChunk) {
      bodyBytes += data.size();
      KJ_IF_SOME(expected, responseMetadata.bodyLength) {
        KJ_REQUIRE(bodyBytes <= expected, "v2 response exceeded declared body length");
      }
      co_await output->write(data);
    }
    for (;;) {
      auto chunk = co_await readChunk();
      KJ_IF_SOME(data, chunk) {
        bodyBytes += data.size();
        KJ_IF_SOME(expected, responseMetadata.bodyLength) {
          KJ_REQUIRE(bodyBytes <= expected, "v2 response exceeded declared body length");
        }
        co_await output->write(data);
      } else {
        break;
      }
    }
    KJ_IF_SOME(expected, responseMetadata.bodyLength) {
      KJ_REQUIRE(bodyBytes == expected, "v2 response ended before declared body length");
    }
    co_await upload;
  }

 private:
  kj::Own<V2FetchHostChannel> channel;
  size_t activeOperations = 0;
};

kj::String serializeWorkerBundle(const WorkerBundle& bundle) {
  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":1,\"worker_version\":"_kj);
  appendJsonString(output, bundle.workerVersion);
  output.addAll(",\"compatibility_date\":"_kj);
  appendJsonString(output, bundle.compatibilityDate);
  output.addAll(",\"compatibility_flags\":["_kj);
  bool first = true;
  for (const auto& flag: bundle.compatibilityFlags) {
    if (!first) output.add(',');
    first = false;
    appendJsonString(output, flag);
  }
  output.addAll("],\"main_module\":"_kj);
  appendJsonString(output, bundle.mainModule);
  output.addAll(",\"modules\":["_kj);
  first = true;
  for (const auto& module: bundle.modules) {
    if (!first) output.add(',');
    first = false;
    output.addAll("{\"name\":"_kj);
    appendJsonString(output, module.name);
    output.addAll(",\"type\":"_kj);
    appendJsonString(output, moduleTypeName(module.type));
    output.addAll(",\"source\":"_kj);
    appendJsonString(output, module.source);
    output.add('}');
  }
  output.addAll("]}"_kj);
  KJ_REQUIRE(output.size() <= MAX_ENVELOPE_BYTES, "init envelope exceeds limit");
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

kj::String serializeResponse(kj::StringPtr requestId, const Response& response) {
  KJ_REQUIRE(response.statusCode >= 100 && response.statusCode <= 599, "invalid response status");
  KJ_REQUIRE(response.headers.size() <= MAX_HEADERS, "too many response headers");
  KJ_REQUIRE(response.body.size() <= MAX_BODY_BYTES, "response body exceeds size limit");

  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
  appendJsonString(output, requestId);
  output.addAll(",\"status\":"_kj);
  output.addAll(kj::str(response.statusCode));
  output.addAll(",\"headers\":["_kj);
  size_t aggregateSize = 0;
  bool first = true;
  for (const auto& header: response.headers) {
    KJ_REQUIRE(
        validHeaderName(header.name) && validHeaderValue(header.value), "invalid response header");
    aggregateSize += header.name.size() + header.value.size();
    KJ_REQUIRE(aggregateSize <= MAX_HEADER_BYTES, "response headers exceed size limit");
    if (!first) output.add(',');
    first = false;
    output.addAll("{\"name\":"_kj);
    appendJsonString(output, header.name);
    output.addAll(",\"value\":"_kj);
    appendJsonString(output, header.value);
    output.add('}');
  }
  output.addAll("],\"body_base64\":"_kj);
  auto body = kj::encodeBase64(response.body.asBytes());
  appendJsonString(output, body);
  output.addAll("}\n"_kj);
  KJ_REQUIRE(output.size() <= MAX_ENVELOPE_BYTES + 1, "response envelope exceeds size limit");
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

template <typename Write>
int writeProtocolMessageWith(int fd, kj::ArrayPtr<const char> message, Write&& writeFunction) {
  if (message.size() < 2 || message.size() > MAX_ENVELOPE_BYTES + 1 ||
      message[message.size() - 2] != '}' || message[message.size() - 1] != '\n') {
    return -1;
  }
  for (auto c: message.first(message.size() - 1)) {
    if (c == '\n') return -1;
  }
  return writeAllWith(fd, message, kj::fwd<Write>(writeFunction));
}

int writeProtocolMessage(int fd, kj::ArrayPtr<const char> message) {
  return writeProtocolMessageWith(
      fd, message, [](int fd, const void* buffer, size_t size) { return write(fd, buffer, size); });
}

class Executor {
 public:
  void initialize(kj::ArrayPtr<const char> initJson) {
    KJ_REQUIRE(runtime == kj::none, "executor already initialized");
    auto bundle = parseWorkerBundle(initJson);
    KJ_REQUIRE(serializeWorkerBundle(bundle) == initJson, "init envelope is not canonical JSON");
    runtime = kj::heap<SandboxRuntime>(bundle, kj::rc<StreamingV2FetchBroker>());
    workerVersion = kj::mv(bundle.workerVersion);
  }

  kj::String fetch(kj::ArrayPtr<const char> requestJson) {
    auto& worker = KJ_REQUIRE_NONNULL(runtime, "executor is not initialized");
    auto request = parseRequest(requestJson);
    auto response = worker->runRequest(request.method, request.url, request.headers, request.body);
    return serializeResponse(request.requestId, response);
  }

 private:
  kj::Maybe<kj::Own<SandboxRuntime>> runtime;
  kj::Maybe<kj::String> workerVersion;
};

Executor executor;

int dispatch(const uint8_t* call, size_t callLength) {
  try {
    auto argument = KJ_REQUIRE_NONNULL(firstStringArgument(call, callLength), "missing argument");
    if (nameEquals(call, callLength, "init"_kj)) {
      executor.initialize(argument);
      return 0;
    }
    if (nameEquals(call, callLength, "fetch"_kj)) {
      auto response = executor.fetch(argument);
      return writeProtocolMessage(protocolOutputFd, response.slice(0, response.size()));
    }
    return -1;
  } catch (...) {
    return -1;
  }
}

class SelfTestTimer final: public TimerChannel {
 public:
  void syncTime() override {}
  kj::Date now(kj::Maybe<kj::Date>) override {
    return kj::UNIX_EPOCH;
  }
  kj::Promise<void> atTime(kj::Date) override {
    return kj::READY_NOW;
  }
  kj::Promise<void> afterLimitTimeout(kj::Duration) override {
    ++waits;
    if (block) return kj::NEVER_DONE;
    return kj::READY_NOW;
  }

  size_t waits = 0;
  bool block = false;
};

class SelfTestInputStream final: public kj::AsyncInputStream {
 public:
  explicit SelfTestInputStream(kj::ArrayPtr<const byte> data): data(data) {}

  kj::Promise<size_t> tryRead(void* buffer, size_t, size_t maxBytes) override {
    auto output = kj::arrayPtr(static_cast<byte*>(buffer), maxBytes);
    auto amount = kj::min(data.size(), output.size());
    output.first(amount).copyFrom(data.first(amount));
    data = data.slice(amount);
    return amount;
  }

 private:
  kj::ArrayPtr<const byte> data;
};

class CapturingOutput final: public kj::AsyncOutputStream {
 public:
  explicit CapturingOutput(kj::Vector<byte>& body): body(body) {}

  kj::Promise<void> write(kj::ArrayPtr<const byte> buffer) override {
    body.addAll(buffer);
    return kj::READY_NOW;
  }
  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    for (auto piece: pieces) {
      body.addAll(piece);
    }
    return kj::READY_NOW;
  }
  kj::Promise<void> whenWriteDisconnected() override {
    return kj::NEVER_DONE;
  }

 private:
  kj::Vector<byte>& body;
};

class CapturingResponse final: public kj::HttpService::Response {
 public:
  kj::Own<kj::AsyncOutputStream> send(uint statusCode,
      kj::StringPtr,
      const kj::HttpHeaders& sourceHeaders,
      kj::Maybe<uint64_t> expectedBodySize) override {
    this->statusCode = statusCode;
    this->expectedBodySize = expectedBodySize;
    sourceHeaders.forEach([&](kj::StringPtr name, kj::StringPtr value) {
      headers.add(Header{kj::str(name), kj::str(value)});
    });
    return kj::heap<CapturingOutput>(body);
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders&) override {
    KJ_FAIL_REQUIRE("outbound fetch unexpectedly returned a WebSocket");
  }

  uint statusCode = 0;
  kj::Maybe<uint64_t> expectedBodySize;
  kj::Vector<Header> headers;
  kj::Vector<byte> body;
};

class FakeV2FetchHostChannel final: public V2FetchHostChannel {
 public:
  explicit FakeV2FetchHostChannel(kj::String responseHeaders, kj::String responseBody)
      : responseHeaders(kj::mv(responseHeaders)),
        responseBody(kj::mv(responseBody)) {}

  kj::String start(kj::StringPtr metadata) override {
    ++startCount;
    startMetadata = kj::str(metadata);
    if (startFailure != kj::none) {
      return kj::str(
          R"JSON({"protocol_version":2,"operation_id":0,"max_write_chunk":0,"max_read_chunk":0,"error":{"code":"policy_denied","message":"blocked by host policy"}})JSON");
    }
    return kj::str(
        R"JSON({"protocol_version":2,"operation_id":7,"max_write_chunk":5,"max_read_chunk":7,"error":null})JSON");
  }

  int32_t write(uint64_t operationId, kj::ArrayPtr<const byte> bytes) override {
    KJ_REQUIRE(operationId == 7, "fake v2 write used wrong operation");
    KJ_IF_SOME(limit, epipeAfterBytes) {
      if (writes.size() >= limit) return -EPIPE;
    }
    if (eagainWrites > 0) {
      --eagainWrites;
      return -EAGAIN;
    }
    auto amount = kj::min(bytes.size(), maxWriteAccept);
    writes.addAll(bytes.first(amount));
    return static_cast<int32_t>(amount);
  }

  int32_t finish(uint64_t operationId) override {
    KJ_REQUIRE(operationId == 7, "fake v2 finish used wrong operation");
    ++finishCount;
    return earlyResponse ? -EPIPE : 0;
  }

  kj::String poll(uint64_t operationId) override {
    KJ_REQUIRE(operationId == 7, "fake v2 poll used wrong operation");
    ++pollCount;
    if (malformedPoll) return kj::str("{}");
    if (timeoutPoll) {
      return kj::str(
          R"JSON({"protocol_version":2,"operation_id":7,"state":"complete","response":null,"error":{"code":"timeout","message":"host deadline expired"}})JSON");
    }
    if (pollCount == 1) {
      return kj::str(
          R"JSON({"protocol_version":2,"operation_id":7,"state":"uploading","response":null,"error":null})JSON");
    }
    return kj::str(
        R"JSON({"protocol_version":2,"operation_id":7,"state":"response","response":{"protocol_version":2,"request_id":"r-1","status":201,"header_block_length":)JSON",
        responseHeaders.size(), R"JSON(,"body_length":)JSON", responseBody.size(),
        R"JSON(},"error":null})JSON");
  }

  kj::Array<byte> read(uint64_t operationId, size_t maxBytes) override {
    KJ_REQUIRE(operationId == 7 && maxBytes == 7, "fake v2 read ignored negotiated limit");
    if (pendingReads > 0) {
      --pendingReads;
      auto result = kj::heapArray<byte>(1);
      result[0] = 0;
      return result;
    }
    auto all = kj::str(responseHeaders, responseBody);
    if (readOffset == all.size()) {
      auto result = kj::heapArray<byte>(1);
      result[0] = 2;
      return result;
    }
    auto amount = kj::min(maxBytes, all.size() - readOffset);
    auto result = kj::heapArray<byte>(amount + 1);
    result[0] = 1;
    memcpy(result.begin() + 1, all.begin() + readOffset, amount);
    readOffset += amount;
    return result;
  }

  int32_t cancel(uint64_t operationId) override {
    KJ_REQUIRE(operationId == 7, "fake v2 cancel used wrong operation");
    ++cancelCount;
    return 0;
  }

  kj::String responseHeaders;
  kj::String responseBody;
  kj::Maybe<bool> startFailure;
  bool malformedPoll = false;
  bool timeoutPoll = false;
  bool earlyResponse = false;
  size_t eagainWrites = 0;
  size_t maxWriteAccept = static_cast<size_t>(-1);
  kj::Maybe<size_t> epipeAfterBytes;
  size_t pendingReads = 0;
  size_t startCount = 0;
  size_t finishCount = 0;
  size_t pollCount = 0;
  size_t cancelCount = 0;
  size_t readOffset = 0;
  kj::String startMetadata;
  kj::Vector<byte> writes;
};

int selfTest() {
  auto expectRejected = [](kj::ArrayPtr<const char> input) {
    Executor executor;
    try {
      executor.initialize(input);
    } catch (...) {
      return;
    }
    KJ_FAIL_REQUIRE("invalid init envelope was accepted");
  };

  expectRejected(
      R"JSON({ "protocol_version":1,"worker_version":"noncanonical","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":1,"worker_version":"unknown-field","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","unknown":true,"modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":1,"worker_version":"duplicate-module","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"},{"name":"worker.js","type":"esModule","source":"export default {}"}]})JSON"_kj);
  auto oversized = kj::heapArray<char>(MAX_ENVELOPE_BYTES + 1);
  memset(oversized.begin(), 'x', oversized.size());
  expectRejected(oversized.asPtr());

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestTimer timer;
    auto fake = kj::heap<FakeV2FetchHostChannel>(
        kj::str(
            R"JSON([{"name":"x-duplicate","value":"one"},{"name":"x-duplicate","value":"two"}])JSON"),
        kj::str("response-body"));
    auto& fakeRef = *fake;
    fakeRef.eagainWrites = 1;
    fakeRef.maxWriteAccept = 3;
    fakeRef.pendingReads = 1;
    auto broker = kj::rc<StreamingV2FetchBroker>(kj::mv(fake));

    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders requestHeaders(headerTable);
    requestHeaders.addPtrPtr("x-duplicate"_kj, "request-one"_kj);
    requestHeaders.addPtrPtr("x-duplicate"_kj, "request-two"_kj);
    auto headers = kj::heapArrayBuilder<Header>(2);
    headers.add(Header{kj::str("x-duplicate"), kj::str("request-one")});
    headers.add(Header{kj::str("x-duplicate"), kj::str("request-two")});
    auto requestBodyText = kj::str("hello");
    SelfTestInputStream requestBody(requestBodyText.asBytes());
    CapturingResponse response;
    broker
        ->request(FetchRequest{kj::str("r-1"), kj::HttpMethod::POST,
                    kj::str("https://example.test/"), headers.finish(), requestBodyText.size()},
            requestHeaders, requestBody, response, timer)
        .wait(waitScope);

    auto expectedRequestHeaders = kj::str(
        R"JSON([{"name":"x-duplicate","value":"request-one"},{"name":"x-duplicate","value":"request-two"}])JSON");
    KJ_REQUIRE(fakeRef.startMetadata ==
            R"JSON({"protocol_version":2,"request_id":"r-1","method":"POST","url":"https://example.test/","header_block_length":91,"body_length":5,"preferred_write_chunk":32768,"preferred_read_chunk":32768})JSON"_kj,
        "v2 start metadata self-test failed", fakeRef.startMetadata);
    KJ_REQUIRE(fakeRef.writes.asPtr() == kj::str(expectedRequestHeaders, requestBodyText).asBytes(),
        "v2 request streaming self-test failed");
    KJ_REQUIRE(fakeRef.finishCount == 1 && fakeRef.cancelCount == 0 && timer.waits >= 3,
        "v2 backpressure or completion self-test failed");
    KJ_REQUIRE(response.statusCode == 201 &&
            KJ_REQUIRE_NONNULL(response.expectedBodySize) == fakeRef.responseBody.size() &&
            response.headers.size() == 2 && response.headers[0].name == "x-duplicate"_kj &&
            response.headers[0].value == "one"_kj && response.headers[1].name == "x-duplicate"_kj &&
            response.headers[1].value == "two"_kj &&
            response.body.asPtr() == fakeRef.responseBody.asBytes(),
        "v2 response streaming self-test failed");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestTimer timer;
    auto fake = kj::heap<FakeV2FetchHostChannel>(kj::str("[]"), kj::str("early"));
    auto& fakeRef = *fake;
    fakeRef.epipeAfterBytes = 2;
    auto broker = kj::rc<StreamingV2FetchBroker>(kj::mv(fake));
    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    auto requestBodyText = kj::str("not-uploaded");
    SelfTestInputStream requestBody(requestBodyText.asBytes());
    CapturingResponse response;
    broker
        ->request(
            FetchRequest{kj::str("r-1"), kj::HttpMethod::POST, kj::str("https://early.example/"),
              kj::heapArray<Header>(0), requestBodyText.size()},
            headers, requestBody, response, timer)
        .wait(waitScope);
    KJ_REQUIRE(fakeRef.writes.asPtr() == "[]"_kjb && fakeRef.finishCount == 0 &&
            response.body.asPtr() == "early"_kjb,
        "v2 early response self-test failed");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestTimer timer;
    auto fake = kj::heap<FakeV2FetchHostChannel>(kj::str("[]"), kj::str(""));
    auto& fakeRef = *fake;
    fakeRef.timeoutPoll = true;
    auto broker = kj::rc<StreamingV2FetchBroker>(kj::mv(fake));
    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    SelfTestInputStream requestBody{kj::ArrayPtr<const byte>()};
    CapturingResponse response;
    try {
      broker
          ->request(FetchRequest{kj::str("r-1"), kj::HttpMethod::GET,
                      kj::str("https://timeout.example/"), kj::heapArray<Header>(0), uint64_t(0)},
              headers, requestBody, response, timer)
          .wait(waitScope);
      KJ_FAIL_REQUIRE("v2 timeout self-test was accepted");
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getType() == kj::Exception::Type::OVERLOADED,
          "v2 timeout used wrong exception type");
    }
    KJ_REQUIRE(fakeRef.cancelCount == 0, "completed v2 timeout was redundantly cancelled");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestTimer timer;
    auto fake = kj::heap<FakeV2FetchHostChannel>(kj::str("[]"), kj::str(""));
    auto& fakeRef = *fake;
    fakeRef.startFailure = true;
    auto broker = kj::rc<StreamingV2FetchBroker>(kj::mv(fake));
    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    SelfTestInputStream requestBody{kj::ArrayPtr<const byte>()};
    CapturingResponse response;
    try {
      broker
          ->request(FetchRequest{kj::str("r-1"), kj::HttpMethod::GET,
                      kj::str("https://denied.example/"), kj::heapArray<Header>(0), uint64_t(0)},
              headers, requestBody, response, timer)
          .wait(waitScope);
      KJ_FAIL_REQUIRE("v2 policy denial self-test was accepted");
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getType() == kj::Exception::Type::FAILED,
          "v2 policy denial used wrong exception type");
    }
    KJ_REQUIRE(fakeRef.startCount == 1 && fakeRef.writes.size() == 0,
        "v2 policy denial transmitted request data");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestTimer timer;
    auto fake = kj::heap<FakeV2FetchHostChannel>(kj::str("[]"), kj::str(""));
    auto& fakeRef = *fake;
    fakeRef.malformedPoll = true;
    auto broker = kj::rc<StreamingV2FetchBroker>(kj::mv(fake));
    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    SelfTestInputStream requestBody{kj::ArrayPtr<const byte>()};
    CapturingResponse response;
    try {
      broker
          ->request(FetchRequest{kj::str("r-1"), kj::HttpMethod::GET,
                      kj::str("https://malformed.example/"), kj::heapArray<Header>(0), uint64_t(0)},
              headers, requestBody, response, timer)
          .wait(waitScope);
      KJ_FAIL_REQUIRE("malformed v2 poll self-test was accepted");
    } catch (const kj::Exception&) {}
    KJ_REQUIRE(fakeRef.cancelCount == 1, "malformed v2 operation was not cancelled");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestTimer timer;
    timer.block = true;
    auto fake = kj::heap<FakeV2FetchHostChannel>(kj::str("[]"), kj::str(""));
    auto& fakeRef = *fake;
    auto broker = kj::rc<StreamingV2FetchBroker>(kj::mv(fake));
    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    SelfTestInputStream requestBody{kj::ArrayPtr<const byte>()};
    CapturingResponse response;
    {
      auto pending = broker
                         ->request(FetchRequest{kj::str("r-1"), kj::HttpMethod::GET,
                                     kj::str("https://cancel.example/"), kj::heapArray<Header>(0),
                                     uint64_t(0)},
                             headers, requestBody, response, timer)
                         .eagerlyEvaluate(nullptr);
      KJ_REQUIRE(!pending.poll(waitScope), "v2 cancellation self-test unexpectedly completed");
      KJ_REQUIRE(timer.waits == 1, "v2 cancellation self-test did not become pending");
    }
    KJ_REQUIRE(fakeRef.cancelCount == 1, "dropping v2 request did not cancel host operation");
  }

  auto expectHostProtocolRejected = [](kj::FunctionParam<void()> parse) {
    try {
      parse();
    } catch (const kj::Exception&) {
      return;
    }
    KJ_FAIL_REQUIRE("invalid outbound fetch host response was accepted");
  };
  expectHostProtocolRejected([]() {
    parseV2Start(
        R"JSON({"protocol_version":2,"operation_id":1,"max_write_chunk":61441,"max_read_chunk":32768,"error":null})JSON"_kj);
  });
  expectHostProtocolRejected([]() {
    parseV2Poll(
        R"JSON({"protocol_version":2,"operation_id":7,"state":"response","response":{"protocol_version":2,"request_id":"r-1","status":200,"header_block_length":65537,"body_length":null},"error":null})JSON"_kj,
        7, "r-1"_kj);
  });
  expectHostProtocolRejected([]() {
    parseV2Poll(
        R"JSON({"protocol_version":2,"operation_id":8,"state":"uploading","response":null,"error":null})JSON"_kj,
        7, "r-1"_kj);
  });
  expectHostProtocolRejected([]() {
    parseV2Poll(
        R"JSON({"protocol_version":2,"operation_id":7,"state":"response","response":{"protocol_version":2,"request_id":"another-request","status":200,"header_block_length":2,"body_length":0},"error":null})JSON"_kj,
        7, "r-1"_kj);
  });
  expectHostProtocolRejected([]() {
    FetchRequest request{kj::str("r-1"), kj::HttpMethod::GET,
      kj::str(kj::repeat('x', MAX_OUTBOUND_FETCH_URL_BYTES + 1)), kj::heapArray<Header>(0),
      uint64_t(0)};
    serializeV2Metadata(request, 2);
  });

  {
    kj::Vector<char> body;
    for (size_t i = 0; i < 3 * MAX_PROTOCOL_WRITE_BYTES; ++i) {
      body.add('x');
    }
    body.add('\0');
    auto serialized = serializeResponse("large-writer-test"_kj,
        Response{200, kj::heapArray<Header>(0), kj::String(body.releaseAsArray())});
    KJ_REQUIRE(serialized.size() > 4096, "protocol writer self-test response is too small");

    int fds[2];
    KJ_REQUIRE(pipe(fds) == 0, "failed to create protocol writer self-test pipe");
    auto child = fork();
    KJ_REQUIRE(child >= 0, "failed to fork protocol writer self-test reader");
    if (child == 0) {
      close(fds[1]);
      size_t offset = 0;
      char buffer[777];
      for (;;) {
        auto amount = read(fds[0], buffer, sizeof(buffer));
        if (amount < 0 && errno == EINTR) continue;
        if (amount < 0 || offset + static_cast<size_t>(amount) > serialized.size() ||
            memcmp(serialized.begin() + offset, buffer, static_cast<size_t>(amount)) != 0) {
          _exit(1);
        }
        if (amount == 0) break;
        offset += static_cast<size_t>(amount);
      }
      _exit(offset == serialized.size() ? 0 : 1);
    }

    close(fds[0]);
    auto writeResult = writeProtocolMessage(fds[1], serialized.slice(0, serialized.size()));
    close(fds[1]);
    int status;
    KJ_REQUIRE(waitpid(child, &status, 0) == child, "failed to wait for protocol writer self-test");
    KJ_REQUIRE(writeResult == 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0,
        "protocol writer self-test failed");

    kj::Vector<char> captured;
    size_t writes = 0;
    auto shortWriteResult = writeProtocolMessageWith(
        -1, serialized.slice(0, serialized.size()), [&](int, const void* buffer, size_t size) {
      KJ_REQUIRE(size <= MAX_PROTOCOL_WRITE_BYTES, "protocol writer emitted an oversized chunk");
      auto amount = kj::min(size, size_t(257));
      captured.addAll(kj::arrayPtr(static_cast<const char*>(buffer), amount));
      ++writes;
      return static_cast<ssize_t>(amount);
    });
    KJ_REQUIRE(shortWriteResult == 0 && writes > 1 &&
            captured.asPtr() == serialized.slice(0, serialized.size()),
        "protocol writer short-write handling failed");
  }

  auto run = [](WorkerBundle bundle, kj::StringPtr request,
                 kj::FunctionParam<void(kj::StringPtr)> check) {
    Executor executor;
    auto init = serializeWorkerBundle(bundle);
    executor.initialize(init);
    auto serializedResponse = executor.fetch(request);
    KJ_REQUIRE(serializedResponse.size() >= 2 &&
            serializedResponse[serializedResponse.size() - 2] == '}' &&
            serializedResponse[serializedResponse.size() - 1] == '\n',
        "self-test response must end with a compact JSON object followed by LF");

    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder message;
    auto root = message.initRoot<capnp::JsonValue>();
    codec.decodeRaw(serializedResponse, root);
    KJ_REQUIRE(root.isObject(), "self-test response must be an object");
    for (auto field: root.getObject()) {
      if (field.getName() == "body_base64"_kj) {
        KJ_REQUIRE(field.getValue().isString(), "invalid self-test response body");
        auto decoded = kj::decodeBase64(field.getValue().getString());
        KJ_REQUIRE(!decoded.hadErrors, "invalid self-test response encoding");
        check(copyBytes(decoded));
        return;
      }
    }
    KJ_FAIL_REQUIRE("self-test response is missing body");
  };

  auto noFlags = []() { return kj::heapArray<kj::String>(0); };
  auto oneModule = [](kj::StringPtr name, kj::StringPtr source) {
    auto modules = kj::heapArrayBuilder<Module>(1);
    modules.add(Module{kj::str(name), ModuleType::ES_MODULE, kj::str(source)});
    return modules.finish();
  };

  run(WorkerBundle{kj::str("helloworld-v1"), kj::str("2023-02-28"), noFlags(), kj::str("worker.js"),
        oneModule("worker.js"_kj, HELLOWORLD_WORKER)},
      R"JSON({"protocol_version":1,"request_id":"hello","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body == "Hello World\n"_kj, "helloworld_esm self-test failed");
  });

  auto streamModules = kj::heapArrayBuilder<Module>(2);
  streamModules.add(
      Module{kj::str("worker.js"), ModuleType::ES_MODULE, kj::str(WEB_STREAMS_WORKER)});
  streamModules.add(Module{kj::str("streams-util"), ModuleType::ES_MODULE, kj::str(STREAMS_UTIL)});
  run(WorkerBundle{kj::str("web-streams-v1"), kj::str("2025-12-31"), noFlags(),
        kj::str("worker.js"), streamModules.finish()},
      R"JSON({"protocol_version":1,"request_id":"streams","method":"GET","url":"https://example.test/sync","headers":[],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body.size() > 0, "web-streams self-test returned no data");
    for (char c: body) {
      KJ_REQUIRE(c < 'a' || c > 'z', "web-streams transform did not uppercase its output");
    }
  });

  run(WorkerBundle{kj::str("wintertc-smoke-v1"), kj::str("2025-12-31"), noFlags(),
        kj::str("worker.js"), oneModule("worker.js"_kj, WINTERTC_SMOKE)},
      R"JSON({"protocol_version":1,"request_id":"wintertc","method":"POST","url":"https://example.test/wintertc-smoke","headers":[{"name":"x-smoke","value":"yes"}],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body ==
            R"JSON({"smoke":"WinterTC minimum-common API smoke (not conformance)","url":true,"urlPattern":true,"request":true,"response":true,"headers":true,"formData":true,"blob":true,"textCodec":true,"cryptoDigest":true,"cryptoRandom":true,"readableStream":true,"transformStream":true,"compression":true,"performance":true,"webAssembly":"blocked by executor embedder policy","timers":"not exercised by smoke","outboundFetch":"not exercised by smoke","capabilityBackedUnavailable":["WebSocket","connect","bindings","actors"]})JSON"_kj,
        "WinterTC API smoke self-test failed", body);
  });

  return writeProtocolMessage(protocolOutputFd,
      R"JSON({"protocol_version":1,"self_test":"passed","bundles":["samples/helloworld_esm","samples/web-streams","wintertc-api-smoke"],"wintertc_label":"smoke-not-conformance"})JSON"
      "\n"_kj);
}

}  // namespace
}  // namespace workerd::server::sandbox_executor

int main(int argc, char** argv) {
  using namespace workerd::server::sandbox_executor;
  if (argc == 2 && kj::StringPtr(argv[1]) == "--self-test"_kj) {
    _exit(selfTest());
  }
  if (argc != 1 || isolateProtocolOutput() != 0 || initializeDriver() != 0) return 1;
  runDriver(dispatch);
}
