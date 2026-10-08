// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "hyperlight-driver.h"
#include "sandbox-logical-service.h"
#include "sandbox-runtime.h"
#include "sandbox-websocket.h"
#include "src/workerd/server/helloworld_worker.embed.h"
#include "src/workerd/server/streams_util.embed.h"
#include "src/workerd/server/tests/filesystem_evidence.embed.h"
#include "src/workerd/server/web_streams_worker.embed.h"
#include "src/workerd/server/wintertc_smoke.embed.h"
#include "workerd_runtime_contract.embed.h"

#include <workerd/io/compatibility-date.h>
#include <workerd/rust/sandbox-executor/lib.rs.h>

#include <openssl/sha.h>
#include <simdutf.h>
#include <strings.h>
#include <sys/wait.h>
#include <unistd.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/encoding.h>

#include <cmath>
#include <cstdio>

namespace workerd::server::sandbox_executor {
namespace {

namespace composite = logical_service_broker::composite;

class GuestExceptionCallback final: public kj::ExceptionCallback {
 public:
  StackTraceMode stackTraceMode() override {
    return StackTraceMode::ADDRESS_ONLY;
  }
};

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
constexpr size_t MAX_MODULE_SOURCE_BYTES = 48 * 1024;
constexpr size_t MAX_MODULE_SOURCES_BYTES = 48 * 1024;
constexpr size_t MAX_STORAGE_MOUNTS = 8;
constexpr size_t MAX_STORAGE_NAME_BYTES = 64;
constexpr size_t MAX_BINDINGS = 64;
constexpr size_t MAX_CRON_BYTES = 256;
constexpr size_t MAX_QUEUE_NAME_BYTES = 128;
constexpr size_t MAX_QUEUE_MESSAGES = 100;
constexpr size_t MAX_HOST_CALL_CHUNK_BYTES = 60 * 1024;
constexpr uint64_t MAX_HOST_OPERATION_ID = 9'007'199'254'740'991;
constexpr kj::Duration FETCH_POLL_INTERVAL = 1 * kj::MILLISECONDS;

namespace rust_protocol = workerd::rust::sandbox_executor;

struct Request {
  kj::String requestId;
  kj::HttpMethod method;
  kj::String url;
  kj::Array<Header> headers;
  kj::String body;
};

struct ScheduledRequest {
  kj::String requestId;
  kj::Date scheduledTime;
  kj::String cron;
};

struct ParsedQueueRequest {
  kj::String requestId;
  QueueRequest request;
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

kj::StringPtr moduleTypeName(ModuleType type) {
  switch (type) {
    case ModuleType::ES_MODULE:
      return "esModule"_kj;
    case ModuleType::COMMON_JS_MODULE:
      return "commonJsModule"_kj;
    case ModuleType::TEXT:
      return "text"_kj;
    case ModuleType::JSON:
      return "json"_kj;
    case ModuleType::WASM:
      KJ_FAIL_REQUIRE("module type is not supported by the legacy protocol");
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

Request parseRequestLegacy(kj::ArrayPtr<const char> input) {
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

ScheduledRequest parseScheduledRequest(kj::ArrayPtr<const char> input) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "scheduled envelope exceeds limit");
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "scheduled envelope must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(fields.size() == 4, "invalid scheduled field count");
  KJ_REQUIRE(fields[0].getName() == "protocol_version"_kj && fields[0].getValue().isNumber() &&
          fields[0].getValue().getNumber() == 1,
      "invalid scheduled protocol version");
  KJ_REQUIRE(fields[1].getName() == "request_id"_kj && fields[1].getValue().isString(),
      "invalid scheduled request ID");
  KJ_REQUIRE(fields[2].getName() == "scheduled_time_unix_ms"_kj && fields[2].getValue().isNumber(),
      "invalid scheduled time");
  KJ_REQUIRE(fields[3].getName() == "cron"_kj && fields[3].getValue().isString(),
      "invalid scheduled cron");

  auto requestId = kj::str(fields[1].getValue().getString());
  KJ_REQUIRE(validIdentifier(requestId, MAX_REQUEST_ID_BYTES), "invalid scheduled request ID");
  auto rawTime = fields[2].getValue().getNumber();
  KJ_REQUIRE(rawTime >= 0 && rawTime <= static_cast<double>(MAX_HOST_OPERATION_ID) &&
          std::floor(rawTime) == rawTime,
      "invalid scheduled time");
  auto cron = kj::str(fields[3].getValue().getString());
  KJ_REQUIRE(cron.size() > 0 && cron.size() <= MAX_CRON_BYTES, "invalid scheduled cron");
  for (char c: cron) {
    KJ_REQUIRE(static_cast<unsigned char>(c) >= 0x20 && c != 0x7f, "invalid scheduled cron");
  }

  return ScheduledRequest{
    .requestId = kj::mv(requestId),
    .scheduledTime = kj::UNIX_EPOCH + static_cast<int64_t>(rawTime) * kj::MILLISECONDS,
    .cron = kj::mv(cron),
  };
}

ParsedQueueRequest parseQueueRequest(kj::ArrayPtr<const char> input) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "queue envelope exceeds limit");
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "queue envelope must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(fields.size() == 5, "invalid queue field count");
  KJ_REQUIRE(fields[0].getName() == "protocol_version"_kj && fields[0].getValue().isNumber() &&
          fields[0].getValue().getNumber() == 1,
      "invalid queue protocol version");
  KJ_REQUIRE(fields[1].getName() == "request_id"_kj && fields[1].getValue().isString(),
      "invalid queue request ID");
  KJ_REQUIRE(
      fields[2].getName() == "queue"_kj && fields[2].getValue().isString(), "invalid queue name");
  KJ_REQUIRE(fields[3].getName() == "messages"_kj && fields[3].getValue().isArray(),
      "invalid queue messages");
  KJ_REQUIRE(fields[4].getName() == "metadata"_kj && fields[4].getValue().isObject(),
      "invalid queue metadata");

  auto requestId = kj::str(fields[1].getValue().getString());
  KJ_REQUIRE(validIdentifier(requestId, MAX_REQUEST_ID_BYTES), "invalid queue request ID");
  auto queueName = kj::str(fields[2].getValue().getString());
  KJ_REQUIRE(validIdentifier(queueName, MAX_QUEUE_NAME_BYTES), "invalid queue name");

  auto messageValues = fields[3].getValue().getArray();
  KJ_REQUIRE(messageValues.size() > 0 && messageValues.size() <= MAX_QUEUE_MESSAGES,
      "invalid queue batch");
  auto messages = kj::heapArrayBuilder<QueueMessage>(messageValues.size());
  size_t aggregateBodyBytes = 0;
  kj::Maybe<kj::StringPtr> previousMessageId;
  for (auto value: messageValues) {
    KJ_REQUIRE(value.isObject(), "invalid queue message");
    auto messageFields = value.getObject();
    KJ_REQUIRE(messageFields.size() == 5 && messageFields[0].getName() == "id"_kj &&
            messageFields[1].getName() == "timestamp_unix_ms"_kj &&
            messageFields[2].getName() == "body_base64"_kj &&
            messageFields[3].getName() == "content_type"_kj &&
            messageFields[4].getName() == "attempts"_kj,
        "noncanonical queue message");
    KJ_REQUIRE(messageFields[0].getValue().isString() && messageFields[1].getValue().isNumber() &&
            messageFields[2].getValue().isString() &&
            (messageFields[3].getValue().isNull() || messageFields[3].getValue().isString()) &&
            messageFields[4].getValue().isNumber(),
        "invalid queue message field type");

    auto id = kj::str(messageFields[0].getValue().getString());
    KJ_REQUIRE(validIdentifier(id, MAX_REQUEST_ID_BYTES), "invalid queue message ID");
    KJ_IF_SOME(previous, previousMessageId) {
      KJ_REQUIRE(
          lexicographicallyBefore(previous, id), "queue message IDs must be sorted and unique");
    }
    previousMessageId = id;
    auto rawTimestamp = messageFields[1].getValue().getNumber();
    KJ_REQUIRE(rawTimestamp >= 0 && rawTimestamp <= static_cast<double>(MAX_HOST_OPERATION_ID) &&
            std::floor(rawTimestamp) == rawTimestamp,
        "invalid queue message timestamp");
    auto encodedBody = kj::str(messageFields[2].getValue().getString());
    auto decodedBody = kj::decodeBase64(encodedBody);
    KJ_REQUIRE(!decodedBody.hadErrors, "invalid queue message body");
    aggregateBodyBytes += decodedBody.size();
    KJ_REQUIRE(aggregateBodyBytes <= MAX_BODY_BYTES, "queue batch body exceeds limit");
    kj::Maybe<kj::String> contentType;
    if (messageFields[3].getValue().isString()) {
      auto value = kj::str(messageFields[3].getValue().getString());
      KJ_REQUIRE(
          value == "text"_kj || value == "bytes"_kj || value == "json"_kj || value == "v8"_kj,
          "invalid queue message content type");
      contentType = kj::mv(value);
    }
    auto rawAttempts = messageFields[4].getValue().getNumber();
    KJ_REQUIRE(rawAttempts >= 0 && rawAttempts <= static_cast<uint16_t>(kj::maxValue) &&
            std::floor(rawAttempts) == rawAttempts,
        "invalid queue message attempts");
    messages.add(QueueMessage{
      .id = kj::mv(id),
      .timestamp = kj::UNIX_EPOCH + static_cast<int64_t>(rawTimestamp) * kj::MILLISECONDS,
      .body = kj::heapArray<kj::byte>(decodedBody.asBytes()),
      .contentType = kj::mv(contentType),
      .attempts = static_cast<uint16_t>(rawAttempts),
    });
  }

  auto metadataFields = fields[4].getValue().getObject();
  KJ_REQUIRE(metadataFields.size() == 3 && metadataFields[0].getName() == "backlog_count"_kj &&
          metadataFields[1].getName() == "backlog_bytes"_kj &&
          metadataFields[2].getName() == "oldest_message_timestamp_unix_ms"_kj &&
          metadataFields[0].getValue().isNumber() && metadataFields[1].getValue().isNumber() &&
          (metadataFields[2].getValue().isNull() || metadataFields[2].getValue().isNumber()),
      "invalid queue metadata");
  auto backlogCount = metadataFields[0].getValue().getNumber();
  auto backlogBytes = metadataFields[1].getValue().getNumber();
  KJ_REQUIRE(std::isfinite(backlogCount) && backlogCount >= 0, "invalid queue backlog count");
  KJ_REQUIRE(std::isfinite(backlogBytes) && backlogBytes >= 0, "invalid queue backlog bytes");
  kj::Maybe<kj::Date> oldestMessageTimestamp;
  if (metadataFields[2].getValue().isNumber()) {
    auto rawTimestamp = metadataFields[2].getValue().getNumber();
    KJ_REQUIRE(rawTimestamp >= 0 && rawTimestamp <= static_cast<double>(MAX_HOST_OPERATION_ID) &&
            std::floor(rawTimestamp) == rawTimestamp,
        "invalid oldest queue message timestamp");
    oldestMessageTimestamp = kj::UNIX_EPOCH + static_cast<int64_t>(rawTimestamp) * kj::MILLISECONDS;
  }

  return {
    .requestId = kj::mv(requestId),
    .request =
        QueueRequest{
          .queueName = kj::mv(queueName),
          .messages = messages.finish(),
          .backlogCount = backlogCount,
          .backlogBytes = backlogBytes,
          .oldestMessageTimestamp = oldestMessageTimestamp,
        },
  };
}

kj::String copyRustString(const ::rust::String& value) {
  auto result = kj::heapArray<char>(value.size() + 1);
  memcpy(result.begin(), value.data(), value.size());
  result[value.size()] = '\0';
  return kj::String(kj::mv(result));
}

kj::Array<byte> copyRustBytes(const ::rust::Vec<uint8_t>& value) {
  auto result = kj::heapArray<byte>(value.size());
  memcpy(result.begin(), value.data(), value.size());
  return result;
}

kj::Array<byte> copyModuleBytes(kj::StringPtr value) {
  auto result = kj::heapArray<byte>(value.size());
  memcpy(result.begin(), value.begin(), value.size());
  return result;
}

::rust::Vec<uint8_t> copyToRustBytes(kj::ArrayPtr<const byte> value) {
  ::rust::Vec<uint8_t> result;
  result.reserve(value.size());
  for (auto byte: value) {
    result.push_back(byte);
  }
  return result;
}

kj::HttpMethod convertMethod(rust_protocol::Method method) {
  switch (method) {
    case rust_protocol::Method::Get:
      return kj::HttpMethod::GET;
    case rust_protocol::Method::Head:
      return kj::HttpMethod::HEAD;
    case rust_protocol::Method::Post:
      return kj::HttpMethod::POST;
    case rust_protocol::Method::Put:
      return kj::HttpMethod::PUT;
    case rust_protocol::Method::Delete:
      return kj::HttpMethod::DELETE;
    case rust_protocol::Method::Patch:
      return kj::HttpMethod::PATCH;
    case rust_protocol::Method::Options:
      return kj::HttpMethod::OPTIONS;
    case rust_protocol::Method::Trace:
      return kj::HttpMethod::TRACE;
    case rust_protocol::Method::Purge:
      return kj::HttpMethod::PURGE;
    default:
      KJ_FAIL_REQUIRE("invalid Rust request method");
  }
}

bool validStorageName(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > MAX_STORAGE_NAME_BYTES) return false;
  for (char c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) return false;
  }
  return value != "."_kj && value != ".."_kj;
}

StorageMode parseStorageMode(kj::StringPtr value) {
  if (value == "ro"_kj) return StorageMode::READ_ONLY;
  if (value == "rw"_kj) return StorageMode::READ_WRITE;
  KJ_FAIL_REQUIRE("unsupported storage mode");
}

kj::StringPtr storageModeName(StorageMode mode) {
  switch (mode) {
    case StorageMode::READ_ONLY:
      return "ro"_kj;
    case StorageMode::READ_WRITE:
      return "rw"_kj;
  }
  KJ_UNREACHABLE;
}

ModuleType parseModuleType(kj::StringPtr value) {
  if (value == "esModule"_kj) return ModuleType::ES_MODULE;
  if (value == "commonJsModule"_kj) return ModuleType::COMMON_JS_MODULE;
  if (value == "text"_kj) return ModuleType::TEXT;
  if (value == "json"_kj) return ModuleType::JSON;
  KJ_FAIL_REQUIRE("unsupported module type");
}

ModuleType convertModuleType(rust_protocol::ModuleType type) {
  switch (type) {
    case rust_protocol::ModuleType::EsModule:
      return ModuleType::ES_MODULE;
    case rust_protocol::ModuleType::Text:
      return ModuleType::TEXT;
    case rust_protocol::ModuleType::Json:
      return ModuleType::JSON;
    case rust_protocol::ModuleType::Wasm:
      return ModuleType::WASM;
    default:
      KJ_FAIL_REQUIRE("invalid Rust module type");
  }
}

rust_protocol::ModuleType convertModuleType(ModuleType type) {
  switch (type) {
    case ModuleType::COMMON_JS_MODULE:
      KJ_FAIL_REQUIRE("module type is not supported by the Rust protocol");
    case ModuleType::ES_MODULE:
      return rust_protocol::ModuleType::EsModule;
    case ModuleType::TEXT:
      return rust_protocol::ModuleType::Text;
    case ModuleType::JSON:
      return rust_protocol::ModuleType::Json;
    case ModuleType::WASM:
      return rust_protocol::ModuleType::Wasm;
  }
  KJ_UNREACHABLE;
}

WorkerBundle convertBundle(rust_protocol::InitBundle input) {
  auto flags = kj::heapArrayBuilder<kj::String>(input.compatibility_flags.size());
  for (const auto& flag: input.compatibility_flags) {
    flags.add(copyRustString(flag));
  }
  auto modules = kj::heapArrayBuilder<Module>(input.modules.size());
  for (const auto& module: input.modules) {
    modules.add(Module{copyRustString(module.name), convertModuleType(module.module_type),
      copyRustBytes(module.source)});
  }
  return WorkerBundle{copyRustString(input.worker_version),
    copyRustString(input.compatibility_date), flags.finish(), copyRustString(input.main_module),
    modules.finish()};
}

rust_protocol::InitBundle convertBundle(const WorkerBundle& input) {
  rust_protocol::InitBundle result;
  result.worker_version = ::rust::String(input.workerVersion.begin(), input.workerVersion.size());
  result.compatibility_date =
      ::rust::String(input.compatibilityDate.begin(), input.compatibilityDate.size());
  for (const auto& flag: input.compatibilityFlags) {
    result.compatibility_flags.push_back(::rust::String(flag.begin(), flag.size()));
  }
  result.main_module = ::rust::String(input.mainModule.begin(), input.mainModule.size());
  for (const auto& module: input.modules) {
    result.modules.push_back(rust_protocol::Module{
      ::rust::String(module.name.begin(), module.name.size()),
      convertModuleType(module.type),
      copyToRustBytes(module.source),
    });
  }
  return result;
}

Request convertRequest(rust_protocol::Request input) {
  auto headers = kj::heapArrayBuilder<Header>(input.headers.size());
  for (const auto& header: input.headers) {
    headers.add(Header{copyRustString(header.name), copyRustString(header.value)});
  }
  return Request{copyRustString(input.request_id), convertMethod(input.method),
    copyRustString(input.url), headers.finish(), copyBytes(copyRustBytes(input.body))};
}

rust_protocol::Response convertResponse(Response input) {
  rust_protocol::Response result;
  KJ_REQUIRE(input.statusCode <= kj::maxValueForBits<16>(), "invalid response status");
  result.status_code = static_cast<uint16_t>(input.statusCode);
  for (const auto& header: input.headers) {
    result.headers.push_back(rust_protocol::Header{
      ::rust::String(header.name.begin(), header.name.size()),
      ::rust::String(header.value.begin(), header.value.size()),
    });
  }
  result.body = copyToRustBytes(input.body.asBytes());
  return result;
}

::rust::Slice<const uint8_t> asRustBytes(kj::ArrayPtr<const char> value) {
  return ::rust::Slice<const uint8_t>(
      reinterpret_cast<const uint8_t*>(value.begin()), value.size());
}

WorkerBundle parseWorkerBundleLegacy(kj::ArrayPtr<const char> input) {
  KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "init envelope exceeds limit");
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "init envelope must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(
      fields.size() == 6 || fields.size() == 7 || fields.size() == 8, "invalid init field count");
  KJ_REQUIRE(fields[0].getName() == "protocol_version"_kj && fields[0].getValue().isNumber(),
      "invalid init protocol version");
  auto rawProtocolVersion = fields[0].getValue().getNumber();
  KJ_REQUIRE(rawProtocolVersion == 1 || rawProtocolVersion == 2 || rawProtocolVersion == 3,
      "invalid init protocol version");
  auto protocolVersion = static_cast<uint>(rawProtocolVersion);
  KJ_REQUIRE(fields.size() == protocolVersion + 5, "init fields do not match protocol version");
  static constexpr kj::StringPtr FIELD_NAMES[] = {
    "protocol_version"_kj,
    "worker_version"_kj,
    "compatibility_date"_kj,
    "compatibility_flags"_kj,
    "main_module"_kj,
    "modules"_kj,
    "storage"_kj,
    "bindings"_kj,
  };
  for (auto i: kj::indices(fields)) {
    KJ_REQUIRE(fields[i].getName() == FIELD_NAMES[i], "noncanonical init field order");
  }

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
    moduleBuilder.add(Module{kj::mv(name), type, copyModuleBytes(source)});
  }

  kj::Array<StorageMount> storageMounts;
  if (protocolVersion >= 2) {
    KJ_REQUIRE(fields[6].getValue().isArray(), "invalid storage manifest");
    auto mountValues = fields[6].getValue().getArray();
    KJ_REQUIRE(mountValues.size() <= MAX_STORAGE_MOUNTS, "too many storage mounts");
    auto mountBuilder = kj::heapArrayBuilder<StorageMount>(mountValues.size());
    kj::Maybe<kj::StringPtr> previousName;
    for (auto value: mountValues) {
      KJ_REQUIRE(value.isObject(), "invalid storage mount");
      auto mountFields = value.getObject();
      KJ_REQUIRE(mountFields.size() == 2 && mountFields[0].getName() == "name"_kj &&
              mountFields[1].getName() == "mode"_kj && mountFields[0].getValue().isString() &&
              mountFields[1].getValue().isString(),
          "invalid storage mount");
      auto name = kj::str(mountFields[0].getValue().getString());
      KJ_REQUIRE(validStorageName(name), "invalid storage name");
      KJ_IF_SOME(previous, previousName) {
        KJ_REQUIRE(
            lexicographicallyBefore(previous, name), "storage names must be sorted and unique");
      }
      previousName = name;
      mountBuilder.add(StorageMount{
        .name = kj::mv(name),
        .mode = parseStorageMode(mountFields[1].getValue().getString()),
      });
    }
    storageMounts = mountBuilder.finish();
  }

  kj::Array<Binding> bindings;
  if (protocolVersion == 3) {
    KJ_REQUIRE(fields[7].getValue().isArray(), "invalid binding manifest");
    auto bindingValues = fields[7].getValue().getArray();
    KJ_REQUIRE(bindingValues.size() <= MAX_BINDINGS, "too many bindings");
    auto bindingBuilder = kj::heapArrayBuilder<Binding>(bindingValues.size());
    kj::Maybe<kj::StringPtr> previousName;
    for (auto value: bindingValues) {
      KJ_REQUIRE(value.isObject(), "invalid binding manifest entry");
      auto bindingFields = value.getObject();
      KJ_REQUIRE(bindingFields.size() == 2 && bindingFields[0].getName() == "name"_kj &&
              bindingFields[1].getName() == "kind"_kj && bindingFields[0].getValue().isString() &&
              bindingFields[1].getValue().isString(),
          "invalid binding manifest entry");
      auto name = kj::str(bindingFields[0].getValue().getString());
      KJ_REQUIRE(composite::isBindingName(name), "invalid binding name");
      KJ_IF_SOME(previous, previousName) {
        KJ_REQUIRE(lexicographicallyBefore(previous, name),
            "binding names must be globally sorted and unique");
      }
      previousName = name;
      bindingBuilder.add(Binding{
        .name = kj::mv(name),
        .kind = composite::parseBindingKind(bindingFields[1].getValue().getString()),
      });
    }
    bindings = bindingBuilder.finish();
  }

  return WorkerBundle{
    .workerVersion = kj::mv(workerVersion),
    .compatibilityDate = kj::mv(compatibilityDate),
    .compatibilityFlags = flagBuilder.finish(),
    .mainModule = kj::mv(mainModule),
    .modules = moduleBuilder.finish(),
    .protocolVersion = protocolVersion,
    .storageMounts = kj::mv(storageMounts),
    .bindings = kj::mv(bindings),
  };
}

kj::String copyProtocolOutput(::rust::Vec<uint8_t> value) {
  return copyBytes(kj::arrayPtr(value.data(), value.size()));
}

bool usesRustProtocol(kj::ArrayPtr<const char> input) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  if (!root.isObject()) return true;
  for (auto field: root.getObject()) {
    if (field.getName() == "protocol_version" && field.getValue().isNumber() &&
        field.getValue().getNumber() != 1) {
      return false;
    }
    if (field.getName() != "modules" || !field.getValue().isArray()) continue;
    for (auto module: field.getValue().getArray()) {
      if (!module.isObject()) continue;
      for (auto moduleField: module.getObject()) {
        if (moduleField.getName() == "type" && moduleField.getValue().isString() &&
            moduleField.getValue().getString() == "commonJsModule"_kj) {
          return false;
        }
      }
    }
  }
  return true;
}

bool usesRustProtocol(const WorkerBundle& bundle) {
  if (bundle.protocolVersion != 1 || bundle.storageMounts.size() != 0) return false;
  for (const auto& module: bundle.modules) {
    if (module.type == ModuleType::COMMON_JS_MODULE) return false;
  }
  return true;
}

WorkerBundle parseWorkerBundle(kj::ArrayPtr<const char> input) {
  return parseWorkerBundleLegacy(input);
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
      return JSG_KJ_EXCEPTION(FAILED, Error, "outbound fetch denied by host: ", failure.message);
    case FetchError::TIMEOUT:
      return JSG_KJ_EXCEPTION(OVERLOADED, Error, "outbound fetch timed out: ", failure.message);
    case FetchError::CANCELED:
      return JSG_KJ_EXCEPTION(
          DISCONNECTED, DOMAbortError, "outbound fetch canceled: ", failure.message);
    case FetchError::MALFORMED_RESPONSE:
      return JSG_KJ_EXCEPTION(
          FAILED, Error, "host returned a malformed outbound fetch response: ", failure.message);
    case FetchError::HOST_FAILURE:
      return JSG_KJ_EXCEPTION(FAILED, Error, "outbound fetch host call failed: ", failure.message);
    case FetchError::OVERLOADED:
      return JSG_KJ_EXCEPTION(
          OVERLOADED, Error, "outbound fetch host is overloaded: ", failure.message);
    case FetchError::SIZE_LIMIT:
      return JSG_KJ_EXCEPTION(
          FAILED, Error, "outbound fetch exceeded host size limit: ", failure.message);
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

  bool isQuiescent() const override {
    return activeOperations == 0 && !cleanupFailed;
  }

  kj::Promise<void> request(FetchRequest request,
      const kj::HttpHeaders& requestHeaders,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& httpResponse,
      TimerChannel& timer) override {
    if (activeOperations >= MAX_CONCURRENT_OUTBOUND_FETCHES) {
      kj::throwRecoverableException(JSG_KJ_EXCEPTION(
          OVERLOADED, Error, "overloaded: too many concurrent outbound fetch v2 operations"));
      KJ_UNREACHABLE;
    }
    ++activeOperations;
    KJ_DEFER(--activeOperations);
    auto headerBlock = serializeFetchHeaderBlock(request.headers);
    auto start = [&]() {
      KJ_ON_SCOPE_FAILURE(cleanupFailed = true);
      return parseV2Start(channel->start(serializeV2Metadata(request, headerBlock.size())));
    }();
    KJ_IF_SOME(error, start.error) {
      kj::throwRecoverableException(fetchFailureToException(kj::mv(error)));
      KJ_UNREACHABLE;
    }

    struct Operation: public kj::Refcounted {
      Operation(V2FetchHostChannel& channel,
          uint64_t id,
          size_t writeChunk,
          size_t readChunk,
          bool& cleanupFailed)
          : channel(channel),
            id(id),
            writeChunk(writeChunk),
            readChunk(readChunk),
            cleanupFailed(cleanupFailed) {}
      ~Operation() noexcept {
        if (!collected) {
          try {
            auto result = channel.cancel(id);
            if (result != 0 && result != -ENOENT) {
              cleanupFailed = true;
              KJ_LOG(ERROR, "v2 outbound fetch cancellation failed", id, result);
            }
          } catch (const kj::Exception& exception) {
            cleanupFailed = true;
            KJ_LOG(ERROR, "v2 outbound fetch cancellation host call failed", id, exception);
          }
        }
      }
      V2FetchHostChannel& channel;
      uint64_t id;
      size_t writeChunk;
      size_t readChunk;
      bool collected = false;
      bool& cleanupFailed;
    };
    auto operation = kj::rc<Operation>(
        *channel, start.operationId, start.maxWriteChunk, start.maxReadChunk, cleanupFailed);

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
  bool cleanupFailed = false;
};

kj::String serializeWorkerBundleLegacy(const WorkerBundle& bundle) {
  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":"_kj);
  output.addAll(kj::str(bundle.protocolVersion));
  output.addAll(",\"worker_version\":"_kj);
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
    auto moduleSourceText = kj::str(module.source.asPtr().asChars());
    appendJsonString(output, moduleSourceText);
    output.add('}');
  }
  output.add(']');
  if (bundle.protocolVersion >= 2) {
    output.addAll(",\"storage\":["_kj);
    first = true;
    for (const auto& mount: bundle.storageMounts) {
      if (!first) output.add(',');
      first = false;
      output.addAll("{\"name\":"_kj);
      appendJsonString(output, mount.name);
      output.addAll(",\"mode\":"_kj);
      appendJsonString(output, storageModeName(mount.mode));
      output.add('}');
    }
    output.add(']');
  } else {
    KJ_REQUIRE(bundle.protocolVersion == 1 && bundle.storageMounts.size() == 0 &&
            bundle.bindings.size() == 0,
        "protocol v1 cannot contain storage mounts");
  }
  if (bundle.protocolVersion == 3) {
    output.addAll(",\"bindings\":["_kj);
    first = true;
    for (const auto& binding: bundle.bindings) {
      if (!first) output.add(',');
      first = false;
      output.addAll("{\"name\":"_kj);
      appendJsonString(output, binding.name);
      output.addAll(",\"kind\":"_kj);
      appendJsonString(output, composite::bindingKindName(binding.kind));
      output.add('}');
    }
    output.add(']');
  } else {
    KJ_REQUIRE(bundle.protocolVersion <= 2 && bundle.bindings.size() == 0,
        "bindings require init protocol v3");
  }
  output.add('}');
  KJ_REQUIRE(output.size() <= MAX_ENVELOPE_BYTES, "init envelope exceeds limit");
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

kj::String serializeWorkerBundleRust(const WorkerBundle& bundle) {
  return copyProtocolOutput(rust_protocol::serialize_init(convertBundle(bundle)));
}

kj::String serializeWorkerBundle(const WorkerBundle& bundle) {
  if (usesRustProtocol(bundle)) return serializeWorkerBundleRust(bundle);
  return serializeWorkerBundleLegacy(bundle);
}

kj::String serializeResponseLegacy(kj::StringPtr requestId, const Response& response) {
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

kj::String serializeResponse(kj::StringPtr requestId, Response response) {
  return copyProtocolOutput(rust_protocol::serialize_response(
      ::rust::Str(requestId.begin(), requestId.size()), convertResponse(kj::mv(response))));
}

kj::String serializeScheduledResponse(kj::StringPtr requestId, const ScheduledResponse& response) {
  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
  appendJsonString(output, requestId);
  output.addAll(",\"outcome\":"_kj);
  appendJsonString(output, kj::str(response.outcome));
  output.addAll(",\"retry\":"_kj);
  output.addAll(response.retry ? "true}\n"_kj : "false}\n"_kj);
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

void appendNullableDelay(kj::Vector<char>& output, const kj::Maybe<int>& delay) {
  KJ_IF_SOME(value, delay) {
    output.addAll(kj::str(value));
  } else {
    output.addAll("null"_kj);
  }
}

kj::String serializeQueueResponse(kj::StringPtr requestId, QueueResponse response) {
  for (size_t i = 0; i < response.explicitAcks.size(); ++i) {
    for (size_t j = i + 1; j < response.explicitAcks.size(); ++j) {
      if (lexicographicallyBefore(response.explicitAcks[j], response.explicitAcks[i])) {
        auto temporary = kj::mv(response.explicitAcks[i]);
        response.explicitAcks[i] = kj::mv(response.explicitAcks[j]);
        response.explicitAcks[j] = kj::mv(temporary);
      }
    }
  }
  for (size_t i = 0; i < response.retryMessages.size(); ++i) {
    for (size_t j = i + 1; j < response.retryMessages.size(); ++j) {
      if (lexicographicallyBefore(
              response.retryMessages[j].messageId, response.retryMessages[i].messageId)) {
        auto temporary = kj::mv(response.retryMessages[i]);
        response.retryMessages[i] = kj::mv(response.retryMessages[j]);
        response.retryMessages[j] = kj::mv(temporary);
      }
    }
  }

  kj::Vector<char> output;
  output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
  appendJsonString(output, requestId);
  output.addAll(",\"outcome\":"_kj);
  appendJsonString(output, kj::str(response.outcome));
  output.addAll(",\"ack_all\":"_kj);
  output.addAll(response.ackAll ? "true"_kj : "false"_kj);
  output.addAll(",\"retry_batch\":{\"retry\":"_kj);
  output.addAll(response.retryBatch ? "true"_kj : "false"_kj);
  output.addAll(",\"delay_seconds\":"_kj);
  appendNullableDelay(output, response.retryBatchDelaySeconds);
  output.addAll("},\"explicit_acks\":["_kj);
  bool first = true;
  for (const auto& messageId: response.explicitAcks) {
    if (!first) output.add(',');
    first = false;
    appendJsonString(output, messageId);
  }
  output.addAll("],\"retry_messages\":["_kj);
  first = true;
  for (const auto& retry: response.retryMessages) {
    if (!first) output.add(',');
    first = false;
    output.addAll("{\"id\":"_kj);
    appendJsonString(output, retry.messageId);
    output.addAll(",\"delay_seconds\":"_kj);
    appendNullableDelay(output, retry.delaySeconds);
    output.add('}');
  }
  output.addAll("]}\n"_kj);
  KJ_REQUIRE(output.size() <= MAX_ENVELOPE_BYTES, "queue response exceeds limit");
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

capnp::JsonValue::Reader jsonField(capnp::JsonValue::Reader object, kj::StringPtr name) {
  KJ_REQUIRE(object.isObject(), "expected a JSON object");
  kj::Maybe<capnp::JsonValue::Reader> result;
  for (auto field: object.getObject()) {
    if (field.getName() == name) {
      KJ_REQUIRE(result == kj::none, "duplicate JSON field", name);
      result = field.getValue();
    }
  }
  return KJ_REQUIRE_NONNULL(result, "missing JSON field", name);
}

uint64_t jsonInteger(capnp::JsonValue::Reader value, uint64_t maximum) {
  KJ_REQUIRE(value.isNumber(), "expected a JSON integer");
  auto number = value.getNumber();
  KJ_REQUIRE(
      std::isfinite(number) && number >= 0 && number <= maximum && std::floor(number) == number,
      "JSON integer out of range");
  return static_cast<uint64_t>(number);
}

constexpr size_t MAX_LOADED_MODULE_BYTES = 1024 * 1024;
constexpr size_t MAX_LOADED_BUNDLE_BYTES = 8 * 1024 * 1024;
constexpr size_t MAX_BUNDLE_READ_BYTES = 16 * 1024;

class BundleHostChannel: public kj::Refcounted {
 public:
  virtual kj::Array<byte> read(kj::StringPtr query, size_t maxBytes) = 0;
};

class HyperlightBundleHostChannel final: public BundleHostChannel {
 public:
  kj::Array<byte> read(kj::StringPtr query, size_t maxBytes) override {
    auto argument = hostCallStringArg(query);
    return hostCallBytes("WorkerdBundleV1Read"_kj, kj::arrayPtr(&argument, 1), maxBytes);
  }
};

bool validSha256(kj::StringPtr digest) {
  if (digest.size() != 64) return false;
  for (char c: digest) {
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
  }
  return true;
}

kj::String sha256Hex(kj::ArrayPtr<const byte> source) {
  byte digest[SHA256_DIGEST_LENGTH];
  KJ_REQUIRE(SHA256(source.begin(), source.size(), digest) == digest, "source SHA256 failed");
  constexpr kj::StringPtr HEX = "0123456789abcdef"_kj;
  auto result = kj::heapArray<char>(65);
  for (auto i: kj::indices(digest)) {
    result[2 * i] = HEX[digest[i] >> 4];
    result[2 * i + 1] = HEX[digest[i] & 15];
  }
  result[64] = '\0';
  return kj::String(kj::mv(result));
}

WorkerBundle loadBundleV4(capnp::JsonValue::Reader root, BundleHostChannel& host) {
  KJ_REQUIRE(root.isObject() && root.getObject().size() == 9 &&
          jsonInteger(jsonField(root, "protocol_version"_kj), 4) == 4,
      "invalid chunked init envelope");
  auto digestValue = jsonField(root, "bundle_sha256"_kj);
  KJ_REQUIRE(digestValue.isString() && validSha256(digestValue.getString()),
      "invalid immutable bundle digest");
  auto bundleDigest = digestValue.getString();
  auto descriptors = jsonField(root, "modules"_kj);
  KJ_REQUIRE(descriptors.isArray() && descriptors.getArray().size() > 0 &&
          descriptors.getArray().size() <= MAX_MODULES,
      "invalid chunked module count");

  capnp::JsonCodec codec;
  kj::Vector<char> metadata;
  metadata.addAll("{\"protocol_version\":3,\"worker_version\":"_kj);
  metadata.addAll(codec.encodeRaw(jsonField(root, "worker_version"_kj)));
  metadata.addAll(",\"compatibility_date\":"_kj);
  metadata.addAll(codec.encodeRaw(jsonField(root, "compatibility_date"_kj)));
  metadata.addAll(",\"compatibility_flags\":"_kj);
  metadata.addAll(codec.encodeRaw(jsonField(root, "compatibility_flags"_kj)));
  metadata.addAll(",\"main_module\":"_kj);
  metadata.addAll(codec.encodeRaw(jsonField(root, "main_module"_kj)));
  metadata.addAll(",\"modules\":["_kj);
  size_t aggregate = 0;
  bool first = true;
  for (auto descriptor: descriptors.getArray()) {
    KJ_REQUIRE(descriptor.isObject() && descriptor.getObject().size() == 4,
        "invalid chunked module descriptor");
    auto name = jsonField(descriptor, "name"_kj);
    auto type = jsonField(descriptor, "type"_kj);
    auto digest = jsonField(descriptor, "source_sha256"_kj);
    KJ_REQUIRE(
        name.isString() && type.isString() && digest.isString() && validSha256(digest.getString()),
        "invalid chunked module descriptor fields");
    auto size = jsonInteger(jsonField(descriptor, "source_bytes"_kj), MAX_LOADED_MODULE_BYTES);
    aggregate += size;
    KJ_REQUIRE(aggregate <= MAX_LOADED_BUNDLE_BYTES, "loaded bundle source exceeds limit");
    bool wasm = type.getString() == "wasm"_kj;
    KJ_REQUIRE(!first || !wasm, "main module must be an ES module");
    if (!first) metadata.add(',');
    first = false;
    metadata.addAll("{\"name\":"_kj);
    appendJsonString(metadata, name.getString());
    metadata.addAll(",\"type\":"_kj);
    appendJsonString(metadata, wasm ? "text"_kj : type.getString());
    metadata.addAll(",\"source\":\"\"}"_kj);
  }
  metadata.addAll("],\"storage\":"_kj);
  metadata.addAll(codec.encodeRaw(jsonField(root, "storage"_kj)));
  metadata.addAll(",\"bindings\":"_kj);
  metadata.addAll(codec.encodeRaw(jsonField(root, "bindings"_kj)));
  metadata.add('}');
  auto bundle = parseWorkerBundleLegacy(metadata.asPtr());
  bundle.protocolVersion = 4;
  for (auto i: kj::indices(bundle.modules)) {
    auto descriptor = descriptors.getArray()[i];
    auto size = jsonInteger(jsonField(descriptor, "source_bytes"_kj), MAX_LOADED_MODULE_BYTES);
    auto source = kj::heapArray<byte>(size);
    size_t offset = 0;
    while (offset < size) {
      auto amount = kj::min(size - offset, MAX_BUNDLE_READ_BYTES);
      kj::Vector<char> query;
      query.addAll("{\"protocol_version\":1,\"worker_version\":"_kj);
      appendJsonString(query, bundle.workerVersion);
      query.addAll(",\"bundle_sha256\":"_kj);
      appendJsonString(query, bundleDigest);
      query.addAll(",\"module_name\":"_kj);
      appendJsonString(query, bundle.modules[i].name);
      query.addAll(kj::str(",\"offset\":", offset, ",\"max_bytes\":", amount, "}"));
      query.add('\0');
      auto bytes = host.read(kj::String(query.releaseAsArray()), amount);
      KJ_REQUIRE(
          bytes.size() == amount, "immutable bundle read returned a short or oversized chunk");
      source.slice(offset, offset + amount).copyFrom(bytes);
      offset += amount;
    }
    KJ_REQUIRE(sha256Hex(source) == jsonField(descriptor, "source_sha256"_kj).getString(),
        "immutable module source digest mismatch");
    auto& module = bundle.modules[i];
    if (jsonField(descriptor, "type"_kj).getString() == "wasm"_kj) {
      KJ_REQUIRE(source.size() >= 8 &&
              source.first(8) == kj::arrayPtr(reinterpret_cast<const byte*>("\0asm\1\0\0\0"), 8),
          "invalid Wasm module header");
      module.type = ModuleType::WASM;
    } else {
      KJ_REQUIRE(simdutf::validate_utf8(source.asChars().begin(), source.size()),
          "module source must be valid UTF-8");
    }
    module.source = kj::mv(source);
  }
  return bundle;
}

class IngressHostChannel: public kj::Refcounted {
 public:
  virtual kj::String send(kj::StringPtr frame) = 0;
  virtual kj::String receive(kj::StringPtr query) = 0;
};

class HyperlightIngressHostChannel final: public IngressHostChannel {
 public:
  kj::String send(kj::StringPtr frame) override {
    auto argument = hostCallStringArg(frame);
    return hostCallString("WorkerdIngressV1Send"_kj, kj::arrayPtr(&argument, 1));
  }
  kj::String receive(kj::StringPtr query) override {
    auto argument = hostCallStringArg(query);
    return hostCallString("WorkerdIngressV1Receive"_kj, kj::arrayPtr(&argument, 1));
  }
};

class IngressSession final: public kj::Refcounted {
 public:
  struct Frame {
    kj::String kind;
    kj::Array<byte> body;
    uint8_t opcode;
  };

  IngressSession(
      kj::Rc<IngressHostChannel> host, kj::String requestId, bool websocket, kj::Timer& pollTimer)
      : websocketAllowed(websocket),
        host(kj::mv(host)),
        requestId(kj::mv(requestId)),
        pollTimer(pollTimer) {
    auto paf = kj::newPromiseAndFulfiller<void>();
    aborted = paf.promise.fork();
    abortFulfiller = kj::mv(paf.fulfiller);
    auto closePaf = kj::newPromiseAndFulfiller<void>();
    closed = closePaf.promise.fork();
    closeFulfiller = kj::mv(closePaf.fulfiller);
  }

  bool trySend(kj::StringPtr kind, kj::StringPtr fields = ""_kj) {
    KJ_REQUIRE(!cancelled, "ingress transport cancelled");
    KJ_REQUIRE(outgoingSequence < MAX_HOST_OPERATION_ID, "ingress sequence exhausted");
    kj::Vector<char> output;
    output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
    appendJsonString(output, requestId);
    output.addAll(kj::str(",\"sequence\":", outgoingSequence, ",\"kind\":"));
    appendJsonString(output, kind);
    output.addAll(fields);
    output.add('}');
    output.add('\0');
    auto reply = host->send(kj::String(output.releaseAsArray()));
    KJ_REQUIRE(reply.size() <= 2048, "ingress acknowledgement exceeds limit");
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(reply, root);
    KJ_REQUIRE(root.isObject() && (root.getObject().size() == 5 || root.getObject().size() == 6),
        "invalid ingress acknowledgement");
    validateIdentity(root, outgoingSequence);
    auto accepted = jsonField(root, "accepted"_kj);
    auto wasCancelled = jsonField(root, "cancelled"_kj);
    KJ_REQUIRE(
        accepted.isBoolean() && wasCancelled.isBoolean(), "invalid ingress acknowledgement state");
    if (wasCancelled.getBoolean()) {
      cancel();
      KJ_FAIL_REQUIRE("ingress transport cancelled");
    }
    if (!accepted.getBoolean()) {
      auto error = jsonField(root, "error"_kj);
      KJ_REQUIRE(error.isString() && error.getString().size() <= 1024, "invalid ingress rejection");
      if (error.getString() == "backpressure"_kj) return false;
      KJ_FAIL_REQUIRE("ingress frame rejected", error.getString());
    }
    KJ_REQUIRE(root.getObject().size() == 5, "unexpected ingress acknowledgement field");
    ++outgoingSequence;
    return true;
  }

  kj::Promise<void> sendAwait(kj::StringPtr kind, kj::StringPtr fields = ""_kj) {
    while (!trySend(kind, fields)) {
      co_await pollTimer.afterDelay(1 * kj::MILLISECONDS);
    }
  }

  kj::Promise<void> flushHeaders() {
    if (headersDelivered || !headersSent) return kj::READY_NOW;
    KJ_IF_SOME(fields, pendingHeaders) {
      auto ownedFields = kj::mv(fields);
      pendingHeaders = kj::none;
      headersFlush = sendAwait("headers"_kj, ownedFields)
                         .attach(kj::mv(ownedFields))
                         .then([this]() {
        headersDelivered = true;
      }).fork();
    }
    KJ_IF_SOME(flush, headersFlush) {
      return flush.addBranch();
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> sendBody(
      kj::StringPtr kind, kj::ArrayPtr<const byte> body, kj::Maybe<uint8_t> opcode) {
    KJ_REQUIRE(body.size() <= MAX_INGRESS_FRAME_BYTES, "ingress frame exceeds limit");
    auto encoded = kj::encodeBase64(body);
    kj::Vector<char> fields;
    fields.addAll(",\"body_base64\":"_kj);
    appendJsonString(fields, encoded);
    KJ_IF_SOME(value, opcode) {
      fields.addAll(kj::str(",\"opcode\":", static_cast<uint>(value)));
    }
    fields.add('\0');
    auto fieldText = kj::String(fields.releaseAsArray());
    if (headersDelivered) {
      return sendAwait(kind, fieldText).attach(kj::mv(fieldText));
    }
    return flushHeaders().then(
        [self = kj::addRef(*this), kind = kj::str(kind), fields = kj::mv(fieldText)]() mutable {
      return self->sendAwait(kind, fields).attach(kj::mv(kind), kj::mv(fields), kj::mv(self));
    });
  }

  kj::Promise<Frame> receive() {
    co_await flushHeaders();
    for (;;) {
      KJ_REQUIRE(!cancelled, "ingress transport cancelled");
      KJ_REQUIRE(incomingSequence < MAX_HOST_OPERATION_ID, "ingress sequence exhausted");
      auto query = kj::str("{\"protocol_version\":1,\"request_id\":\"", requestId, "\"}");
      auto reply = host->receive(query);
      KJ_REQUIRE(reply.size() <= 24 * 1024, "ingress frame envelope exceeds limit");
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(reply, root);
      KJ_REQUIRE(root.isObject(), "invalid ingress frame");
      validateIdentity(root, incomingSequence);
      auto kind = jsonField(root, "kind"_kj);
      KJ_REQUIRE(kind.isString(), "invalid ingress frame kind");
      if (kind.getString() == "pending"_kj) {
        KJ_REQUIRE(root.getObject().size() == 4, "invalid ingress pending frame");
        co_await pollTimer.afterDelay(1 * kj::MILLISECONDS);
        continue;
      }
      ++incomingSequence;
      if (kind.getString() == "cancel"_kj) {
        KJ_REQUIRE(root.getObject().size() == 4, "invalid ingress cancellation frame");
        cancel();
        KJ_FAIL_REQUIRE("ingress transport cancelled");
      }
      if (kind.getString() == "end"_kj) {
        KJ_REQUIRE(root.getObject().size() == 4, "invalid ingress end frame");
        co_return Frame{kj::str("end"), kj::heapArray<byte>(0), 0};
      }
      bool websocket = kind.getString() == "websocket"_kj;
      KJ_REQUIRE(websocket || kind.getString() == "data"_kj, "unexpected inbound frame kind");
      KJ_REQUIRE(root.getObject().size() == (websocket ? 6 : 5), "invalid ingress data fields");
      auto body = jsonField(root, "body_base64"_kj);
      KJ_REQUIRE(body.isString() && body.getString().size() <= 21848,
          "ingress encoded body exceeds limit");
      auto decoded = kj::decodeBase64(body.getString());
      KJ_REQUIRE(!decoded.hadErrors && decoded.size() <= MAX_INGRESS_FRAME_BYTES &&
              kj::encodeBase64(decoded) == body.getString(),
          "invalid ingress frame body");
      auto opcode = websocket ? jsonInteger(jsonField(root, "opcode"_kj), 8) : 0;
      KJ_REQUIRE(!websocket || opcode == 1 || opcode == 2 || opcode == 8,
          "invalid ingress WebSocket opcode");
      co_return Frame{kj::str(kind.getString()), kj::mv(decoded), static_cast<uint8_t>(opcode)};
    }
  }

  void headers(uint status, const kj::HttpHeaders& headers) {
    KJ_REQUIRE(!headersSent && status >= 100 && status <= 599, "invalid ingress response status");
    kj::Vector<char> fields;
    fields.addAll(kj::str(",\"status\":", status, ",\"headers\":["));
    size_t count = 0;
    size_t bytes = 0;
    headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
      KJ_REQUIRE(
          validHeaderName(name) && validHeaderValue(value), "invalid ingress response header");
      bytes += name.size() + value.size();
      KJ_REQUIRE(++count <= MAX_HEADERS && bytes <= MAX_HEADER_BYTES,
          "ingress response headers exceed limit");
      if (count > 1) fields.add(',');
      fields.addAll("{\"name\":"_kj);
      appendJsonString(fields, name);
      fields.addAll(",\"value\":"_kj);
      appendJsonString(fields, value);
      fields.add('}');
    });
    fields.add(']');
    fields.add('\0');
    pendingHeaders = kj::String(fields.releaseAsArray());
    headersSent = true;
  }

  kj::Promise<void> completeResponse() {
    KJ_REQUIRE(headersSent, "worker produced no ingress response");
    co_await flushHeaders();
    if (upgraded) {
      co_await closed.addBranch().exclusiveJoin(whenAborted().then(
          [] { KJ_FAIL_REQUIRE("ingress WebSocket cancelled before close handshake"); }));
    }
    if (!upgraded && !ended) {
      co_await sendAwait("end"_kj);
      ended = true;
    }
  }

  kj::Promise<void> complete() {
    if (upgraded) {
      KJ_REQUIRE(sentClose && receivedClose, "WebSocket lifetime ended before close handshake");
      if (!ended) {
        co_await sendAwait("end"_kj);
        ended = true;
      }
    } else {
      co_await completeResponse();
    }
  }

  void cancel() {
    if (!cancelled) {
      cancelled = true;
      abortFulfiller->fulfill();
    }
  }

  kj::Promise<void> whenAborted() {
    return aborted.addBranch();
  }

  void markSentClose() {
    sentClose = true;
    if (receivedClose) closeFulfiller->fulfill();
  }

  void markReceivedClose() {
    receivedClose = true;
    if (sentClose) closeFulfiller->fulfill();
  }

  bool websocketAllowed;
  bool upgraded = false;
  bool headersSent = false;
  bool bodyEnded = false;
  bool sentClose = false;
  bool receivedClose = false;
  bool ended = false;
  bool cancelled = false;

 private:
  void validateIdentity(capnp::JsonValue::Reader root, uint64_t sequence) {
    KJ_REQUIRE(jsonInteger(jsonField(root, "protocol_version"_kj), 1) == 1 &&
            jsonInteger(jsonField(root, "sequence"_kj), MAX_HOST_OPERATION_ID) == sequence,
        "ingress frame version or sequence mismatch");
    auto id = jsonField(root, "request_id"_kj);
    KJ_REQUIRE(id.isString() && id.getString() == requestId, "ingress request ID mismatch");
  }

  kj::Rc<IngressHostChannel> host;
  kj::String requestId;
  uint64_t outgoingSequence = 0;
  uint64_t incomingSequence = 0;
  kj::ForkedPromise<void> aborted = nullptr;
  kj::Own<kj::PromiseFulfiller<void>> abortFulfiller;
  kj::ForkedPromise<void> closed = nullptr;
  kj::Own<kj::PromiseFulfiller<void>> closeFulfiller;
  kj::Timer& pollTimer;
  kj::Maybe<kj::String> pendingHeaders;
  kj::Maybe<kj::ForkedPromise<void>> headersFlush;
  bool headersDelivered = false;
};

class IngressInput final: public kj::AsyncInputStream {
 public:
  IngressInput(kj::Rc<IngressSession> session, kj::Maybe<uint64_t> length)
      : session(kj::mv(session)),
        length(length) {}

  kj::Maybe<uint64_t> tryGetLength() override {
    return length;
  }

  kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {
    KJ_REQUIRE(minBytes <= maxBytes, "invalid ingress read bounds");
    size_t copied = 0;
    while (copied < minBytes || (copied == 0 && maxBytes > 0)) {
      if (offset == pending.size()) {
        if (session->bodyEnded) break;
        auto frame = co_await kj::evalLater([this]() { return session->receive(); });
        if (frame.kind == "end"_kj) {
          session->bodyEnded = true;
          break;
        }
        KJ_REQUIRE(frame.kind == "data"_kj && frame.body.size() > 0,
            "expected a nonempty HTTP body frame");
        pending = kj::mv(frame.body);
        offset = 0;
      }
      auto amount = kj::min(pending.size() - offset, maxBytes - copied);
      memcpy(static_cast<byte*>(buffer) + copied, pending.begin() + offset, amount);
      copied += amount;
      offset += amount;
    }
    co_return copied;
  }

 private:
  kj::Rc<IngressSession> session;
  kj::Array<byte> pending;
  size_t offset = 0;
  kj::Maybe<uint64_t> length;
};

class IngressOutput final: public kj::AsyncOutputStream {
 public:
  explicit IngressOutput(kj::Rc<IngressSession> session): session(kj::mv(session)) {}

  kj::Promise<void> write(kj::ArrayPtr<const byte> data) override {
    while (data.size() > 0) {
      auto chunk = data.first(kj::min(data.size(), MAX_INGRESS_FRAME_BYTES));
      co_await kj::evalLater(
          [this, chunk]() { return session->sendBody("data"_kj, chunk, kj::none); });
      data = data.slice(chunk.size());
    }
  }
  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    for (auto piece: pieces) co_await write(piece);
  }
  kj::Promise<void> whenWriteDisconnected() override {
    return session->whenAborted();
  }

 private:
  kj::Rc<IngressSession> session;
};

bool validWebSocketClose(uint16_t code) {
  return code == 1000 || code == 1001 || code == 1002 || code == 1003 ||
      (code >= 1007 && code <= 1014) || (code >= 3000 && code <= 4999);
}

void requireUtf8(kj::ArrayPtr<const byte> body) {
  KJ_REQUIRE(
      simdutf::validate_utf8(body.asChars().begin(), body.size()), "invalid WebSocket UTF-8");
}

class IngressWebSocket final: public kj::WebSocket {
 public:
  explicit IngressWebSocket(kj::Rc<IngressSession> session): session(kj::mv(session)) {}
  kj::Promise<void> send(kj::ArrayPtr<const byte> message) override {
    return sendMessage(message, 2);
  }
  kj::Promise<void> send(kj::ArrayPtr<const char> message) override {
    requireUtf8(message.asBytes());
    return sendMessage(message.asBytes(), 1);
  }
  kj::Promise<void> close(uint16_t code, kj::StringPtr reason) override {
    KJ_REQUIRE(!session->sentClose && validWebSocketClose(code) && reason.size() <= 123,
        "invalid WebSocket close");
    requireUtf8(reason.asBytes());
    auto body = kj::heapArray<byte>(2 + reason.size());
    body[0] = code >> 8;
    body[1] = code & 255;
    memcpy(body.begin() + 2, reason.begin(), reason.size());
    co_await session->sendBody("websocket"_kj, body, uint8_t(8));
    session->markSentClose();
    sent += body.size();
  }
  void disconnect() override {
    session->cancel();
  }
  void abort() override {
    session->cancel();
  }
  kj::Promise<void> whenAborted() override {
    return session->whenAborted();
  }
  kj::Promise<Message> receive(size_t maxSize) override {
    KJ_REQUIRE(!session->receivedClose, "WebSocket read after close");
    auto frame = co_await kj::evalLater([this]() { return session->receive(); });
    if (!session->bodyEnded && frame.kind == "end"_kj) {
      session->bodyEnded = true;
      frame = co_await kj::evalLater([this]() { return session->receive(); });
    }
    KJ_REQUIRE(frame.kind == "websocket"_kj && frame.body.size() <= maxSize,
        "invalid WebSocket inbound frame");
    received += frame.body.size();
    if (frame.opcode == 1) {
      requireUtf8(frame.body);
      co_return Message(copyBytes(frame.body));
    }
    if (frame.opcode == 2) co_return Message(kj::mv(frame.body));
    KJ_REQUIRE(frame.body.size() >= 2 && frame.body.size() <= 125, "invalid WebSocket close frame");
    auto code = static_cast<uint16_t>((uint16_t(frame.body[0]) << 8) | frame.body[1]);
    KJ_REQUIRE(validWebSocketClose(code), "invalid WebSocket close code");
    requireUtf8(frame.body.slice(2));
    session->markReceivedClose();
    co_return Message(Close{code, copyBytes(frame.body.slice(2))});
  }
  kj::Promise<void> pumpTo(kj::WebSocket& other) override {
    auto pump = [](IngressWebSocket& source, kj::WebSocket& other) -> kj::Promise<void> {
      for (;;) {
        auto message = co_await source.receive(MAX_INGRESS_FRAME_BYTES);
        KJ_SWITCH_ONEOF(message) {
          KJ_CASE_ONEOF(text, kj::String) {
            co_await other.send(text);
          }
          KJ_CASE_ONEOF(data, kj::Array<byte>) {
            co_await other.send(data);
          }
          KJ_CASE_ONEOF(close, Close) {
            try {
              co_await other.close(close.code, close.reason);
            } catch (const kj::Exception& exception) {
              // The native pipe can release its endpoint while acknowledging a completed close.
              if (exception.getType() != kj::Exception::Type::DISCONNECTED ||
                  !source.session->sentClose || !source.session->receivedClose) {
                throw;
              }
            }
            co_return;
          }
        }
      }
    };
    auto aborted = other.whenAborted().then([this]() -> kj::Promise<void> {
      if (session->sentClose && session->receivedClose) return kj::READY_NOW;
      abort();
      return KJ_EXCEPTION(DISCONNECTED, "inbound WebSocket peer disconnected before close");
    });
    return pump(*this, other).exclusiveJoin(kj::mv(aborted));
  }
  uint64_t sentByteCount() override {
    return sent;
  }
  uint64_t receivedByteCount() override {
    return received;
  }
  kj::Maybe<kj::String> getPreferredExtensions(ExtensionsContext) override {
    return kj::none;
  }

 private:
  kj::Promise<void> sendMessage(kj::ArrayPtr<const byte> body, uint8_t opcode) {
    KJ_REQUIRE(!session->sentClose, "WebSocket send after close");
    co_await session->sendBody("websocket"_kj, body, opcode);
    sent += body.size();
  }
  kj::Rc<IngressSession> session;
  uint64_t sent = 0;
  uint64_t received = 0;
};

class IngressResponse final: public kj::HttpService::Response {
 public:
  explicit IngressResponse(kj::Rc<IngressSession> session): session(kj::mv(session)) {}
  kj::Own<kj::AsyncOutputStream> send(
      uint status, kj::StringPtr, const kj::HttpHeaders& headers, kj::Maybe<uint64_t>) override {
    KJ_REQUIRE(status != 101, "WebSocket upgrade requires acceptWebSocket");
    session->headers(status, headers);
    return kj::heap<IngressOutput>(session.addRef());
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
    KJ_REQUIRE(session->websocketAllowed, "inbound WebSocket capability was not negotiated");
    session->headers(101, headers);
    session->upgraded = true;
    return kj::heap<IngressWebSocket>(session.addRef());
  }

 private:
  kj::Rc<IngressSession> session;
};

class Executor {
 public:
  Executor(): Executor(newHyperlightLogicalServiceHostChannel()) {}
  explicit Executor(kj::Rc<LogicalServiceHostChannel> logicalServiceHost)
      : Executor(kj::mv(logicalServiceHost), kj::rc<HyperlightIngressHostChannel>()) {}
  Executor(
      kj::Rc<LogicalServiceHostChannel> logicalServiceHost, kj::Rc<IngressHostChannel> ingressHost)
      : Executor(kj::mv(logicalServiceHost), kj::mv(ingressHost), newHyperlightTimerHostChannel()) {
  }
  Executor(kj::Rc<LogicalServiceHostChannel> logicalServiceHost,
      kj::Rc<IngressHostChannel> ingressHost,
      kj::Rc<TimerHostChannel> timerHost)
      : Executor(kj::mv(logicalServiceHost),
            kj::mv(ingressHost),
            kj::mv(timerHost),
            kj::rc<HyperlightBundleHostChannel>()) {}
  Executor(kj::Rc<LogicalServiceHostChannel> logicalServiceHost,
      kj::Rc<IngressHostChannel> ingressHost,
      kj::Rc<TimerHostChannel> timerHost,
      kj::Rc<BundleHostChannel> bundleHost)
      : Executor(kj::mv(logicalServiceHost),
            kj::mv(ingressHost),
            kj::mv(timerHost),
            kj::mv(bundleHost),
            newSystemEntropyHostChannel()) {}
  Executor(kj::Rc<LogicalServiceHostChannel> logicalServiceHost,
      kj::Rc<IngressHostChannel> ingressHost,
      kj::Rc<TimerHostChannel> timerHost,
      kj::Rc<BundleHostChannel> bundleHost,
      kj::Rc<EntropyHostChannel> entropyHost)
      : Executor(kj::mv(logicalServiceHost),
            kj::mv(ingressHost),
            kj::mv(timerHost),
            kj::mv(bundleHost),
            kj::mv(entropyHost),
            newHyperlightWebSocketHostChannel()) {}
  Executor(kj::Rc<LogicalServiceHostChannel> logicalServiceHost,
      kj::Rc<IngressHostChannel> ingressHost,
      kj::Rc<TimerHostChannel> timerHost,
      kj::Rc<BundleHostChannel> bundleHost,
      kj::Rc<EntropyHostChannel> entropyHost,
      kj::Rc<WebSocketHostChannel> webSocketHost)
      : logicalServiceHost(kj::mv(logicalServiceHost)),
        ingressHost(kj::mv(ingressHost)),
        timerHost(kj::mv(timerHost)),
        bundleHost(kj::mv(bundleHost)),
        entropyHost(kj::mv(entropyHost)),
        webSocketBroker(newAuthenticatedWebSocketBroker(kj::mv(webSocketHost))) {}

  void initialize(kj::ArrayPtr<const char> initJson) {
    KJ_REQUIRE(runtime == kj::none, "executor already initialized");
    auto bundle = [&]() {
      KJ_REQUIRE(initJson.size() <= MAX_ENVELOPE_BYTES, "init envelope exceeds limit");
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(kj::str(initJson), root);
      if (root.isObject() && jsonInteger(jsonField(root, "protocol_version"_kj), 4) == 4) {
        return loadBundleV4(root, *bundleHost);
      }
      if (usesRustProtocol(initJson)) {
        rustProtocol = true;
        return convertBundle(protocol->initialize(asRustBytes(initJson)));
      }
      auto legacyBundle = parseWorkerBundleLegacy(initJson);
      KJ_REQUIRE(serializeWorkerBundleLegacy(legacyBundle) == initJson,
          "init envelope is not canonical JSON");
      return legacyBundle;
    }();
    runtime = kj::heap<SandboxRuntime>(bundle, kj::rc<StreamingV2FetchBroker>(),
        logicalServiceHost.addRef(), timerHost.addRef(), SandboxRuntime::Limits{},
        entropyHost.addRef(), webSocketBroker.addRef());
  }

  kj::String fetch(
      kj::ArrayPtr<const char> requestJson, kj::Maybe<kj::Duration> lifetimeBudget = kj::none) {
    auto& worker = KJ_REQUIRE_NONNULL(runtime, "executor is not initialized");
    if (!rustProtocol) {
      auto request = parseRequestLegacy(requestJson);
      try {
        auto response = worker->runRequest(
            request.method, request.url, request.headers, request.body, lifetimeBudget);
        return serializeResponseLegacy(request.requestId, response);
      } catch (const kj::Exception& exception) {
        kj::Vector<char> body;
        body.addAll("{\"error\":\"worker_execution_failed\",\"exception\":"_kj);
        appendJsonString(body, exception.getDescription());
        body.add('}');
        body.add('\0');
        auto headers = kj::heapArray<Header>(1);
        headers[0] = Header{kj::str("content-type"), kj::str("application/json")};
        return serializeResponseLegacy(
            request.requestId, Response{502, kj::mv(headers), kj::String(body.releaseAsArray())});
      }
    }

    auto request = convertRequest(protocol->begin_request(asRustBytes(requestJson)));
    try {
      auto response = worker->runRequest(
          request.method, request.url, request.headers, request.body, lifetimeBudget);
      return copyProtocolOutput(protocol->complete_response(
          ::rust::Str(request.requestId.begin(), request.requestId.size()),
          convertResponse(kj::mv(response))));
    } catch (const kj::Exception& exception) {
      auto description = exception.getDescription();
      return copyProtocolOutput(protocol->complete_failure(
          ::rust::Str(request.requestId.begin(), request.requestId.size()),
          ::rust::Str(description.begin(), description.size())));
    }
  }

  kj::String scheduled(
      kj::ArrayPtr<const char> requestJson, kj::Maybe<kj::Duration> lifetimeBudget = kj::none) {
    auto& worker = KJ_REQUIRE_NONNULL(runtime, "executor is not initialized");
    auto request = parseScheduledRequest(requestJson);
    auto response = worker->runScheduled(request.scheduledTime, request.cron, lifetimeBudget);
    return serializeScheduledResponse(request.requestId, response);
  }

  kj::String queue(
      kj::ArrayPtr<const char> requestJson, kj::Maybe<kj::Duration> lifetimeBudget = kj::none) {
    auto& worker = KJ_REQUIRE_NONNULL(runtime, "executor is not initialized");
    auto request = parseQueueRequest(requestJson);
    auto response = worker->runQueue(kj::mv(request.request), lifetimeBudget);
    return serializeQueueResponse(request.requestId, kj::mv(response));
  }

  kj::String invoke(kj::ArrayPtr<const char> input) {
    KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "invocation envelope exceeds limit");
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(kj::str(input), root);
    KJ_REQUIRE(root.isObject(), "invocation must be an object");
    auto kindValue = jsonField(root, "kind"_kj);
    auto requestValue = jsonField(root, "request"_kj);
    KJ_REQUIRE(root.getObject().size() == 4 &&
            jsonInteger(jsonField(root, "protocol_version"_kj), 2) == 2 && kindValue.isString() &&
            requestValue.isObject(),
        "invalid invocation envelope");
    auto milliseconds = jsonInteger(jsonField(root, "lifetime_budget_ms"_kj), UINT32_MAX);
    KJ_REQUIRE(milliseconds > 0, "invalid invocation lifetime budget");
    KJ_REQUIRE(extensionsNegotiated, "invocation budget capability was not negotiated");
    auto budget = static_cast<uint32_t>(milliseconds) * kj::MILLISECONDS;
    auto request = codec.encodeRaw(requestValue);
    auto kind = kindValue.getString();
    if (kind == "fetch"_kj) return fetch(request, budget);
    if (kind == "scheduled"_kj) return scheduled(request, budget);
    if (kind == "queue"_kj) return queue(request, budget);
    KJ_FAIL_REQUIRE("unsupported invocation kind");
  }

  void ingressStream(kj::ArrayPtr<const char> input) {
    auto& worker = KJ_REQUIRE_NONNULL(runtime, "executor is not initialized");
    KJ_REQUIRE(extensionsNegotiated, "ingress streaming capability was not negotiated");
    KJ_REQUIRE(!ingressActive && !ingressFailed, "ingress executor is not reusable");
    KJ_REQUIRE(input.size() <= MAX_ENVELOPE_BYTES, "ingress invocation envelope exceeds limit");
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(kj::str(input), root);
    KJ_REQUIRE(root.isObject() && root.getObject().size() == 5 &&
            jsonInteger(jsonField(root, "protocol_version"_kj), 1) == 1,
        "invalid ingress invocation");
    auto requestId = jsonField(root, "request_id"_kj);
    auto websocket = jsonField(root, "websocket"_kj);
    KJ_REQUIRE(requestId.isString() &&
            validIdentifier(requestId.getString(), MAX_REQUEST_ID_BYTES) && websocket.isBoolean(),
        "invalid ingress identity or WebSocket negotiation");
    auto milliseconds = jsonInteger(jsonField(root, "lifetime_budget_ms"_kj), UINT32_MAX);
    KJ_REQUIRE(milliseconds > 0, "invocation lifetime budget must be positive");
    auto requestJson = codec.encodeRaw(jsonField(root, "request"_kj));
    auto request = parseRequestLegacy(requestJson);
    KJ_REQUIRE(request.body.size() == 0 && request.requestId == requestId.getString(),
        "ingress body must be streamed and request IDs must match");
    auto session = kj::rc<IngressSession>(ingressHost.addRef(), kj::mv(request.requestId),
        websocket.getBoolean(), worker->getNativeTimer());
    bool noBody = request.method == kj::HttpMethod::GET || request.method == kj::HttpMethod::HEAD;
    kj::Maybe<uint64_t> bodyLength = noBody ? kj::Maybe<uint64_t>(uint64_t(0)) : kj::none;
    if (noBody) {
      for (const auto& header: request.headers) {
        if (strcasecmp(header.name.cStr(), "content-length") == 0) {
          KJ_REQUIRE(header.value == "0"_kj, "GET or HEAD ingress must not have a body");
        }
        KJ_REQUIRE(strcasecmp(header.name.cStr(), "transfer-encoding") != 0,
            "GET or HEAD ingress must not have a streamed body");
      }
    }
    for (const auto& header: request.headers) {
      if (strcasecmp(header.name.cStr(), "content-length") == 0 && header.value == "0"_kj) {
        bodyLength = uint64_t(0);
      }
    }
    IngressInput body(session.addRef(), bodyLength);
    IngressResponse response(session.addRef());
    ingressActive = true;
    KJ_DEFER(ingressActive = false);
    KJ_ON_SCOPE_FAILURE(ingressFailed = true; session->cancel());
    try {
      worker->runRequestStream(request.method, request.url, request.headers, body, response,
          milliseconds * kj::MILLISECONDS, [&]() { return session->completeResponse(); },
          [&]() { return session->complete(); });
    } catch (const kj::Exception& exception) {
      if (!session->cancelled && !session->ended) {
        try {
          KJ_REQUIRE(session->trySend("error"_kj, ",\"error\":\"worker_execution_failed\""_kj),
              "ingress error delivery was backpressured");
        } catch (const kj::Exception& transportException) {
          KJ_LOG(ERROR, "ingress error delivery failed", transportException);
        }
      }
      kj::throwRecoverableException(exception.clone());
      KJ_UNREACHABLE;
    }
  }

  kj::String control(kj::ArrayPtr<const char> input, bool checkpoint) {
    auto& worker = KJ_REQUIRE_NONNULL(runtime, "executor is not initialized");
    KJ_REQUIRE(input.size() <= 256, "control envelope exceeds limit");
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(kj::str(input), root);
    KJ_REQUIRE(root.isObject(), "control request must be an object");
    auto requestId = jsonField(root, "request_id"_kj);
    KJ_REQUIRE(root.getObject().size() == 2 &&
            jsonInteger(jsonField(root, "protocol_version"_kj), 1) == 1 && requestId.isString() &&
            validIdentifier(requestId.getString(), MAX_REQUEST_ID_BYTES),
        "invalid control request");
    kj::Vector<char> output;
    output.addAll("{\"protocol_version\":1,\"request_id\":"_kj);
    appendJsonString(output, requestId.getString());
    if (checkpoint) {
      KJ_REQUIRE(extensionsNegotiated, "safe-point capability was not negotiated");
      KJ_REQUIRE(worker->isQuiescent() && !ingressActive && !ingressFailed,
          "executor has outstanding external work");
      output.addAll(",\"quiescent\":true,\"active_invocations\":0,\"active_streams\":0,"
                    "\"active_websockets\":0,\"active_tasks\":0}\n"_kj);
    } else {
      extensionsNegotiated = true;
      worker->negotiateProviderWebSockets();
      output.addAll(",\"extensions\":[\"tracked-work-drain-v1\",\"safe-point-v1\","
                    "\"ingress-stream-v1\",\"invocation-budget-v2\",\"secure-entropy-v1\","
                    "\"authenticated-egress-websocket-v1\"],"
                    "\"max_frame_bytes\":16384}\n"_kj);
    }
    output.add('\0');
    return kj::String(output.releaseAsArray());
  }

 private:
  ::rust::Box<rust_protocol::ProtocolState> protocol = rust_protocol::new_protocol_state();
  kj::Rc<LogicalServiceHostChannel> logicalServiceHost;
  kj::Rc<IngressHostChannel> ingressHost;
  kj::Rc<TimerHostChannel> timerHost;
  kj::Rc<BundleHostChannel> bundleHost;
  kj::Rc<EntropyHostChannel> entropyHost;
  kj::Rc<WebSocketBroker> webSocketBroker;
  kj::Maybe<kj::Own<SandboxRuntime>> runtime;
  bool rustProtocol = false;
  bool ingressActive = false;
  bool ingressFailed = false;
  bool extensionsNegotiated = false;
};

Executor executor(newHyperlightLogicalServiceHostChannel(),
    kj::rc<HyperlightIngressHostChannel>(),
    newHyperlightTimerHostChannel(),
    kj::rc<HyperlightBundleHostChannel>(),
    newHyperlightEntropyHostChannel());

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
    if (nameEquals(call, callLength, "scheduled"_kj)) {
      auto response = executor.scheduled(argument);
      return writeProtocolMessage(protocolOutputFd, response.slice(0, response.size()));
    }
    if (nameEquals(call, callLength, "queue"_kj)) {
      auto response = executor.queue(argument);
      return writeProtocolMessage(protocolOutputFd, response.slice(0, response.size()));
    }
    if (nameEquals(call, callLength, "invoke"_kj)) {
      auto response = executor.invoke(argument);
      return writeProtocolMessage(protocolOutputFd, response.slice(0, response.size()));
    }
    if (nameEquals(call, callLength, "ingress_stream"_kj)) {
      executor.ingressStream(argument);
      return 0;
    }
    if (nameEquals(call, callLength, "runtime_capabilities"_kj) ||
        nameEquals(call, callLength, "checkpoint"_kj)) {
      auto response = executor.control(argument, nameEquals(call, callLength, "checkpoint"_kj));
      return writeProtocolMessage(protocolOutputFd, response.slice(0, response.size()));
    }
    return -1;
  } catch (const kj::Exception& exception) {
    KJ_LOG(ERROR, "sandbox executor dispatch failed", exception);
    return -1;
  } catch (...) {
    KJ_LOG(ERROR, "sandbox executor dispatch failed with a non-KJ exception");
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

class SelfTestPollTimer final: public kj::Timer {
 public:
  kj::TimePoint now() const override {
    return kj::origin<kj::TimePoint>();
  }
  kj::Promise<void> atTime(kj::TimePoint time) override {
    return afterDelay(time - now());
  }
  kj::Promise<void> afterDelay(kj::Duration delay) override {
    delays.add(delay);
    if (block) return kj::NEVER_DONE;
    return kj::READY_NOW;
  }

  kj::Vector<kj::Duration> delays;
  bool block = false;
};

class FakeTimerHostChannel final: public TimerHostChannel {
 public:
  struct Entry {
    uint64_t id;
    size_t pendingReads;
    bool cancelled = false;
    bool released = false;
  };

  kj::String start(uint64_t delayNs) override {
    startDelays.add(delayNs);
    KJ_IF_SOME(override, startOverride) {
      return kj::str(override);
    }
    if (startErrorCode != kj::none) {
      return kj::str(
          R"JSON({"protocol_version":1,"timer_id":0,"state":"error","error":{"message":")JSON",
          KJ_REQUIRE_NONNULL(startErrorMessage), R"JSON(","code":")JSON",
          KJ_REQUIRE_NONNULL(startErrorCode), R"JSON("}})JSON");
    }
    auto id = nextId++;
    entries.add(Entry{
      id, blockLongDelays && delayNs > 100'000'000 ? size_t(kj::maxValue) : readsBeforeFire});
    if (reorderStartFields) {
      return kj::str(R"JSON({"state":"pending","error":null,"timer_id":)JSON", id,
          R"JSON(,"protocol_version":1})JSON");
    }
    return kj::str(R"JSON({"protocol_version":1,"timer_id":)JSON", id,
        R"JSON(,"state":"pending","error":null})JSON");
  }

  kj::String read(uint64_t timerId) override {
    readIds.add(timerId);
    KJ_IF_SOME(override, readOverride) {
      return kj::str(override);
    }
    auto entry = find(timerId);
    if (entry == nullptr || entry->released) {
      return kj::str(R"JSON({"protocol_version":1,"timer_id":)JSON", timerId,
          R"JSON(,"state":"error","error":{"code":"unknown_timer","message":"unknown or released timer"}})JSON");
    }
    if (entry->cancelled) {
      entry->released = true;
      return kj::str(R"JSON({"protocol_version":1,"timer_id":)JSON", timerId,
          R"JSON(,"state":"cancelled","error":null})JSON");
    }
    if (entry->pendingReads > 0) {
      --entry->pendingReads;
      return kj::str(R"JSON({"protocol_version":1,"timer_id":)JSON", timerId,
          R"JSON(,"state":"pending","error":null})JSON");
    }
    entry->released = true;
    return kj::str(R"JSON({"protocol_version":1,"timer_id":)JSON", timerId,
        R"JSON(,"state":"fired","error":null})JSON");
  }

  int32_t cancel(uint64_t timerId) override {
    cancelIds.add(timerId);
    auto entry = find(timerId);
    if (entry == nullptr || entry->released) return -ENOENT;
    entry->cancelled = true;
    return 0;
  }

  Entry* find(uint64_t timerId) {
    for (auto& entry: entries) {
      if (entry.id == timerId) return &entry;
    }
    return nullptr;
  }

  uint64_t nextId = 1;
  size_t readsBeforeFire = 0;
  bool blockLongDelays = false;
  bool reorderStartFields = false;
  kj::Maybe<kj::String> startOverride;
  kj::Maybe<kj::String> readOverride;
  kj::Maybe<kj::String> startErrorCode;
  kj::Maybe<kj::String> startErrorMessage;
  kj::Vector<uint64_t> startDelays;
  kj::Vector<uint64_t> readIds;
  kj::Vector<uint64_t> cancelIds;
  kj::Vector<Entry> entries;
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

class SelfTestIngressHost final: public IngressHostChannel {
 public:
  kj::String send(kj::StringPtr frame) override {
    ++sendAttempts;
    KJ_IF_SOME(callback, beforeSend) {
      callback(frame);
    }
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(frame, root);
    auto id = jsonField(root, "request_id"_kj).getString();
    auto sequence = jsonInteger(jsonField(root, "sequence"_kj), MAX_HOST_OPERATION_ID);
    KJ_REQUIRE(sequence == sent.size(), "guest output sequence mismatch");
    bool backpressure = backpressureCount > 0;
    if (backpressure) --backpressureCount;
    if (jsonField(root, "kind"_kj).getString() == "websocket"_kj &&
        jsonField(root, "opcode"_kj).getNumber() == 8 && closeBackpressureCount > 0) {
      backpressure = true;
      --closeBackpressureCount;
    }
    if (backpressure) {
      return kj::str("{\"protocol_version\":1,\"request_id\":\"", id, "\",\"sequence\":", sequence,
          ",\"accepted\":false,\"cancelled\":false,\"error\":\"backpressure\"}");
    }
    sent.add(kj::str(frame));
    return kj::str("{\"protocol_version\":1,\"request_id\":\"", id, "\",\"sequence\":", sequence,
        ",\"accepted\":", cancelSend ? "false" : "true",
        ",\"cancelled\":", cancelSend ? "true" : "false", "}");
  }

  kj::String receive(kj::StringPtr query) override {
    KJ_REQUIRE(query.size() > 0 && received < incoming.size(),
        "guest requested an unexpected ingress frame", received);
    if (pendingReceives > 0) {
      --pendingReceives;
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(incoming[received], root);
      return kj::str("{\"protocol_version\":1,\"request_id\":\"",
          jsonField(root, "request_id"_kj).getString(),
          "\",\"sequence\":", jsonInteger(jsonField(root, "sequence"_kj), MAX_HOST_OPERATION_ID),
          ",\"kind\":\"pending\"}");
    }
    return kj::str(incoming[received++]);
  }

  kj::Vector<kj::String> incoming;
  kj::Vector<kj::String> sent;
  size_t received = 0;
  bool cancelSend = false;
  size_t backpressureCount = 0;
  size_t closeBackpressureCount = 0;
  size_t pendingReceives = 0;
  size_t sendAttempts = 0;
  kj::Maybe<kj::Function<void(kj::StringPtr)>> beforeSend;
};

class SelfTestBundleHost final: public BundleHostChannel {
 public:
  struct Source {
    kj::String name;
    kj::String type;
    kj::Array<byte> bytes;
  };

  kj::Array<byte> read(kj::StringPtr query, size_t maxBytes) override {
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(query, root);
    KJ_REQUIRE(root.isObject() && root.getObject().size() == 6 &&
            jsonInteger(jsonField(root, "protocol_version"_kj), 1) == 1 &&
            jsonField(root, "worker_version"_kj).getString() == "chunked-v4"_kj &&
            jsonField(root, "bundle_sha256"_kj).getString() == bundleDigest,
        "bundle read did not bind immutable worker identity");
    auto name = jsonField(root, "module_name"_kj).getString();
    for (const auto& source: sources) {
      if (source.name != name) continue;
      auto offset = jsonInteger(jsonField(root, "offset"_kj), UINT32_MAX);
      auto amount = jsonInteger(jsonField(root, "max_bytes"_kj), MAX_BUNDLE_READ_BYTES);
      KJ_REQUIRE(amount > 0 && amount == maxBytes && offset < source.bytes.size() &&
              amount <= source.bytes.size() - offset,
          "bundle read ignored descriptor/chunk bounds");
      ++reads;
      auto result = kj::heapArray<byte>(shortRead ? amount - 1 : amount);
      result.asPtr().copyFrom(source.bytes.slice(offset, offset + result.size()));
      if (corrupt && result.size() > 0) result[0] ^= 1;
      return result;
    }
    KJ_FAIL_REQUIRE("unknown immutable module");
  }

  kj::String manifest() {
    kj::Vector<char> output;
    output.addAll(
        "{\"protocol_version\":4,\"worker_version\":\"chunked-v4\",\"bundle_sha256\":"_kj);
    appendJsonString(output, bundleDigest);
    output.addAll(",\"compatibility_date\":\"2025-12-31\",\"compatibility_flags\":[],"
                  "\"main_module\":\"worker.js\",\"modules\":["_kj);
    for (auto i: kj::indices(sources)) {
      if (i > 0) output.add(',');
      output.addAll("{\"name\":"_kj);
      appendJsonString(output, sources[i].name);
      output.addAll(",\"type\":"_kj);
      appendJsonString(output, sources[i].type);
      output.addAll(kj::str(",\"source_bytes\":", sources[i].bytes.size(), ",\"source_sha256\":"));
      appendJsonString(output, sha256Hex(sources[i].bytes));
      output.add('}');
    }
    output.addAll("],\"storage\":[],\"bindings\":[]}"_kj);
    output.add('\0');
    return kj::String(output.releaseAsArray());
  }

  kj::String bundleDigest =
      kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  kj::Vector<Source> sources;
  size_t reads = 0;
  bool corrupt = false;
  bool shortRead = false;
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

class FailingFetchBroker final: public FetchBroker {
 public:
  explicit FailingFetchBroker(kj::Exception exception): exception(kj::mv(exception)) {}

  kj::Promise<void> request(FetchRequest,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      kj::HttpService::Response&,
      TimerChannel&) override {
    kj::throwRecoverableException(kj::mv(exception));
    KJ_UNREACHABLE;
  }

 private:
  kj::Exception exception;
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

class ConcurrentV2FetchHostChannel final: public V2FetchHostChannel {
 public:
  kj::String start(kj::StringPtr) override {
    auto id = ++startCount;
    pollCounts.add(0);
    readOffsets.add(0);
    return kj::str(R"JSON({"protocol_version":2,"operation_id":)JSON", id,
        R"JSON(,"max_write_chunk":5,"max_read_chunk":7,"error":null})JSON");
  }

  int32_t write(uint64_t operationId, kj::ArrayPtr<const byte> bytes) override {
    KJ_REQUIRE(operationId > 0 && operationId <= startCount);
    return static_cast<int32_t>(bytes.size());
  }

  int32_t finish(uint64_t operationId) override {
    KJ_REQUIRE(operationId > 0 && operationId <= startCount);
    return 0;
  }

  kj::String poll(uint64_t operationId) override {
    auto& count = pollCounts[operationId - 1];
    if (count++ == 0) {
      return kj::str(R"JSON({"protocol_version":2,"operation_id":)JSON", operationId,
          R"JSON(,"state":"uploading","response":null,"error":null})JSON");
    }
    return kj::str(R"JSON({"protocol_version":2,"operation_id":)JSON", operationId,
        R"JSON(,"state":"response","response":{"protocol_version":2,"request_id":"r-1","status":200,"header_block_length":2,"body_length":0},"error":null})JSON");
  }

  kj::Array<byte> read(uint64_t operationId, size_t maxBytes) override {
    KJ_REQUIRE(operationId > 0 && operationId <= startCount && maxBytes == 7);
    auto& offset = readOffsets[operationId - 1];
    if (offset == 0) {
      offset = 2;
      auto result = kj::heapArray<byte>(3);
      result[0] = 1;
      result[1] = '[';
      result[2] = ']';
      return result;
    }
    auto result = kj::heapArray<byte>(1);
    result[0] = 2;
    return result;
  }

  int32_t cancel(uint64_t operationId) override {
    KJ_REQUIRE(operationId > 0 && operationId <= startCount);
    return 0;
  }

  uint64_t startCount = 0;
  kj::Vector<size_t> pollCounts;
  kj::Vector<size_t> readOffsets;
};

class ControlledSelfTestTimer final: public TimerChannel {
 public:
  void syncTime() override {}
  kj::Date now(kj::Maybe<kj::Date>) override {
    return kj::UNIX_EPOCH;
  }
  kj::Promise<void> atTime(kj::Date) override {
    return kj::READY_NOW;
  }
  kj::Promise<void> afterLimitTimeout(kj::Duration) override {
    auto paf = kj::newPromiseAndFulfiller<void>();
    fulfillers.add(kj::mv(paf.fulfiller));
    return kj::mv(paf.promise);
  }

  void releaseAll() {
    for (auto& fulfiller: fulfillers) {
      fulfiller->fulfill();
    }
    fulfillers.clear();
  }

 private:
  kj::Vector<kj::Own<kj::PromiseFulfiller<void>>> fulfillers;
};

class SelfTestLogicalServiceHost final: public LogicalServiceHostChannel {
 public:
  explicit SelfTestLogicalServiceHost(kj::String response): response(kj::mv(response)) {}

  kj::String invoke(kj::ArrayPtr<const char> canonicalEnvelope) override {
    ++invokeCount;
    request = kj::str(canonicalEnvelope);
    return kj::str(response);
  }

  size_t invokeCount = 0;
  kj::Maybe<kj::String> request;

 private:
  kj::String response;
};

class SelfTestEntropyHost final: public EntropyHostChannel {
 public:
  kj::Array<byte> read(size_t amount) override {
    KJ_REQUIRE(amount > 0 && amount <= 16 * 1024, "entropy call ignored bounded ABI");
    amounts.add(amount);
    KJ_REQUIRE(!fail, "test entropy provider unavailable");
    return system->read(shortRead ? amount - 1 : amount);
  }

  kj::Rc<EntropyHostChannel> system = newSystemEntropyHostChannel();
  kj::Vector<size_t> amounts;
  bool fail = false;
  bool shortRead = false;
};

class SelfTestProviderHost final: public WebSocketHostChannel {
 public:
  struct Session {
    kj::String id;
    uint64_t handle;
    uint64_t sent = 0;
    uint64_t received = 0;
    bool opened = false;
    bool closing = false;
    bool released = false;
    uint16_t code = 1000;
    kj::String reason = kj::str("");
    struct Frame {
      uint8_t opcode;
      kj::String body;
    };
    kj::Vector<Frame> frames;
    size_t readOffset = 0;
  };

  kj::String open(kj::StringPtr query) override {
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(query, root);
    KJ_REQUIRE(root.getObject().size() == 5 &&
            jsonField(root, "binding"_kj).getString() == "transcribe"_kj &&
            jsonField(root, "url"_kj).getString() ==
                "wss://provider.example/realtime?model=public"_kj,
        "provider open changed scope, routing, or supplied credentials");
    auto id = kj::str(jsonField(root, "request_id"_kj).getString());
    ++openCount;
    if (deny) return reply(id, "denied"_kj, "scope_denied"_kj);
    auto handle = sessions.size() + 1;
    sessions.add(Session{kj::mv(id), handle});
    return kj::str(
        prefix(sessions.back().id, "pending"_kj, "connecting"_kj), ",\"handle_id\":", handle, "}");
  }
  kj::String send(kj::StringPtr query) override {
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(query, root);
    auto& session = get(root);
    KJ_REQUIRE(root.getObject().size() == 7 &&
            jsonInteger(jsonField(root, "sequence"_kj), MAX_HOST_OPERATION_ID) == session.sent,
        "provider send consumed sequence on backpressure");
    if (sendBackpressure > 0) {
      --sendBackpressure;
      return kj::str(prefix(session.id, "pending"_kj, "backpressure"_kj), ",\"accepted\":false}");
    }
    auto body = kj::str(jsonField(root, "body_base64"_kj).getString());
    auto decoded = kj::decodeBase64(body);
    KJ_REQUIRE(!decoded.hadErrors && decoded.size() <= MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES &&
            session.frames.size() - session.readOffset < 4,
        "provider send ignored frame/queue cap");
    auto opcode = jsonInteger(jsonField(root, "opcode"_kj), 2);
    session.frames.add(Session::Frame{static_cast<uint8_t>(opcode), kj::mv(body)});
    ++session.sent;
    ++acceptedSends;
    return kj::str(prefix(session.id, "ok"_kj, "accepted"_kj), ",\"accepted\":true}");
  }
  kj::String receive(kj::StringPtr query) override {
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(query, root);
    auto& session = get(root);
    KJ_REQUIRE(root.getObject().size() == 5 &&
            jsonInteger(jsonField(root, "sequence"_kj), MAX_HOST_OPERATION_ID) == session.received,
        "provider receive consumed a pending sequence");
    if (beforeReceive != kj::none) KJ_ASSERT_NONNULL(beforeReceive)();
    KJ_REQUIRE(!revokeCalls || !session.opened, "provider callback authority revoked");
    if (receiveFailure && session.opened) {
      return reply(session.id, "host_error"_kj, "transport_failed"_kj);
    }
    if (pendingReceives > 0) {
      --pendingReceives;
      return reply(session.id, "pending"_kj, "pending"_kj);
    }
    auto base = kj::str(prefix(session.id, "ok"_kj, "event"_kj), ",\"handle_id\":", session.handle,
        ",\"sequence\":", session.received);
    if (!session.opened) {
      session.opened = true;
      ++session.received;
      return kj::str(base, ",\"kind\":\"open\"}");
    }
    if (session.readOffset < session.frames.size()) {
      auto& frame = session.frames[session.readOffset++];
      ++session.received;
      return kj::str(base, ",\"kind\":\"message\",\"opcode\":", static_cast<uint>(frame.opcode),
          ",\"body_base64\":\"", frame.body, "\"}");
    }
    if (session.closing) {
      session.released = true;
      ++session.received;
      kj::Vector<char> reason;
      appendJsonString(reason, session.reason);
      return kj::str(base, ",\"kind\":\"close\",\"close_code\":", session.code,
          ",\"reason\":", reason.asPtr(), "}");
    }
    return reply(session.id, "pending"_kj, "pending"_kj);
  }
  kj::String close(kj::StringPtr query) override {
    KJ_REQUIRE(!revokeCalls, "provider callback authority revoked");
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(query, root);
    auto& session = get(root);
    if (jsonField(root, "mode"_kj).getString() == "cancel"_kj) {
      KJ_REQUIRE(root.getObject().size() == 5, "provider cancellation supplied close payload");
      ++cancelCount;
      if (cancelPending) {
        return kj::str(prefix(session.id, "pending"_kj, "cancelling"_kj), ",\"accepted\":true}");
      }
      session.released = true;
      return reply(session.id, "ok"_kj, "cancelled"_kj);
    }
    KJ_REQUIRE(root.getObject().size() == 7, "provider close supplied unexpected fields");
    if (closeBackpressure > 0) {
      --closeBackpressure;
      return kj::str(prefix(session.id, "pending"_kj, "backpressure"_kj), ",\"accepted\":false}");
    }
    session.code = jsonInteger(jsonField(root, "code"_kj), 4999);
    session.reason = kj::str(jsonField(root, "reason"_kj).getString());
    session.closing = true;
    return kj::str(prefix(session.id, "pending"_kj, "closing"_kj), ",\"accepted\":true}");
  }

  size_t openCount = 0;
  size_t acceptedSends = 0;
  size_t cancelCount = 0;
  size_t pendingReceives = 3;
  size_t sendBackpressure = 2;
  size_t closeBackpressure = 1;
  bool deny = false;
  bool cancelPending = false;
  bool receiveFailure = false;
  bool revokeCalls = false;
  kj::Maybe<kj::Function<void()>> beforeReceive;
  kj::Vector<Session> sessions;

 private:
  Session& get(capnp::JsonValue::Reader root) {
    auto handle = jsonInteger(jsonField(root, "handle_id"_kj), MAX_HOST_OPERATION_ID);
    KJ_REQUIRE(handle > 0 && handle <= sessions.size(), "provider used an unknown handle");
    auto& session = sessions[handle - 1];
    KJ_REQUIRE(jsonField(root, "binding"_kj).getString() == "transcribe"_kj &&
            jsonField(root, "request_id"_kj).getString() == session.id && !session.released,
        "provider callback used revoked session authority");
    return session;
  }
  kj::String prefix(kj::StringPtr id, kj::StringPtr status, kj::StringPtr code) {
    return kj::str("{\"code\":\"", code, "\",\"status\":\"", status, "\",\"request_id\":\"", id,
        "\",\"protocol_version\":1");
  }
  kj::String reply(kj::StringPtr id, kj::StringPtr status, kj::StringPtr code) {
    return kj::str(prefix(id, status, code), "}");
  }
};

int selfTest(bool filesystemEvidence) {
  auto expectRejected = [](kj::ArrayPtr<const char> input) {
    Executor executor;
    try {
      executor.initialize(input);
    } catch (...) {
      return;
    }
    KJ_FAIL_REQUIRE("invalid init envelope was accepted");
  };

  auto expectCanonicalBundle = [](kj::StringPtr input) {
    auto bundle = parseWorkerBundle(input);
    KJ_REQUIRE(
        serializeWorkerBundle(bundle) == input, "canonical init envelope did not round-trip");
  };

  expectCanonicalBundle(
      R"JSON({"protocol_version":1,"worker_version":"v1","compatibility_date":"2025-01-01","compatibility_flags":["nodejs_compat"],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}]})JSON"_kj);
  expectCanonicalBundle(
      R"JSON({"protocol_version":2,"worker_version":"v2","compatibility_date":"2025-01-01","compatibility_flags":["nodejs_compat"],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[{"name":"readonly","mode":"ro"},{"name":"scratch","mode":"rw"}]})JSON"_kj);
  expectCanonicalBundle(
      R"JSON({"protocol_version":3,"worker_version":"v3","compatibility_date":"2025-01-01","compatibility_flags":["nodejs_compat"],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[],"bindings":[{"name":"cache","kind":"cache"},{"name":"database","kind":"d1"},{"name":"objects","kind":"durable_object"},{"name":"settings","kind":"kv"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":1,"worker_version":"v1-storage","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":2,"worker_version":"unsorted-storage","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[{"name":"scratch","mode":"rw"},{"name":"readonly","mode":"ro"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":2,"worker_version":"duplicate-storage","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[{"name":"scratch","mode":"rw"},{"name":"scratch","mode":"ro"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":2,"worker_version":"storage-host-path","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[{"name":"scratch","mode":"rw","host_path":"/secret"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":2,"worker_version":"storage-limit","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[{"name":"scratch","mode":"rw","max_write_bytes":16}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":2,"worker_version":"storage-mode","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[{"name":"scratch","mode":"read-write"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":2,"worker_version":"v2-bindings","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[],"bindings":[]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":3,"worker_version":"duplicate-bindings","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[],"bindings":[{"name":"settings","kind":"kv"},{"name":"settings","kind":"cache"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":3,"worker_version":"unknown-binding-kind","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[],"bindings":[{"name":"settings","kind":"identity"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":3,"worker_version":"binding-host-path","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}],"storage":[],"bindings":[{"name":"settings","kind":"kv","host_path":"/secret"}]})JSON"_kj);

  auto expectScheduledRejected = [](kj::StringPtr input) {
    try {
      parseScheduledRequest(input);
    } catch (const kj::Exception&) {
      return;
    }
    KJ_FAIL_REQUIRE("invalid scheduled envelope was accepted");
  };
  {
    auto request = parseScheduledRequest(
        R"JSON({"protocol_version":1,"request_id":"scheduled-1","scheduled_time_unix_ms":1767225600000,"cron":"0 0 * * *"})JSON"_kj);
    KJ_REQUIRE(request.requestId == "scheduled-1"_kj &&
            request.scheduledTime == kj::UNIX_EPOCH + 1767225600000 * kj::MILLISECONDS &&
            request.cron == "0 0 * * *"_kj,
        "canonical scheduled envelope parsed incorrectly");
  }
  expectScheduledRejected(
      R"JSON({"protocol_version":1,"request_id":"scheduled-1","cron":"0 0 * * *","scheduled_time_unix_ms":1767225600000})JSON"_kj);
  expectScheduledRejected(
      R"JSON({"protocol_version":1,"request_id":"scheduled-1","scheduled_time_unix_ms":1767225600000.5,"cron":"0 0 * * *"})JSON"_kj);
  expectScheduledRejected(
      R"JSON({"protocol_version":1,"request_id":"scheduled-1","scheduled_time_unix_ms":1767225600000,"cron":""})JSON"_kj);

  auto expectQueueRejected = [](kj::StringPtr input) {
    try {
      parseQueueRequest(input);
    } catch (const kj::Exception&) {
      return;
    }
    KJ_FAIL_REQUIRE("invalid queue envelope was accepted");
  };
  {
    auto request = parseQueueRequest(
        R"JSON({"protocol_version":1,"request_id":"queue-1","queue":"jobs","messages":[{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"aGVsbG8=","content_type":"text","attempts":2}],"metadata":{"backlog_count":3,"backlog_bytes":5,"oldest_message_timestamp_unix_ms":1767225500000}})JSON"_kj);
    KJ_REQUIRE(request.requestId == "queue-1"_kj && request.request.queueName == "jobs"_kj &&
            request.request.messages.size() == 1 &&
            request.request.messages[0].id == "message-1"_kj &&
            kj::str(request.request.messages[0].body.asChars()) == "hello"_kj &&
            request.request.messages[0].attempts == 2 && request.request.backlogCount == 3 &&
            request.request.backlogBytes == 5,
        "canonical queue envelope parsed incorrectly");
  }
  expectQueueRejected(
      R"JSON({"protocol_version":1,"request_id":"queue-1","queue":"jobs","messages":[{"id":"message-2","timestamp_unix_ms":1767225600000,"body_base64":"dHdv","content_type":"text","attempts":1},{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"b25l","content_type":"text","attempts":1}],"metadata":{"backlog_count":2,"backlog_bytes":6,"oldest_message_timestamp_unix_ms":null}})JSON"_kj);
  expectQueueRejected(
      R"JSON({"protocol_version":1,"request_id":"queue-1","queue":"jobs","messages":[{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"***","content_type":"text","attempts":1}],"metadata":{"backlog_count":1,"backlog_bytes":3,"oldest_message_timestamp_unix_ms":null}})JSON"_kj);

  {
    auto previousCapacity = callBufferCapacity;
    KJ_DEFER(callBufferCapacity = previousCapacity);
    callBufferCapacity = 4096;
    auto output = kj::heapArray<char>(callBufferCapacity);
    for (auto function: {"WorkerdTimerV1Start"_kj, "WorkerdTimerV1Read"_kj}) {
      auto arg = hostCallU64(1);
      auto call = makeHostCall(function, HLCALL_TYPE_STRING, kj::arrayPtr(&arg, 1));
      call.output = output.begin();
      call.output_cap = callBufferCapacity;
      validateHostCall(call);
      call.output_cap = callBufferCapacity + 1;
      try {
        validateHostCall(call);
      } catch (const kj::Exception&) {
        continue;
      }
      KJ_FAIL_REQUIRE("timer host call accepted a non-runtime output capacity", function);
    }
  }

  auto expectFetchFailure = [](FetchFailure failure, kj::Exception::Type expectedType,
                                kj::StringPtr expectedDescription) {
    auto exception = fetchFailureToException(kj::mv(failure));
    KJ_REQUIRE(exception.getType() == expectedType, "fetch failure used wrong KJ exception type");
    KJ_REQUIRE(jsg::isTunneledException(exception.getDescription()) &&
            exception.getDescription().contains(expectedDescription),
        "fetch failure was not tunneled with its host classification", exception);
  };
  expectFetchFailure(FetchFailure{FetchError::DENIED, kj::str("policy_denied: blocked")},
      kj::Exception::Type::FAILED, "jsg.Error: outbound fetch denied by host: policy_denied"_kj);
  expectFetchFailure(FetchFailure{FetchError::HOST_FAILURE, kj::str("dns_failed: no records")},
      kj::Exception::Type::FAILED, "jsg.Error: outbound fetch host call failed: dns_failed"_kj);
  expectFetchFailure(FetchFailure{FetchError::TIMEOUT, kj::str("timeout: deadline")},
      kj::Exception::Type::OVERLOADED, "jsg.Error: outbound fetch timed out: timeout"_kj);
  expectFetchFailure(FetchFailure{FetchError::OVERLOADED, kj::str("overloaded: active limit")},
      kj::Exception::Type::OVERLOADED,
      "jsg.Error: outbound fetch host is overloaded: overloaded"_kj);
  expectFetchFailure(FetchFailure{FetchError::CANCELED, kj::str("cancelled: request aborted")},
      kj::Exception::Type::DISCONNECTED,
      "jsg.DOMException(AbortError): outbound fetch canceled: cancelled"_kj);

  auto expectWorkerFailure = [](kj::Exception failure, kj::StringPtr expectedDescription) {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestTimer timer;
    auto worker =
        newOutboundFetchWorker(kj::rc<FailingFetchBroker>(kj::mv(failure)), timer, kj::str("r-1"));
    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    SelfTestInputStream requestBody{kj::ArrayPtr<const byte>()};
    CapturingResponse response;
    try {
      worker
          ->request(
              kj::HttpMethod::GET, "http://127.0.0.1:8080/"_kj, headers, requestBody, response)
          .wait(waitScope);
      KJ_FAIL_REQUIRE("outbound fetch failure was accepted");
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains(expectedDescription),
          "outbound fetch exception used the wrong JavaScript boundary", exception);
    }
  };
  expectWorkerFailure(
      KJ_EXCEPTION(FAILED, "frozen protocol mismatch"), "jsg.Error: outbound fetch failed: "_kj);
  expectWorkerFailure(JSG_KJ_EXCEPTION(DISCONNECTED, DOMAbortError, "request aborted"),
      "jsg.DOMException(AbortError)"_kj);

  expectRejected(
      R"JSON({ "protocol_version":1,"worker_version":"noncanonical","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":1,"worker_version":"unknown-field","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","unknown":true,"modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":1,"worker_version":"duplicate-module","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"},{"name":"worker.js","type":"esModule","source":"export default {}"}]})JSON"_kj);
  expectRejected(
      R"JSON({"protocol_version":1,"worker_version":"noncanonical-commonjs-type","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"},{"name":"dependency.js","type":"commonjsModule","source":"module.exports = true;"}]})JSON"_kj);
  auto oversized = kj::heapArray<char>(MAX_ENVELOPE_BYTES + 1);
  memset(oversized.begin(), 'x', oversized.size());
  expectRejected(oversized.asPtr());

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestPollTimer pollTimer;
    auto fake = kj::rc<FakeTimerHostChannel>();
    auto& fakeRef = *fake;
    fakeRef.readsBeforeFire = 1;
    fakeRef.reorderStartFields = true;
    auto timer = newTimerChannel(kj::mv(fake), pollTimer);
    timer->afterLimitTimeout(20 * kj::MILLISECONDS).wait(waitScope);
    KJ_REQUIRE(fakeRef.startDelays.size() == 1 &&
            fakeRef.startDelays[0] == 20 * kj::MILLISECONDS / kj::NANOSECONDS &&
            fakeRef.readIds.size() == 2 && fakeRef.readIds[0] == 1 && fakeRef.readIds[1] == 1 &&
            pollTimer.delays.size() == 1 && pollTimer.delays[0] == 1 * kj::MILLISECONDS &&
            fakeRef.cancelIds.size() == 1 && fakeRef.cancelIds[0] == 1 &&
            fakeRef.cancel(1) == -ENOENT,
        "timer pending/fired/release self-test failed");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestPollTimer pollTimer;
    auto fake = kj::rc<FakeTimerHostChannel>();
    auto& fakeRef = *fake;
    auto timer = newTimerChannel(kj::mv(fake), pollTimer);
    timer->afterLimitTimeout(0 * kj::NANOSECONDS).wait(waitScope);
    KJ_REQUIRE(fakeRef.startDelays.size() == 1 && fakeRef.startDelays[0] == 0 &&
            fakeRef.readIds.size() == 1 && pollTimer.delays.size() == 0,
        "zero-delay timer self-test failed");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestPollTimer pollTimer;
    pollTimer.block = true;
    auto fake = kj::rc<FakeTimerHostChannel>();
    auto& fakeRef = *fake;
    fakeRef.readsBeforeFire = 100;
    auto timer = newTimerChannel(kj::mv(fake), pollTimer);
    {
      auto pending = timer->afterLimitTimeout(1 * kj::MILLISECONDS).eagerlyEvaluate(nullptr);
      KJ_REQUIRE(!pending.poll(waitScope), "timer cancellation self-test unexpectedly completed");
    }
    KJ_REQUIRE(fakeRef.cancelIds.size() == 1 && fakeRef.cancelIds[0] == 1 &&
            fakeRef.readIds.size() == 2 && fakeRef.readIds[0] == 1 && fakeRef.readIds[1] == 1 &&
            fakeRef.cancel(1) == -ENOENT,
        "timer cancellation cleanup self-test failed");
  }

  {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestPollTimer pollTimer;
    auto fake = kj::rc<FakeTimerHostChannel>();
    auto& fakeRef = *fake;
    auto timer = newTimerChannel(kj::mv(fake), pollTimer);
    timer->afterLimitTimeout(1 * kj::MILLISECONDS).wait(waitScope);
    timer->afterLimitTimeout(20 * kj::MILLISECONDS).wait(waitScope);
    timer->afterLimitTimeout(35 * kj::MILLISECONDS).wait(waitScope);
    KJ_REQUIRE(fakeRef.startDelays.size() == 3 &&
            fakeRef.startDelays[0] == 1 * kj::MILLISECONDS / kj::NANOSECONDS &&
            fakeRef.startDelays[1] == 20 * kj::MILLISECONDS / kj::NANOSECONDS &&
            fakeRef.startDelays[2] == 35 * kj::MILLISECONDS / kj::NANOSECONDS &&
            fakeRef.readIds.size() == 3 && fakeRef.readIds[0] == 1 && fakeRef.readIds[1] == 2 &&
            fakeRef.readIds[2] == 3,
        "timer ordering self-test failed");
  }

  auto expectTimerRejected = [](kj::Rc<FakeTimerHostChannel> fake) {
    kj::EventLoop eventLoop;
    kj::WaitScope waitScope(eventLoop);
    SelfTestPollTimer pollTimer;
    auto timer = newTimerChannel(kj::mv(fake), pollTimer);
    try {
      timer->afterLimitTimeout(0 * kj::NANOSECONDS).wait(waitScope);
    } catch (const kj::Exception&) {
      return;
    }
    KJ_FAIL_REQUIRE("invalid timer host response was accepted");
  };

  {
    auto fake = kj::rc<FakeTimerHostChannel>();
    fake->startErrorCode = kj::str("invalid_duration");
    fake->startErrorMessage = kj::str("timer duration overflows monotonic time");
    expectTimerRejected(kj::mv(fake));
  }
  {
    auto fake = kj::rc<FakeTimerHostChannel>();
    fake->startErrorCode = kj::str("overloaded");
    fake->startErrorMessage = kj::str("active timer limit reached");
    expectTimerRejected(kj::mv(fake));
  }
  {
    auto fake = kj::rc<FakeTimerHostChannel>();
    fake->startOverride = kj::str("{}");
    expectTimerRejected(kj::mv(fake));
  }
  {
    auto fake = kj::rc<FakeTimerHostChannel>();
    fake->readOverride = kj::str(
        R"JSON({"protocol_version":1,"timer_id":1,"state":"error","error":{"code":"unknown_timer","message":"unknown or released timer"}})JSON");
    expectTimerRejected(kj::mv(fake));
  }
  {
    auto fake = kj::rc<FakeTimerHostChannel>();
    fake->readOverride = kj::str(
        R"JSON({"protocol_version":1,"timer_id":1,"state":"pending","error":null,"extra":true})JSON");
    expectTimerRejected(kj::mv(fake));
  }

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
    ControlledSelfTestTimer timer;
    auto fake = kj::heap<ConcurrentV2FetchHostChannel>();
    auto& fakeRef = *fake;
    auto broker = kj::rc<StreamingV2FetchBroker>(kj::mv(fake));
    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    kj::Vector<kj::Own<SelfTestInputStream>> requestBodies;
    kj::Vector<kj::Own<CapturingResponse>> responses;
    kj::Vector<kj::Promise<void>> pending;

    for (size_t i = 0; i < MAX_CONCURRENT_OUTBOUND_FETCHES; ++i) {
      requestBodies.add(kj::heap<SelfTestInputStream>(kj::ArrayPtr<const byte>()));
      responses.add(kj::heap<CapturingResponse>());
      auto promise = broker
                         ->request(FetchRequest{kj::str("r-1"), kj::HttpMethod::GET,
                                     kj::str("https://concurrency.example/"),
                                     kj::heapArray<Header>(0), uint64_t(0)},
                             headers, *requestBodies.back(), *responses.back(), timer)
                         .eagerlyEvaluate(nullptr);
      KJ_REQUIRE(
          !promise.poll(waitScope), "bounded v2 fetch unexpectedly completed before release");
      pending.add(kj::mv(promise));
    }
    KJ_REQUIRE(fakeRef.startCount == MAX_CONCURRENT_OUTBOUND_FETCHES,
        "bounded v2 fetch did not start operations 1-16");

    SelfTestInputStream overflowBody{kj::ArrayPtr<const byte>()};
    CapturingResponse overflowResponse;
    try {
      broker
          ->request(
              FetchRequest{kj::str("r-1"), kj::HttpMethod::GET,
                kj::str("https://concurrency.example/"), kj::heapArray<Header>(0), uint64_t(0)},
              headers, overflowBody, overflowResponse, timer)
          .wait(waitScope);
      KJ_FAIL_REQUIRE("17th concurrent v2 fetch operation was accepted");
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getType() == kj::Exception::Type::OVERLOADED &&
              jsg::isTunneledException(exception.getDescription()) &&
              exception.getDescription().contains(
                  "overloaded: too many concurrent outbound fetch v2 operations"),
          "17th concurrent v2 fetch operation did not surface stable overload", exception);
    }
    KJ_REQUIRE(fakeRef.startCount == MAX_CONCURRENT_OUTBOUND_FETCHES,
        "overloaded v2 fetch reached the host");

    timer.releaseAll();
    for (auto& promise: pending) {
      promise.wait(waitScope);
    }
    for (const auto& response: responses) {
      KJ_REQUIRE(response->statusCode == 200 && response->body.size() == 0,
          "bounded v2 fetch operation did not fulfill");
    }
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

  auto fetchBody = [](Executor& executor, kj::StringPtr request) {
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
        return copyBytes(decoded);
      }
    }
    KJ_FAIL_REQUIRE("self-test response is missing body");
  };

  auto run = [&fetchBody](WorkerBundle bundle, kj::StringPtr request,
                 kj::FunctionParam<void(kj::StringPtr)> check) {
    Executor executor;
    auto init = serializeWorkerBundle(bundle);
    executor.initialize(init);
    check(fetchBody(executor, request));
  };

  auto expectInitFailure = [](WorkerBundle bundle, kj::StringPtr expectedDescription) {
    Executor executor;
    try {
      executor.initialize(serializeWorkerBundle(bundle));
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains(expectedDescription),
          "bundle initialization failed for the wrong reason", exception);
      return;
    }
    KJ_FAIL_REQUIRE("invalid bundle graph initialized successfully");
  };

  {
    auto provider = kj::rc<SelfTestProviderHost>();
    fprintf(stderr, "self-test: scoped provider duplex and JS bounds\n");
    auto timers = kj::rc<FakeTimerHostChannel>();
    Executor executor(newHyperlightLogicalServiceHostChannel(),
        kj::rc<HyperlightIngressHostChannel>(), timers.addRef(),
        kj::rc<HyperlightBundleHostChannel>(), newSystemEntropyHostChannel(), provider.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
      export default {
        async fetch(request, env) {
          for (const invalid of [
            () => env.transcribe.connect('ws://provider.example/realtime'),
            () => env.transcribe.connect('wss://user:secret@provider.example/realtime'),
            () => env.transcribe.connect('wss://provider.example/realtime#fragment'),
            () => env.transcribe.connect('wss://provider.example/realtime', ['duplicate', 'duplicate']),
            () => env.transcribe.connect('wss://provider.example/realtime', ['invalid protocol']),
            () => env.transcribe.connect('wss://provider.example/realtime', [], {headers: {'api-key': 'forbidden'}})
          ]) {
            let rejected = false;
            try { invalid(); } catch (error) { rejected = error instanceof TypeError; }
            if (!rejected) throw new Error('invalid provider input was accepted');
          }
          const socket = env.transcribe.connect(
            'wss://provider.example/realtime?model=public', ['public']);
          if (socket.readyState !== WebSocket.CONNECTING) throw new Error('not async connecting');
          if (!(socket instanceof WebSocket) || socket.bufferedAmount !== 0 ||
              'bufferedAmount' in WebSocket.prototype) throw new Error(
                'provider shape: ' + (socket instanceof WebSocket) + ',' +
                socket.bufferedAmount + ',' + ('bufferedAmount' in WebSocket.prototype));
          socket.binaryType = 'arraybuffer';
          const defaultClose = new URL(request.url).pathname === '/default-close';
          const result = await new Promise((resolve, reject) => {
            const events = [];
            socket.onopen = () => {
              try {
                events.push('open');
                let oversized = false;
                try { socket.send(new Uint8Array(16385)); }
                catch (error) { oversized = error instanceof RangeError; }
                if (!oversized) throw new Error('provider accepted an oversized JS frame');
                socket.send('hello');
                socket.send(new Uint8Array([0, 1, 255]));
                socket.send('');
                socket.send(new ArrayBuffer(0));
                let capped = false;
                try { socket.send('fifth'); }
                catch (error) { capped = error instanceof RangeError; }
                if (!capped) throw new Error('provider JS queue exceeded four frames');
                if (socket.bufferedAmount !== 8) throw new Error('queued bytes were not accounted');
              } catch (error) {
                socket.close(1011, 'test failure');
                reject(error);
              }
            };
            socket.onmessage = event => {
              events.push(typeof event.data === 'string' ? event.data :
                Array.from(new Uint8Array(event.data)).join(','));
              if (events.length === 5) {
                if (socket.bufferedAmount !== 0) throw new Error('accepted bytes remained buffered');
                if (defaultClose) socket.close();
                else socket.close(1000, 'done');
              }
            };
            socket.onerror = event => reject(new Error(
              'provider error: ' + (event.error?.message ?? event.message ?? 'unknown')));
            socket.onclose = event => {
              if (event.reason !== (defaultClose ? '' : 'done')) {
                reject(new Error('provider close reason changed'));
                return;
              }
              events.push(String(event.code));
              resolve(events.join('|'));
            };
          });
          return new Response(result);
        }
      };
    )JS"_kj)};
    auto bindings = kj::heapArray<Binding>(1);
    bindings[0] = Binding{kj::str("transcribe"), composite::BindingKind::PROVIDER_WEBSOCKET};
    WorkerBundle bundle{
      .workerVersion = kj::str("authenticated-provider-websocket"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
      .protocolVersion = 3,
      .storageMounts = kj::heapArray<StorageMount>(0),
      .bindings = kj::mv(bindings),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    executor.control(
        R"JSON({"protocol_version":1,"request_id":"provider-capabilities"})JSON"_kj, false);
    provider->beforeReceive = [&]() {
      bool rejected = false;
      try {
        executor.control(R"JSON({"protocol_version":1,"request_id":"provider-park"})JSON"_kj, true);
      } catch (const kj::Exception&) {
        rejected = true;
      }
      KJ_REQUIRE(rejected, "active outbound provider WebSocket was falsely snapshot-safe");
    };
    for (size_t i = 0; i < 2; ++i) {
      auto request = i == 0
          ? R"JSON({"protocol_version":1,"request_id":"provider-session","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj
          : R"JSON({"protocol_version":1,"request_id":"provider-default-close","method":"GET","url":"https://example.test/default-close","headers":[],"body_base64":""})JSON"_kj;
      auto result = fetchBody(executor, request);
      KJ_REQUIRE(result == "open|hello|0,1,255|||1000"_kj,
          "native provider WebSocket open/text/binary/close semantics failed", result);
      executor.control(
          R"JSON({"protocol_version":1,"request_id":"provider-drained"})JSON"_kj, true);
    }
    KJ_REQUIRE(
        provider->openCount == 2 && provider->acceptedSends == 8 && provider->cancelCount == 0,
        "provider WebSocket repeated sessions lost data or required abnormal cancellation");
    for (const auto& session: provider->sessions) {
      KJ_REQUIRE(session.released, "provider close returned before joined terminal handle release");
    }
  }

  auto oneModule = [](kj::StringPtr name, kj::StringPtr source) {
    auto modules = kj::heapArrayBuilder<Module>(1);
    modules.add(Module{kj::str(name), ModuleType::ES_MODULE, copyModuleBytes(source)});
    return modules.finish();
  };

  for (size_t failure = 0; failure < 4; ++failure) {
    fprintf(stderr, "self-test: scoped provider failure %zu\n", failure);
    auto provider = kj::rc<SelfTestProviderHost>();
    provider->pendingReceives = 0;
    provider->deny = failure == 0;
    provider->receiveFailure = failure == 1;
    provider->revokeCalls = failure == 3;
    Executor executor(newHyperlightLogicalServiceHostChannel(),
        kj::rc<HyperlightIngressHostChannel>(), kj::rc<FakeTimerHostChannel>(),
        kj::rc<HyperlightBundleHostChannel>(), newSystemEntropyHostChannel(), provider.addRef());
    auto bindings = kj::heapArray<Binding>(1);
    bindings[0] = Binding{kj::str("transcribe"), composite::BindingKind::PROVIDER_WEBSOCKET};
    WorkerBundle bundle{
      .workerVersion = kj::str("provider-websocket-failure"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = oneModule("worker.js"_kj, R"JS(
        export default {
          async fetch(request, env) {
            let socket;
            try {
              socket = env.transcribe.connect('wss://provider.example/realtime?model=public');
            } catch (error) {
              return new Response('rejected');
            }
            const result = await new Promise((resolve, reject) => {
              const events = [];
              socket.onopen = () => events.push('open');
              socket.onerror = event => {
                if (String(event.error?.message ?? event.message ?? '').includes('provider.example')) {
                  reject(new Error('provider error leaked its endpoint'));
                }
                events.push('error');
              };
              socket.onclose = event => {
                events.push(String(event.code));
                resolve(events.join('|'));
              };
            });
            return new Response(result);
          }
        };
      )JS"_kj),
      .protocolVersion = 3,
      .storageMounts = kj::heapArray<StorageMount>(0),
      .bindings = kj::mv(bindings),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    if (failure != 2) {
      executor.control(
          R"JSON({"protocol_version":1,"request_id":"provider-failure-capabilities"})JSON"_kj,
          false);
    }
    auto result = fetchBody(executor,
        R"JSON({"protocol_version":1,"request_id":"provider-failure","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj);
    auto expected = failure == 0 ? "error|1006"_kj
        : failure == 1           ? "open|error|1006"_kj
                                 : "rejected"_kj;
    if (failure == 3) {
      KJ_REQUIRE(result.contains("\"error\":\"worker_execution_failed\""_kj) &&
              result.contains("unsupported live external handles"_kj) &&
              !provider->sessions[0].released,
          "revoked provider callbacks falsely confirmed joined invocation completion", result);
    } else {
      KJ_REQUIRE(
          result == expected, "native provider failure events or negotiation gate changed", result);
    }
    KJ_REQUIRE(provider->openCount == (failure != 2 ? 1 : 0),
        "unnegotiated provider capability invoked a host callback");
    if (failure == 1) {
      KJ_REQUIRE(provider->cancelCount == 1 && provider->sessions[0].released,
          "provider EOF did not cancel and join the host worker");
    }
    if (failure == 2) {
      executor.control(
          R"JSON({"protocol_version":1,"request_id":"provider-post-denial-capabilities"})JSON"_kj,
          false);
    }
    if (failure == 3) {
      bool refused = false;
      try {
        executor.control(
            R"JSON({"protocol_version":1,"request_id":"provider-revoked-park"})JSON"_kj, true);
      } catch (const kj::Exception&) {
        refused = true;
      }
      KJ_REQUIRE(refused, "revoked provider callback lifetime was falsely snapshot-safe");
    } else {
      executor.control(
          R"JSON({"protocol_version":1,"request_id":"provider-failure-drained"})JSON"_kj, true);
    }
  }

  {
    kj::EventLoop loop;
    kj::WaitScope waitScope(loop);
    SelfTestTimer timer;
    fprintf(stderr, "self-test: scoped provider broker bounds and joined cancellation\n");
    auto provider = kj::rc<SelfTestProviderHost>();
    provider->pendingReceives = 0;
    auto broker = newAuthenticatedWebSocketBroker(provider.addRef());
    auto protocols = []() { return kj::heapArray<kj::String>(0); };
    auto socket =
        broker
            ->connect(kj::str("transcribe"),
                kj::str("wss://provider.example/realtime?model=public"), protocols(), timer)
            .wait(waitScope);
    KJ_REQUIRE(!broker->isQuiescent(), "open outbound WebSocket was falsely idle");
    auto exact = kj::heapArray<byte>(MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES);
    exact.asPtr().fill(42);
    socket->send(exact).wait(waitScope);
    auto echoed = socket->receive(MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES).wait(waitScope);
    KJ_REQUIRE(
        echoed.is<kj::Array<byte>>() && echoed.get<kj::Array<byte>>().asPtr() == exact.asPtr(),
        "provider binary message lost bytes at exact negotiated limit");
    bool oversized = false;
    try {
      auto tooLarge = kj::heapArray<byte>(MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES + 1);
      socket->send(tooLarge).wait(waitScope);
    } catch (const kj::Exception&) {
      oversized = true;
    }
    KJ_REQUIRE(oversized, "provider accepted a decoded message above the frame limit");
    socket->close(1000, "complete"_kj).wait(waitScope);
    auto close = socket->receive().wait(waitScope);
    KJ_REQUIRE(close.is<kj::WebSocket::Close>() && broker->isQuiescent(),
        "provider normal close did not join and release its handle");
    socket = nullptr;

    {
      auto host = kj::rc<SelfTestProviderHost>();
      host->pendingReceives = 0;
      auto cappedBroker = newAuthenticatedWebSocketBroker(host.addRef());
      kj::Vector<kj::Own<kj::WebSocket>> sockets;
      for (size_t i = 0; i < 4; ++i) {
        sockets.add(
            cappedBroker
                ->connect(kj::str("transcribe"),
                    kj::str("wss://provider.example/realtime?model=public"), protocols(), timer)
                .wait(waitScope));
      }
      bool capped = false;
      try {
        cappedBroker
            ->connect(kj::str("transcribe"),
                kj::str("wss://provider.example/realtime?model=public"), protocols(), timer)
            .wait(waitScope);
      } catch (const kj::Exception&) {
        capped = true;
      }
      KJ_REQUIRE(capped && host->openCount == 4,
          "provider connection cap was exceeded before host admission");
      sockets.clear();
      KJ_REQUIRE(cappedBroker->isQuiescent() && host->cancelCount == 4,
          "provider connection-cap cleanup retained a host handle");
    }

    {
      auto host = kj::rc<SelfTestProviderHost>();
      host->pendingReceives = 0;
      host->sendBackpressure = 0;
      auto boundedBroker = newAuthenticatedWebSocketBroker(host.addRef());
      auto boundedSocket =
          boundedBroker
              ->connect(kj::str("transcribe"),
                  kj::str("wss://provider.example/realtime?model=public"), protocols(), timer)
              .wait(waitScope);
      for (size_t i = 0; i < 256; ++i) {
        boundedSocket->send(exact).wait(waitScope);
        boundedSocket->receive(MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES).wait(waitScope);
      }
      KJ_REQUIRE(
          boundedSocket->sentByteCount() + boundedSocket->receivedByteCount() == 8 * 1024 * 1024,
          "provider session byte budget was not exercised exactly");
      bool bounded = false;
      try {
        boundedSocket->send(kj::arrayPtr(exact.begin(), 1)).wait(waitScope);
      } catch (const kj::Exception&) {
        bounded = true;
      }
      KJ_REQUIRE(bounded && host->acceptedSends == 256,
          "provider cumulative byte budget allowed an extra byte");
      boundedSocket = nullptr;
      KJ_REQUIRE(boundedBroker->isQuiescent(), "provider byte-budget cleanup did not join");
    }

    {
      auto host = kj::rc<SelfTestProviderHost>();
      host->pendingReceives = 0;
      host->cancelPending = true;
      auto cancelledBroker = newAuthenticatedWebSocketBroker(host.addRef());
      auto cancelledSocket =
          cancelledBroker
              ->connect(kj::str("transcribe"),
                  kj::str("wss://provider.example/realtime?model=public"), protocols(), timer)
              .wait(waitScope);
      cancelledSocket->abort();
      KJ_REQUIRE(!cancelledBroker->isQuiescent(), "pending cancellation was falsely snapshot-safe");
      host->cancelPending = false;
      cancelledSocket->abort();
      KJ_REQUIRE(cancelledBroker->isQuiescent() && host->cancelCount == 2,
          "repeated provider cancellation did not wait for the joined result");
    }

    for (auto pending: {false, true}) {
      auto host = kj::rc<SelfTestProviderHost>();
      host->pendingReceives = 0;
      host->cancelPending = pending;
      auto cancelledBroker = newAuthenticatedWebSocketBroker(host.addRef());
      {
        auto cancelledSocket =
            cancelledBroker
                ->connect(kj::str("transcribe"),
                    kj::str("wss://provider.example/realtime?model=public"), protocols(), timer)
                .wait(waitScope);
      }
      KJ_REQUIRE(host->cancelCount == 1 && cancelledBroker->isQuiescent() == !pending,
          "pending provider cancellation was falsely reported joined");
    }
    auto deniedHost = kj::rc<SelfTestProviderHost>();
    deniedHost->deny = true;
    auto deniedBroker = newAuthenticatedWebSocketBroker(deniedHost.addRef());
    bool denied = false;
    try {
      deniedBroker
          ->connect(kj::str("transcribe"), kj::str("wss://provider.example/realtime?model=public"),
              protocols(), timer)
          .wait(waitScope);
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains("Provider WebSocket operation failed") &&
              !exception.getDescription().contains("provider.example"),
          "provider denial leaked routing or returned the wrong failure");
      denied = true;
    }
    KJ_REQUIRE(denied && deniedHost->openCount == 1 && deniedBroker->isQuiescent(),
        "provider policy denial did not fail closed without retaining a phantom handle");
  }

  {
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
      export default {
        fetch() {
          const empty = crypto.getRandomValues(new Uint8Array(0));
          const values = crypto.getRandomValues(new Uint8Array(65536));
          return Response.json({
            uuid: crypto.randomUUID(),
            empty: empty.length,
            first: Array.from(values.slice(0, 32)),
            last: Array.from(values.slice(-32))
          });
        }
      };
    )JS"_kj)};
    WorkerBundle bundle{
      .workerVersion = kj::str("fresh-secure-entropy"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    auto entropy = kj::rc<SelfTestEntropyHost>();
    SandboxRuntime runtime(bundle, kj::rc<StreamingV2FetchBroker>(),
        newHyperlightLogicalServiceHostChannel(), newHyperlightTimerHostChannel(),
        SandboxRuntime::Limits{}, entropy.addRef());
    auto headers = kj::heapArray<Header>(0);
    auto first = runtime.runRequest(kj::HttpMethod::GET, "https://example.test/", headers, ""_kj);
    auto second = runtime.runRequest(kj::HttpMethod::GET, "https://example.test/", headers, ""_kj);
    KJ_REQUIRE(first.body != second.body && entropy->amounts.size() == 10,
        "secure entropy was cached or reused across invocations");
    for (size_t i = 0; i < entropy->amounts.size(); ++i) {
      KJ_REQUIRE(entropy->amounts[i] == (i % 5 == 4 ? 16 : 16384),
          "secure entropy chunk/UUID bounds changed");
    }
    for (auto shortRead: {false, true}) {
      entropy->fail = !shortRead;
      entropy->shortRead = shortRead;
      bool rejected = false;
      try {
        runtime.runRequest(kj::HttpMethod::GET, "https://example.test/", headers, ""_kj);
      } catch (const kj::Exception&) {
        rejected = true;
      }
      KJ_REQUIRE(rejected && !runtime.isQuiescent(),
          "entropy provider failure returned successful or checkpoint-safe data");
    }
  }

  for (auto response:
      {
        R"JSON({"version":2,"request_id":"webhook-1","status":"ok","code":"ok","value":{"valid":true,"event":{"id":"evt-1"}}})JSON"_kj,
        R"JSON({"version":2,"request_id":"webhook-1","status":"ok","code":"ok","value":{"valid":false}})JSON"_kj,
        R"JSON({"version":2,"request_id":"webhook-1","status":"denied","code":"denied"})JSON"_kj,
        R"JSON({"version":2,"request_id":"webhook-1","status":"host_error","code":"host_failure"})JSON"_kj,
        R"JSON({"version":2,"request_id":"wrong-id","status":"ok","code":"ok","value":{"valid":true}})JSON"_kj,
        R"JSON({"version":2,"request_id":"webhook-1","status":"ok","code":"ok","value":{"valid":false,"event":{"id":"unverified"}}})JSON"_kj,
        R"JSON({"version":2,"request_id":"webhook-1","status":"ok","code":"ok","value":{"valid":true,"unexpected":"not-an-event"}})JSON"_kj,
      }) {
    auto host = kj::rc<SelfTestLogicalServiceHost>(kj::str(response));
    auto bundleHost = kj::rc<SelfTestBundleHost>();
    Executor executor(host.addRef(), kj::rc<HyperlightIngressHostChannel>(),
        newHyperlightTimerHostChannel(), bundleHost.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
      export default {
        async fetch(request, env) {
          if (typeof env.webhook.verify !== 'function' || env.webhook.fetch !== undefined ||
              Object.keys(env.webhook).some(key => /secret|token|key/i.test(key))) {
            throw new Error('webhook binding leaked an unsupported interface');
          }
          try {
            const result = await env.webhook.verify(new Uint8Array([255, 0, 1]), 't=1,v1=signature');
            return Response.json(result);
          } catch (error) {
            return new Response('verification-error');
          }
        }
      };
    )JS"_kj)};
    auto bindings = kj::heapArray<Binding>(1);
    bindings[0] = Binding{kj::str("webhook"), composite::BindingKind::WEBHOOK};
    WorkerBundle bundle{
      .workerVersion = kj::str("native-webhook"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
      .protocolVersion = 3,
      .storageMounts = kj::heapArray<StorageMount>(0),
      .bindings = kj::mv(bindings),
    };
    if (response.contains("\"id\":\"evt-1\""_kj)) {
      bundleHost->sources.add(SelfTestBundleHost::Source{
        kj::str("worker.js"), kj::str("esModule"), kj::heapArray<byte>(bundle.modules[0].source)});
      auto manifest = bundleHost->manifest();
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(manifest, root);
      for (auto field: root.getObject()) {
        if (field.getName() == "bindings"_kj) {
          auto binding = field.getValue().initArray(1)[0].initObject(2);
          binding[0].setName("name");
          binding[0].getValue().setString("webhook");
          binding[1].setName("kind");
          binding[1].getValue().setString("webhook");
        }
      }
      executor.initialize(codec.encodeRaw(root));
    } else {
      executor.initialize(serializeWorkerBundle(bundle));
    }
    auto result = fetchBody(executor,
        R"JSON({"protocol_version":1,"request_id":"verify","method":"POST","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj);
    auto expected = response.contains("\"id\":\"evt-1\""_kj)
        ? "{\"valid\":true,\"event\":{\"id\":\"evt-1\"}}"_kj
        : response.endsWith("\"value\":{\"valid\":false}}"_kj) ? "{\"valid\":false}"_kj
                                                               : "verification-error"_kj;
    KJ_REQUIRE(result == expected && host->invokeCount == 1,
        "native webhook status/result validation failed", result);
    KJ_REQUIRE(KJ_REQUIRE_NONNULL(host->request) ==
            R"JSON({"version":2,"request_id":"webhook-1","binding":"webhook","operation":{"kind":"webhook_verify","body_base64":"/wAB","signature":"t=1,v1=signature"}})JSON"_kj,
        "native webhook verification changed raw bytes or included credentials");
  }

  {
    auto host = kj::rc<SelfTestLogicalServiceHost>(kj::str(
        R"JSON({"version":2,"request_id":"webhook-1","status":"ok","code":"ok","value":{"valid":false}})JSON"));
    Executor executor(host.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
      export default {
        async fetch(request, env) {
          let rejected = 0;
          for (const [body, signature] of [
            [new Uint8Array(32769), 'signature'], ['', 's'.repeat(1025)], ['', '']
          ]) {
            try { await env.webhook.verify(body, signature); }
            catch (error) { if (error instanceof RangeError) ++rejected; }
          }
          const result = await env.webhook.verify(new Uint8Array(32768), 's'.repeat(1024));
          return Response.json({ rejected, valid: result.valid });
        }
      };
    )JS"_kj)};
    auto bindings = kj::heapArray<Binding>(1);
    bindings[0] = Binding{kj::str("webhook"), composite::BindingKind::WEBHOOK};
    WorkerBundle bundle{
      .workerVersion = kj::str("native-webhook-bounds"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
      .protocolVersion = 3,
      .storageMounts = kj::heapArray<StorageMount>(0),
      .bindings = kj::mv(bindings),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    KJ_REQUIRE(
        fetchBody(executor,
            R"JSON({"protocol_version":1,"request_id":"verify-bounds","method":"POST","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj) ==
                "{\"rejected\":3,\"valid\":false}"_kj &&
            host->invokeCount == 1,
        "native webhook body/signature bounds were not enforced before dispatch");
  }

  {
    auto host = kj::rc<SelfTestBundleHost>();
    fprintf(stderr, "self-test: immutable bundle loader\n");
    kj::Vector<char> source;
    source.addAll("let counter = 0; export default { fetch() { "
                  "return new Response(String(++counter)); } }; /*"_kj);
    while (source.size() < 757595) source.add('x');
    source.addAll("*/"_kj);
    host->sources.add(SelfTestBundleHost::Source{
      kj::str("worker.js"), kj::str("esModule"), kj::heapArray<byte>(source.asPtr().asBytes())});
    host->sources.add(SelfTestBundleHost::Source{kj::str("addon.wasm"), kj::str("wasm"),
      kj::heapArray<byte>(kj::arrayPtr(reinterpret_cast<const byte*>("\0asm\1\0\0\0"), 8))});
    Executor executor(newHyperlightLogicalServiceHostChannel(),
        kj::rc<HyperlightIngressHostChannel>(), newHyperlightTimerHostChannel(), host.addRef());
    auto manifest = host->manifest();
    KJ_REQUIRE(manifest.size() <= MAX_ENVELOPE_BYTES, "chunked manifest exceeded legacy call cap");
    executor.initialize(manifest);
    KJ_REQUIRE(host->reads == 48 && host->sources[0].bytes.size() == 757597,
        "large immutable module did not use bounded exact reads", host->reads);
    auto request =
        R"JSON({"protocol_version":1,"request_id":"large-source","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj;
    KJ_REQUIRE(fetchBody(executor, request) == "1"_kj && fetchBody(executor, request) == "2"_kj,
        "chunk-loaded JavaScript did not preserve native resident heap");
    executor.control(
        R"JSON({"protocol_version":1,"request_id":"large-capabilities"})JSON"_kj, false);
    executor.control(R"JSON({"protocol_version":1,"request_id":"large-checkpoint"})JSON"_kj, true);
    for (auto failure: {"digest"_kj, "short"_kj}) {
      host->corrupt = failure == "digest"_kj;
      host->shortRead = failure == "short"_kj;
      Executor invalid(newHyperlightLogicalServiceHostChannel(),
          kj::rc<HyperlightIngressHostChannel>(), newHyperlightTimerHostChannel(), host.addRef());
      bool rejected = false;
      try {
        invalid.initialize(manifest);
      } catch (const kj::Exception& exception) {
        KJ_REQUIRE(exception.getDescription().contains(failure == "digest"_kj
                           ? "source digest mismatch"_kj
                           : "short or oversized chunk"_kj),
            "immutable module failure was rejected for the wrong reason", exception);
        rejected = true;
      }
      KJ_REQUIRE(rejected, "invalid immutable module was loaded", failure);
    }
  }

  {
    for (auto failure: {"oversized"_kj, "utf8"_kj, "wasm"_kj}) {
      auto host = kj::rc<SelfTestBundleHost>();
      auto bytes = kj::heapArray<byte>(failure == "oversized"_kj ? MAX_LOADED_MODULE_BYTES + 1
              : failure == "utf8"_kj                             ? 1
                                                                 : 8);
      bytes.asPtr().fill(0xff);
      host->sources.add(
          SelfTestBundleHost::Source{kj::str("worker.js"), kj::str("esModule"), kj::mv(bytes)});
      if (failure == "wasm"_kj) {
        host->sources[0].bytes = copyModuleBytes("export default {};"_kj);
        host->sources.add(SelfTestBundleHost::Source{
          kj::str("invalid.wasm"), kj::str("wasm"), kj::heapArray<byte>(8)});
        host->sources[1].bytes.asPtr().fill(0xff);
      }
      Executor invalid(newHyperlightLogicalServiceHostChannel(),
          kj::rc<HyperlightIngressHostChannel>(), newHyperlightTimerHostChannel(), host.addRef());
      bool rejected = false;
      try {
        invalid.initialize(host->manifest());
      } catch (const kj::Exception& exception) {
        auto expected = failure == "oversized"_kj ? "integer out of range"_kj
            : failure == "utf8"_kj                ? "valid UTF-8"_kj
                                                  : "Wasm module header"_kj;
        KJ_REQUIRE(exception.getDescription().contains(expected),
            "bounded module loader failed for the wrong reason", failure, exception);
        rejected = true;
      }
      KJ_REQUIRE(rejected && (failure != "oversized"_kj || host->reads == 0),
          "bounded module loader accepted invalid source or read before validating size", failure);
    }
  }

  for (auto count: {size_t(8), size_t(9)}) {
    auto host = kj::rc<SelfTestBundleHost>();
    for (size_t i = 0; i < count; ++i) {
      auto source = kj::heapArray<byte>(MAX_LOADED_MODULE_BYTES);
      source.asPtr().fill(' ');
      host->sources.add(
          SelfTestBundleHost::Source{i == 0 ? kj::str("worker.js") : kj::str("module-", i, ".txt"),
            kj::str(i == 0 ? "esModule" : "text"), kj::mv(source)});
    }
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(host->manifest(), root);
    bool rejected = false;
    try {
      auto bundle = loadBundleV4(root, *host);
      KJ_REQUIRE(bundle.modules.size() == 8 && host->reads == 512,
          "exact aggregate/module source boundary was not honored");
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(count == 9 && exception.getDescription().contains("loaded bundle source exceeds"),
          "aggregate source boundary failed for the wrong reason", exception);
      rejected = true;
    }
    KJ_REQUIRE(rejected == (count == 9) && (count != 9 || host->reads == 0),
        "oversized aggregate was read or the exact limit was rejected");
  }

  for (auto method: {"GET"_kj, "HEAD"_kj}) {
    for (auto hasLength: {false, true}) {
      auto ingress = kj::rc<SelfTestIngressHost>();
      Executor executor(newHyperlightLogicalServiceHostChannel(), ingress.addRef());
      auto modules = kj::heapArray<Module>(1);
      modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
      export default {
        fetch(request) {
          if (request.body !== null) throw new Error('body must be absent');
          const clone = new Request(request);
          const cloned = request.clone();
          if (clone.body !== null || cloned.body !== null || clone.method !== request.method) {
            throw new Error('body absence was not preserved when cloning');
          }
          if (request.headers.get('content-length') !== clone.headers.get('content-length')) {
            throw new Error('framing headers were stripped when cloning');
          }
          return new Response(null, {
            status: 204,
            headers: { 'x-body-length': request.headers.get('content-length') ?? 'absent' }
          });
        }
      };
    )JS"_kj)};
      WorkerBundle bundle{
        .workerVersion = kj::str("absent-ingress-body"),
        .compatibilityDate = kj::str("2025-12-31"),
        .compatibilityFlags = kj::heapArray<kj::String>(0),
        .mainModule = kj::str("worker.js"),
        .modules = kj::mv(modules),
      };
      executor.initialize(serializeWorkerBundle(bundle));
      executor.control(
          R"JSON({"protocol_version":1,"request_id":"body-capabilities"})JSON"_kj, false);
      auto invocation =
          kj::str("{\"protocol_version\":1,\"request_id\":\"absent-body\","
                  "\"request\":{\"protocol_version\":1,\"request_id\":\"absent-body\","
                  "\"method\":\"",
              method, "\",\"url\":\"https://example.test/\",\"headers\":",
              hasLength ? "[{\"name\":\"content-length\",\"value\":\"0\"}]" : "[]",
              ",\"body_base64\":\"\"},"
              "\"websocket\":false,\"lifetime_budget_ms\":1000}");
      executor.ingressStream(invocation);
      KJ_REQUIRE(ingress->received == 0 && ingress->sent.size() == 2 &&
              ingress->sent[0].contains("\"status\":204"_kj) &&
              ingress->sent[0].contains(hasLength
                      ? "\"name\":\"x-body-length\",\"value\":\"0\""_kj
                      : "\"name\":\"x-body-length\",\"value\":\"absent\""_kj) &&
              ingress->sent[1].contains("\"kind\":\"end\""_kj),
          "GET/HEAD ingress body presence or Request cloning changed", method);
      executor.control(R"JSON({"protocol_version":1,"request_id":"body-checkpoint"})JSON"_kj, true);
    }
  }

  {
    auto ingress = kj::rc<SelfTestIngressHost>();
    ingress->backpressureCount = 2;
    fprintf(stderr, "self-test: incremental HTTP and tracked work\n");
    ingress->pendingReceives = 2;
    auto requestBytes = kj::heapArray<byte>(40000);
    requestBytes.asPtr().fill('x');
    memcpy(requestBytes.begin(), "hello world", 11);
    size_t sequence = 0;
    for (size_t offset = 0; offset < requestBytes.size(); offset += MAX_INGRESS_FRAME_BYTES) {
      auto amount = kj::min(MAX_INGRESS_FRAME_BYTES, requestBytes.size() - offset);
      ingress->incoming.add(kj::str("{\"protocol_version\":1,\"request_id\":\"stream-1\","
                                    "\"sequence\":",
          sequence++,
          ",\"kind\":\"data\","
          "\"body_base64\":\"",
          kj::encodeBase64(requestBytes.slice(offset, offset + amount)), "\"}"));
    }
    ingress->incoming.add(kj::str(
        R"JSON({"protocol_version":1,"request_id":"stream-1","sequence":3,"kind":"end"})JSON"));
    auto timers = kj::rc<FakeTimerHostChannel>();
    timers->readsBeforeFire = 50;
    Executor executor(newHyperlightLogicalServiceHostChannel(), ingress.addRef(), timers.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(kj::str(R"JS(
      let completed = false;
      export default {
        async fetch(request, env, ctx) {
          if (new URL(request.url).pathname === '/state') return new Response(String(completed));
          const text = await request.text();
          ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 5)).then(() => {
            completed = true;
          }));
          return new Response(new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(text));
              controller.enqueue(new Uint8Array(40000).fill(120));
              controller.close();
            }
          }));
        }
      };
    )JS"))};
    WorkerBundle bundle{
      .workerVersion = kj::str("streaming-e2e"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    executor.control(
        R"JSON({"protocol_version":1,"request_id":"stream-capabilities"})JSON"_kj, false);
    bool responseEndedBeforeDrain = false;
    ingress->beforeSend = [&](kj::StringPtr frame) {
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(frame, root);
      if (jsonField(root, "kind"_kj).getString() == "end"_kj) {
        for (const auto& timer: timers->entries) {
          if (!timer.released) responseEndedBeforeDrain = true;
        }
      }
      try {
        executor.control(R"JSON({"protocol_version":1,"request_id":"active-check"})JSON"_kj, true);
      } catch (const kj::Exception& exception) {
        KJ_REQUIRE(exception.getDescription().contains("outstanding external work"),
            "active checkpoint rejected for the wrong reason");
        return;
      }
      KJ_FAIL_REQUIRE("active stream was falsely acknowledged as snapshot-safe");
    };
    executor.ingressStream(
        R"JSON({"protocol_version":1,"request_id":"stream-1","request":{"protocol_version":1,"request_id":"stream-1","method":"POST","url":"https://example.test/","headers":[],"body_base64":""},"websocket":false,"lifetime_budget_ms":1000})JSON"_kj);
    KJ_REQUIRE(ingress->received == 4 && ingress->sent.size() >= 7,
        "streaming ingress did not incrementally transfer body");
    KJ_REQUIRE(ingress->sendAttempts == ingress->sent.size() + 2,
        "backpressure retry changed frame ordering");
    KJ_REQUIRE(responseEndedBeforeDrain, "HTTP end was conflated with tracked-work completion");
    kj::Vector<byte> responseBytes;
    for (const auto& frame: ingress->sent) {
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(frame, root);
      auto kind = jsonField(root, "kind"_kj).getString();
      if (kind == "data"_kj) {
        auto decoded = kj::decodeBase64(jsonField(root, "body_base64"_kj).getString());
        KJ_REQUIRE(!decoded.hadErrors && decoded.size() <= MAX_INGRESS_FRAME_BYTES,
            "guest output exceeded negotiated frame limit");
        responseBytes.addAll(decoded);
      }
    }
    KJ_REQUIRE(responseBytes.size() == 80000 &&
            responseBytes.asPtr().first(11).asChars() == "hello world"_kj.asArray(),
        "streaming output lost or duplicated bytes");
    KJ_REQUIRE(
        fetchBody(executor,
            R"JSON({"protocol_version":1,"request_id":"stream-state","method":"GET","url":"https://example.test/state","headers":[],"body_base64":""})JSON"_kj) ==
            "true"_kj,
        "streaming CallDone preceded tracked work completion");
    auto checkpoint =
        executor.control(R"JSON({"protocol_version":1,"request_id":"checkpoint-1"})JSON"_kj, true);
    KJ_REQUIRE(checkpoint ==
            "{\"protocol_version\":1,\"request_id\":\"checkpoint-1\",\"quiescent\":true,"
            "\"active_invocations\":0,\"active_streams\":0,\"active_websockets\":0,"
            "\"active_tasks\":0}\n"_kj,
        "guest safe-point golden vector mismatch");
  }

  {
    auto ingress = kj::rc<SelfTestIngressHost>();
    ingress->pendingReceives = 3;
    fprintf(stderr, "self-test: inbound WebSocket lifetime\n");
    ingress->backpressureCount = 1;
    ingress->closeBackpressureCount = 2;
    ingress->incoming.add(
        kj::str(R"JSON({"protocol_version":1,"request_id":"ws-1","sequence":0,"kind":"end"})JSON"));
    ingress->incoming.add(kj::str(
        R"JSON({"protocol_version":1,"request_id":"ws-1","sequence":1,"kind":"websocket","body_base64":"aGVsbG8=","opcode":1})JSON"));
    ingress->incoming.add(kj::str(
        R"JSON({"protocol_version":1,"request_id":"ws-1","sequence":2,"kind":"websocket","body_base64":"AAEC","opcode":2})JSON"));
    ingress->incoming.add(kj::str(
        R"JSON({"protocol_version":1,"request_id":"ws-1","sequence":3,"kind":"websocket","body_base64":"A+hkb25l","opcode":8})JSON"));
    Executor executor(newHyperlightLogicalServiceHostChannel(), ingress.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(kj::str(R"JS(
      export default {
        fetch() {
          const pair = new WebSocketPair();
          pair[1].accept();
          pair[1].send('ready');
          pair[1].addEventListener('message', event => pair[1].send(event.data));
          pair[1].addEventListener('close', event => pair[1].close(event.code, event.reason));
          return new Response(null, { status: 101, webSocket: pair[0] });
        }
      };
    )JS"))};
    WorkerBundle bundle{
      .workerVersion = kj::str("websocket-e2e"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    executor.control(R"JSON({"protocol_version":1,"request_id":"ws-capabilities"})JSON"_kj, false);
    executor.ingressStream(
        R"JSON({"protocol_version":1,"request_id":"ws-1","request":{"protocol_version":1,"request_id":"ws-1","method":"GET","url":"https://example.test/","headers":[{"name":"upgrade","value":"websocket"}],"body_base64":""},"websocket":true,"lifetime_budget_ms":1000})JSON"_kj);
    KJ_REQUIRE(ingress->received == 4 && ingress->sent.size() == 6,
        "inbound WebSocket bridge did not complete text/binary/close lifetime");
    for (size_t i = 1; i <= 3; ++i) {
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder expectedArena;
      capnp::MallocMessageBuilder actualArena;
      auto expected = expectedArena.initRoot<capnp::JsonValue>();
      auto actual = actualArena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(ingress->incoming[i], expected);
      codec.decodeRaw(ingress->sent[i + 1], actual);
      KJ_REQUIRE(jsonField(actual, "kind"_kj).getString() == "websocket"_kj &&
              jsonField(actual, "opcode"_kj).getNumber() ==
                  jsonField(expected, "opcode"_kj).getNumber() &&
              jsonField(actual, "body_base64"_kj).getString() ==
                  jsonField(expected, "body_base64"_kj).getString(),
          "inbound WebSocket echo corrupted a message", i);
    }
    executor.control(R"JSON({"protocol_version":1,"request_id":"ws-checkpoint"})JSON"_kj, true);
  }

  {
    auto ingress = kj::rc<SelfTestIngressHost>();
    ingress->pendingReceives = 8;
    fprintf(stderr, "self-test: bounded WebSocket queue\n");
    ingress->backpressureCount = 2;
    ingress->closeBackpressureCount = 2;
    ingress->incoming.add(kj::str(
        R"JSON({"protocol_version":1,"request_id":"ws-limits","sequence":0,"kind":"end"})JSON"));
    ingress->incoming.add(kj::str(
        R"JSON({"protocol_version":1,"request_id":"ws-limits","sequence":1,"kind":"websocket","body_base64":"A+hkb25l","opcode":8})JSON"));
    Executor executor(newHyperlightLogicalServiceHostChannel(), ingress.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
      export default {
        fetch() {
          const pair = new WebSocketPair();
          pair[1].accept();
          let oversized = false, overloaded = false, queued = 0;
          try { pair[1].send(new Uint8Array(16385)); }
          catch (error) { oversized = error instanceof RangeError; }
          for (let i = 0; i < 32; ++i) {
            try { pair[1].send(new Uint8Array(16384).fill(42)); ++queued; }
            catch (error) { overloaded = error instanceof RangeError; break; }
          }
          pair[1].close(1000, [oversized, overloaded, queued].join(':'));
          return new Response(null, { status: 101, webSocket: pair[0] });
        }
      };
    )JS"_kj)};
    WorkerBundle bundle{
      .workerVersion = kj::str("websocket-limits"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    executor.control(
        R"JSON({"protocol_version":1,"request_id":"ws-limits-capabilities"})JSON"_kj, false);
    executor.ingressStream(
        R"JSON({"protocol_version":1,"request_id":"ws-limits","request":{"protocol_version":1,"request_id":"ws-limits","method":"GET","url":"https://example.test/","headers":[{"name":"upgrade","value":"websocket"}],"body_base64":""},"websocket":true,"lifetime_budget_ms":1000})JSON"_kj);
    size_t messages = 0;
    kj::Maybe<kj::String> reason;
    for (const auto& frame: ingress->sent) {
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(frame, root);
      if (jsonField(root, "kind"_kj).getString() != "websocket"_kj) continue;
      auto body = kj::decodeBase64(jsonField(root, "body_base64"_kj).getString());
      if (jsonField(root, "opcode"_kj).getNumber() == 2) {
        KJ_REQUIRE(body.size() == MAX_INGRESS_FRAME_BYTES,
            "exact WebSocket message limit was not transmitted");
        ++messages;
      } else {
        reason = copyBytes(body.slice(2));
      }
    }
    KJ_REQUIRE(messages >= 4 && messages <= 5 &&
            KJ_REQUIRE_NONNULL(reason) == kj::str("true:true:", messages),
        "native JavaScript WebSocket message/queue bounds were not enforced", messages, reason);
  }

  {
    Executor executor;
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
      export default {
        async fetch(request, env, ctx) {
          const pair = new WebSocketPair();
          pair[0].accept();
          pair[1].accept();
          const received = new Promise(resolve => {
            let count = 0, bytes = 0;
            pair[1].addEventListener('message', event => {
              ++count;
              bytes += typeof event.data === 'string' ? event.data.length : event.data.byteLength;
              if (count === 8) resolve(bytes);
            });
          });
          for (let i = 0; i < 8; ++i) pair[0].send(new Uint8Array(20000));
          const bytes = await received;
          pair[0].close(1000, 'ordinary');
          pair[1].close(1000, 'ordinary');
          return new Response(String(bytes));
        }
      };
    )JS"_kj)};
    WorkerBundle bundle{
      .workerVersion = kj::str("ordinary-websocket-limits-unset"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    KJ_REQUIRE(
        fetchBody(executor,
            R"JSON({"protocol_version":1,"request_id":"ordinary-ws","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj) ==
            "160000"_kj,
        "default-none WebSocket limits changed ordinary Workerd behavior");
  }

  for (auto invalid:
      {
        R"JSON({"protocol_version":1,"request_id":"invalid-ws","sequence":0,"kind":"websocket","body_base64":"/w==","opcode":1})JSON"_kj,
        R"JSON({"protocol_version":1,"request_id":"invalid-ws","sequence":0,"kind":"websocket","body_base64":"A+4=","opcode":8})JSON"_kj,
        R"JSON({"protocol_version":1,"request_id":"invalid-ws","sequence":0,"kind":"websocket","body_base64":"A+j/","opcode":8})JSON"_kj,
      }) {
    kj::EventLoop loop;
    kj::WaitScope waitScope(loop);
    SelfTestPollTimer timer;
    auto host = kj::rc<SelfTestIngressHost>();
    host->incoming.add(kj::str(invalid));
    auto session = kj::rc<IngressSession>(host.addRef(), kj::str("invalid-ws"), true, timer);
    IngressWebSocket websocket(session.addRef());
    bool rejected = false;
    try {
      websocket.receive(MAX_INGRESS_FRAME_BYTES).wait(waitScope);
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains("UTF-8"_kj) ||
              exception.getDescription().contains("close code"_kj),
          "invalid WebSocket payload failed for the wrong reason", exception);
      rejected = true;
    }
    KJ_REQUIRE(rejected, "invalid inbound WebSocket UTF-8/close code was accepted");
  }

  {
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto contract = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(WORKERD_RUNTIME_CONTRACT, contract);
    size_t tested = 0;
    for (auto vector: jsonField(contract, "x-negative-vectors"_kj).getArray()) {
      auto schema = jsonField(vector, "schema"_kj).getString();
      if (schema != "Frame"_kj && schema != "GuestInvocation"_kj) continue;
      auto value = codec.encodeRaw(jsonField(vector, "value"_kj));
      kj::EventLoop loop;
      kj::WaitScope waitScope(loop);
      SelfTestPollTimer timer;
      bool rejected = false;
      try {
        if (schema == "GuestInvocation"_kj) {
          Executor executor;
          executor.invoke(value);
        } else {
          auto host = kj::rc<SelfTestIngressHost>();
          host->incoming.add(kj::mv(value));
          auto session = kj::rc<IngressSession>(host.addRef(), kj::str("stream-1"), false, timer);
          auto frame = jsonField(vector, "value"_kj);
          if (jsonField(frame, "kind"_kj).getString() == "headers"_kj &&
              jsonField(frame, "sequence"_kj).getNumber() == 0) {
            kj::HttpHeaderTable::Builder builder;
            auto table = builder.build();
            kj::HttpHeaders headers(*table);
            for (auto header: jsonField(frame, "headers"_kj).getArray()) {
              headers.addPtrPtr(jsonField(header, "name"_kj).getString(),
                  jsonField(header, "value"_kj).getString());
            }
            session->headers(jsonInteger(jsonField(frame, "status"_kj), 599), headers);
          } else {
            session->receive().wait(waitScope);
          }
        }
      } catch (const kj::Exception& exception) {
        auto expected = schema == "GuestInvocation"_kj ? "lifetime budget"_kj
            : jsonField(jsonField(vector, "value"_kj), "sequence"_kj).getNumber() != 0
            ? "sequence mismatch"_kj
            : jsonField(jsonField(vector, "value"_kj), "kind"_kj).getString() == "headers"_kj
            ? "header"_kj
            : "frame body"_kj;
        KJ_REQUIRE(exception.getDescription().contains(expected),
            "shared negative vector failed for the wrong reason", schema, exception);
        rejected = true;
      }
      KJ_REQUIRE(rejected, "guest decoder accepted a shared negative vector", schema);
      ++tested;
    }
    KJ_REQUIRE(tested == 4, "guest shared negative vectors were not exercised", tested);
  }

  {
    auto ingress = kj::rc<SelfTestIngressHost>();
    ingress->cancelSend = true;
    ingress->incoming.add(kj::str(
        R"JSON({"protocol_version":1,"request_id":"cancel-1","sequence":0,"kind":"end"})JSON"));
    auto timers = kj::rc<FakeTimerHostChannel>();
    timers->readsBeforeFire = 3;
    Executor executor(newHyperlightLogicalServiceHostChannel(), ingress.addRef(), timers.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(kj::str(R"JS(
      let completed = false;
      export default {
        fetch(request, env, ctx) {
          if (new URL(request.url).pathname === '/state') return new Response(String(completed));
          ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 5)).then(() => {
            completed = true;
          }));
          return new Response('cancelled');
        }
      };
    )JS"))};
    WorkerBundle bundle{
      .workerVersion = kj::str("cancel-e2e"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    executor.control(
        R"JSON({"protocol_version":1,"request_id":"cancel-capabilities"})JSON"_kj, false);
    bool cancelled = false;
    try {
      executor.ingressStream(
          R"JSON({"protocol_version":1,"request_id":"cancel-1","request":{"protocol_version":1,"request_id":"cancel-1","method":"GET","url":"https://example.test/","headers":[],"body_base64":""},"websocket":false,"lifetime_budget_ms":1000})JSON"_kj);
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains("cancelled"),
          "transport cancellation failed for the wrong reason", exception);
      cancelled = true;
    }
    KJ_REQUIRE(cancelled &&
            fetchBody(executor,
                R"JSON({"protocol_version":1,"request_id":"cancel-state","method":"GET","url":"https://example.test/state","headers":[],"body_base64":""})JSON"_kj) ==
                "true"_kj,
        "transport cancellation returned before tracked work ended");
    bool rejected = false;
    try {
      executor.control(R"JSON({"protocol_version":1,"request_id":"cancel-check"})JSON"_kj, true);
    } catch (const kj::Exception&) {
      rejected = true;
    }
    KJ_REQUIRE(rejected, "failed ingress transport was falsely declared snapshot-safe");
  }

  for (auto kind: {"fetch"_kj, "scheduled"_kj, "queue"_kj}) {
    bool queue = kind == "queue"_kj;
    for (auto noWait: {false, true}) {
      for (auto task: {"delayed"_kj, "rejecting"_kj, "never"_kj, "no-io"_kj}) {
        auto source = kj::str(R"JS(
          let completed = 0;
          function tracked(ctx) {
            ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 5))
              .then(() => { ++completed; }));
            if (TASK === 'rejecting') ctx.waitUntil(Promise.reject(new Error('tracked rejection')));
            if (TASK === 'never') ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 10000))
              .then(() => { completed += 1000; }));
            if (TASK === 'no-io') ctx.waitUntil(new Promise(() => {}));
          }
          export default {
            fetch(request, env, ctx) {
              if (new URL(request.url).pathname !== '/state') tracked(ctx);
              return new Response(String(completed));
            },
            queue(batch, env, ctx) { batch.ackAll(); tracked(ctx); },
            scheduled(controller, env, ctx) { tracked(ctx); }
          };
        )JS");
        source = kj::str("const TASK = '", task, "';\n", source);
        auto modules = kj::heapArray<Module>(1);
        modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(source)};
        auto flags = kj::heapArray<kj::String>(1);
        flags[0] = kj::str(noWait ? "queue_consumer_no_wait_for_wait_until"_kj
                                  : "queue_consumer_wait_for_wait_until"_kj);
        WorkerBundle bundle{
          .workerVersion = kj::str("tracked-work"),
          .compatibilityDate = kj::str("2025-12-31"),
          .compatibilityFlags = kj::mv(flags),
          .mainModule = kj::str("worker.js"),
          .modules = kj::mv(modules),
        };
        auto timerHost = kj::rc<FakeTimerHostChannel>();
        timerHost->readsBeforeFire = 3;
        timerHost->blockLongDelays = true;
        SandboxRuntime runtime(bundle, kj::rc<StreamingV2FetchBroker>(),
            newHyperlightLogicalServiceHostChannel(), timerHost.addRef(),
            SandboxRuntime::Limits{200 * kj::MILLISECONDS, 200 * kj::MILLISECONDS});
        auto headers = kj::heapArray<Header>(0);
        bool timedOut = false;
        bool aborted = false;
        auto started = runtime.getNativeTimer().now();
        if (queue) {
          QueueRequest request{
            .queueName = kj::str("jobs"),
            .messages = kj::heapArrayBuilder<QueueMessage>(0).finish(),
            .backlogCount = 0,
            .backlogBytes = 0,
            .oldestMessageTimestamp = kj::none,
          };
          try {
            auto result = runtime.runQueue(kj::mv(request), 200 * kj::MILLISECONDS);
            auto expected = task == "never"_kj ? EventOutcome::EXCEEDED_WALL_TIME
                : (task == "rejecting"_kj || task == "no-io"_kj) && !noWait
                ? EventOutcome::EXCEPTION
                : EventOutcome::OK;
            KJ_REQUIRE(result.outcome == expected && result.ackAll,
                "queue tracked-work outcome or acknowledgement changed", noWait, task);
            timedOut = result.outcome == EventOutcome::EXCEEDED_WALL_TIME;
            aborted = task == "no-io"_kj;
          } catch (const kj::Exception& exception) {
            KJ_REQUIRE(noWait &&
                    ((task == "never"_kj &&
                         exception.getDescription().contains("drain exceeded wall time")) ||
                        (task == "no-io"_kj &&
                            exception.getDescription().contains("tracked work was aborted"))),
                "unexpected queue tracked-work failure", exception);
            timedOut = task == "never"_kj;
            aborted = task == "no-io"_kj;
          }
        } else if (kind == "scheduled"_kj) {
          auto result =
              runtime.runScheduled(kj::UNIX_EPOCH, "* * * * *"_kj, 200 * kj::MILLISECONDS);
          auto expected = task == "never"_kj                 ? EventOutcome::EXCEEDED_WALL_TIME
              : task == "rejecting"_kj || task == "no-io"_kj ? EventOutcome::EXCEPTION
                                                             : EventOutcome::OK;
          KJ_REQUIRE(result.outcome == expected, "scheduled tracked-work outcome changed", task);
          timedOut = result.outcome == EventOutcome::EXCEEDED_WALL_TIME;
          aborted = task == "no-io"_kj;
        } else {
          try {
            auto response =
                runtime.runRequest(kj::HttpMethod::GET, "https://example.test/", headers, ""_kj,
                    noWait ? kj::Maybe<kj::Duration>(200 * kj::MILLISECONDS) : kj::none);
            KJ_REQUIRE(response.statusCode == 200 && response.body == "0"_kj,
                "fetch response changed during tracked-work drain");
          } catch (const kj::Exception& exception) {
            KJ_REQUIRE(
                (task == "never"_kj && exception.getDescription().contains("exceeded wall time")) ||
                    (task == "no-io"_kj &&
                        exception.getDescription().contains("tracked work was aborted")),
                "unexpected fetch tracked-work failure", exception);
            timedOut = task == "never"_kj;
            aborted = task == "no-io"_kj;
          }
        }
        KJ_REQUIRE(timedOut == (task == "never"_kj), "tracked-work timeout not reported", task);
        KJ_REQUIRE(aborted == (task == "no-io"_kj), "tracked-work cancellation not reported", task);
        if (timedOut) {
          auto elapsed = runtime.getNativeTimer().now() - started;
          KJ_REQUIRE(elapsed >= 200 * kj::MILLISECONDS && elapsed < 2 * kj::SECONDS,
              "tracked-work timeout did not obey the bounded invocation lifetime", kind, elapsed);
        }
        auto state =
            runtime.runRequest(kj::HttpMethod::GET, "https://example.test/state", headers, ""_kj);
        KJ_REQUIRE(state.body == "1"_kj, "invocation returned before delayed side effect", kind,
            noWait, task, state.body);
        for (const auto& timer: timerHost->entries) {
          KJ_REQUIRE(timer.released, "tracked-work invocation retained a host timer");
        }
        KJ_REQUIRE(runtime.isQuiescent() == (task != "never"_kj && task != "no-io"_kj),
            "guest snapshot safety did not account for lifetime exhaustion", kind, task);
      }
    }
  }

  for (auto kind: {"fetch"_kj, "scheduled"_kj, "queue"_kj}) {
    for (auto noWait: {false, true}) {
      auto modules = kj::heapArray<Module>(1);
      modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
        let completed = false, prematureAbort = false;
        function tracked(ctx) {
          const signal = AbortSignal.any([AbortSignal.timeout(5000)]);
          ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 5)).then(() => {
            prematureAbort = signal.aborted;
            completed = true;
          }));
        }
        export default {
          fetch(request, env, ctx) {
            if (new URL(request.url).pathname === '/fires') {
              completed = false;
              const signal = AbortSignal.timeout(5);
              ctx.waitUntil(new Promise(resolve => signal.addEventListener('abort', () => {
                completed = true;
                resolve();
              }, { once: true })));
            } else if (new URL(request.url).pathname !== '/state') {
              tracked(ctx);
            }
            return Response.json({ completed, prematureAbort });
          },
          scheduled(controller, env, ctx) { tracked(ctx); },
          queue(batch, env, ctx) { batch.ackAll(); tracked(ctx); }
        };
      )JS"_kj)};
      auto flags = kj::heapArray<kj::String>(1);
      flags[0] = kj::str(noWait ? "queue_consumer_no_wait_for_wait_until"_kj
                                : "queue_consumer_wait_for_wait_until"_kj);
      WorkerBundle bundle{
        .workerVersion = kj::str("request-deadline-lifetime"),
        .compatibilityDate = kj::str("2025-12-31"),
        .compatibilityFlags = kj::mv(flags),
        .mainModule = kj::str("worker.js"),
        .modules = kj::mv(modules),
      };
      auto timers = kj::rc<FakeTimerHostChannel>();
      timers->readsBeforeFire = 3;
      timers->blockLongDelays = true;
      SandboxRuntime runtime(bundle, kj::rc<StreamingV2FetchBroker>(),
          newHyperlightLogicalServiceHostChannel(), timers.addRef(), SandboxRuntime::Limits{});
      if (kind == "fetch"_kj) {
        runtime.runRequest(kj::HttpMethod::GET, "https://example.test/", kj::heapArray<Header>(0),
            ""_kj, 1000 * kj::MILLISECONDS);
      } else if (kind == "scheduled"_kj) {
        KJ_REQUIRE(
            runtime.runScheduled(kj::UNIX_EPOCH, "* * * * *"_kj, 1000 * kj::MILLISECONDS).outcome ==
                EventOutcome::OK,
            "request deadline changed scheduled outcome");
      } else {
        auto request = parseQueueRequest(
            R"JSON({"protocol_version":1,"request_id":"deadline-queue","queue":"jobs","messages":[{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"b25l","content_type":"text","attempts":1}],"metadata":{"backlog_count":1,"backlog_bytes":3,"oldest_message_timestamp_unix_ms":null}})JSON"_kj);
        KJ_REQUIRE(runtime.runQueue(kj::mv(request.request), 1000 * kj::MILLISECONDS).outcome ==
                EventOutcome::OK,
            "request deadline changed queue outcome");
      }
      auto state = runtime.runRequest(
          kj::HttpMethod::GET, "https://example.test/state", kj::heapArray<Header>(0), ""_kj);
      KJ_REQUIRE(
          state.body == "{\"completed\":true,\"prematureAbort\":false}"_kj && runtime.isQuiescent(),
          "internal deadline retired before tracked work or retained an external handle", kind);
      for (const auto& timer: timers->entries) {
        KJ_REQUIRE(timer.released, "completed request retained its internal deadline timer");
      }
      runtime.runRequest(kj::HttpMethod::GET, "https://example.test/fires",
          kj::heapArray<Header>(0), ""_kj, 1000 * kj::MILLISECONDS);
      auto fired = runtime.runRequest(
          kj::HttpMethod::GET, "https://example.test/state", kj::heapArray<Header>(0), ""_kj);
      KJ_REQUIRE(
          fired.body == "{\"completed\":true,\"prematureAbort\":false}"_kj && runtime.isQuiescent(),
          "tracked work awaiting an internal deadline was cancelled rather than completed");
    }
  }

  for (auto noWait: {false, true}) {
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(R"JS(
        let completed = false;
        export default {
          queue(batch, env, ctx) {
            batch.messages[0].ack();
            ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 5)).then(() => {
              batch.messages[1].retry({ delaySeconds: 7 });
              completed = true;
            }));
          },
          fetch() { return new Response(String(completed)); }
        };
      )JS"_kj)};
    auto flags = kj::heapArray<kj::String>(1);
    flags[0] = kj::str(noWait ? "queue_consumer_no_wait_for_wait_until"_kj
                              : "queue_consumer_wait_for_wait_until"_kj);
    WorkerBundle bundle{
      .workerVersion = kj::str("queue-result-boundary"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::mv(flags),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    auto timerHost = kj::rc<FakeTimerHostChannel>();
    timerHost->readsBeforeFire = 3;
    SandboxRuntime runtime(bundle, kj::rc<StreamingV2FetchBroker>(),
        newHyperlightLogicalServiceHostChannel(), timerHost.addRef(), SandboxRuntime::Limits{});
    auto request = parseQueueRequest(
        R"JSON({"protocol_version":1,"request_id":"queue-result","queue":"jobs","messages":[{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"b25l","content_type":"text","attempts":1},{"id":"message-2","timestamp_unix_ms":1767225601000,"body_base64":"dHdv","content_type":"text","attempts":2}],"metadata":{"backlog_count":2,"backlog_bytes":6,"oldest_message_timestamp_unix_ms":1767225600000}})JSON"_kj);
    auto result = runtime.runQueue(kj::mv(request.request), 1000 * kj::MILLISECONDS);
    KJ_REQUIRE(result.outcome == EventOutcome::OK && result.explicitAcks.size() == 1 &&
            result.explicitAcks[0] == "message-1"_kj &&
            result.retryMessages.size() == (noWait ? 0 : 1),
        "tracked-work drain changed the native queue result boundary", noWait);
    auto state = runtime.runRequest(
        kj::HttpMethod::GET, "https://example.test/", kj::heapArray<Header>(0), ""_kj);
    KJ_REQUIRE(state.body == "true"_kj, "queue result preservation discarded tracked work", noWait);
  }

  {
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE,
      copyModuleBytes(
          "export default { fetch() { AbortSignal.timeout(20000); setTimeout(() => {}, 10000); "
          "return new Response('live-timer'); } };"_kj)};
    WorkerBundle bundle{
      .workerVersion = kj::str("unsupported-live-timer"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    auto timerHost = kj::rc<FakeTimerHostChannel>();
    timerHost->blockLongDelays = true;
    SandboxRuntime runtime(bundle, kj::rc<StreamingV2FetchBroker>(),
        newHyperlightLogicalServiceHostChannel(), timerHost.addRef(), SandboxRuntime::Limits{});
    bool rejected = false;
    try {
      runtime.runRequest(
          kj::HttpMethod::GET, "https://example.test/", kj::heapArray<Header>(0), ""_kj);
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains("unsupported live external handles"),
          "untracked timer was rejected for the wrong reason", exception);
      rejected = true;
    }
    KJ_REQUIRE(rejected && !runtime.isQuiescent(),
        "untracked application timer was silently discarded to claim a safe point");
  }

  {
    Executor executor;
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE,
      copyModuleBytes(
          kj::str("export default { scheduled(controller) { controller.noRetry(); } }"))};
    WorkerBundle bundle{
      .workerVersion = kj::str("scheduled-v1"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    auto response = executor.scheduled(
        R"JSON({"protocol_version":1,"request_id":"scheduled-e2e","scheduled_time_unix_ms":1767225600000,"cron":"0 0 * * *"})JSON"_kj);
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder message;
    auto root = message.initRoot<capnp::JsonValue>();
    codec.decodeRaw(response, root);
    auto fields = root.getObject();
    KJ_REQUIRE(fields.size() == 4 && fields[0].getName() == "protocol_version"_kj &&
            fields[0].getValue().getNumber() == 1 && fields[1].getName() == "request_id"_kj &&
            fields[1].getValue().getString() == "scheduled-e2e"_kj &&
            fields[2].getName() == "outcome"_kj &&
            fields[2].getValue().getString() == kj::str(EventOutcome::OK) &&
            fields[3].getName() == "retry"_kj && fields[3].getValue().isBoolean() &&
            !fields[3].getValue().getBoolean(),
        "scheduled ingress self-test failed");
    executor.control(
        R"JSON({"protocol_version":1,"request_id":"scheduled-capabilities"})JSON"_kj, false);
    KJ_REQUIRE(
        executor.invoke(
            R"JSON({"protocol_version":2,"kind":"scheduled","request":{"protocol_version":1,"request_id":"scheduled-e2e","scheduled_time_unix_ms":1767225600000,"cron":"0 0 * * *"},"lifetime_budget_ms":1000})JSON"_kj) ==
            response,
        "budgeted scheduled invocation changed native result");
    bool rejected = false;
    try {
      executor.invoke(
          R"JSON({"kind":"scheduled","lifetime_budget_ms":1000,"protocol_version":2,"request":{"cron":"0 0 * * *","protocol_version":1,"request_id":"scheduled-e2e","scheduled_time_unix_ms":1767225600000}})JSON"_kj);
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains("invalid scheduled protocol version"),
          "noncanonical nested scheduled envelope failed for the wrong reason");
      rejected = true;
    }
    KJ_REQUIRE(rejected, "invoke silently weakened the frozen scheduled envelope");
  }

  {
    Executor executor;
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE,
      copyModuleBytes(kj::str("export default { queue(batch) { batch.messages[0].ack(); "
                              "batch.messages[1].retry({ delaySeconds: 7 }); } }"))};
    WorkerBundle bundle{
      .workerVersion = kj::str("queue-v1"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    auto response = executor.queue(
        R"JSON({"protocol_version":1,"request_id":"queue-e2e","queue":"jobs","messages":[{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"b25l","content_type":"text","attempts":1},{"id":"message-2","timestamp_unix_ms":1767225601000,"body_base64":"dHdv","content_type":"text","attempts":2}],"metadata":{"backlog_count":2,"backlog_bytes":6,"oldest_message_timestamp_unix_ms":1767225600000}})JSON"_kj);
    auto expected = kj::str(R"JSON({"protocol_version":1,"request_id":"queue-e2e","outcome":")JSON",
        EventOutcome::OK,
        R"JSON(","ack_all":false,"retry_batch":{"retry":false,"delay_seconds":null},"explicit_acks":["message-1"],"retry_messages":[{"id":"message-2","delay_seconds":7}]}
)JSON");
    KJ_REQUIRE(response == expected, "queue ingress self-test failed", response);
    executor.control(
        R"JSON({"protocol_version":1,"request_id":"queue-capabilities"})JSON"_kj, false);
    KJ_REQUIRE(
        executor.invoke(
            R"JSON({"protocol_version":2,"kind":"queue","request":{"protocol_version":1,"request_id":"queue-e2e","queue":"jobs","messages":[{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"b25l","content_type":"text","attempts":1},{"id":"message-2","timestamp_unix_ms":1767225601000,"body_base64":"dHdv","content_type":"text","attempts":2}],"metadata":{"backlog_count":2,"backlog_bytes":6,"oldest_message_timestamp_unix_ms":1767225600000}},"lifetime_budget_ms":1000})JSON"_kj) ==
            response,
        "budgeted queue invocation changed acknowledgement or retry result");
    bool rejected = false;
    try {
      executor.invoke(
          R"JSON({"kind":"queue","lifetime_budget_ms":1000,"protocol_version":2,"request":{"messages":[],"metadata":{"backlog_bytes":0,"backlog_count":0,"oldest_message_timestamp_unix_ms":null},"protocol_version":1,"queue":"jobs","request_id":"queue-e2e"}})JSON"_kj);
    } catch (const kj::Exception& exception) {
      KJ_REQUIRE(exception.getDescription().contains("invalid queue protocol version"),
          "noncanonical nested queue envelope failed for the wrong reason");
      rejected = true;
    }
    KJ_REQUIRE(rejected, "invoke silently weakened the frozen queue envelope");
  }

  {
    auto host = kj::rc<SelfTestLogicalServiceHost>(
        kj::str(R"JSON({"version":2,"request_id":"req-1","status":"ok","code":"ok"})JSON"));
    auto& hostRef = *host;
    Executor executor(host.addRef());
    auto modules = kj::heapArray<Module>(1);
    modules[0] = Module{kj::str("worker.js"), ModuleType::ES_MODULE,
      copyModuleBytes(
          kj::str("export default { async fetch(request, env) { "
                  "return env.settings.fetch('https://logical.invalid/', { method: 'POST', "
                  "body: '{\"version\":2,\"request_id\":\"req-1\",\"binding\":\"settings\","
                  "\"operation\":{\"kind\":\"kv_get\"}}' }); } }"))};
    auto bindings = kj::heapArray<Binding>(1);
    bindings[0] = Binding{
      .name = kj::str("settings"),
      .kind = composite::BindingKind::KV,
    };
    WorkerBundle bundle{
      .workerVersion = kj::str("logical-service-v2"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = kj::heapArray<kj::String>(0),
      .mainModule = kj::str("worker.js"),
      .modules = kj::mv(modules),
      .protocolVersion = 3,
      .storageMounts = kj::heapArray<StorageMount>(0),
      .bindings = kj::mv(bindings),
    };
    executor.initialize(serializeWorkerBundle(bundle));
    auto body = fetchBody(executor,
        R"JSON({"protocol_version":1,"request_id":"binding-e2e","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj);
    KJ_REQUIRE(
        body == R"JSON({"version":2,"request_id":"req-1","status":"ok","code":"ok"})JSON"_kj &&
            hostRef.invokeCount == 1 &&
            KJ_REQUIRE_NONNULL(hostRef.request) ==
                R"JSON({"version":2,"request_id":"req-1","binding":"settings","operation":{"kind":"kv_get"}})JSON"_kj,
        "logical service binding self-test failed");
  }

  auto noFlags = []() { return kj::heapArray<kj::String>(0); };

  {
    constexpr size_t COMPONENT_ADAPTER_BYTES = 45194;
    auto prefix = "export default {};"_kj;
    auto source = kj::str(prefix, kj::repeat(' ', COMPONENT_ADAPTER_BYTES - prefix.size()));
    KJ_REQUIRE(source.size() == COMPONENT_ADAPTER_BYTES);

    Executor executor;
    executor.initialize(serializeWorkerBundle(WorkerBundle{
      .workerVersion = kj::str("component-adapter-size-v1"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = noFlags(),
      .mainModule = kj::str("worker.js"),
      .modules = oneModule("worker.js"_kj, source),
      .protocolVersion = 2,
      .storageMounts = kj::heapArray<StorageMount>(0),
    }));
  }

  {
    auto flags = kj::heapArrayBuilder<kj::String>(1);
    flags.add(kj::str("enable_web_file_system"));
    run(WorkerBundle{kj::str("web-filesystem-v1"), kj::str("2025-12-31"), flags.finish(),
          kj::str("worker.js"),
          oneModule("worker.js"_kj,
              "export default { fetch() { return new Response(String(navigator.storage instanceof "
              "StorageManager)); } };"_kj)},
        R"JSON({"protocol_version":1,"request_id":"web-filesystem","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj,
        [](kj::StringPtr body) {
      KJ_REQUIRE(body == "true"_kj, "allowlisted web filesystem flag self-test failed");
    });
  }
  {
    auto flags = kj::heapArrayBuilder<kj::String>(1);
    flags.add(kj::str("experimental"));
    expectInitFailure(
        WorkerBundle{kj::str("experimental-denied-v1"), kj::str("2025-12-31"), flags.finish(),
          kj::str("worker.js"), oneModule("worker.js"_kj, "export default {}"_kj)},
        "flag experimental"_kj);
  }

  run(WorkerBundle{kj::str("helloworld-v1"), kj::str("2023-02-28"), noFlags(), kj::str("worker.js"),
        oneModule("worker.js"_kj, HELLOWORLD_WORKER)},
      R"JSON({"protocol_version":1,"request_id":"hello","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body == "Hello World\n"_kj, "helloworld_esm self-test failed");
  });

  auto streamModules = kj::heapArrayBuilder<Module>(2);
  streamModules.add(
      Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(WEB_STREAMS_WORKER)});
  streamModules.add(
      Module{kj::str("streams-util"), ModuleType::ES_MODULE, copyModuleBytes(STREAMS_UTIL)});
  run(WorkerBundle{kj::str("web-streams-v1"), kj::str("2025-12-31"), noFlags(),
        kj::str("worker.js"), streamModules.finish()},
      R"JSON({"protocol_version":1,"request_id":"streams","method":"GET","url":"https://example.test/sync","headers":[],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body.size() == 9441, "web-streams /sync fixture changed size", body.size());
    for (char c: body) {
      KJ_REQUIRE(c < 'a' || c > 'z', "web-streams transform did not uppercase its output");
    }
  });

  auto commonJsFlags = kj::heapArrayBuilder<kj::String>(2);
  commonJsFlags.add(kj::str("enable_nodejs_fs_module"));
  commonJsFlags.add(kj::str("nodejs_compat"));
  auto commonJsModules = kj::heapArrayBuilder<Module>(7);
  commonJsModules.add(
      Module{kj::str("worker.js"), ModuleType::ES_MODULE, copyModuleBytes(kj::str(R"JS(
import { createRequire } from 'node:module';
const require = createRequire('file:///bundle/');
export default {
  fetch() {
    const graceful = require('./node_modules/graceful-fs/graceful-fs.js');
    const yazl = require('./node_modules/yazl/index.js');
    const satisfies = require('./node_modules/semver/functions/satisfies.js');
    return Response.json({
      graceful: graceful.packageName,
      yazl: yazl.packageName,
      crc32: yazl.crc32,
      range: satisfies.range,
      semver: satisfies('7.7.3', '^7.0.0'),
    });
  },
};
)JS"))});
  commonJsModules.add(Module{kj::str("node_modules/buffer-crc32/dist/index.cjs"),
    ModuleType::COMMON_JS_MODULE, copyModuleBytes(kj::str("module.exports = { value: 32 };"_kj))});
  commonJsModules.add(Module{kj::str("node_modules/buffer-crc32/package.json"), ModuleType::TEXT,
    copyModuleBytes(kj::str(R"JSON({"main":"dist/index.cjs"})JSON"_kj))});
  commonJsModules.add(
      Module{kj::str("node_modules/graceful-fs/graceful-fs.js"), ModuleType::COMMON_JS_MODULE,
        copyModuleBytes(kj::str("module.exports = { packageName: 'graceful-fs' };"_kj))});
  commonJsModules.add(
      Module{kj::str("node_modules/semver/classes/range.js"), ModuleType::COMMON_JS_MODULE,
        copyModuleBytes(kj::str("module.exports = { marker: 'range' };"_kj))});
  commonJsModules.add(Module{kj::str("node_modules/semver/functions/satisfies.js"),
    ModuleType::COMMON_JS_MODULE, copyModuleBytes(kj::str(R"JS(
const Range = require('../classes/range');
const satisfies = (version, range) => version === '7.7.3' && range === '^7.0.0';
satisfies.range = Range.marker;
module.exports = satisfies;
)JS"))});
  commonJsModules.add(Module{kj::str("node_modules/yazl/index.js"), ModuleType::COMMON_JS_MODULE,
    copyModuleBytes(kj::str(R"JS(
const crc32 = require('buffer-crc32');
module.exports = { packageName: 'yazl', crc32: crc32.value };
)JS"))});
  run(WorkerBundle{kj::str("commonjs-packages-v1"), kj::str("2025-12-31"), commonJsFlags.finish(),
        kj::str("worker.js"), commonJsModules.finish()},
      R"JSON({"protocol_version":1,"request_id":"commonjs","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body ==
            R"JSON({"graceful":"graceful-fs","yazl":"yazl","crc32":32,"range":"range","semver":true})JSON"_kj,
        "CommonJS package module self-test failed", body);
  });

  for (auto invalidRequire: {kj::StringPtr("../../../host-secret"), kj::StringPtr("./missing")}) {
    auto flags = kj::heapArrayBuilder<kj::String>(2);
    flags.add(kj::str("enable_nodejs_fs_module"));
    flags.add(kj::str("nodejs_compat"));
    auto modules = kj::heapArrayBuilder<Module>(2);
    modules.add(Module{kj::str("worker.js"), ModuleType::ES_MODULE,
      copyModuleBytes(
          kj::str("export default { fetch() { return new Response('unreachable'); } };"_kj))});
    modules.add(Module{kj::str("node_modules/example/index.js"), ModuleType::COMMON_JS_MODULE,
      copyModuleBytes(kj::str("module.exports = require('", invalidRequire, "');"))});
    expectInitFailure(WorkerBundle{kj::str("commonjs-invalid-graph-v1"), kj::str("2025-12-31"),
                        flags.finish(), kj::str("worker.js"), modules.finish()},
        invalidRequire.startsWith("..") ? "CommonJS require escapes the bundle"_kj
                                        : "unregistered CommonJS module"_kj);
  }

  run(WorkerBundle{kj::str("wintertc-smoke-v1"), kj::str("2025-12-31"), noFlags(),
        kj::str("worker.js"), oneModule("worker.js"_kj, WINTERTC_SMOKE)},
      R"JSON({"protocol_version":1,"request_id":"wintertc","method":"POST","url":"https://example.test/wintertc-smoke","headers":[{"name":"x-smoke","value":"yes"}],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body ==
            R"JSON({"smoke":"WinterTC minimum-common API smoke (not conformance)","url":true,"urlPattern":true,"request":true,"response":true,"headers":true,"formData":true,"blob":true,"textCodec":true,"cryptoDigest":true,"cryptoRandom":true,"readableStream":true,"transformStream":true,"compression":true,"performance":true,"webAssembly":"blocked by executor embedder policy","timers":"not exercised by smoke","outboundFetch":"not exercised by smoke","capabilityBackedUnavailable":["WebSocket","connect","bindings","actors"]})JSON"_kj,
        "WinterTC API smoke self-test failed", body);
  });

  run(WorkerBundle{kj::str("error-tunnel-v1"), kj::str("2025-12-31"), noFlags(),
        kj::str("worker.js"),
        oneModule("worker.js"_kj,
            "export default { fetch() { throw new Error('executor-error-tunnel'); } };"_kj)},
      R"JSON({"protocol_version":1,"request_id":"error-tunnel","method":"GET","url":"https://example.test/","headers":[],"body_base64":""})JSON"_kj,
      [](kj::StringPtr body) {
    KJ_REQUIRE(body.contains("\"error\":\"worker_execution_failed\""_kj) &&
            body.contains("executor-error-tunnel"_kj),
        "executor error tunneling self-test failed", body);
  });

  if (filesystemEvidence) {
    auto flags = kj::heapArrayBuilder<kj::String>(3);
    flags.add(kj::str("enable_nodejs_fs_module"));
    flags.add(kj::str("enable_web_file_system"));
    flags.add(kj::str("nodejs_compat"));
    auto mounts = kj::heapArrayBuilder<StorageMount>(2);
    mounts.add(StorageMount{kj::str("readonly"), StorageMode::READ_ONLY});
    mounts.add(StorageMount{kj::str("scratch"), StorageMode::READ_WRITE});
    WorkerBundle bundle{
      .workerVersion = kj::str("filesystem-evidence-v1"),
      .compatibilityDate = kj::str("2025-12-31"),
      .compatibilityFlags = flags.finish(),
      .mainModule = kj::str("worker.js"),
      .modules = oneModule("worker.js"_kj, FILESYSTEM_EVIDENCE),
      .protocolVersion = 2,
      .storageMounts = mounts.finish(),
    };

    Executor executor;
    executor.initialize(serializeWorkerBundle(bundle));
    auto vfsRequest =
        R"JSON({"protocol_version":1,"request_id":"vfs","method":"GET","url":"https://example.test/evidence/workerd-vfs","headers":[],"body_base64":""})JSON"_kj;
    auto first = fetchBody(executor, vfsRequest);
    auto second = fetchBody(executor, vfsRequest);
    KJ_REQUIRE(first.contains(R"JSON("immutable":true)JSON"_kj) &&
            first.contains(R"JSON("freshRequest":true)JSON"_kj) &&
            second.contains(R"JSON("freshRequest":true)JSON"_kj) &&
            first.contains(R"JSON("nullDiscardThenEof":true)JSON"_kj) &&
            first.contains(R"JSON("zeroOnly":true)JSON"_kj),
        "pinned VFS filesystem evidence failed", first, second);

    auto hostfs = fetchBody(executor,
        R"JSON({"protocol_version":1,"request_id":"hostfs","method":"GET","url":"https://example.test/evidence/hostfs","headers":[],"body_base64":""})JSON"_kj);
    KJ_REQUIRE(hostfs.contains(R"JSON("allowedRead":{"ok":true)JSON"_kj) &&
            hostfs.contains(R"JSON("readOnlyWrite":{"ok":false,"code":"EPERM")JSON"_kj) &&
            hostfs.contains(R"JSON("boundedWrite":{"ok":true,"value":"bounded-write"})JSON"_kj) &&
            hostfs.contains(R"JSON("traversal":{"ok":false,"code":"ENOENT")JSON"_kj) &&
            hostfs.contains(R"JSON("unlisted":{"ok":false,"code":"ENOENT")JSON"_kj),
        "host filesystem evidence failed", hostfs);
  }

  return writeProtocolMessage(protocolOutputFd,
      kj::str(
          R"JSON({"protocol_version":1,"self_test":"passed","bundles":["samples/helloworld_esm","samples/web-streams","wintertc-api-smoke"],"wintertc_label":"smoke-not-conformance","filesystem_evidence":)JSON",
          filesystemEvidence ? "true"_kj : "false"_kj,
          ",\"runtime_extensions\":[\"tracked-work-drain-v1\",\"safe-point-v1\","
          "\"ingress-stream-v1\",\"invocation-budget-v2\",\"secure-entropy-v1\","
          "\"authenticated-egress-websocket-v1\"],\"tracked_work_cases\":24,"
          "\"shared_negative_vectors\":4,\"bundle_loader\":{\"protocol_version\":4,"
          "\"executed_source_bytes\":757597,\"max_module_bytes\":1048576,"
          "\"max_bundle_bytes\":8388608},\"native_webhook_cases\":8,\"secure_entropy_cases\":4,"
          "\"request_deadline_cases\":6,\"provider_websocket_cases\":["
          "\"native-duplex-repeat-close\",\"buffered-amount\",\"invalid-guest-input\","
          "\"active-checkpoint-refusal\",\"policy-denial\",\"eof-cancellation\","
          "\"negotiation-required\",\"exact-frame-bound\",\"connection-bound\","
          "\"exact-session-byte-bound\",\"joined-cancellation\",\"pending-cancellation\","
          "\"repeated-cancellation\",\"callback-revocation\",\"unordered-replies\","
          "\"default-close\"]}\n"_kj));
}

int selfTestBundle(kj::StringPtr path) {
  auto rawFile = fopen(path.cStr(), "rb");
  KJ_REQUIRE(rawFile != nullptr, "failed to open bundle", path);
  KJ_DEFER(fclose(rawFile));
  KJ_REQUIRE(fseek(rawFile, 0, SEEK_END) == 0, "failed to seek bundle", path);
  auto size = ftell(rawFile);
  KJ_REQUIRE(size >= 0 && size <= MAX_ENVELOPE_BYTES, "bundle has invalid size", path, size);
  KJ_REQUIRE(fseek(rawFile, 0, SEEK_SET) == 0, "failed to rewind bundle", path);
  auto file = kj::heapArray<char>(size);
  KJ_REQUIRE(
      fread(file.begin(), 1, file.size(), rawFile) == file.size(), "failed to read bundle", path);
  auto input = file.asPtr();
  if (input.endsWith("\n"_kj)) {
    input = input.first(input.size() - 1);
    if (input.endsWith("\r"_kj)) {
      input = input.first(input.size() - 1);
    }
  }
  Executor executor;
  executor.initialize(input);
  return writeProtocolMessage(
      STDOUT_FILENO, "{\"protocol_version\":1,\"bundle_initialize\":\"passed\"}\n"_kj);
}

}  // namespace
}  // namespace workerd::server::sandbox_executor

int main(int argc, char** argv) {
  using namespace workerd::server::sandbox_executor;
  if (argc == 2 && kj::StringPtr(argv[1]) == "--self-test"_kj) {
    _exit(selfTest(false));
  }
  if (argc == 2 && kj::StringPtr(argv[1]) == "--self-test-filesystem"_kj) {
    _exit(selfTest(true));
  }
  if (argc == 3 && kj::StringPtr(argv[1]) == "--self-test-bundle"_kj) {
    _exit(selfTestBundle(argv[2]));
  }
  GuestExceptionCallback exceptionCallback;
  if (argc != 1 || isolateProtocolOutput() != 0 || initializeDriver() != 0) return 1;
  runDriver(dispatch);
}
