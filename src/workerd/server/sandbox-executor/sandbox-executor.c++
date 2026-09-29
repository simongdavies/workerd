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
    runtime = kj::heap<SandboxRuntime>(bundle);
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
            R"JSON({"smoke":"WinterTC minimum-common API smoke (not conformance)","url":true,"urlPattern":true,"request":true,"response":true,"headers":true,"formData":true,"blob":true,"textCodec":true,"cryptoDigest":true,"cryptoRandom":true,"readableStream":true,"transformStream":true,"compression":true,"performance":true,"webAssembly":"blocked by executor embedder policy","timers":"not exercised: executor timer channel is disabled","capabilityBackedUnavailable":["outbound fetch","WebSocket","connect","bindings","actors"]})JSON"_kj,
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
