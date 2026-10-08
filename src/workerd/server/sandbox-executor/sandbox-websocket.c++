// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-websocket.h"

#include "hyperlight-driver.h"
#include "sandbox-runtime.h"

#include <workerd/io/io-context.h>
#include <workerd/jsg/exception.h>

#include <simdutf.h>
#include <strings.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/encoding.h>

#include <cmath>
namespace workerd::server::sandbox_executor {
namespace {

class HyperlightWebSocketHostChannel final: public WebSocketHostChannel {
 public:
  kj::String open(kj::StringPtr request) override {
    return call("WorkerdWebSocketV1Open"_kj, request);
  }
  kj::String send(kj::StringPtr request) override {
    return call("WorkerdWebSocketV1Send"_kj, request);
  }
  kj::String receive(kj::StringPtr request) override {
    return call("WorkerdWebSocketV1Receive"_kj, request);
  }
  kj::String close(kj::StringPtr request) override {
    return call("WorkerdWebSocketV1Close"_kj, request);
  }

 private:
  kj::String call(kj::StringPtr name, kj::StringPtr request) {
    auto argument = hostCallStringArg(request);
    return hostCallString(name, kj::arrayPtr(&argument, 1));
  }
};

using Json = capnp::JsonValue::Reader;
constexpr uint64_t MAX_ID = 9'007'199'254'740'991;
constexpr size_t MAX_CONNECTIONS = 4;
constexpr uint64_t MAX_SESSION_BYTES = 8 * 1024 * 1024;
constexpr kj::Duration POLL_INTERVAL = 1 * kj::MILLISECONDS;

Json field(Json object, kj::StringPtr name) {
  KJ_REQUIRE(object.isObject(), "provider WebSocket reply must be an object");
  kj::Maybe<Json> result;
  for (auto entry: object.getObject()) {
    if (entry.getName() == name) {
      KJ_REQUIRE(result == kj::none, "duplicate provider WebSocket field", name);
      result = entry.getValue();
    }
  }
  return KJ_REQUIRE_NONNULL(result, "missing provider WebSocket field", name);
}

uint64_t integer(Json value, uint64_t max) {
  KJ_REQUIRE(value.isNumber(), "invalid provider WebSocket integer");
  auto number = value.getNumber();
  KJ_REQUIRE(std::isfinite(number) && number >= 0 && number <= max && std::floor(number) == number,
      "provider WebSocket integer exceeds limit");
  return static_cast<uint64_t>(number);
}

kj::StringPtr text(Json value) {
  KJ_REQUIRE(value.isString(), "invalid provider WebSocket string");
  return value.getString();
}

struct Registry: public kj::Refcounted {
  size_t active = 0;
  uint64_t nextId = 1;
  bool uncertain = false;
};

struct Envelope {
  capnp::MallocMessageBuilder arena;
  capnp::JsonValue::Builder root = arena.initRoot<capnp::JsonValue>();
  capnp::List<capnp::JsonValue::Field>::Builder fields;
  size_t next = 0;

  Envelope(kj::StringPtr requestId, kj::StringPtr binding, size_t extra)
      : fields(root.initObject(3 + extra)) {
    number("protocol_version"_kj, 1);
    string("request_id"_kj, requestId);
    string("binding"_kj, binding);
  }
  void number(kj::StringPtr name, uint64_t value) {
    fields[next].setName(name);
    fields[next++].getValue().setNumber(value);
  }
  void string(kj::StringPtr name, kj::StringPtr value) {
    fields[next].setName(name);
    fields[next++].getValue().setString(value);
  }
  kj::String encode() {
    KJ_REQUIRE(next == fields.size(), "incomplete provider WebSocket query");
    capnp::JsonCodec codec;
    return codec.encodeRaw(root);
  }
};

struct Reply {
  capnp::MallocMessageBuilder arena;
  capnp::JsonValue::Builder root = arena.initRoot<capnp::JsonValue>();

  Reply(kj::StringPtr encoded, kj::StringPtr requestId, bool allowFailure = false) {
    KJ_REQUIRE(encoded.size() <= 24 * 1024, "provider WebSocket reply exceeds limit");
    capnp::JsonCodec codec;
    codec.decodeRaw(encoded, root);
    KJ_REQUIRE(integer(field(root, "protocol_version"_kj), 1) == 1 &&
            text(field(root, "request_id"_kj)) == requestId,
        "provider WebSocket reply identity mismatch");
    auto status = text(field(root, "status"_kj));
    auto code = text(field(root, "code"_kj));
    if (status == "denied"_kj || status == "invalid_request"_kj || status == "host_error"_kj) {
      KJ_REQUIRE(root.getObject().size() == 4, "invalid provider WebSocket error reply");
      if (!allowFailure) requireSuccess();
      return;
    }
    KJ_REQUIRE(
        status == "ok"_kj || status == "pending"_kj, "unknown provider WebSocket reply status");
    KJ_REQUIRE(code.size() > 0 && code.size() <= 64, "invalid provider WebSocket status code");
  }
  bool is(kj::StringPtr status, kj::StringPtr code) const {
    return text(field(root, "status"_kj)) == status && text(field(root, "code"_kj)) == code;
  }
  bool isFailure() const {
    auto status = text(field(root, "status"_kj));
    return status == "denied"_kj || status == "invalid_request"_kj || status == "host_error"_kj;
  }
  void requireSuccess() const {
    if (isFailure()) {
      kj::throwRecoverableException(
          JSG_KJ_EXCEPTION(FAILED, Error, "Provider WebSocket operation failed."));
      KJ_UNREACHABLE;
    }
  }
};

class ProviderSocket final: public kj::WebSocket {
 public:
  ProviderSocket(kj::Rc<WebSocketHostChannel> host,
      kj::Rc<Registry> registry,
      kj::String requestId,
      kj::String binding,
      uint64_t handle,
      TimerChannel& timer)
      : host(kj::mv(host)),
        registry(kj::mv(registry)),
        requestId(kj::mv(requestId)),
        binding(kj::mv(binding)),
        handle(handle),
        timer(timer) {}

  ~ProviderSocket() noexcept {
    if (!released) {
      try {
        cancel();
        if (!released) registry->uncertain = true;
      } catch (const kj::Exception& exception) {
        registry->uncertain = true;
        KJ_LOG(ERROR, "provider WebSocket cancellation failed", exception.getType());
      }
    }
  }

  kj::Promise<void> awaitOpen() {
    for (;;) {
      auto query = receiveQuery();
      auto encoded = host->receive(query);
      Reply reply(encoded, requestId);
      if (reply.is("pending"_kj, "pending"_kj)) {
        KJ_REQUIRE(reply.root.getObject().size() == 4, "invalid pending provider open reply");
        co_await timer.afterLimitTimeout(POLL_INTERVAL);
        continue;
      }
      validateEvent(reply.root);
      KJ_REQUIRE(
          text(field(reply.root, "kind"_kj)) == "open"_kj && reply.root.getObject().size() == 7,
          "provider WebSocket did not establish an open session");
      ++receivedSequence;
      co_return;
    }
  }

  kj::Promise<void> send(kj::ArrayPtr<const byte> message) override {
    return sendMessage(message, 2);
  }
  kj::Promise<void> send(kj::ArrayPtr<const char> message) override {
    requireUtf8(message.asBytes());
    return sendMessage(message.asBytes(), 1);
  }
  kj::Promise<void> close(uint16_t code, kj::StringPtr reason) override {
    if (released) return kj::READY_NOW;
    KJ_REQUIRE(
        !sentClose && validClose(code) && reason.size() <= 123, "invalid provider WebSocket close");
    requireUtf8(reason.asBytes());
    return closeGracefully(code, kj::str(reason));
  }
  void disconnect() override {
    cancel();
  }
  void abort() override {
    cancel();
  }
  kj::Promise<void> whenAborted() override {
    return kj::NEVER_DONE;
  }
  kj::Promise<Message> receive(size_t maxSize) override {
    KJ_REQUIRE(!released, "provider WebSocket read after release");
    for (;;) {
      auto encoded = host->receive(receiveQuery());
      Reply reply(encoded, requestId);
      if (reply.is("pending"_kj, "pending"_kj)) {
        KJ_REQUIRE(reply.root.getObject().size() == 4, "invalid pending provider receive reply");
        co_await timer.afterLimitTimeout(POLL_INTERVAL);
        continue;
      }
      validateEvent(reply.root);
      auto kind = text(field(reply.root, "kind"_kj));
      if (kind == "close"_kj) {
        KJ_REQUIRE(reply.root.getObject().size() == 9, "invalid provider close event fields");
        auto code = integer(field(reply.root, "close_code"_kj), 65535);
        auto reason = kj::str(text(field(reply.root, "reason"_kj)));
        KJ_REQUIRE(validClose(code) && reason.size() <= 123, "invalid provider close event");
        requireUtf8(reason.asBytes());
        release();
        co_return Message(Close{static_cast<uint16_t>(code), kj::mv(reason)});
      }
      KJ_REQUIRE(kind == "message"_kj && reply.root.getObject().size() == 9,
          "invalid provider WebSocket message event");
      auto opcode = integer(field(reply.root, "opcode"_kj), 2);
      KJ_REQUIRE(opcode == 1 || opcode == 2, "invalid provider WebSocket message opcode");
      auto encodedBody = text(field(reply.root, "body_base64"_kj));
      KJ_REQUIRE(encodedBody.size() <= 21848, "provider WebSocket encoded message exceeds limit");
      auto body = kj::decodeBase64(encodedBody);
      KJ_REQUIRE(!body.hadErrors && body.size() <= MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES &&
              body.size() <= maxSize && kj::encodeBase64(body) == encodedBody,
          "invalid provider WebSocket message body");
      KJ_REQUIRE(body.size() <= MAX_SESSION_BYTES - sentBytes - receivedBytes,
          "provider WebSocket session byte budget exceeded");
      ++receivedSequence;
      receivedBytes += body.size();
      if (opcode == 1) {
        requireUtf8(body);
        co_return Message(kj::str(body.asPtr().asChars()));
      }
      kj::Array<byte> binary = kj::mv(body);
      co_return Message(kj::mv(binary));
    }
  }
  uint64_t sentByteCount() override {
    return sentBytes;
  }
  uint64_t receivedByteCount() override {
    return receivedBytes;
  }
  kj::Maybe<kj::String> getPreferredExtensions(ExtensionsContext) override {
    return kj::none;
  }

 private:
  static bool validClose(uint64_t code) {
    return code == 1000 || code == 1001 || code == 1002 || code == 1003 ||
        (code >= 1007 && code <= 1014) || (code >= 3000 && code <= 4999);
  }
  static void requireUtf8(kj::ArrayPtr<const byte> bytes) {
    KJ_REQUIRE(simdutf::validate_utf8(bytes.asChars().begin(), bytes.size()),
        "invalid provider WebSocket UTF-8");
  }
  kj::String receiveQuery() {
    KJ_REQUIRE(receivedSequence < MAX_ID, "provider WebSocket receive sequence exhausted");
    Envelope query(requestId, binding, 2);
    query.number("handle_id"_kj, handle);
    query.number("sequence"_kj, receivedSequence);
    return query.encode();
  }
  void validateEvent(Json root) {
    KJ_REQUIRE(text(field(root, "status"_kj)) == "ok"_kj &&
            text(field(root, "code"_kj)) == "event"_kj &&
            integer(field(root, "handle_id"_kj), MAX_ID) == handle &&
            integer(field(root, "sequence"_kj), MAX_ID) == receivedSequence,
        "provider WebSocket event handle or sequence mismatch");
  }
  kj::Promise<void> sendMessage(kj::ArrayPtr<const byte> bytes, uint8_t opcode) {
    KJ_REQUIRE(!released && !sentClose && bytes.size() <= MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES,
        "provider WebSocket message exceeds limit or connection is closed");
    KJ_REQUIRE(bytes.size() <= MAX_SESSION_BYTES - sentBytes - receivedBytes,
        "provider WebSocket session byte budget exceeded");
    KJ_REQUIRE(sentSequence < MAX_ID, "provider WebSocket send sequence exhausted");
    auto body = kj::encodeBase64(bytes);
    for (;;) {
      KJ_REQUIRE(!released && !sentClose, "provider WebSocket connection is closed");
      KJ_REQUIRE(bytes.size() <= MAX_SESSION_BYTES - sentBytes - receivedBytes,
          "provider WebSocket session byte budget exceeded");
      Envelope query(requestId, binding, 4);
      query.number("handle_id"_kj, handle);
      query.number("sequence"_kj, sentSequence);
      query.number("opcode"_kj, opcode);
      query.string("body_base64"_kj, body);
      auto encoded = host->send(query.encode());
      Reply reply(encoded, requestId);
      KJ_REQUIRE(reply.root.getObject().size() == 5 && field(reply.root, "accepted"_kj).isBoolean(),
          "invalid provider WebSocket send acknowledgement");
      if (reply.is("pending"_kj, "backpressure"_kj)) {
        KJ_REQUIRE(!field(reply.root, "accepted"_kj).getBoolean(),
            "backpressured provider WebSocket message was committed");
        co_await timer.afterLimitTimeout(POLL_INTERVAL);
        continue;
      }
      KJ_REQUIRE(reply.is("ok"_kj, "accepted"_kj) && field(reply.root, "accepted"_kj).getBoolean(),
          "provider WebSocket message was not accepted");
      ++sentSequence;
      sentBytes += bytes.size();
      co_return;
    }
  }
  kj::Promise<void> closeGracefully(uint16_t code, kj::String reason) {
    for (;;) {
      if (released) co_return;
      Envelope query(requestId, binding, 4);
      query.number("handle_id"_kj, handle);
      query.string("mode"_kj, "graceful"_kj);
      query.number("code"_kj, code);
      query.string("reason"_kj, reason);
      auto encoded = host->close(query.encode());
      Reply reply(encoded, requestId);
      KJ_REQUIRE(reply.root.getObject().size() == 5 && field(reply.root, "accepted"_kj).isBoolean(),
          "invalid provider WebSocket close acknowledgement");
      if (reply.is("pending"_kj, "backpressure"_kj)) {
        KJ_REQUIRE(!field(reply.root, "accepted"_kj).getBoolean(),
            "backpressured provider close was committed");
        co_await timer.afterLimitTimeout(POLL_INTERVAL);
        continue;
      }
      KJ_REQUIRE(
          reply.is("pending"_kj, "closing"_kj) && field(reply.root, "accepted"_kj).getBoolean(),
          "provider WebSocket close was not accepted");
      sentClose = true;
      co_return;
    }
  }
  void cancel() {
    if (released) return;
    Envelope query(requestId, binding, 2);
    query.number("handle_id"_kj, handle);
    query.string("mode"_kj, "cancel"_kj);
    auto encoded = host->close(query.encode());
    Reply reply(encoded, requestId);
    if (reply.is("pending"_kj, "cancelling"_kj)) {
      KJ_REQUIRE(reply.root.getObject().size() == 5 &&
              field(reply.root, "accepted"_kj).isBoolean() &&
              field(reply.root, "accepted"_kj).getBoolean(),
          "invalid pending provider cancellation");
      return;
    }
    KJ_REQUIRE(reply.root.getObject().size() == 4 && reply.is("ok"_kj, "cancelled"_kj),
        "provider WebSocket cancellation did not confirm worker termination");
    release();
  }
  void release() {
    KJ_REQUIRE(
        !released && registry->active > 0, "provider WebSocket lifetime accounting mismatch");
    released = true;
    --registry->active;
  }

  kj::Rc<WebSocketHostChannel> host;
  kj::Rc<Registry> registry;
  kj::String requestId;
  kj::String binding;
  uint64_t handle;
  TimerChannel& timer;
  uint64_t sentSequence = 0;
  uint64_t receivedSequence = 0;
  uint64_t sentBytes = 0;
  uint64_t receivedBytes = 0;
  bool sentClose = false;
  bool released = false;
};

class AuthenticatedWebSocketBroker final: public WebSocketBroker {
 public:
  explicit AuthenticatedWebSocketBroker(kj::Rc<WebSocketHostChannel> host)
      : host(kj::mv(host)),
        registry(kj::rc<Registry>()) {}
  kj::Promise<kj::Own<kj::WebSocket>> connect(kj::String binding,
      kj::String url,
      kj::Array<kj::String> protocols,
      TimerChannel& timer) override {
    KJ_REQUIRE(registry->active < MAX_CONNECTIONS, "provider WebSocket connection budget exceeded");
    KJ_REQUIRE(registry->nextId < MAX_ID, "provider WebSocket request ID space exhausted");
    auto requestId = kj::str("websocket-", registry->nextId++);
    ++registry->active;
    bool knownFailure = false;
    bool hasHandle = false;
    KJ_ON_SCOPE_FAILURE({
      if (!knownFailure && !hasHandle) registry->uncertain = true;
    });
    Envelope query(requestId, binding, 2);
    query.string("url"_kj, url);
    query.fields[query.next].setName("subprotocols");
    auto protocolList = query.fields[query.next++].getValue().initArray(protocols.size());
    for (auto i: kj::indices(protocols)) protocolList[i].setString(protocols[i]);
    auto encoded = host->open(query.encode());
    Reply reply(encoded, requestId, true);
    if (reply.isFailure()) {
      --registry->active;
      knownFailure = true;
      reply.requireSuccess();
    }
    KJ_REQUIRE(reply.root.getObject().size() == 5 && reply.is("pending"_kj, "connecting"_kj),
        "invalid provider WebSocket connection reply");
    auto handle = integer(field(reply.root, "handle_id"_kj), MAX_ID);
    KJ_REQUIRE(handle > 0, "invalid provider WebSocket handle");
    auto socket = kj::heap<ProviderSocket>(
        host.addRef(), registry.addRef(), kj::mv(requestId), kj::mv(binding), handle, timer);
    hasHandle = true;
    co_await socket->awaitOpen();
    co_return kj::mv(socket);
  }
  bool isQuiescent() const override {
    return registry->active == 0 && !registry->uncertain;
  }

 private:
  kj::Rc<WebSocketHostChannel> host;
  kj::Rc<Registry> registry;
};

}  // namespace

kj::Rc<WebSocketHostChannel> newHyperlightWebSocketHostChannel() {
  return kj::rc<HyperlightWebSocketHostChannel>();
}

kj::Rc<WebSocketBroker> newAuthenticatedWebSocketBroker(kj::Rc<WebSocketHostChannel> host) {
  return kj::rc<AuthenticatedWebSocketBroker>(kj::mv(host));
}

}  // namespace workerd::server::sandbox_executor
