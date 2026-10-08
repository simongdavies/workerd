// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "webhook-binding.h"

#include "logical-service-broker/composite-data-router.h"
#include "logical-service-broker/logical-service-broker.h"

#include <workerd/io/io-context.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/encoding.h>

namespace workerd::server {
namespace {

namespace composite = logical_service_broker::composite;

kj::Promise<kj::String> invokeWebhook(
    kj::Own<kj::HttpClient> client, kj::HttpHeaders headers, kj::String envelope) {
  auto request = client->request(
      kj::HttpMethod::POST, "https://webhook.invalid/verify"_kj, headers, envelope.size());
  auto response = request.response.eagerlyEvaluate(nullptr);
  co_await request.body->write(envelope.asBytes());
  request.body = nullptr;
  auto result = co_await response;
  KJ_REQUIRE(result.statusCode == 200, "host webhook verification transport failed");
  co_return co_await result.body->readAllText(logical_service_broker::MAX_ENVELOPE_BYTES);
}

}  // namespace

WebhookBinding::WebhookBinding(uint channel, kj::String bindingName)
    : channel(channel),
      bindingName(kj::mv(bindingName)) {
  KJ_REQUIRE(composite::isBindingName(this->bindingName), "invalid webhook binding name");
}

jsg::Promise<jsg::Value> WebhookBinding::verify(
    jsg::Lock& js, kj::OneOf<jsg::JsBufferSource, kj::String> body, kj::String signature) {
  JSG_REQUIRE(signature.size() > 0 && signature.size() <= composite::MAX_WEBHOOK_SIGNATURE_BYTES,
      RangeError, "Webhook signature exceeds the verification limit.");
  kj::ArrayPtr<const byte> raw;
  KJ_SWITCH_ONEOF(body) {
    KJ_CASE_ONEOF(buffer, jsg::JsBufferSource) {
      raw = buffer.asArrayPtr();
    }
    KJ_CASE_ONEOF(text, kj::String) {
      raw = text.asBytes();
    }
  }
  JSG_REQUIRE(raw.size() <= composite::MAX_WEBHOOK_BODY_BYTES, RangeError,
      "Webhook body exceeds the verification limit.");
  JSG_REQUIRE(nextRequestId != kj::maxValue, Error, "Webhook request ID space exhausted.");
  auto requestId = kj::str("webhook-", nextRequestId++);
  auto encoded = kj::encodeBase64(raw);
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder arena;
  auto root = arena.initRoot<capnp::JsonValue>();
  auto fields = root.initObject(4);
  fields[0].setName("version");
  fields[0].getValue().setNumber(2);
  fields[1].setName("request_id");
  fields[1].getValue().setString(requestId);
  fields[2].setName("binding");
  fields[2].getValue().setString(bindingName);
  fields[3].setName("operation");
  auto operation = fields[3].getValue().initObject(3);
  operation[0].setName("kind");
  operation[0].getValue().setString("webhook_verify");
  operation[1].setName("body_base64");
  operation[1].getValue().setString(encoded);
  operation[2].setName("signature");
  operation[2].getValue().setString(signature);
  auto envelope = codec.encodeRaw(root);
  auto& context = IoContext::current();
  auto client = context.getHttpClient(channel, true, kj::none, "webhook_verify"_kjc);
  kj::HttpHeaders headers(context.getHeaderTable());
  headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json"_kj);
  return context.awaitIo(js, invokeWebhook(kj::mv(client), kj::mv(headers), kj::mv(envelope)),
      [requestId = kj::mv(requestId)](jsg::Lock& js, kj::String response) -> jsg::Value {
    auto metadata = composite::parseResponseEnvelope(response);
    JSG_REQUIRE(metadata.requestId == requestId && metadata.status == composite::ResponseStatus::OK,
        Error, "Host webhook verification failed.");
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder arena;
    auto root = arena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(response, root);
    kj::Maybe<capnp::JsonValue::Reader> value;
    for (auto field: root.getObject()) {
      if (field.getName() == "value"_kj) {
        JSG_REQUIRE(value == kj::none, Error, "Invalid host webhook verification response.");
        value = field.getValue();
      }
    }
    auto result = KJ_REQUIRE_NONNULL(value, "missing host webhook verification result");
    JSG_REQUIRE(result.isObject(), Error, "Invalid host webhook verification result.");
    bool hasValid = false;
    bool valid = false;
    bool hasEvent = false;
    for (auto field: result.getObject()) {
      if (field.getName() == "valid"_kj) {
        JSG_REQUIRE(!hasValid && field.getValue().isBoolean(), Error,
            "Invalid host webhook verification result.");
        hasValid = true;
        valid = field.getValue().getBoolean();
      } else if (field.getName() == "event"_kj) {
        JSG_REQUIRE(!hasEvent, Error, "Invalid host webhook verification result.");
        hasEvent = true;
      } else {
        JSG_FAIL_REQUIRE(Error, "Invalid host webhook verification result.");
      }
    }
    JSG_REQUIRE(
        hasValid && (!hasEvent || valid), Error, "Invalid host webhook verification result.");
    return js.parseJson(codec.encodeRaw(result));
  });
}

}  // namespace workerd::server
