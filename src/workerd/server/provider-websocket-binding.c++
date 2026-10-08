// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "provider-websocket-binding.h"

#include <workerd/io/io-context.h>

#include <kj/compat/url.h>

namespace workerd::server {

jsg::Ref<api::WebSocket> ProviderWebSocketBinding::connect(jsg::Lock& js,
    kj::String url,
    jsg::Optional<kj::Array<kj::String>> protocols,
    jsg::Arguments<jsg::Value> extra) {
  JSG_REQUIRE(extra.size() == 0, TypeError,
      "Provider WebSocket does not accept authentication parameters.");
  JSG_REQUIRE(url.size() <= 8 * 1024 && url.startsWith("wss://"), TypeError,
      "Provider WebSocket requires a public secure endpoint.");
  auto parsed = JSG_REQUIRE_NONNULL(kj::Url::tryParse(url, kj::Url::REMOTE_HREF), TypeError,
      "Invalid provider WebSocket endpoint.");
  JSG_REQUIRE(parsed.userInfo == kj::none && parsed.fragment == kj::none, TypeError,
      "Provider WebSocket endpoint must not contain credentials or a fragment.");
  auto selectedProtocols = kj::heapArray<kj::String>(0);
  KJ_IF_SOME(values, protocols) {
    JSG_REQUIRE(values.size() <= 8, RangeError, "Too many provider WebSocket subprotocols.");
    for (auto i: kj::indices(values)) {
      auto value = values[i].asPtr();
      JSG_REQUIRE(value.size() > 0 && value.size() <= 128, TypeError,
          "Invalid provider WebSocket subprotocol.");
      for (char c: value) {
        JSG_REQUIRE(c >= 0x21 && c <= 0x7e && strchr("()<>@,;:\\\"/[]?={} ", c) == nullptr,
            TypeError, "Invalid provider WebSocket subprotocol.");
      }
      for (size_t j = 0; j < i; ++j) {
        JSG_REQUIRE(values[j] != value, TypeError, "Duplicate provider WebSocket subprotocol.");
      }
    }
    selectedProtocols = kj::mv(values);
  }
  auto& context = IoContext::current();
  context.getLimitEnforcer().newSubrequest(true);
  auto connection = context.getIoChannelFactory().openProviderWebSocket(
      channel, kj::str(url), kj::mv(selectedProtocols));
  return api::WebSocket::fromScopedConnection(js, kj::mv(url), kj::mv(connection));
}

}  // namespace workerd::server
