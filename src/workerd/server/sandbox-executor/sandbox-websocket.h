// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include "sandbox-fetch.h"

namespace workerd::server::sandbox_executor {

inline constexpr size_t MAX_AUTHENTICATED_WEBSOCKET_FRAME_BYTES = 16 * 1024;
inline constexpr size_t MAX_AUTHENTICATED_WEBSOCKET_QUEUED_MESSAGES = 4;

class WebSocketHostChannel: public kj::Refcounted {
 public:
  virtual kj::String open(kj::StringPtr request) = 0;
  virtual kj::String send(kj::StringPtr request) = 0;
  virtual kj::String receive(kj::StringPtr request) = 0;
  virtual kj::String close(kj::StringPtr request) = 0;
};

kj::Rc<WebSocketHostChannel> newHyperlightWebSocketHostChannel();

class WebSocketBroker: public kj::Refcounted {
 public:
  virtual kj::Promise<kj::Own<kj::WebSocket>> connect(
      kj::String binding, kj::String url, kj::Array<kj::String> protocols, TimerChannel& timer) = 0;
  virtual bool isQuiescent() const = 0;
};

kj::Rc<WebSocketBroker> newAuthenticatedWebSocketBroker(kj::Rc<WebSocketHostChannel> host);

}  // namespace workerd::server::sandbox_executor
