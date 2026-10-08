// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/api/web-socket.h>

namespace workerd::server {

class ProviderWebSocketBinding final: public jsg::Object {
 public:
  explicit ProviderWebSocketBinding(uint channel): channel(channel) {}

  jsg::Ref<api::WebSocket> connect(jsg::Lock& js,
      kj::String url,
      jsg::Optional<kj::Array<kj::String>> protocols,
      jsg::Arguments<jsg::Value> extra);

  JSG_RESOURCE_TYPE(ProviderWebSocketBinding) {
    JSG_METHOD(connect);
  }

 private:
  uint channel;
};

}  // namespace workerd::server
