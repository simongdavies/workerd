// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/jsg/jsg.h>

namespace workerd::server {

class WebhookBinding final: public jsg::Object {
 public:
  WebhookBinding(uint channel, kj::String bindingName);

  jsg::Promise<jsg::Value> verify(
      jsg::Lock& js, kj::OneOf<jsg::JsBufferSource, kj::String> body, kj::String signature);

  JSG_RESOURCE_TYPE(WebhookBinding) {
    JSG_METHOD(verify);
  }

 private:
  uint channel;
  kj::String bindingName;
  uint64_t nextRequestId = 1;
};

}  // namespace workerd::server
