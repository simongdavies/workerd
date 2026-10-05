// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/io-channels.h>
#include <workerd/io/worker-interface.h>
#include <workerd/server/logical-service-broker/composite-data-router.h>

#include <kj/array.h>
#include <kj/compat/http.h>
#include <kj/refcount.h>
#include <kj/string.h>

namespace workerd::server::sandbox_executor {

class LogicalServiceHostChannel: public kj::Refcounted {
 public:
  virtual ~LogicalServiceHostChannel() noexcept(false) {}
  virtual kj::String invoke(kj::ArrayPtr<const char> canonicalEnvelope) = 0;
};

kj::Rc<LogicalServiceHostChannel> newHyperlightLogicalServiceHostChannel();

kj::Own<IoChannelFactory::SubrequestChannel> newCompositeServiceChannel(
    kj::Rc<LogicalServiceHostChannel> host,
    kj::HttpHeaderTable& headerTable,
    kj::String binding,
    logical_service_broker::composite::BindingKind kind);

}  // namespace workerd::server::sandbox_executor
