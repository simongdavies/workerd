// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/debug.h>
#include <kj/string.h>

namespace workerd::server::sandbox_executor {

struct SnapshotIdentity {
  kj::String workerVersion;
  kj::String snapshotId;
};

struct LogicalResourceIdentity {
  kj::String tenantId;
  kj::String namespaceId;
  kj::String resourceId;
  SnapshotIdentity snapshot;

  kj::String canonicalKey() const;
};

void validateLogicalResourceIdentity(const LogicalResourceIdentity& identity);
LogicalResourceIdentity cloneLogicalResourceIdentity(const LogicalResourceIdentity& identity);

}  // namespace workerd::server::sandbox_executor
