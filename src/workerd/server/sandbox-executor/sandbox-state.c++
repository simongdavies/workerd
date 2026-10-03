// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-state.h"

namespace workerd::server::sandbox_executor {
namespace {

bool isOpaqueIdentifier(kj::StringPtr value, size_t maxBytes) {
  if (value.size() == 0 || value.size() > maxBytes) return false;
  for (char c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' ||
            c == '_' || c == '.' || c == ':')) {
      return false;
    }
  }
  return true;
}

}  // namespace

kj::String LogicalResourceIdentity::canonicalKey() const {
  validateLogicalResourceIdentity(*this);
  return kj::str(tenantId, '\n', namespaceId, '\n', resourceId, '\n', snapshot.workerVersion, '\n',
      snapshot.snapshotId);
}

void validateLogicalResourceIdentity(const LogicalResourceIdentity& identity) {
  KJ_REQUIRE(isOpaqueIdentifier(identity.tenantId, 128), "invalid logical tenant ID");
  KJ_REQUIRE(isOpaqueIdentifier(identity.namespaceId, 128), "invalid logical namespace ID");
  KJ_REQUIRE(isOpaqueIdentifier(identity.resourceId, 256), "invalid logical resource ID");
  KJ_REQUIRE(
      isOpaqueIdentifier(identity.snapshot.workerVersion, 256), "invalid logical worker version");
  KJ_REQUIRE(isOpaqueIdentifier(identity.snapshot.snapshotId, 256), "invalid logical snapshot ID");
}

LogicalResourceIdentity cloneLogicalResourceIdentity(const LogicalResourceIdentity& identity) {
  return {
    .tenantId = kj::str(identity.tenantId),
    .namespaceId = kj::str(identity.namespaceId),
    .resourceId = kj::str(identity.resourceId),
    .snapshot =
        {
          .workerVersion = kj::str(identity.snapshot.workerVersion),
          .snapshotId = kj::str(identity.snapshot.snapshotId),
        },
  };
}

}  // namespace workerd::server::sandbox_executor
