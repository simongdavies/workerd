// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include "logical-service-broker.h"

namespace workerd::server::logical_service_broker {

enum class AuditDecision {
  IDENTITY_DENIED,
  BINDING_DENIED,
  OPERATION_DENIED,
  REQUEST_TOO_LARGE,
  QUOTA_DENIED,
  BUDGET_ERROR,
  DISPATCHED,
  INVALID_RESPONSE,
  ADAPTER_ERROR,
};

struct AuditEvent {
  kj::String requestId;
  kj::String workloadId;
  kj::String snapshotId;
  uint32_t attempt;
  kj::String binding;
  Operation operation;
  AuditDecision decision;
  ResponseStatus status;
  size_t requestBytes;
  size_t responseBytes;
};

class Budget {
 public:
  virtual ~Budget() noexcept(false) {}
  virtual kj::Maybe<uint64_t> tryReserve(
      const RequestIdentity& identity, const Request& request, size_t requestBytes) = 0;
  virtual void settle(
      uint64_t reservation, ResponseStatus status, size_t responseBytes) noexcept = 0;
};

class ServiceAdapter {
 public:
  virtual ~ServiceAdapter() noexcept(false) {}
  virtual Response dispatch(const RequestIdentity& identity, const Request& request) = 0;
};

class AuditSink {
 public:
  virtual ~AuditSink() noexcept(false) {}
  virtual void record(const AuditEvent& event) noexcept = 0;
};

class Dispatcher {
 public:
  Dispatcher(const Policy& policy, Budget& budget, ServiceAdapter& adapter, AuditSink& audit);

  kj::String invoke(const RequestIdentity& identity, kj::ArrayPtr<const char> canonicalRequest);

 private:
  const Policy& policy;
  Budget& budget;
  ServiceAdapter& adapter;
  AuditSink& audit;
};

kj::StringPtr auditDecisionName(AuditDecision decision);
kj::String serializeAuditEvent(const AuditEvent& event);

}  // namespace workerd::server::logical_service_broker
