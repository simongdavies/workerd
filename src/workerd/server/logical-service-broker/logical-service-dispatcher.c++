// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "logical-service-dispatcher.h"

#include <kj/debug.h>
#include <kj/vector.h>

namespace workerd::server::logical_service_broker {
namespace {

void appendJsonString(kj::Vector<char>& output, kj::StringPtr value) {
  static constexpr char HEX[] = "0123456789abcdef";
  output.add('"');
  for (auto c: value) {
    switch (c) {
      case '"':
        output.addAll("\\\""_kj);
        break;
      case '\\':
        output.addAll("\\\\"_kj);
        break;
      case '\b':
        output.addAll("\\b"_kj);
        break;
      case '\f':
        output.addAll("\\f"_kj);
        break;
      case '\n':
        output.addAll("\\n"_kj);
        break;
      case '\r':
        output.addAll("\\r"_kj);
        break;
      case '\t':
        output.addAll("\\t"_kj);
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          output.addAll("\\u00"_kj);
          output.add(HEX[(static_cast<unsigned char>(c) >> 4) & 0x0f]);
          output.add(HEX[static_cast<unsigned char>(c) & 0x0f]);
        } else {
          output.add(c);
        }
        break;
    }
  }
  output.add('"');
}

AuditDecision auditDecisionFor(Authorization authorization) {
  switch (authorization) {
    case Authorization::IDENTITY_MISMATCH:
      return AuditDecision::IDENTITY_DENIED;
    case Authorization::BINDING_NOT_GRANTED:
      return AuditDecision::BINDING_DENIED;
    case Authorization::OPERATION_NOT_GRANTED:
      return AuditDecision::OPERATION_DENIED;
    case Authorization::REQUEST_TOO_LARGE:
      return AuditDecision::REQUEST_TOO_LARGE;
    case Authorization::ALLOW:
      KJ_FAIL_ASSERT("allowed authorization has no denial audit decision");
  }
  KJ_UNREACHABLE;
}

ResponseStatus responseStatusFor(Authorization authorization) {
  return authorization == Authorization::REQUEST_TOO_LARGE ? ResponseStatus::QUOTA_EXCEEDED
                                                           : ResponseStatus::DENIED;
}

Response makeEmptyResponse(kj::String requestId, ResponseStatus status) {
  return Response{
    kj::mv(requestId),
    status,
    kj::none,
    kj::heapArray<Attribute>(0),
    kj::none,
  };
}

AuditEvent makeAuditEvent(const RequestIdentity& identity,
    const Request& request,
    AuditDecision decision,
    ResponseStatus status,
    size_t requestBytes,
    size_t responseBytes) {
  return AuditEvent{
    kj::str(request.requestId),
    kj::str(identity.workloadId),
    kj::str(identity.snapshotId),
    identity.attempt,
    kj::str(request.binding),
    request.operation,
    decision,
    status,
    requestBytes,
    responseBytes,
  };
}

}  // namespace

Dispatcher::Dispatcher(
    const Policy& policy, Budget& budget, ServiceAdapter& adapter, AuditSink& audit)
    : policy(policy),
      budget(budget),
      adapter(adapter),
      audit(audit) {}

kj::String Dispatcher::invoke(
    const RequestIdentity& identity, kj::ArrayPtr<const char> canonicalRequest) {
  validateRequestIdentity(identity);
  auto request = parseCanonicalRequest(canonicalRequest);
  auto authorization = policy.authorize(identity, request);
  if (authorization != Authorization::ALLOW) {
    auto response = serializeResponse(
        makeEmptyResponse(kj::str(request.requestId), responseStatusFor(authorization)));
    auto status = responseStatusFor(authorization);
    auto event = makeAuditEvent(identity, request, auditDecisionFor(authorization), status,
        canonicalRequest.size(), response.size());
    audit.record(event);
    return response;
  }

  auto reservation = [&]() {
    try {
      return budget.tryReserve(identity, request, canonicalRequest.size());
    } catch (const kj::Exception&) {
      auto event = makeAuditEvent(identity, request, AuditDecision::BUDGET_ERROR,
          ResponseStatus::HOST_ERROR, canonicalRequest.size(), 0);
      audit.record(event);
      throw;
    }
  }();
  if (reservation == kj::none) {
    auto response = serializeResponse(
        makeEmptyResponse(kj::str(request.requestId), ResponseStatus::QUOTA_EXCEEDED));
    auto event = makeAuditEvent(identity, request, AuditDecision::QUOTA_DENIED,
        ResponseStatus::QUOTA_EXCEEDED, canonicalRequest.size(), response.size());
    audit.record(event);
    return response;
  }

  auto reservationId = KJ_ASSERT_NONNULL(reservation);
  Response response = [&]() {
    try {
      return adapter.dispatch(identity, request);
    } catch (const kj::Exception&) {
      budget.settle(reservationId, ResponseStatus::HOST_ERROR, 0);
      auto event = makeAuditEvent(identity, request, AuditDecision::ADAPTER_ERROR,
          ResponseStatus::HOST_ERROR, canonicalRequest.size(), 0);
      audit.record(event);
      throw;
    }
  }();

  try {
    KJ_REQUIRE(
        response.requestId == request.requestId, "logical service adapter response ID mismatch");
    auto serialized = serializeResponse(response);
    auto validated = parseCanonicalResponse(serialized);
    budget.settle(reservationId, validated.status, serialized.size());
    auto event = makeAuditEvent(identity, request, AuditDecision::DISPATCHED, validated.status,
        canonicalRequest.size(), serialized.size());
    audit.record(event);
    return serialized;
  } catch (const kj::Exception&) {
    budget.settle(reservationId, ResponseStatus::HOST_ERROR, 0);
    auto event = makeAuditEvent(identity, request, AuditDecision::INVALID_RESPONSE,
        ResponseStatus::HOST_ERROR, canonicalRequest.size(), 0);
    audit.record(event);
    throw;
  }
}

kj::StringPtr auditDecisionName(AuditDecision decision) {
  switch (decision) {
    case AuditDecision::IDENTITY_DENIED:
      return "identity_denied"_kj;
    case AuditDecision::BINDING_DENIED:
      return "binding_denied"_kj;
    case AuditDecision::OPERATION_DENIED:
      return "operation_denied"_kj;
    case AuditDecision::REQUEST_TOO_LARGE:
      return "request_too_large"_kj;
    case AuditDecision::QUOTA_DENIED:
      return "quota_denied"_kj;
    case AuditDecision::BUDGET_ERROR:
      return "budget_error"_kj;
    case AuditDecision::DISPATCHED:
      return "dispatched"_kj;
    case AuditDecision::INVALID_RESPONSE:
      return "invalid_response"_kj;
    case AuditDecision::ADAPTER_ERROR:
      return "adapter_error"_kj;
  }
  KJ_UNREACHABLE;
}

kj::String serializeAuditEvent(const AuditEvent& event) {
  kj::Vector<char> output;
  output.addAll("{\"event_version\":1,\"request_id\":"_kj);
  appendJsonString(output, event.requestId);
  output.addAll(",\"workload_id\":"_kj);
  appendJsonString(output, event.workloadId);
  output.addAll(",\"snapshot_id\":"_kj);
  appendJsonString(output, event.snapshotId);
  output.addAll(",\"attempt\":"_kj);
  output.addAll(kj::str(event.attempt));
  output.addAll(",\"binding\":"_kj);
  appendJsonString(output, event.binding);
  output.addAll(",\"operation\":"_kj);
  appendJsonString(output, operationName(event.operation));
  output.addAll(",\"decision\":"_kj);
  appendJsonString(output, auditDecisionName(event.decision));
  output.addAll(",\"status\":"_kj);
  appendJsonString(output, responseStatusName(event.status));
  output.addAll(",\"request_bytes\":"_kj);
  output.addAll(kj::str(event.requestBytes));
  output.addAll(",\"response_bytes\":"_kj);
  output.addAll(kj::str(event.responseBytes));
  output.addAll("}\0"_kj);
  return kj::String(output.releaseAsArray());
}

}  // namespace workerd::server::logical_service_broker
