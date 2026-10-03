// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "logical-service-broker.h"
#include "logical-service-dispatcher.h"
#include "src/workerd/server/logical-service-broker/audit.embed.h"
#include "src/workerd/server/logical-service-broker/request.embed.h"
#include "src/workerd/server/logical-service-broker/response.embed.h"

#include <kj/debug.h>
#include <kj/test.h>
#include <kj/vector.h>

namespace workerd::server::logical_service_broker {
namespace {

kj::String fixtureContent(kj::StringPtr fixture) {
  if (fixture.endsWith("\r\n"_kj)) return kj::str(fixture.slice(0, fixture.size() - 2));
  if (fixture.endsWith("\n"_kj)) return kj::str(fixture.slice(0, fixture.size() - 1));
  return kj::str(fixture);
}

class TestBudget final: public Budget {
 public:
  kj::Maybe<uint64_t> tryReserve(
      const RequestIdentity&, const Request&, size_t requestBytes) override {
    ++reserveCount;
    lastRequestBytes = requestBytes;
    if (fail) KJ_FAIL_REQUIRE("budget failure");
    if (deny) return kj::none;
    return uint64_t(41);
  }

  void settle(uint64_t reservation, ResponseStatus status, size_t responseBytes) noexcept override {
    ++settleCount;
    lastReservation = reservation;
    lastStatus = status;
    lastResponseBytes = responseBytes;
  }

  bool deny = false;
  bool fail = false;
  uint32_t reserveCount = 0;
  uint32_t settleCount = 0;
  size_t lastRequestBytes = 0;
  uint64_t lastReservation = 0;
  ResponseStatus lastStatus = ResponseStatus::HOST_ERROR;
  size_t lastResponseBytes = 0;
};

class TestAdapter final: public ServiceAdapter {
 public:
  Response dispatch(const RequestIdentity&, const Request& request) override {
    ++dispatchCount;
    if (fail) KJ_FAIL_REQUIRE("adapter failure");
    return Response{
      mismatchResponseId ? kj::str("wrong-request") : kj::str(request.requestId),
      ResponseStatus::OK,
      kj::heapArray<kj::byte>({7, 8}),
      kj::heapArray<Attribute>(0),
      kj::none,
    };
  }

  bool fail = false;
  bool mismatchResponseId = false;
  uint32_t dispatchCount = 0;
};

class TestAuditSink final: public AuditSink {
 public:
  void record(const AuditEvent& event) noexcept override {
    events.add(AuditEvent{
      kj::str(event.requestId),
      kj::str(event.workloadId),
      kj::str(event.snapshotId),
      event.attempt,
      kj::str(event.binding),
      event.operation,
      event.decision,
      event.status,
      event.requestBytes,
      event.responseBytes,
    });
  }

  kj::Vector<AuditEvent> events;
};

RequestIdentity identity(
    kj::StringPtr workload = "worker-a"_kj, kj::StringPtr snapshot = "snapshot-7"_kj) {
  return RequestIdentity{kj::str(workload), kj::str(snapshot), 2};
}

Grant grant(kj::StringPtr binding,
    Service service,
    kj::Array<Operation> operations,
    size_t maxValueBytes = 1024) {
  return Grant{
    kj::str(binding),
    service,
    kj::mv(operations),
    maxValueBytes,
    1024,
    100,
  };
}

kj::Array<Grant> oneGrant(Grant value) {
  auto grants = kj::heapArrayBuilder<Grant>(1);
  grants.add(kj::mv(value));
  return grants.finish();
}

Request request(Operation operation,
    kj::StringPtr binding,
    kj::StringPtr target,
    kj::Maybe<kj::Array<kj::byte>> value = kj::none,
    kj::Array<Attribute> attributes = kj::heapArray<Attribute>(0),
    kj::Maybe<uint32_t> limit = kj::none,
    kj::Maybe<kj::String> cursor = kj::none,
    kj::Maybe<uint64_t> expiration = kj::none) {
  return Request{
    kj::str("req-17"),
    kj::str(binding),
    operation,
    kj::str(target),
    kj::mv(value),
    kj::mv(attributes),
    limit,
    kj::mv(cursor),
    expiration,
  };
}

KJ_TEST("logical service request fixture is canonical and round-trips") {
  auto fixture = fixtureContent(REQUEST);
  auto parsed = parseCanonicalRequest(fixture);
  KJ_EXPECT(parsed.operation == Operation::CACHE_PUT);
  KJ_EXPECT(parsed.binding == "edge-cache"_kj);
  KJ_EXPECT(parsed.target == "https://example.test/data"_kj);
  KJ_EXPECT(kj::str(KJ_ASSERT_NONNULL(parsed.value).asChars()) == "hello"_kj);
  KJ_EXPECT(parsed.attributes.size() == 2);
  KJ_EXPECT(parsed.attributes[0].name == "status"_kj);
  KJ_EXPECT(parsed.attributes[0].value == "200"_kj);
  KJ_EXPECT(parsed.expirationUnixMs == uint64_t(1735689600000));
  KJ_EXPECT(serializeRequest(parsed) == fixture);
}

KJ_TEST("logical service response fixture is canonical and round-trips") {
  auto fixture = fixtureContent(RESPONSE);
  auto parsed = parseCanonicalResponse(fixture);
  KJ_EXPECT(parsed.status == ResponseStatus::OK);
  KJ_EXPECT(kj::str(KJ_ASSERT_NONNULL(parsed.value).asChars()) == "hello"_kj);
  KJ_EXPECT(parsed.attributes.size() == 1);
  KJ_EXPECT(parsed.attributes[0].name == "etag"_kj);
  KJ_EXPECT(parsed.attributes[0].value == "abc123"_kj);
  KJ_EXPECT(serializeResponse(parsed) == fixture);
}

KJ_TEST("logical service audit fixture is deterministic and excludes payload data") {
  AuditEvent event{
    kj::str("req-17"),
    kj::str("worker-a"),
    kj::str("snapshot-7"),
    2,
    kj::str("edge-cache"),
    Operation::CACHE_PUT,
    AuditDecision::DISPATCHED,
    ResponseStatus::OK,
    308,
    146,
  };
  KJ_EXPECT(serializeAuditEvent(event) == fixtureContent(AUDIT));
}

KJ_TEST("logical service policy denies by default") {
  Policy policy(identity(), kj::heapArray<Grant>(0));
  auto input = request(Operation::KV_GET, "settings"_kj, "theme"_kj);
  KJ_EXPECT(policy.authorize(identity(), input) == Authorization::BINDING_NOT_GRANTED);
}

KJ_TEST("logical service policy binds grants to host identity and exact operation") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_GET})));
  Policy policy(identity(), kj::mv(grants));

  auto get = request(Operation::KV_GET, "settings"_kj, "theme"_kj);
  KJ_EXPECT(policy.authorize(identity(), get) == Authorization::ALLOW);
  KJ_EXPECT(policy.authorize(identity("other-worker"_kj), get) == Authorization::IDENTITY_MISMATCH);

  auto value = kj::heapArray<kj::byte>({1, 2, 3});
  auto put = request(Operation::KV_PUT, "settings"_kj, "theme"_kj, kj::mv(value));
  KJ_EXPECT(policy.authorize(identity(), put) == Authorization::OPERATION_NOT_GRANTED);

  auto cache = request(Operation::CACHE_MATCH, "settings"_kj, "https://example.test/"_kj);
  KJ_EXPECT(policy.authorize(identity(), cache) == Authorization::OPERATION_NOT_GRANTED);
}

KJ_TEST("logical service policy enforces binding quotas before dispatch") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_PUT}), 2));
  Policy policy(identity(), kj::mv(grants));
  auto value = kj::heapArray<kj::byte>({1, 2, 3});
  auto put = request(Operation::KV_PUT, "settings"_kj, "theme"_kj, kj::mv(value));
  KJ_EXPECT(policy.authorize(identity(), put) == Authorization::REQUEST_TOO_LARGE);
}

KJ_TEST("logical service parser rejects unknown fields and path-shaped extensions") {
  auto input =
      kj::str("{\"protocol_version\":1,\"request_id\":\"req-1\",\"binding\":\"settings\","
              "\"operation\":\"kv.get\",\"target\":\"theme\",\"value_base64\":null,"
              "\"attributes\":[],\"limit\":null,\"cursor\":null,\"expiration_unix_ms\":null,"
              "\"host_path\":\"forbidden\"}");
  KJ_EXPECT_THROW_MESSAGE("unknown logical service request field", parseRequest(input));
}

KJ_TEST("logical service parser rejects operation payload confusion") {
  auto input =
      kj::str("{\"protocol_version\":1,\"request_id\":\"req-1\",\"binding\":\"identity\","
              "\"operation\":\"identity.get\",\"target\":\"claims\",\"value_base64\":null,"
              "\"attributes\":[],\"limit\":null,\"cursor\":null,\"expiration_unix_ms\":null}");
  KJ_EXPECT_THROW_MESSAGE("invalid logical identity request shape", parseRequest(input));
}

KJ_TEST("failed logical service responses cannot expose host details") {
  auto input = kj::str("{\"protocol_version\":1,\"request_id\":\"req-1\",\"status\":\"denied\","
                       "\"value_base64\":\"c2VjcmV0\",\"attributes\":[],\"cursor\":null}");
  KJ_EXPECT_THROW_MESSAGE(
      "failed logical service response must not expose result data", parseResponse(input));
}

KJ_TEST("dispatcher authorizes, pre-charges, validates, settles, and audits") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_GET})));
  Policy policy(identity(), kj::mv(grants));
  TestBudget budget;
  TestAdapter adapter;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto canonical = serializeRequest(request(Operation::KV_GET, "settings"_kj, "theme"_kj));

  auto response = parseCanonicalResponse(dispatcher.invoke(identity(), canonical));
  KJ_EXPECT(response.status == ResponseStatus::OK);
  auto& value = KJ_ASSERT_NONNULL(response.value);
  KJ_EXPECT(value.size() == 2);
  KJ_EXPECT(value[0] == 7 && value[1] == 8);
  KJ_EXPECT(budget.reserveCount == 1);
  KJ_EXPECT(budget.settleCount == 1);
  KJ_EXPECT(budget.lastReservation == 41);
  KJ_EXPECT(budget.lastStatus == ResponseStatus::OK);
  KJ_EXPECT(adapter.dispatchCount == 1);
  KJ_EXPECT(audit.events.size() == 1);
  KJ_EXPECT(audit.events[0].decision == AuditDecision::DISPATCHED);
  KJ_EXPECT(audit.events[0].status == ResponseStatus::OK);
}

KJ_TEST("dispatcher denial never charges or calls the adapter") {
  Policy policy(identity(), kj::heapArray<Grant>(0));
  TestBudget budget;
  TestAdapter adapter;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto canonical = serializeRequest(request(Operation::KV_GET, "settings"_kj, "theme"_kj));

  auto response = parseCanonicalResponse(dispatcher.invoke(identity(), canonical));
  KJ_EXPECT(response.status == ResponseStatus::DENIED);
  KJ_EXPECT(budget.reserveCount == 0);
  KJ_EXPECT(adapter.dispatchCount == 0);
  KJ_EXPECT(audit.events.size() == 1);
  KJ_EXPECT(audit.events[0].decision == AuditDecision::BINDING_DENIED);
  KJ_EXPECT(audit.events[0].status == ResponseStatus::DENIED);
}

KJ_TEST("dispatcher rejects malformed host identity before parsing or auditing") {
  Policy policy(identity(), kj::heapArray<Grant>(0));
  TestBudget budget;
  TestAdapter adapter;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto canonical = serializeRequest(request(Operation::KV_GET, "settings"_kj, "theme"_kj));

  KJ_EXPECT_THROW_MESSAGE("invalid logical service workload identity",
      dispatcher.invoke(identity("invalid workload"_kj), canonical));
  KJ_EXPECT(budget.reserveCount == 0);
  KJ_EXPECT(adapter.dispatchCount == 0);
  KJ_EXPECT(audit.events.size() == 0);
}

KJ_TEST("dispatcher quota denial is deterministic and does not dispatch") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_GET})));
  Policy policy(identity(), kj::mv(grants));
  TestBudget budget;
  budget.deny = true;
  TestAdapter adapter;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto canonical = serializeRequest(request(Operation::KV_GET, "settings"_kj, "theme"_kj));

  auto response = parseCanonicalResponse(dispatcher.invoke(identity(), canonical));
  KJ_EXPECT(response.status == ResponseStatus::QUOTA_EXCEEDED);
  KJ_EXPECT(budget.reserveCount == 1);
  KJ_EXPECT(budget.settleCount == 0);
  KJ_EXPECT(adapter.dispatchCount == 0);
  KJ_EXPECT(audit.events[0].decision == AuditDecision::QUOTA_DENIED);
  KJ_EXPECT(audit.events[0].status == ResponseStatus::QUOTA_EXCEEDED);
}

KJ_TEST("dispatcher audits budget subsystem errors without dispatching") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_GET})));
  Policy policy(identity(), kj::mv(grants));
  TestBudget budget;
  budget.fail = true;
  TestAdapter adapter;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto canonical = serializeRequest(request(Operation::KV_GET, "settings"_kj, "theme"_kj));

  KJ_EXPECT_THROW_MESSAGE("budget failure", dispatcher.invoke(identity(), canonical));
  KJ_EXPECT(budget.reserveCount == 1);
  KJ_EXPECT(budget.settleCount == 0);
  KJ_EXPECT(adapter.dispatchCount == 0);
  KJ_EXPECT(audit.events.size() == 1);
  KJ_EXPECT(audit.events[0].decision == AuditDecision::BUDGET_ERROR);
  KJ_EXPECT(audit.events[0].status == ResponseStatus::HOST_ERROR);
}

KJ_TEST("dispatcher rejects non-canonical requests before authorization") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_GET})));
  Policy policy(identity(), kj::mv(grants));
  TestBudget budget;
  TestAdapter adapter;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto nonCanonical =
      kj::str("{ \"protocol_version\":1,\"request_id\":\"req-17\",\"binding\":\"settings\","
              "\"operation\":\"kv.get\",\"target\":\"theme\",\"value_base64\":null,"
              "\"attributes\":[],\"limit\":null,\"cursor\":null,\"expiration_unix_ms\":null}");

  KJ_EXPECT_THROW_MESSAGE(
      "logical service request is not canonical", dispatcher.invoke(identity(), nonCanonical));
  KJ_EXPECT(budget.reserveCount == 0);
  KJ_EXPECT(adapter.dispatchCount == 0);
  KJ_EXPECT(audit.events.size() == 0);
}

KJ_TEST("dispatcher settles and audits adapter exceptions before rethrowing") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_GET})));
  Policy policy(identity(), kj::mv(grants));
  TestBudget budget;
  TestAdapter adapter;
  adapter.fail = true;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto canonical = serializeRequest(request(Operation::KV_GET, "settings"_kj, "theme"_kj));

  KJ_EXPECT_THROW_MESSAGE("adapter failure", dispatcher.invoke(identity(), canonical));
  KJ_EXPECT(budget.settleCount == 1);
  KJ_EXPECT(budget.lastStatus == ResponseStatus::HOST_ERROR);
  KJ_EXPECT(audit.events.size() == 1);
  KJ_EXPECT(audit.events[0].decision == AuditDecision::ADAPTER_ERROR);
  KJ_EXPECT(audit.events[0].status == ResponseStatus::HOST_ERROR);
}

KJ_TEST("dispatcher rejects, settles, and audits invalid adapter responses") {
  auto grants =
      oneGrant(grant("settings"_kj, Service::KV, kj::heapArray<Operation>({Operation::KV_GET})));
  Policy policy(identity(), kj::mv(grants));
  TestBudget budget;
  TestAdapter adapter;
  adapter.mismatchResponseId = true;
  TestAuditSink audit;
  Dispatcher dispatcher(policy, budget, adapter, audit);
  auto canonical = serializeRequest(request(Operation::KV_GET, "settings"_kj, "theme"_kj));

  KJ_EXPECT_THROW_MESSAGE(
      "logical service adapter response ID mismatch", dispatcher.invoke(identity(), canonical));
  KJ_EXPECT(budget.settleCount == 1);
  KJ_EXPECT(budget.lastStatus == ResponseStatus::HOST_ERROR);
  KJ_EXPECT(audit.events.size() == 1);
  KJ_EXPECT(audit.events[0].decision == AuditDecision::INVALID_RESPONSE);
  KJ_EXPECT(audit.events[0].status == ResponseStatus::HOST_ERROR);
}

}  // namespace
}  // namespace workerd::server::logical_service_broker
