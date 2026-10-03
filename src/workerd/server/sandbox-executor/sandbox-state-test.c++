// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-actors.h"
#include "sandbox-storage.h"

#include <kj/test.h>

namespace workerd::server::sandbox_executor {
namespace {

LogicalResourceIdentity identity(kj::StringPtr resourceId, kj::StringPtr snapshotId = "snap-1") {
  return {
    .tenantId = kj::str("tenant-1"),
    .namespaceId = kj::str("namespace-1"),
    .resourceId = kj::str(resourceId),
    .snapshot =
        {
          .workerVersion = kj::str("worker-v1"),
          .snapshotId = kj::str(snapshotId),
        },
  };
}

SqliteTransactionRequest request(
    kj::StringPtr sql, SqliteTransactionMode mode = SqliteTransactionMode::READ_ONLY) {
  kj::Vector<SqliteStatement> statements;
  statements.add(SqliteStatement{
    .sql = kj::str(sql),
    .parameters = kj::heapArray<SqliteValue>(0),
  });
  return {
    .protocolVersion = SQLITE_BROKER_ABI_VERSION,
    .requestId = kj::str("request-1"),
    .database = identity("database-1"),
    .mode = mode,
    .statements = statements.releaseAsArray(),
  };
}

SqliteTransactionRequest request(kj::StringPtr firstSql,
    kj::StringPtr secondSql,
    SqliteTransactionMode mode = SqliteTransactionMode::READ_WRITE) {
  kj::Vector<SqliteStatement> statements;
  statements.add(SqliteStatement{
    .sql = kj::str(firstSql),
    .parameters = kj::heapArray<SqliteValue>(0),
  });
  statements.add(SqliteStatement{
    .sql = kj::str(secondSql),
    .parameters = kj::heapArray<SqliteValue>(0),
  });
  return {
    .protocolVersion = SQLITE_BROKER_ABI_VERSION,
    .requestId = kj::str("request-atomic"),
    .database = identity("database-1"),
    .mode = mode,
    .statements = statements.releaseAsArray(),
  };
}

class RecordingSqliteAdapter final: public SqlitePersistenceAdapter {
 public:
  SqliteTransactionResponse transact(const SqliteTransactionRequest& request) override {
    ++calls;
    KJ_REQUIRE(!fail, "host persistence failed");
    seenKey = request.database.canonicalKey();
    kj::Vector<SqliteStatementResult> results;
    auto resultCount = outcome == SqliteTransactionOutcome::COMMITTED ? request.statements.size()
        : exposePartialResults                                        ? 1
                                                                      : 0;
    for (size_t i = 0; i < resultCount; ++i) {
      kj::Vector<SqliteRow> rows;
      for (size_t row = 0; row < returnedRows; ++row) {
        kj::Vector<SqliteColumn> columns;
        if (returnedValue.size() > 0) {
          columns.add(SqliteColumn{
            .name = kj::str("value"),
            .value = SqliteValue(kj::str(returnedValue)),
          });
        }
        rows.add(SqliteRow{.columns = columns.releaseAsArray()});
      }
      results.add(SqliteStatementResult{
        .rowsRead = rowsRead,
        .rowsWritten = rowsWritten,
        .lastInsertRowId = kj::none,
        .rows = rows.releaseAsArray(),
      });
    }
    return {
      .protocolVersion = SQLITE_BROKER_ABI_VERSION,
      .requestId = kj::str(request.requestId),
      .outcome = outcome,
      .logicalCommit = outcome == SqliteTransactionOutcome::COMMITTED
          ? kj::Maybe<uint64_t>(7)
          : kj::Maybe<uint64_t>(kj::none),
      .failedStatement = failedStatement,
      .errorCode = kj::str(errorCode),
      .results = results.releaseAsArray(),
    };
  }

  kj::String seenKey;
  size_t calls = 0;
  SqliteTransactionOutcome outcome = SqliteTransactionOutcome::COMMITTED;
  kj::Maybe<uint32_t> failedStatement = kj::none;
  kj::String errorCode;
  bool exposePartialResults = false;
  bool fail = false;
  uint64_t rowsWritten = 0;
  uint64_t rowsRead = 0;
  size_t returnedRows = 0;
  kj::String returnedValue;
};

class RecordingSqliteAudit final: public SqliteAuditSink {
 public:
  void record(SqliteAuditEvent event) override {
    events.add(kj::mv(event));
  }

  kj::Vector<SqliteAuditEvent> events;
};

class CounterBehavior final: public ActorBehavior {
 public:
  ActorInvocationResult invoke(const ActorIdentity&,
      kj::ArrayPtr<const kj::byte> event,
      kj::ArrayPtr<const kj::byte> previousState) override {
    uint8_t value = previousState.size() == 0 ? 0 : previousState[0];
    value += event.size() == 0 ? 1 : event[0];
    return {
      .response = kj::heapArray<kj::byte>({value}),
      .state = kj::heapArray<kj::byte>({value}),
      .keepActive = true,
    };
  }
};

class OversizedBehavior final: public ActorBehavior {
 public:
  ActorInvocationResult invoke(
      const ActorIdentity&, kj::ArrayPtr<const kj::byte>, kj::ArrayPtr<const kj::byte>) override {
    return {
      .response = kj::heapArray<kj::byte>({1}),
      .state = kj::heapArray<kj::byte>({1, 2}),
      .keepActive = true,
    };
  }
};

class FailingActorPersistence final: public ActorPersistenceAdapter {
 public:
  kj::Maybe<ActorPersistedState> load(const ActorIdentity&) override {
    return kj::none;
  }

  uint64_t save(const ActorIdentity&, uint64_t, kj::ArrayPtr<const kj::byte>) override {
    KJ_FAIL_REQUIRE("actor persistence failed");
  }

  void erase(const ActorIdentity&) override {}
};

class RecordingLifecycle final: public ActorLifecycleObserver {
 public:
  void record(const ActorIdentity& actor, const ActorLifecycleEvent& event) override {
    events.add(kj::str(actor.resourceId, ":g", event.routeGeneration, ":v", event.persistedVersion,
        ':', static_cast<uint32_t>(event.from), '>', static_cast<uint32_t>(event.to), ':',
        event.reason));
  }

  kj::Vector<kj::String> events;
};

ActorEvent event(kj::StringPtr actorId, uint8_t increment = 1, uint64_t routeGeneration = 1) {
  return {
    .protocolVersion = ACTOR_BROKER_ABI_VERSION,
    .requestId = kj::str("request-", actorId),
    .actor = identity(actorId),
    .routeGeneration = routeGeneration,
    .payload = kj::heapArray<kj::byte>({increment}),
  };
}

KJ_TEST("logical identities are opaque and snapshot-scoped") {
  auto first = identity("database-1", "snap-1");
  auto second = identity("database-1", "snap-2");
  KJ_EXPECT(first.canonicalKey() != second.canonicalKey());

  auto path = identity("../host.db");
  KJ_EXPECT_THROW_MESSAGE("invalid logical resource ID", path.canonicalKey());
  auto credential = identity("database@credential");
  KJ_EXPECT_THROW_MESSAGE("invalid logical resource ID", credential.canonicalKey());
}

KJ_TEST("SQLite broker enforces transaction policy before host persistence") {
  RecordingSqliteAdapter adapter;
  RecordingSqliteAudit audit;
  SqliteBroker broker(adapter, {}, {}, audit);

  auto response = broker.transact(request("SELECT value FROM records WHERE id = ?"));
  KJ_EXPECT(KJ_ASSERT_NONNULL(response.logicalCommit) == 7);
  KJ_EXPECT(adapter.seenKey == identity("database-1").canonicalKey());
  KJ_EXPECT(audit.events.size() == 1);
  KJ_EXPECT(audit.events[0].outcome == SqliteAuditOutcome::COMMITTED);
  KJ_EXPECT(KJ_ASSERT_NONNULL(audit.events[0].logicalCommit) == 7);
  KJ_EXPECT(audit.events[0].protocolVersion == SQLITE_BROKER_ABI_VERSION);
  KJ_EXPECT(audit.events[0].requestId == "request-1");
  KJ_EXPECT(audit.events[0].tenantId == "tenant-1");
  KJ_EXPECT(audit.events[0].namespaceId == "namespace-1");
  KJ_EXPECT(audit.events[0].databaseId == "database-1");
  KJ_EXPECT(audit.events[0].inputBytes > 0);
  KJ_EXPECT(audit.events[0].detail == "committed");

  KJ_EXPECT_THROW_MESSAGE("host-owned or transaction-control operation",
      broker.transact(request("ATTACH DATABASE '/host/db' AS escaped")));
  KJ_EXPECT_THROW_MESSAGE("read-only transaction contains a write",
      broker.transact(request("INSERT INTO records VALUES (1)")));
  KJ_EXPECT_THROW_MESSAGE("exactly one statement", broker.transact(request("SELECT 1; SELECT 2")));
  KJ_EXPECT_THROW_MESSAGE("host-owned or transaction-control operation",
      broker.transact(request("PRAGMA database_list")));
  KJ_EXPECT_THROW_MESSAGE(
      "host-owned or transaction-control operation", broker.transact(request("BEGIN IMMEDIATE")));
  KJ_EXPECT_THROW_MESSAGE(
      "comments are not accepted", broker.transact(request("SELECT 1 /* bypass */")));
  KJ_EXPECT_THROW_MESSAGE("operation is not allowlisted",
      broker.transact(request("WITH hidden AS (SELECT 1) SELECT * FROM hidden")));

  auto badVersion = request("SELECT 1");
  badVersion.protocolVersion = 99;
  KJ_EXPECT_THROW_MESSAGE("unsupported SQLite broker ABI", broker.transact(badVersion));
  auto missingRequestId = request("SELECT 1");
  missingRequestId.requestId = kj::str("");
  KJ_EXPECT_THROW_MESSAGE("invalid SQLite broker request ID", broker.transact(missingRequestId));
  KJ_EXPECT(adapter.calls == 1);
  KJ_EXPECT(audit.events.size() == 10);
  KJ_EXPECT(audit.events[1].outcome == SqliteAuditOutcome::DENIED);
  KJ_EXPECT(audit.events[1].statementCount == 1);
}

KJ_TEST("SQLite broker models atomic rollback without partial results") {
  RecordingSqliteAdapter adapter;
  RecordingSqliteAudit audit;
  adapter.outcome = SqliteTransactionOutcome::ROLLED_BACK;
  adapter.failedStatement = uint32_t{1};
  adapter.errorCode = kj::str("constraint");
  SqliteBroker broker(adapter, {}, {}, audit);

  auto response =
      broker.transact(request("INSERT INTO records VALUES (1)", "INSERT INTO records VALUES (1)"));
  KJ_EXPECT(response.outcome == SqliteTransactionOutcome::ROLLED_BACK);
  KJ_EXPECT(response.logicalCommit == kj::none);
  KJ_EXPECT(KJ_ASSERT_NONNULL(response.failedStatement) == 1);
  KJ_EXPECT(response.results.size() == 0);
  KJ_EXPECT(audit.events.size() == 1);
  KJ_EXPECT(audit.events[0].outcome == SqliteAuditOutcome::ROLLED_BACK);
  KJ_EXPECT(audit.events[0].detail == "constraint");

  adapter.exposePartialResults = true;
  KJ_EXPECT_THROW_MESSAGE("exposed partial statement results",
      broker.transact(request("INSERT INTO records VALUES (2)", "INSERT INTO records VALUES (2)")));
  KJ_EXPECT(audit.events.size() == 2);
  KJ_EXPECT(audit.events[1].outcome == SqliteAuditOutcome::FAILED);

  adapter.exposePartialResults = false;
  adapter.fail = true;
  KJ_EXPECT_THROW_MESSAGE("host persistence failed",
      broker.transact(request("INSERT INTO records VALUES (3)", "INSERT INTO records VALUES (3)")));
  KJ_EXPECT(audit.events.size() == 3);
  KJ_EXPECT(audit.events[2].outcome == SqliteAuditOutcome::FAILED);
}

KJ_TEST("SQLite broker applies statement and result quotas deterministically") {
  RecordingSqliteAdapter adapter;
  SqliteBrokerQuota quota;
  quota.maxSqlBytes = 8;
  SqliteBroker broker(adapter, {}, quota);
  KJ_EXPECT_THROW_MESSAGE(
      "SQL byte quota exceeded", broker.transact(request("SELECT value FROM records")));

  quota.maxSqlBytes = 1024;
  quota.maxRowsWritten = 1;
  adapter.rowsWritten = 2;
  SqliteBroker writeBroker(adapter, {}, quota);
  KJ_EXPECT_THROW_MESSAGE("write quota exceeded",
      writeBroker.transact(request("DELETE FROM records", SqliteTransactionMode::READ_WRITE)));

  auto parameterRequest = request("SELECT value FROM records WHERE id = ?");
  kj::Vector<SqliteValue> parameters;
  parameters.add(SqliteValue(kj::str("too-large")));
  parameterRequest.statements[0].parameters = parameters.releaseAsArray();
  quota.maxRowsWritten = 10;
  quota.maxParameterBytes = 4;
  SqliteBroker parameterBroker(adapter, {}, quota);
  KJ_EXPECT_THROW_MESSAGE(
      "parameter byte quota exceeded", parameterBroker.transact(parameterRequest));

  adapter.rowsWritten = 0;
  adapter.rowsRead = 2;
  quota.maxRowsRead = 1;
  SqliteBroker rowsReadBroker(adapter, {}, quota);
  KJ_EXPECT_THROW_MESSAGE(
      "rows-read quota exceeded", rowsReadBroker.transact(request("SELECT value FROM records")));

  adapter.rowsRead = 0;
  adapter.returnedRows = 2;
  quota.maxParameterBytes = 1024;
  quota.maxRows = 1;
  SqliteBroker rowBroker(adapter, {}, quota);
  KJ_EXPECT_THROW_MESSAGE(
      "result row quota exceeded", rowBroker.transact(request("SELECT value FROM records")));

  adapter.returnedRows = 1;
  adapter.returnedValue = kj::str("oversized");
  quota.maxRows = 10;
  quota.maxResultBytes = 8;
  SqliteBroker resultBroker(adapter, {}, quota);
  KJ_EXPECT_THROW_MESSAGE(
      "result byte quota exceeded", resultBroker.transact(request("SELECT value FROM records")));
}

KJ_TEST("bounded actor router preserves state and reloads after deterministic eviction") {
  CounterBehavior behavior;
  RecordingLifecycle lifecycle;
  InMemoryActorPersistence persistence(4, 1024);
  ActorRouterLimits limits;
  limits.maxActiveActors = 1;
  BoundedInMemoryActorRouter router(behavior, persistence, lifecycle, limits);

  auto first = router.route(event("actor-a", 2));
  auto second = router.route(event("actor-a", 3));
  KJ_EXPECT(first.response.size() == 1 && first.response[0] == 2);
  KJ_EXPECT(first.persistedVersion == 1);
  KJ_EXPECT(second.response.size() == 1 && second.response[0] == 5);
  KJ_EXPECT(second.persistedVersion == 2);

  auto other = router.route(event("actor-b", 4));
  KJ_EXPECT(other.response[0] == 4);
  KJ_EXPECT(router.activeActorCount() == 1);

  auto restored = router.route(event("actor-a", 1));
  KJ_EXPECT(restored.response[0] == 6);
  KJ_EXPECT(restored.persistedVersion == 3);
  KJ_EXPECT(persistence.size() == 2);
  KJ_EXPECT(lifecycle.events.size() == 16);
  KJ_EXPECT(lifecycle.events[0] == "actor-a:g1:v0:0>1:request-actor-a");
  KJ_EXPECT(lifecycle.events[2] == "actor-a:g1:v0:2>3:request-actor-a");
  KJ_EXPECT(lifecycle.events[3] == "actor-a:g1:v1:3>2:request-actor-a");
  KJ_EXPECT(lifecycle.events[6] == "actor-a:g1:v2:2>4:capacity");
}

KJ_TEST("actor route generations survive eviction and reject stale dispatch") {
  CounterBehavior behavior;
  RecordingLifecycle lifecycle;
  InMemoryActorPersistence persistence(2, 1024);
  BoundedInMemoryActorRouter router(behavior, persistence, lifecycle);

  auto initial = router.route(event("actor-a", 2, 1));
  auto upgraded = router.route(event("actor-a", 3, 2));
  KJ_EXPECT(initial.routeGeneration == 1);
  KJ_EXPECT(upgraded.routeGeneration == 2);
  KJ_EXPECT(upgraded.response[0] == 5);
  auto persisted = KJ_ASSERT_NONNULL(persistence.load(identity("actor-a")));
  KJ_EXPECT(persisted.routeGeneration == 2);
  KJ_EXPECT(persisted.version == 2);
  KJ_EXPECT(persisted.state.size() == 1 && persisted.state[0] == 5);
  KJ_EXPECT_THROW_MESSAGE("stale route generation", router.route(event("actor-a", 1, 1)));
  KJ_EXPECT_THROW_MESSAGE("stale route generation",
      persistence.save(identity("actor-a"), 1, kj::heapArray<kj::byte>({9})));

  KJ_EXPECT(router.evict(identity("actor-a"), "test"));
  KJ_EXPECT_THROW_MESSAGE("stale persisted route generation", router.route(event("actor-a", 1, 1)));
  auto restored = router.route(event("actor-a", 1, 3));
  KJ_EXPECT(restored.response[0] == 6);
  KJ_EXPECT(restored.routeGeneration == 3);

  KJ_EXPECT_THROW_MESSAGE("generation must be non-zero", router.route(event("actor-zero", 1, 0)));
}

KJ_TEST("actor router rejects oversized event and state") {
  CounterBehavior behavior;
  RecordingLifecycle lifecycle;
  InMemoryActorPersistence persistence(2, 1024);
  ActorRouterLimits limits;
  limits.maxEventBytes = 0;
  limits.maxStateBytesPerActor = 0;
  BoundedInMemoryActorRouter router(behavior, persistence, lifecycle, limits);

  KJ_EXPECT_THROW_MESSAGE("event byte quota exceeded", router.route(event("actor-a")));

  OversizedBehavior oversized;
  limits.maxEventBytes = 1;
  limits.maxStateBytesPerActor = 1;
  BoundedInMemoryActorRouter stateRouter(oversized, persistence, lifecycle, limits);
  KJ_EXPECT_THROW_MESSAGE("state byte quota exceeded", stateRouter.route(event("actor-b")));
}

KJ_TEST("actor lifecycle records persistence failure without mutating state") {
  CounterBehavior behavior;
  RecordingLifecycle lifecycle;
  FailingActorPersistence persistence;
  BoundedInMemoryActorRouter router(behavior, persistence, lifecycle);

  KJ_EXPECT_THROW_MESSAGE("actor persistence failed", router.route(event("actor-a")));
  KJ_EXPECT(router.activeActorCount() == 1);
  KJ_EXPECT(lifecycle.events.size() == 4);
  KJ_EXPECT(lifecycle.events[2] == "actor-a:g1:v0:2>3:request-actor-a");
  KJ_EXPECT(lifecycle.events[3] == "actor-a:g1:v0:3>2:persistence-failed");
}

}  // namespace
}  // namespace workerd::server::sandbox_executor
