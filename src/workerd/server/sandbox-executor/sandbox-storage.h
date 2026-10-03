// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include "sandbox-state.h"

#include <kj/array.h>
#include <kj/common.h>
#include <kj/one-of.h>
#include <kj/string.h>

namespace workerd::server::sandbox_executor {

inline constexpr uint32_t SQLITE_BROKER_ABI_VERSION = 1;
inline constexpr auto SQLITE_TRANSACTION_HOST_CALL = "WorkerdSqliteV1Transaction";

struct SqliteNull {};
using SqliteValue = kj::OneOf<SqliteNull, int64_t, double, kj::String, kj::Array<kj::byte>>;

struct SqliteStatement {
  kj::String sql;
  kj::Array<SqliteValue> parameters;
};

enum class SqliteTransactionMode {
  READ_ONLY,
  READ_WRITE,
};

enum class SqliteTransactionOutcome {
  COMMITTED,
  ROLLED_BACK,
};

struct SqliteTransactionRequest {
  uint32_t protocolVersion;
  kj::String requestId;
  LogicalResourceIdentity database;
  SqliteTransactionMode mode;
  kj::Array<SqliteStatement> statements;
};

struct SqliteColumn {
  kj::String name;
  SqliteValue value;
};

struct SqliteRow {
  kj::Array<SqliteColumn> columns;
};

struct SqliteStatementResult {
  uint64_t rowsRead;
  uint64_t rowsWritten;
  kj::Maybe<int64_t> lastInsertRowId;
  kj::Array<SqliteRow> rows;
};

struct SqliteTransactionResponse {
  uint32_t protocolVersion;
  kj::String requestId;
  SqliteTransactionOutcome outcome;
  kj::Maybe<uint64_t> logicalCommit;
  kj::Maybe<uint32_t> failedStatement;
  kj::String errorCode;
  kj::Array<SqliteStatementResult> results;
};

enum class SqliteAuditOutcome {
  COMMITTED,
  ROLLED_BACK,
  DENIED,
  FAILED,
};

struct SqliteAuditEvent {
  uint32_t protocolVersion;
  kj::String requestId;
  kj::String tenantId;
  kj::String namespaceId;
  kj::String databaseId;
  SqliteTransactionMode mode;
  SqliteAuditOutcome outcome;
  size_t statementCount;
  size_t inputBytes;
  kj::Maybe<uint64_t> logicalCommit;
  kj::Maybe<uint32_t> failedStatement;
  kj::String detail;
};

struct SqliteBrokerPolicy {
  bool allowRead = true;
  bool allowWrite = true;
  bool allowSchemaChanges = false;
};

struct SqliteBrokerQuota {
  size_t maxStatements = 32;
  size_t maxSqlBytes = 64 * 1024;
  size_t maxParameters = 512;
  size_t maxParameterBytes = 1024 * 1024;
  size_t maxRows = 10'000;
  uint64_t maxRowsRead = 10'000;
  size_t maxResultBytes = 4 * 1024 * 1024;
  uint64_t maxRowsWritten = 10'000;
};

class SqlitePersistenceAdapter {
 public:
  virtual ~SqlitePersistenceAdapter() noexcept(false) {}

  // The adapter owns the physical database and transaction boundary. It must either commit every
  // statement and return COMMITTED, or roll back every statement and return ROLLED_BACK without
  // partial results.
  virtual SqliteTransactionResponse transact(const SqliteTransactionRequest& request) = 0;
};

class SqliteAuditSink {
 public:
  virtual ~SqliteAuditSink() noexcept(false) {}

  virtual void record(SqliteAuditEvent event) = 0;
};

class SqliteBroker {
 public:
  SqliteBroker(SqlitePersistenceAdapter& persistence,
      SqliteBrokerPolicy policy = {},
      SqliteBrokerQuota quota = {},
      kj::Maybe<SqliteAuditSink&> audit = kj::none);

  SqliteTransactionResponse transact(const SqliteTransactionRequest& request);

 private:
  SqlitePersistenceAdapter& persistence;
  SqliteBrokerPolicy policy;
  SqliteBrokerQuota quota;
  kj::Maybe<SqliteAuditSink&> audit;
};

}  // namespace workerd::server::sandbox_executor
