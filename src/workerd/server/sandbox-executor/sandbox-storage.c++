// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-storage.h"

#include <ctype.h>
#include <string.h>

#include <kj/debug.h>

namespace workerd::server::sandbox_executor {
namespace {

bool isTokenCharacter(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

bool tokenEquals(kj::StringPtr sql, size_t start, size_t end, kj::StringPtr expected) {
  if (end - start != expected.size()) return false;
  for (size_t i = 0; i < expected.size(); ++i) {
    if (toupper(static_cast<unsigned char>(sql[start + i])) != expected[i]) return false;
  }
  return true;
}

bool containsToken(kj::StringPtr sql, kj::StringPtr expected) {
  size_t tokenStart = 0;
  bool inToken = false;
  for (size_t i = 0; i <= sql.size(); ++i) {
    bool tokenCharacter = i < sql.size() && isTokenCharacter(sql[i]);
    if (tokenCharacter && !inToken) {
      tokenStart = i;
      inToken = true;
    } else if (!tokenCharacter && inToken) {
      if (tokenEquals(sql, tokenStart, i, expected)) return true;
      inToken = false;
    }
  }
  return false;
}

kj::String firstToken(kj::StringPtr sql) {
  size_t start = 0;
  while (start < sql.size() && isspace(static_cast<unsigned char>(sql[start]))) ++start;
  size_t end = start;
  while (end < sql.size() && isTokenCharacter(sql[end])) ++end;
  KJ_REQUIRE(end > start, "SQLite broker statement has no leading operation");
  auto result = kj::str(sql.slice(start, end));
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = toupper(static_cast<unsigned char>(result[i]));
  }
  return result;
}

bool containsSequence(kj::StringPtr value, kj::StringPtr sequence) {
  if (sequence.size() > value.size()) return false;
  for (size_t i = 0; i <= value.size() - sequence.size(); ++i) {
    if (value.slice(i, i + sequence.size()) == sequence) return true;
  }
  return false;
}

bool isSchemaOperation(kj::StringPtr operation) {
  return operation == "CREATE"_kj || operation == "ALTER"_kj || operation == "DROP"_kj ||
      operation == "REINDEX"_kj;
}

bool isWriteOperation(kj::StringPtr operation) {
  return operation == "INSERT"_kj || operation == "UPDATE"_kj || operation == "DELETE"_kj ||
      operation == "REPLACE"_kj || isSchemaOperation(operation);
}

bool isErrorCode(kj::StringPtr value) {
  if (value.size() == 0 || value.size() > 64) return false;
  for (char c: value) {
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-')) {
      return false;
    }
  }
  return true;
}

template <typename T>
void addWithinQuota(T& total, T amount, T limit, kj::StringPtr message) {
  KJ_REQUIRE(amount <= limit && total <= limit - amount, message);
  total += amount;
}

void validateSingleStatement(kj::StringPtr sql) {
  KJ_REQUIRE(sql.size() > 0, "SQLite broker statement is empty");
  KJ_REQUIRE(!containsSequence(sql, "--") && !containsSequence(sql, "/*"),
      "SQLite broker comments are not accepted by the lightweight policy");
  KJ_IF_SOME(position, sql.findFirst(';')) {
    for (char c: sql.slice(position + 1)) {
      KJ_REQUIRE(isspace(static_cast<unsigned char>(c)),
          "SQLite broker accepts exactly one statement per typed operation");
    }
  }
}

size_t valueBytes(const SqliteValue& value) {
  KJ_IF_SOME(text, value.tryGet<kj::String>()) {
    return text.size();
  }
  KJ_IF_SOME(blob, value.tryGet<kj::Array<kj::byte>>()) {
    return blob.size();
  }
  return sizeof(uint64_t);
}

void validateStatement(const SqliteStatement& statement,
    SqliteTransactionMode mode,
    const SqliteBrokerPolicy& policy) {
  validateSingleStatement(statement.sql);

  for (auto forbidden:
      kj::arr("ATTACH"_kj, "DETACH"_kj, "VACUUM"_kj, "PRAGMA"_kj, "LOAD_EXTENSION"_kj, "BEGIN"_kj,
          "COMMIT"_kj, "ROLLBACK"_kj, "SAVEPOINT"_kj, "RELEASE"_kj)) {
    KJ_REQUIRE(!containsToken(statement.sql, forbidden),
        "SQLite broker statement uses a host-owned or transaction-control operation", forbidden);
  }

  auto operationName = firstToken(statement.sql);
  bool write = isWriteOperation(operationName);
  KJ_REQUIRE(write || operationName == "SELECT"_kj,
      "SQLite broker statement operation is not allowlisted", operationName);
  KJ_REQUIRE(write || policy.allowRead, "SQLite broker reads are disabled by policy");
  KJ_REQUIRE(!write || policy.allowWrite, "SQLite broker writes are disabled by policy");
  KJ_REQUIRE(mode == SqliteTransactionMode::READ_WRITE || !write,
      "SQLite broker read-only transaction contains a write");
  KJ_REQUIRE(!isSchemaOperation(operationName) || policy.allowSchemaChanges,
      "SQLite broker schema changes are disabled by policy");
}

void validateRequest(const SqliteTransactionRequest& request,
    const SqliteBrokerPolicy& policy,
    const SqliteBrokerQuota& quota) {
  KJ_REQUIRE(request.protocolVersion == SQLITE_BROKER_ABI_VERSION, "unsupported SQLite broker ABI");
  KJ_REQUIRE(request.requestId.size() > 0 && request.requestId.size() <= 128,
      "invalid SQLite broker request ID");
  validateLogicalResourceIdentity(request.database);
  KJ_REQUIRE(request.statements.size() > 0 && request.statements.size() <= quota.maxStatements,
      "SQLite broker statement quota exceeded");

  size_t sqlBytes = 0;
  size_t parameterCount = 0;
  size_t parameterBytes = 0;
  for (const auto& statement: request.statements) {
    validateStatement(statement, request.mode, policy);
    addWithinQuota(
        sqlBytes, statement.sql.size(), quota.maxSqlBytes, "SQLite broker SQL byte quota exceeded");
    addWithinQuota(parameterCount, statement.parameters.size(), quota.maxParameters,
        "SQLite broker parameter quota exceeded");
    for (const auto& parameter: statement.parameters) {
      addWithinQuota(parameterBytes, valueBytes(parameter), quota.maxParameterBytes,
          "SQLite broker parameter byte quota exceeded");
    }
  }
}

void validateResponse(const SqliteTransactionRequest& request,
    const SqliteTransactionResponse& response,
    const SqliteBrokerQuota& quota) {
  KJ_REQUIRE(response.protocolVersion == SQLITE_BROKER_ABI_VERSION,
      "persistence adapter returned an unsupported SQLite broker ABI");
  KJ_REQUIRE(response.requestId == request.requestId,
      "persistence adapter returned a mismatched SQLite broker request ID");
  if (response.outcome == SqliteTransactionOutcome::COMMITTED) {
    auto logicalCommit = KJ_REQUIRE_NONNULL(
        response.logicalCommit, "committed SQLite transaction is missing its logical commit");
    KJ_REQUIRE(logicalCommit > 0, "committed SQLite transaction has an invalid logical commit");
    KJ_REQUIRE(response.failedStatement == kj::none && response.errorCode.size() == 0,
        "committed SQLite transaction contains rollback metadata");
    KJ_REQUIRE(response.results.size() == request.statements.size(),
        "persistence adapter returned a mismatched SQLite result count");
  } else {
    KJ_REQUIRE(response.logicalCommit == kj::none,
        "rolled-back SQLite transaction contains a logical commit");
    auto failedStatement = KJ_REQUIRE_NONNULL(
        response.failedStatement, "rolled-back SQLite transaction is missing a failure index");
    KJ_REQUIRE(failedStatement < request.statements.size(),
        "rolled-back SQLite transaction has an invalid failure index");
    KJ_REQUIRE(isErrorCode(response.errorCode),
        "rolled-back SQLite transaction has an invalid error code");
    KJ_REQUIRE(response.results.size() == 0,
        "rolled-back SQLite transaction exposed partial statement results");
  }

  size_t rowCount = 0;
  size_t resultBytes = 0;
  uint64_t rowsRead = 0;
  uint64_t rowsWritten = 0;
  for (const auto& result: response.results) {
    addWithinQuota(
        rowCount, result.rows.size(), quota.maxRows, "SQLite broker result row quota exceeded");
    addWithinQuota(rowsWritten, result.rowsWritten, quota.maxRowsWritten,
        "SQLite broker write quota exceeded");
    addWithinQuota(
        rowsRead, result.rowsRead, quota.maxRowsRead, "SQLite broker rows-read quota exceeded");
    for (const auto& row: result.rows) {
      for (const auto& column: row.columns) {
        addWithinQuota(resultBytes, column.name.size(), quota.maxResultBytes,
            "SQLite broker result byte quota exceeded");
        addWithinQuota(resultBytes, valueBytes(column.value), quota.maxResultBytes,
            "SQLite broker result byte quota exceeded");
      }
    }
  }
}

size_t requestBytes(const SqliteTransactionRequest& request) {
  size_t result = 0;
  const size_t limit = kj::maxValue;
  auto add = [&result](
                 size_t amount) { result = amount > limit - result ? limit : result + amount; };
  add(request.requestId.size());
  add(request.database.tenantId.size());
  add(request.database.namespaceId.size());
  add(request.database.resourceId.size());
  add(request.database.snapshot.workerVersion.size());
  add(request.database.snapshot.snapshotId.size());
  for (const auto& statement: request.statements) {
    add(statement.sql.size());
    for (const auto& parameter: statement.parameters) {
      add(valueBytes(parameter));
    }
  }
  return result;
}

SqliteAuditEvent makeAuditEvent(const SqliteTransactionRequest& request,
    SqliteAuditOutcome outcome,
    kj::Maybe<uint64_t> logicalCommit,
    kj::Maybe<uint32_t> failedStatement,
    kj::String detail) {
  return {
    .protocolVersion = request.protocolVersion,
    .requestId = kj::str(request.requestId),
    .tenantId = kj::str(request.database.tenantId),
    .namespaceId = kj::str(request.database.namespaceId),
    .databaseId = kj::str(request.database.resourceId),
    .mode = request.mode,
    .outcome = outcome,
    .statementCount = request.statements.size(),
    .inputBytes = requestBytes(request),
    .logicalCommit = logicalCommit,
    .failedStatement = failedStatement,
    .detail = kj::mv(detail),
  };
}

}  // namespace

SqliteBroker::SqliteBroker(SqlitePersistenceAdapter& persistence,
    SqliteBrokerPolicy policy,
    SqliteBrokerQuota quota,
    kj::Maybe<SqliteAuditSink&> audit)
    : persistence(persistence),
      policy(policy),
      quota(quota),
      audit(audit) {}

SqliteTransactionResponse SqliteBroker::transact(const SqliteTransactionRequest& request) {
  try {
    validateRequest(request, policy, quota);
  } catch (kj::Exception& exception) {
    KJ_IF_SOME(sink, audit) {
      sink.record(makeAuditEvent(request, SqliteAuditOutcome::DENIED, kj::none, kj::none,
          kj::str(exception.getDescription())));
    }
    throw kj::mv(exception);
  }

  try {
    auto response = persistence.transact(request);
    validateResponse(request, response, quota);
    KJ_IF_SOME(sink, audit) {
      auto outcome = response.outcome == SqliteTransactionOutcome::COMMITTED
          ? SqliteAuditOutcome::COMMITTED
          : SqliteAuditOutcome::ROLLED_BACK;
      auto detail = response.outcome == SqliteTransactionOutcome::COMMITTED
          ? kj::str("committed")
          : kj::str(response.errorCode);
      sink.record(makeAuditEvent(
          request, outcome, response.logicalCommit, response.failedStatement, kj::mv(detail)));
    }
    return response;
  } catch (kj::Exception& exception) {
    KJ_IF_SOME(sink, audit) {
      sink.record(makeAuditEvent(request, SqliteAuditOutcome::FAILED, kj::none, kj::none,
          kj::str(exception.getDescription())));
    }
    throw kj::mv(exception);
  }
}

}  // namespace workerd::server::sandbox_executor
