// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-logical-service.h"

#include <workerd/server/logical-service-broker/logical-service-broker.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/encoding.h>

#include <cmath>

namespace workerd::server::sandbox_executor {
namespace {

namespace composite = logical_service_broker::composite;
using Json = capnp::JsonValue::Reader;
constexpr size_t MAX_ENVELOPE_BYTES = logical_service_broker::MAX_ENVELOPE_BYTES;
constexpr double MAX_SAFE_INTEGER = 9'007'199'254'740'991.0;

Json field(Json object, kj::StringPtr name) {
  KJ_REQUIRE(object.isObject(), "D1 transport value must be an object");
  kj::Maybe<Json> value;
  for (auto entry: object.getObject()) {
    if (entry.getName() == name) {
      KJ_REQUIRE(value == kj::none, "duplicate D1 transport field", name);
      value = entry.getValue();
    }
  }
  return KJ_REQUIRE_NONNULL(value, "missing D1 transport field", name);
}

struct Object {
  capnp::List<capnp::JsonValue::Field>::Builder fields;
  size_t next = 0;

  Object(capnp::JsonValue::Builder value, size_t count): fields(value.initObject(count)) {}
  capnp::JsonValue::Builder add(kj::StringPtr name) {
    KJ_REQUIRE(next < fields.size(), "D1 transport object overflow");
    fields[next].setName(name);
    return fields[next++].getValue();
  }
};

void encodeParameter(capnp::JsonValue::Builder output, Json value) {
  if (value.isNull()) {
    Object parameter(output, 1);
    parameter.add("type").setString("null");
  } else if (value.isString()) {
    Object parameter(output, 2);
    parameter.add("type").setString("text");
    parameter.add("value").setString(value.getString());
  } else if (value.isNumber()) {
    auto number = value.getNumber();
    KJ_REQUIRE(std::isfinite(number), "D1 parameter must be finite");
    bool integer = std::floor(number) == number && std::abs(number) <= MAX_SAFE_INTEGER;
    Object parameter(output, 2);
    parameter.add("type").setString(integer ? "integer" : "real");
    parameter.add("value").setNumber(number);
  } else if (value.isArray()) {
    auto values = value.getArray();
    auto bytes = kj::heapArray<byte>(values.size());
    for (auto i: kj::indices(values)) {
      KJ_REQUIRE(values[i].isNumber(), "D1 blob must contain bytes");
      auto number = values[i].getNumber();
      KJ_REQUIRE(number >= 0 && number <= 255 && std::floor(number) == number,
          "D1 blob must contain bytes");
      bytes[i] = static_cast<byte>(number);
    }
    Object parameter(output, 2);
    parameter.add("type").setString("blob");
    parameter.add("value").setString(kj::encodeBase64(bytes));
  } else {
    KJ_FAIL_REQUIRE("unsupported D1 parameter type");
  }
}

void decodeCell(capnp::JsonValue::Builder output, Json cell) {
  auto type = field(cell, "type");
  KJ_REQUIRE(type.isString(), "invalid D1 cell type");
  if (type.getString() == "null"_kj) {
    KJ_REQUIRE(cell.getObject().size() == 1, "invalid D1 null cell");
    output.setNull();
    return;
  }
  KJ_REQUIRE(cell.getObject().size() == 2, "invalid D1 result cell");
  auto value = field(cell, "value");
  if (type.getString() == "text"_kj) {
    KJ_REQUIRE(value.isString(), "invalid D1 text cell");
    output.setString(value.getString());
  } else if (type.getString() == "integer"_kj || type.getString() == "real"_kj ||
      type.getString() == "number"_kj) {
    KJ_REQUIRE(value.isNumber() && std::isfinite(value.getNumber()), "invalid D1 numeric cell");
    if (type.getString() != "real"_kj && std::floor(value.getNumber()) == value.getNumber()) {
      KJ_REQUIRE(std::abs(value.getNumber()) <= MAX_SAFE_INTEGER,
          "D1 integer result exceeds JavaScript safe-integer range");
    }
    KJ_REQUIRE(
        type.getString() != "integer"_kj || std::floor(value.getNumber()) == value.getNumber(),
        "invalid D1 integer cell");
    output.setNumber(value.getNumber());
  } else if (type.getString() == "blob"_kj) {
    KJ_REQUIRE(value.isString(), "invalid D1 blob cell");
    auto bytes = kj::decodeBase64(value.getString());
    KJ_REQUIRE(!bytes.hadErrors && kj::encodeBase64(bytes) == value.getString(),
        "invalid D1 blob encoding");
    auto array = output.initArray(bytes.size());
    for (auto i: kj::indices(bytes)) array[i].setNumber(bytes[i]);
  } else {
    KJ_FAIL_REQUIRE("unknown D1 result cell type");
  }
}

void validateMeta(Json meta) {
  KJ_REQUIRE(meta.isObject(), "D1 result metadata must be an object");
  auto duration = field(meta, "duration");
  KJ_REQUIRE(
      duration.isNumber() && std::isfinite(duration.getNumber()) && duration.getNumber() >= 0,
      "D1 result requires a measured nonnegative duration");
  for (auto entry: meta.getObject()) {
    auto name = entry.getName();
    auto value = field(meta, name);
    if (name == "duration"_kj) continue;
    if (name == "changes"_kj || name == "last_row_id"_kj || name == "rows_read"_kj ||
        name == "rows_written"_kj || name == "size_after"_kj) {
      KJ_REQUIRE(value.isNumber() && std::isfinite(value.getNumber()) &&
              std::floor(value.getNumber()) == value.getNumber() &&
              std::abs(value.getNumber()) <= MAX_SAFE_INTEGER &&
              (name == "last_row_id"_kj || value.getNumber() >= 0),
          "D1 metadata integer exceeds JavaScript safe-integer range");
    } else if (name == "changed_db"_kj) {
      KJ_REQUIRE(value.isBoolean(), "invalid D1 metadata boolean");
    } else {
      KJ_FAIL_REQUIRE("unknown D1 metadata field", name);
    }
  }
}

class D1ServiceWorker final: public WorkerInterface {
 public:
  D1ServiceWorker(kj::Rc<LogicalServiceHostChannel> host,
      kj::HttpHeaderTable& headerTable,
      kj::String binding,
      kj::String requestId)
      : host(kj::mv(host)),
        headerTable(headerTable),
        binding(kj::mv(binding)),
        requestId(kj::mv(requestId)) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& response) override {
    bool session = false;
    headers.forEach([&](kj::StringPtr name, kj::StringPtr) {
      if (name == "x-cf-d1-session-commit-token"_kj) session = true;
    });
    auto queryEndpoint = url == "http://d1/query?resultsFormat=ROWS_AND_COLUMNS"_kj;
    auto executeEndpoint = url == "http://d1/execute?resultsFormat=NONE"_kj;
    if (method != kj::HttpMethod::POST || session || (!queryEndpoint && !executeEndpoint)) {
      co_await send(response, 501, "{\"error\":\"unsupported_d1_operation\"}"_kj);
      co_return;
    }

    auto body = co_await requestBody.readAllText(MAX_ENVELOPE_BYTES);
    capnp::JsonCodec codec;
    capnp::MallocMessageBuilder inputArena;
    auto input = inputArena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(body, input);
    bool batch = input.isArray();
    size_t count = batch ? input.getArray().size() : 1;
    KJ_REQUIRE(count > 0 && count <= 16, "SQL statement batch exceeds limit");
    capnp::MallocMessageBuilder requestArena;
    auto root = requestArena.initRoot<capnp::JsonValue>();
    Object envelope(root, 4);
    envelope.add("version").setNumber(2);
    envelope.add("request_id").setString(requestId);
    envelope.add("binding").setString(binding);
    Object operation(envelope.add("operation"), 2);
    operation.add("kind").setString("d1_batch");
    auto statements = operation.add("statements").initArray(count);
    for (size_t i = 0; i < count; ++i) {
      Json statement = batch ? input.getArray()[i] : input.asReader();
      auto sql = field(statement, "sql");
      KJ_REQUIRE(sql.isString(), "D1 statement SQL must be a string");
      KJ_REQUIRE(statement.getObject().size() == 1 || statement.getObject().size() == 2,
          "invalid D1 statement fields");
      kj::Maybe<Json> parameters;
      if (statement.getObject().size() == 2) parameters = field(statement, "params");
      Object output(statements[i], 2);
      output.add("sql").setString(sql.getString());
      auto parameterCount = parameters
                                .map([](Json p) {
        KJ_REQUIRE(p.isArray(), "D1 statement parameters must be an array");
        return p.getArray().size();
      }).orDefault(0);
      auto encodedParameters = output.add("parameters").initArray(parameterCount);
      KJ_IF_SOME(values, parameters) {
        for (auto j: kj::indices(values.getArray())) {
          encodeParameter(encodedParameters[j], values.getArray()[j]);
        }
      }
    }

    auto query = codec.encodeRaw(root);
    KJ_REQUIRE(query.size() <= MAX_ENVELOPE_BYTES, "D1 encoded request exceeds limit");
    auto encodedResponse = host->invoke(query);
    KJ_REQUIRE(encodedResponse.size() <= MAX_ENVELOPE_BYTES, "D1 response exceeds limit");
    auto parsed = composite::parseResponseEnvelope(encodedResponse);
    KJ_REQUIRE(parsed.requestId == requestId, "D1 response request ID mismatch");
    capnp::MallocMessageBuilder hostArena;
    auto hostRoot = hostArena.initRoot<capnp::JsonValue>();
    codec.decodeRaw(encodedResponse, hostRoot);
    capnp::MallocMessageBuilder resultArena;
    auto result = resultArena.initRoot<capnp::JsonValue>();
    if (parsed.status != composite::ResponseStatus::OK) {
      KJ_REQUIRE(hostRoot.getObject().size() == 4, "SQL error response must not contain results");
      Object failure(result, 1);
      failure.add("error").setString(parsed.code);
    } else {
      KJ_REQUIRE(hostRoot.getObject().size() == 5, "invalid SQL response fields");
      auto results = field(hostRoot, "results");
      KJ_REQUIRE(results.isArray() && results.getArray().size() == count,
          "D1 result count does not match its atomic batch");
      auto output = result.initArray(count);
      for (size_t i = 0; i < count; ++i) {
        auto hostResult = results.getArray()[i];
        auto columns = field(hostResult, "columns");
        auto rows = field(hostResult, "rows");
        auto meta = field(hostResult, "meta");
        auto affected = field(hostResult, "rows_affected");
        KJ_REQUIRE(hostResult.getObject().size() == 4 && columns.isArray() && rows.isArray() &&
                meta.isObject() && affected.isNumber() && affected.getNumber() >= 0 &&
                affected.getNumber() <= MAX_SAFE_INTEGER &&
                std::floor(affected.getNumber()) == affected.getNumber(),
            "invalid D1 result shape");
        validateMeta(meta);
        Object publicResult(output[i], 3);
        publicResult.add("success").setBoolean(true);
        publicResult.fields[publicResult.next].setName("meta");
        publicResult.fields[publicResult.next++].setValue(meta);
        if (executeEndpoint) {
          auto publicRows = publicResult.add("results").initArray(rows.getArray().size());
          for (auto j: kj::indices(rows.getArray())) {
            auto row = rows.getArray()[j];
            KJ_REQUIRE(row.isArray() && row.getArray().size() == columns.getArray().size(),
                "D1 result row width mismatch");
            Object publicRow(publicRows[j], columns.getArray().size());
            for (auto k: kj::indices(row.getArray())) {
              KJ_REQUIRE(columns.getArray()[k].isString(), "invalid D1 result column");
              decodeCell(publicRow.add(columns.getArray()[k].getString()), row.getArray()[k]);
            }
          }
          continue;
        }
        Object data(publicResult.add("results"), 2);
        auto publicColumns = data.add("columns").initArray(columns.getArray().size());
        for (auto j: kj::indices(columns.getArray())) {
          KJ_REQUIRE(columns.getArray()[j].isString(), "invalid D1 result column");
          publicColumns[j].setString(columns.getArray()[j].getString());
        }
        auto publicRows = data.add("rows").initArray(rows.getArray().size());
        for (auto j: kj::indices(rows.getArray())) {
          auto row = rows.getArray()[j];
          KJ_REQUIRE(row.isArray() && row.getArray().size() == columns.getArray().size(),
              "D1 result row width mismatch");
          auto publicRow = publicRows[j].initArray(row.getArray().size());
          for (auto k: kj::indices(row.getArray())) decodeCell(publicRow[k], row.getArray()[k]);
        }
      }
    }
    Json publicValue = result.asReader();
    if (!batch && result.isArray()) publicValue = result.getArray()[0];
    auto reply = codec.encodeRaw(publicValue);
    KJ_REQUIRE(reply.size() <= MAX_ENVELOPE_BYTES, "D1 public response exceeds limit");
    co_await send(response, 200, reply);
  }

  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncIoStream&,
      ConnectResponse&,
      kj::HttpConnectSettings) override {
    KJ_FAIL_REQUIRE("D1 bindings do not support connect");
  }
  kj::Promise<void> prewarm(kj::StringPtr) override {
    return kj::READY_NOW;
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date, kj::StringPtr) override {
    KJ_FAIL_REQUIRE("D1 bindings do not support scheduled events");
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date, uint32_t) override {
    KJ_FAIL_REQUIRE("D1 bindings do not support alarms");
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return event->notSupported();
  }

 private:
  kj::Promise<void> send(kj::HttpService::Response& response, uint status, kj::StringPtr body) {
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto output =
        response.send(status, status == 200 ? "OK" : "Not Implemented", headers, body.size());
    co_await output->write(body.asBytes()).attach(kj::mv(output));
  }

  kj::Rc<LogicalServiceHostChannel> host;
  kj::HttpHeaderTable& headerTable;
  kj::String binding;
  kj::String requestId;
};

class D1ServiceChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  D1ServiceChannel(kj::Rc<LogicalServiceHostChannel> host,
      kj::HttpHeaderTable& headerTable,
      kj::String binding,
      kj::Rc<uint64_t> nextRequestId)
      : host(kj::mv(host)),
        headerTable(headerTable),
        binding(kj::mv(binding)),
        nextRequestId(kj::mv(nextRequestId)) {}

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata) override {
    KJ_REQUIRE(*nextRequestId != kj::maxValue, "D1 request ID space exhausted");
    return kj::heap<D1ServiceWorker>(
        host.addRef(), headerTable, kj::str(binding), kj::str("d1-", (*nextRequestId)++));
  }
  void requireAllowsTransfer() override {
    KJ_FAIL_REQUIRE("D1 bindings cannot be transferred");
  }
  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    KJ_FAIL_REQUIRE("D1 bindings cannot be tokenized");
  }

 private:
  kj::Rc<LogicalServiceHostChannel> host;
  kj::HttpHeaderTable& headerTable;
  kj::String binding;
  kj::Rc<uint64_t> nextRequestId;
};

}  // namespace

kj::Own<IoChannelFactory::SubrequestChannel> newD1ServiceChannel(
    kj::Rc<LogicalServiceHostChannel> host,
    kj::HttpHeaderTable& headerTable,
    kj::String binding,
    kj::Rc<uint64_t> nextRequestId) {
  KJ_REQUIRE(composite::isBindingName(binding), "invalid D1 binding name");
  return kj::refcounted<D1ServiceChannel>(
      kj::mv(host), headerTable, kj::mv(binding), kj::mv(nextRequestId));
}

}  // namespace workerd::server::sandbox_executor
