// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include "sandbox-fetch.h"

#include <workerd/io/worker.h>
#include <workerd/server/logical-service-broker/composite-data-router.h>

#include <kj/async.h>
#include <kj/compat/http.h>

namespace workerd::server::sandbox_executor {

class LogicalServiceHostChannel;

class TimerHostChannel: public kj::Refcounted {
 public:
  virtual ~TimerHostChannel() noexcept(false) {}
  virtual kj::String start(uint64_t delayNs) = 0;
  virtual kj::String read(uint64_t timerId) = 0;
  virtual int32_t cancel(uint64_t timerId) = 0;
};

kj::Rc<TimerHostChannel> newHyperlightTimerHostChannel();
kj::Own<TimerChannel> newTimerChannel(kj::Rc<TimerHostChannel> host, kj::Timer& pollTimer);

struct Response {
  uint statusCode;
  kj::Array<Header> headers;
  kj::String body;
};

struct ScheduledResponse {
  bool retry;
  EventOutcome outcome;
};

struct QueueMessage {
  kj::String id;
  kj::Date timestamp;
  kj::Array<kj::byte> body;
  kj::Maybe<kj::String> contentType;
  uint16_t attempts;
};

struct QueueRequest {
  kj::String queueName;
  kj::Array<QueueMessage> messages;
  double backlogCount;
  double backlogBytes;
  kj::Maybe<kj::Date> oldestMessageTimestamp;
};

struct QueueRetry {
  kj::String messageId;
  kj::Maybe<int> delaySeconds;
};

struct QueueResponse {
  EventOutcome outcome;
  bool ackAll;
  bool retryBatch;
  kj::Maybe<int> retryBatchDelaySeconds;
  kj::Array<kj::String> explicitAcks;
  kj::Array<QueueRetry> retryMessages;
};

enum class ModuleType {
  ES_MODULE,
  COMMON_JS_MODULE,
  TEXT,
  JSON,
  WASM,
};

struct Module {
  kj::String name;
  ModuleType type;
  kj::Array<byte> source;
};

enum class StorageMode {
  READ_ONLY,
  READ_WRITE,
};

struct StorageMount {
  kj::String name;
  StorageMode mode;
};

struct Binding {
  kj::String name;
  logical_service_broker::composite::BindingKind kind;
};

struct WorkerBundle {
  kj::String workerVersion;
  kj::String compatibilityDate;
  kj::Array<kj::String> compatibilityFlags;
  kj::String mainModule;
  kj::Array<Module> modules;
  uint protocolVersion = 1;
  kj::Array<StorageMount> storageMounts;
  kj::Array<Binding> bindings;
};

class SandboxRuntime {
 public:
  SandboxRuntime(const WorkerBundle& bundle, kj::Rc<FetchBroker> fetchBroker);
  SandboxRuntime(const WorkerBundle& bundle,
      kj::Rc<FetchBroker> fetchBroker,
      kj::Rc<LogicalServiceHostChannel> logicalServiceHost);
  ~SandboxRuntime() noexcept(false);

  Response runRequest(kj::HttpMethod method,
      kj::StringPtr url,
      kj::ArrayPtr<const Header> headers,
      kj::StringPtr body);

  ScheduledResponse runScheduled(kj::Date scheduledTime, kj::StringPtr cron);

  QueueResponse runQueue(QueueRequest request);

 private:
  struct Impl;
  kj::Own<Impl> impl;
};

}  // namespace workerd::server::sandbox_executor
