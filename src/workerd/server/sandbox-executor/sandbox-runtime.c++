// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-runtime.h"

#include "host-fs.h"
#include "hyperlight-driver.h"
#include "sandbox-fetch.h"
#include "sandbox-logical-service.h"

#include <workerd/api/global-scope.h>
#include <workerd/api/memory-cache.h>
#include <workerd/api/queue.h>
#include <workerd/io/actor-cache.h>
#include <workerd/io/compatibility-date.h>
#include <workerd/io/io-channels.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/observer.h>
#include <workerd/io/tracer.h>
#include <workerd/jsg/setup.h>
#include <workerd/server/workerd-api.h>
#include <workerd/util/autogate.h>
#include <workerd/util/stream-utils.h>

#include <capnp/compat/http-over-capnp.h>
#include <capnp/compat/json.h>
#include <kj/async-io.h>

namespace workerd::server::sandbox_executor {
namespace {

constexpr kj::StringPtr SCRIPT_ID = "sandbox-executor"_kj;
constexpr uint64_t MAX_TIMER_ID = 9'007'199'254'740'991;
constexpr kj::Duration TIMER_POLL_INTERVAL = 1 * kj::MILLISECONDS;

jsg::V8System v8System({"--single-threaded"_kj, "--max-old-space-size=64"_kj,
  "--max-semi-space-size=4"_kj, "--no-concurrent-marking"_kj, "--no-concurrent-sweeping"_kj,
  "--no-concurrent-recompilation"_kj, "--no-parallel-scavenge"_kj});

const Module* findBundleModule(const WorkerBundle& bundle, kj::StringPtr name) {
  for (const auto& module: bundle.modules) {
    if (module.name == name) return &module;
  }
  return nullptr;
}

kj::Maybe<kj::String> resolveBundleFile(const WorkerBundle& bundle, kj::StringPtr path) {
  auto candidateBuilder = kj::heapArrayBuilder<kj::String>(5);
  candidateBuilder.add(kj::str(path));
  candidateBuilder.add(kj::str(path, ".js"));
  candidateBuilder.add(kj::str(path, ".cjs"));
  candidateBuilder.add(kj::str(path, "/index.js"));
  candidateBuilder.add(kj::str(path, "/index.cjs"));
  auto candidates = candidateBuilder.finish();
  for (auto& candidate: candidates) {
    if (findBundleModule(bundle, candidate) != nullptr) return kj::mv(candidate);
  }
  return kj::none;
}

kj::Maybe<kj::String> packageMain(const WorkerBundle& bundle, kj::StringPtr packageRoot) {
  auto packageJsonName = kj::str(packageRoot, "/package.json");
  auto packageJson = findBundleModule(bundle, packageJsonName);
  if (packageJson == nullptr) return kj::none;
  KJ_REQUIRE(packageJson->type == ModuleType::TEXT || packageJson->type == ModuleType::JSON,
      "package.json must be text or JSON", packageJsonName);

  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  auto packageJsonText = kj::str(packageJson->source.asPtr().asChars());
  codec.decodeRaw(packageJsonText, root);
  KJ_REQUIRE(root.isObject(), "package.json must contain an object", packageJsonName);
  for (auto field: root.getObject()) {
    if (field.getName() == "main"_kj) {
      KJ_REQUIRE(
          field.getValue().isString(), "package.json main must be a string", packageJsonName);
      auto main = field.getValue().getString();
      KJ_REQUIRE(
          main.size() > 0 && main[0] != '/', "package.json main must be relative", packageJsonName);
      KJ_TRY {
        return kj::Path::parse(packageRoot).eval(main).toString();
      }
      KJ_CATCH(exception) {
        KJ_FAIL_REQUIRE("package.json main escapes its package", packageJsonName, main, exception);
      }
    }
  }
  return kj::none;
}

kj::String relativeModuleSpecifier(kj::StringPtr fromModule, kj::StringPtr targetModule) {
  auto from = kj::Path::parse(fromModule).parent();
  auto target = kj::Path::parse(targetModule);
  size_t common = 0;
  while (common < from.size() && common < target.size() && from[common] == target[common]) {
    ++common;
  }

  auto result = kj::str("");
  for (size_t i = common; i < from.size(); ++i) {
    result = kj::str(result, "../");
  }
  if (result.size() == 0) result = kj::str("./");
  for (size_t i = common; i < target.size(); ++i) {
    result = kj::str(result, i == common ? ""_kj : "/"_kj, target[i]);
  }
  return result;
}

kj::Maybe<kj::String> resolveCommonJsSpecifier(
    const WorkerBundle& bundle, kj::StringPtr fromModule, kj::StringPtr specifier) {
  if (specifier.startsWith(".")) {
    kj::String candidate;
    KJ_TRY {
      candidate = kj::Path::parse(fromModule).parent().eval(specifier).toString();
    }
    KJ_CATCH(exception) {
      KJ_FAIL_REQUIRE("CommonJS require escapes the bundle", fromModule, specifier, exception);
    }
    auto resolved = KJ_REQUIRE_NONNULL(resolveBundleFile(bundle, candidate),
        "unregistered CommonJS module", fromModule, specifier);
    return relativeModuleSpecifier(fromModule, resolved);
  }

  if (specifier.startsWith("node:") || specifier.startsWith("cloudflare:") ||
      specifier.startsWith("workerd:")) {
    return kj::none;
  }

  auto slash = specifier.findFirst('/');
  size_t packageEnd;
  if (specifier.startsWith("@")) {
    KJ_IF_SOME(firstSlash, slash) {
      packageEnd = KJ_REQUIRE_NONNULL(specifier.slice(firstSlash + 1).findFirst('/'),
                       "invalid scoped package specifier", specifier) +
          firstSlash + 1;
    } else {
      return kj::none;
    }
  } else {
    packageEnd = slash.orDefault(specifier.size());
  }
  auto package = specifier.first(packageEnd);
  auto packageRoot = kj::str("node_modules/", package);
  auto packagePrefix = kj::str(packageRoot, "/");
  bool packaged = false;
  for (const auto& module: bundle.modules) {
    if (module.name.startsWith(packagePrefix)) {
      packaged = true;
      break;
    }
  }
  if (!packaged) return kj::none;

  kj::String candidate;
  if (packageEnd < specifier.size()) {
    candidate = kj::str(packageRoot, "/", specifier.slice(packageEnd + 1));
  } else {
    candidate = packageMain(bundle, packageRoot).orDefault(kj::str(packageRoot, "/index"));
  }
  auto resolved = KJ_REQUIRE_NONNULL(
      resolveBundleFile(bundle, candidate), "unregistered CommonJS package", fromModule, specifier);
  return relativeModuleSpecifier(fromModule, resolved);
}

kj::String rewriteCommonJsRequires(const WorkerBundle& bundle, const Module& module) {
  auto source = module.source.asPtr().asChars();
  kj::Vector<char> output;
  size_t copied = 0;
  for (size_t i = 0; i + 8 < source.size(); ++i) {
    if (source.slice(i).startsWith("require("_kj)) {
      auto quoteIndex = i + 8;
      if (source[quoteIndex] != '\'' && source[quoteIndex] != '"') continue;
      auto quote = source[quoteIndex];
      auto end = quoteIndex + 1;
      while (end < source.size() && source[end] != quote) ++end;
      if (end == source.size() || end + 1 >= source.size() || source[end + 1] != ')') continue;

      auto specifier = kj::str(source.slice(quoteIndex + 1, end));
      KJ_IF_SOME(resolved, resolveCommonJsSpecifier(bundle, module.name, specifier)) {
        output.addAll(source.slice(copied, quoteIndex + 1));
        output.addAll(resolved);
        copied = end;
      }
      i = end + 1;
    }
  }
  output.addAll(source.slice(copied));
  output.add('\0');
  return kj::String(output.releaseAsArray());
}

class Cache final: public CacheClient {
 public:
  kj::Own<kj::HttpClient> getDefault(SubrequestMetadata) override {
    KJ_FAIL_REQUIRE("cache is unavailable");
  }
  kj::Own<kj::HttpClient> getNamespace(kj::StringPtr, SubrequestMetadata metadata) override {
    return getDefault(kj::mv(metadata));
  }
};

class Timer final: public kj::Timer {
 public:
  explicit Timer(kj::Timer& inner): inner(inner) {}

  kj::TimePoint now() const override {
    return inner.now();
  }
  kj::Promise<void> atTime(kj::TimePoint time) override {
    return inner.atTime(time);
  }
  kj::Promise<void> afterDelay(kj::Duration delay) override {
    return inner.afterDelay(delay);
  }

 private:
  kj::Timer& inner;
};

struct TimerResult {
  enum class State {
    PENDING,
    FIRED,
    CANCELLED,
    ERROR,
  };

  uint64_t timerId;
  State state;
  kj::Maybe<kj::String> errorCode;
  kj::Maybe<kj::String> errorMessage;
};

TimerResult parseTimerResult(kj::StringPtr input) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::JsonValue>();
  codec.decodeRaw(input, root);
  KJ_REQUIRE(root.isObject(), "timer response must be an object");
  auto fields = root.getObject();
  KJ_REQUIRE(fields.size() == 4, "timer response must contain exactly four fields");
  kj::Maybe<double> protocolVersion;
  kj::Maybe<double> timerIdValue;
  kj::Maybe<kj::String> stateValue;
  kj::Maybe<capnp::JsonValue::Reader> errorValue;
  for (auto field: fields) {
    auto name = field.getName();
    auto value = field.getValue();
    if (name == "protocol_version"_kj) {
      KJ_REQUIRE(protocolVersion == kj::none && value.isNumber(), "invalid timer protocol version");
      protocolVersion = value.getNumber();
    } else if (name == "timer_id"_kj) {
      KJ_REQUIRE(timerIdValue == kj::none && value.isNumber(), "invalid timer ID");
      timerIdValue = value.getNumber();
    } else if (name == "state"_kj) {
      KJ_REQUIRE(stateValue == kj::none && value.isString(), "invalid timer state");
      stateValue = kj::str(value.getString());
    } else if (name == "error"_kj) {
      KJ_REQUIRE(errorValue == kj::none, "duplicate timer error");
      errorValue = value;
    } else {
      KJ_FAIL_REQUIRE("unknown timer response field", name);
    }
  }
  KJ_REQUIRE(KJ_REQUIRE_NONNULL(protocolVersion, "missing timer protocol version") == 1,
      "invalid timer protocol version");
  auto timerIdNumber = KJ_REQUIRE_NONNULL(timerIdValue, "missing timer ID");
  KJ_REQUIRE(timerIdNumber >= 0 && timerIdNumber <= MAX_TIMER_ID &&
          timerIdNumber == static_cast<double>(static_cast<uint64_t>(timerIdNumber)),
      "invalid timer ID");
  auto timerId = static_cast<uint64_t>(timerIdNumber);
  auto& stateName = KJ_REQUIRE_NONNULL(stateValue, "missing timer state");

  TimerResult::State state;
  if (stateName == "pending"_kj) {
    state = TimerResult::State::PENDING;
  } else if (stateName == "fired"_kj) {
    state = TimerResult::State::FIRED;
  } else if (stateName == "cancelled"_kj) {
    state = TimerResult::State::CANCELLED;
  } else if (stateName == "error"_kj) {
    state = TimerResult::State::ERROR;
  } else {
    KJ_FAIL_REQUIRE("unknown timer state", stateName);
  }

  kj::Maybe<kj::String> errorCode;
  kj::Maybe<kj::String> errorMessage;
  auto error = KJ_REQUIRE_NONNULL(errorValue, "missing timer error");
  if (state == TimerResult::State::ERROR) {
    KJ_REQUIRE(error.isObject(), "timer error state must include an error object");
    auto errorFields = error.getObject();
    KJ_REQUIRE(errorFields.size() == 2, "invalid timer error object");
    for (auto field: errorFields) {
      if (field.getName() == "code"_kj) {
        KJ_REQUIRE(
            errorCode == kj::none && field.getValue().isString(), "invalid timer error code");
        errorCode = kj::str(field.getValue().getString());
      } else if (field.getName() == "message"_kj) {
        KJ_REQUIRE(
            errorMessage == kj::none && field.getValue().isString(), "invalid timer error message");
        errorMessage = kj::str(field.getValue().getString());
      } else {
        KJ_FAIL_REQUIRE("unknown timer error field", field.getName());
      }
    }
    KJ_REQUIRE(errorCode != kj::none && errorMessage != kj::none, "incomplete timer error object");
  } else {
    KJ_REQUIRE(error.isNull(), "successful timer response must contain a null error");
    KJ_REQUIRE(timerId > 0, "successful timer response must contain a timer ID");
  }

  return TimerResult{timerId, state, kj::mv(errorCode), kj::mv(errorMessage)};
}

class HyperlightTimerHostChannel final: public TimerHostChannel {
 public:
  kj::String start(uint64_t delayNs) override {
    auto arg = hostCallU64(delayNs);
    return hostCallString("WorkerdTimerV1Start"_kj, kj::arrayPtr(&arg, 1));
  }

  kj::String read(uint64_t timerId) override {
    auto arg = hostCallU64(timerId);
    return hostCallString("WorkerdTimerV1Read"_kj, kj::arrayPtr(&arg, 1));
  }

  int32_t cancel(uint64_t timerId) override {
    auto arg = hostCallU64(timerId);
    return hostCallI32("WorkerdTimerV1Cancel"_kj, kj::arrayPtr(&arg, 1));
  }
};

class TimerChannelImpl final: public TimerChannel {
 public:
  TimerChannelImpl(kj::Rc<TimerHostChannel> host, kj::Timer& pollTimer)
      : host(kj::mv(host)),
        pollTimer(pollTimer) {}

  void syncTime() override {}
  kj::Date now(kj::Maybe<kj::Date>) override {
    return kj::systemPreciseCalendarClock().now();
  }
  kj::Promise<void> atTime(kj::Date when) override {
    auto now = kj::systemPreciseCalendarClock().now();
    return afterLimitTimeout(when <= now ? 0 * kj::NANOSECONDS : when - now);
  }
  kj::Promise<void> afterLimitTimeout(kj::Duration delay) override {
    KJ_REQUIRE(delay >= 0 * kj::NANOSECONDS, "timer delay must not be negative");
    auto delayNs = static_cast<uint64_t>(delay / kj::NANOSECONDS);
    auto started = parseTimerResult(host->start(delayNs));
    if (started.state == TimerResult::State::ERROR) {
      KJ_REQUIRE(started.timerId == 0, "failed timer start returned an ID");
      auto& code = KJ_REQUIRE_NONNULL(started.errorCode);
      KJ_REQUIRE(code == "invalid_duration"_kj || code == "overloaded"_kj,
          "unknown timer start error code", code);
      KJ_FAIL_REQUIRE("timer start failed", code, KJ_REQUIRE_NONNULL(started.errorMessage));
    }
    KJ_REQUIRE(started.state == TimerResult::State::PENDING, "timer start did not return pending");
    auto timerId = started.timerId;
    auto cleanupHost = host.addRef();
    return awaitTimer(timerId).attach(kj::defer([host = kj::mv(cleanupHost), timerId]() mutable {
      try {
        auto result = host->cancel(timerId);
        if (result == 0) {
          auto terminal = parseTimerResult(host->read(timerId));
          KJ_REQUIRE(terminal.timerId == timerId && terminal.state == TimerResult::State::CANCELLED,
              "cancelled timer did not return its terminal state", timerId);
        } else if (result != -ENOENT) {
          KJ_LOG(ERROR, "Hyperlight timer cancellation failed", timerId, result);
        }
      } catch (const kj::Exception& exception) {
        KJ_LOG(ERROR, "Hyperlight timer cancellation host call failed", timerId, exception);
      }
    }));
  }

 private:
  kj::Promise<void> awaitTimer(uint64_t timerId) {
    auto result = parseTimerResult(host->read(timerId));
    KJ_REQUIRE(result.timerId == timerId, "timer response ID mismatch");
    switch (result.state) {
      case TimerResult::State::PENDING:
        return pollTimer.afterDelay(TIMER_POLL_INTERVAL).then([this, timerId]() {
          return awaitTimer(timerId);
        });
      case TimerResult::State::FIRED:
        return kj::READY_NOW;
      case TimerResult::State::CANCELLED:
        KJ_FAIL_REQUIRE("timer was cancelled before firing", timerId);
      case TimerResult::State::ERROR:
        KJ_REQUIRE(KJ_REQUIRE_NONNULL(result.errorCode) == "unknown_timer"_kj &&
                KJ_REQUIRE_NONNULL(result.errorMessage) == "unknown or released timer"_kj,
            "invalid unknown timer response");
        KJ_FAIL_REQUIRE("timer read failed", KJ_REQUIRE_NONNULL(result.errorCode),
            KJ_REQUIRE_NONNULL(result.errorMessage));
    }
    KJ_UNREACHABLE;
  }

  kj::Rc<TimerHostChannel> host;
  kj::Timer& pollTimer;
};

class EntropySource final: public kj::EntropySource {
 public:
  void generate(kj::ArrayPtr<byte> buffer) override {
    for (auto& value: buffer) {
      value = counter++;
    }
  }

 private:
  byte counter = 0;
};

class LimitEnforcerImpl final: public LimitEnforcer {
 public:
  kj::Own<void> enterJs(jsg::Lock&, IoContext&) override {
    return {};
  }
  void topUpActor() override {}
  void newSubrequest(bool) override {}
  void newKvRequest(KvOpType) override {
    KJ_FAIL_REQUIRE("KV is unavailable");
  }
  void newAnalyticsEngineRequest() override {
    KJ_FAIL_REQUIRE("analytics is unavailable");
  }
  kj::Promise<void> limitDrain() override {
    return kj::NEVER_DONE;
  }
  kj::Promise<void> limitScheduled() override {
    return kj::NEVER_DONE;
  }
  kj::Duration getAlarmLimit() override {
    return 0 * kj::SECONDS;
  }
  size_t getBufferingLimit() override {
    return 64 * 1024;
  }
  kj::Maybe<EventOutcome> getLimitsExceeded() override {
    return kj::none;
  }
  kj::Promise<void> onLimitsExceeded() override {
    return kj::NEVER_DONE;
  }
  void setCpuLimitNearlyExceededCallback(kj::Function<void()>) override {}
  void requireLimitsNotExceeded() override {}
  void reportMetrics(RequestObserver&) override {}
  kj::Duration consumeTimeElapsedForPeriodicLogging() override {
    return 0 * kj::SECONDS;
  }
  size_t getSqliteMemoryUsage() const override {
    return 0;
  }
};

class IsolateLimitEnforcerImpl final: public IsolateLimitEnforcer {
 public:
  v8::Isolate::CreateParams getCreateParams() override {
    v8::Isolate::CreateParams params;
    params.constraints.ConfigureDefaultsFromHeapSize(0, 80 * 1024 * 1024);
    params.constraints.set_max_old_generation_size_in_bytes(64 * 1024 * 1024);
    params.constraints.set_max_young_generation_size_in_bytes(8 * 1024 * 1024);
    return params;
  }
  void customizeIsolate(v8::Isolate*) override {}
  ActorCacheSharedLruOptions getActorCacheLruOptions() override {
    return {.softLimit = 1 * 1024 * 1024,
      .hardLimit = 2 * 1024 * 1024,
      .staleTimeout = 1 * kj::SECONDS,
      .dirtyListByteLimit = 1 * 1024 * 1024,
      .maxKeysPerRpc = 1,
      .neverFlush = true};
  }
  kj::Own<void> enterStartupJs(jsg::Lock&, kj::OneOf<kj::Exception, kj::Duration>&) const override {
    return {};
  }
  kj::Own<void> enterStartupPython(
      jsg::Lock&, kj::OneOf<kj::Exception, kj::Duration>&) const override {
    KJ_FAIL_REQUIRE("Python is unavailable");
  }
  kj::Own<void> enterDynamicImportJs(
      jsg::Lock&, kj::OneOf<kj::Exception, kj::Duration>&) const override {
    return {};
  }
  kj::Own<void> enterLoggingJs(jsg::Lock&, kj::OneOf<kj::Exception, kj::Duration>&) const override {
    return {};
  }
  kj::Own<void> enterInspectorJs(
      jsg::Lock&, kj::OneOf<kj::Exception, kj::Duration>&) const override {
    KJ_FAIL_REQUIRE("inspector is unavailable");
  }
  void completedRequest(kj::StringPtr) const override {}
  bool exitJs(jsg::Lock&) const override {
    return false;
  }
  void reportMetrics(IsolateObserver&) const override {}
  kj::Maybe<size_t> checkPbkdfIterations(jsg::Lock&, size_t) const override {
    return kj::none;
  }
  bool hasExcessivelyExceededHeapLimit() const override {
    return false;
  }
  const TrackedWasmInstanceList& getTrackedWasmInstances() const override {
    return trackedWasmInstances;
  }

 private:
  TrackedWasmInstanceList trackedWasmInstances;
};

class ErrorReporter final: public Worker::ValidationErrorReporter {
 public:
  void addError(kj::String error) override {
    KJ_FAIL_REQUIRE("invalid embedded Worker", error);
  }
  void addEntrypoint(kj::Maybe<kj::StringPtr>, kj::Array<kj::String>) override {}
  void addActorClass(kj::StringPtr) override {}
  void addWorkflowClass(kj::StringPtr, kj::Array<kj::String>) override {}
};

class ChannelFactory final: public IoChannelFactory {
 public:
  ChannelFactory(TimerChannel& timer,
      kj::Rc<FetchBroker> fetchBroker,
      kj::Rc<LogicalServiceHostChannel> logicalServiceHost,
      kj::HttpHeaderTable& headerTable,
      kj::ArrayPtr<const Binding> bindings)
      : timer(timer),
        fetchBroker(kj::mv(fetchBroker)),
        logicalServiceHost(kj::mv(logicalServiceHost)),
        headerTable(headerTable),
        bindings(KJ_MAP(binding, bindings) {
          return Binding{
            .name = kj::str(binding.name),
            .kind = binding.kind,
          };
        }),
        nextRequestId(kj::rc<uint64_t>(1)) {}

  void abortIsolate(kj::StringPtr reason) override {
    KJ_FAIL_REQUIRE("isolate aborted", reason);
  }
  kj::Own<WorkerInterface> startSubrequest(uint channel, SubrequestMetadata metadata) override {
    if (channel == 0) {
      KJ_REQUIRE(*nextRequestId != kj::maxValue, "outbound fetch request ID space exhausted");
      return newOutboundFetchWorker(fetchBroker.addRef(), timer, kj::str((*nextRequestId)++));
    }
    auto bindingIndex = channel - 1;
    KJ_REQUIRE(bindingIndex < bindings.size(), "invalid logical service channel");
    const auto& binding = bindings[bindingIndex];
    auto serviceChannel = newCompositeServiceChannel(
        logicalServiceHost.addRef(), headerTable, kj::str(binding.name), binding.kind);
    return serviceChannel->startRequest(kj::mv(metadata));
  }
  kj::Own<SubrequestChannel> getSubrequestChannelResolved(uint channel,
      kj::Maybe<Frankenvalue> props,
      kj::Maybe<VersionRequest> versionRequest,
      Persistent persistent) override {
    KJ_REQUIRE(props == kj::none && versionRequest == kj::none && persistent == Persistent::NO,
        "sandbox executor channels do not support properties, versions, or persistence");
    if (channel == 0) {
      return newOutboundFetchChannel(fetchBroker.addRef(), timer, nextRequestId.addRef());
    }
    auto bindingIndex = channel - 1;
    KJ_REQUIRE(bindingIndex < bindings.size(), "invalid logical service channel");
    const auto& binding = bindings[bindingIndex];
    return newCompositeServiceChannel(
        logicalServiceHost.addRef(), headerTable, kj::str(binding.name), binding.kind);
  }
  kj::Own<ActorClassChannel> getActorClassResolved(
      uint, kj::Maybe<Frankenvalue>, Persistent) override {
    KJ_FAIL_REQUIRE("actor classes are unavailable");
  }
  capnp::Capability::Client getCapability(uint) override {
    KJ_FAIL_REQUIRE("capabilities are unavailable");
  }
  kj::Own<CacheClient> getCache() override {
    return kj::heap<Cache>();
  }
  TimerChannel& getTimer() override {
    return timer;
  }
  kj::Promise<void> writeLogfwdr(
      uint, kj::FunctionParam<void(capnp::AnyPointer::Builder)>) override {
    KJ_FAIL_REQUIRE("log channels are unavailable");
  }
  kj::Own<ActorChannel> getGlobalActor(uint,
      const ActorIdFactory::ActorId&,
      kj::Maybe<kj::String>,
      ActorGetMode,
      bool,
      ActorRoutingMode,
      SpanParent,
      kj::Maybe<ActorVersion>,
      Persistent) override {
    KJ_FAIL_REQUIRE("actors are unavailable");
  }
  kj::Own<ActorChannel> getColoLocalActor(uint, kj::StringPtr, SpanParent) override {
    KJ_FAIL_REQUIRE("actors are unavailable");
  }

 private:
  TimerChannel& timer;
  kj::Rc<FetchBroker> fetchBroker;
  kj::Rc<LogicalServiceHostChannel> logicalServiceHost;
  kj::HttpHeaderTable& headerTable;
  kj::Array<Binding> bindings;
  kj::Rc<uint64_t> nextRequestId;
};

class TaskErrorHandler final: public kj::TaskSet::ErrorHandler {
 public:
  void taskFailed(kj::Exception&& exception) override {
    KJ_LOG(ERROR, "sandbox background task failed", exception);
  }
};

class MemoryOutputStream final: public kj::AsyncOutputStream, public kj::Refcounted {
 public:
  kj::Promise<void> write(kj::ArrayPtr<const byte> buffer) override {
    content.addAll(buffer);
    return kj::READY_NOW;
  }
  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    for (auto piece: pieces) {
      content.addAll(piece);
    }
    return kj::READY_NOW;
  }
  kj::Promise<void> whenWriteDisconnected() override {
    return kj::NEVER_DONE;
  }
  kj::String str() {
    return kj::str(content.asPtr().asChars());
  }

 private:
  kj::Vector<byte> content;
};

class HttpResponse final: public kj::HttpService::Response {
 public:
  kj::Own<kj::AsyncOutputStream> send(uint statusCode,
      kj::StringPtr,
      const kj::HttpHeaders& responseHeaders,
      kj::Maybe<uint64_t>) override {
    status = statusCode;
    responseHeaders.forEach([this](kj::StringPtr name, kj::StringPtr value) {
      headers.add(Header{kj::str(name), kj::str(value)});
    });
    return kj::addRef(*body);
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders&) override {
    KJ_FAIL_REQUIRE("WebSockets are unavailable");
  }

  uint status = 0;
  kj::Vector<Header> headers;
  kj::Own<MemoryOutputStream> body = kj::refcounted<MemoryOutputStream>();
};

server::config::Worker::Reader buildConfig(
    capnp::MallocMessageBuilder& arena, const WorkerBundle& bundle) {
  util::Autogate::initAutogate({});
  auto config = arena.initRoot<server::config::Worker>();
  config.setCompatibilityDate(bundle.compatibilityDate);
  auto flags = config.initCompatibilityFlags(bundle.compatibilityFlags.size());
  for (auto i: kj::indices(bundle.compatibilityFlags)) {
    flags.set(i, bundle.compatibilityFlags[i]);
  }
  auto modules = config.initModules(bundle.modules.size());
  for (auto i: kj::indices(bundle.modules)) {
    const auto& input = bundle.modules[i];
    auto output = modules[i];
    output.setName(input.name);
    auto sourceChars = input.source.asChars();
    auto sourceText = kj::str(sourceChars);
    switch (input.type) {
      case ModuleType::ES_MODULE:
        output.setEsModule(sourceText);
        break;
      case ModuleType::COMMON_JS_MODULE:
        output.setCommonJsModule(rewriteCommonJsRequires(bundle, input));
        break;
      case ModuleType::TEXT:
        output.setText(sourceText);
        break;
      case ModuleType::JSON:
        output.setJson(sourceText);
        break;
      case ModuleType::WASM:
        output.setWasm(input.source);
        break;
    }
  }
  return config;
}

CompatibilityFlags::Reader buildCompatibilityFlags(capnp::MallocMessageBuilder& arena,
    const WorkerBundle& bundle,
    Worker::ValidationErrorReporter& errorReporter) {
  static constexpr kj::StringPtr ALLOWED_EXPERIMENTAL_FLAGS[] = {
    "enable_web_file_system"_kj,
  };
  auto flags = arena.initRoot<CompatibilityFlags>();
  compileCompatibilityFlags(bundle.compatibilityDate, bundle.compatibilityFlags, flags,
      errorReporter, false, CompatibilityDateValidation::CODE_VERSION,
      kj::arrayPtr(ALLOWED_EXPERIMENTAL_FLAGS));
  return flags;
}

kj::Own<VirtualFileSystem> buildFileSystem(const WorkerBundle& bundle) {
  auto fsMap = kj::heap<FsMap>();
  Directory::Builder bundleDirectory;
  kj::Path bundleRoot(nullptr);
  for (const auto& module: bundle.modules) {
    auto path = bundleRoot.eval(module.name);
    bundleDirectory.addPath(
        path, File::newReadable(kj::heapArray<const byte>(module.source.asBytes())));
  }

  Directory::Builder root;
  root.addPath(fsMap->getBundlePath(), bundleDirectory.finish());
  root.addPath(fsMap->getTempPath(), getTmpDirectoryImpl());
  root.addPath(fsMap->getDevPath(), getDevDirectory());

  Directory::Builder storage;
  for (const auto& mount: bundle.storageMounts) {
    storage.add(
        mount.name, newHostStorageDirectory(mount.name, mount.mode == StorageMode::READ_WRITE));
  }
  root.add("storage"_kj, storage.finish());
  return newVirtualFileSystem(kj::mv(fsMap), root.finish());
}

kj::Array<server::WorkerdApi::Global> buildGlobals(const WorkerBundle& bundle) {
  return KJ_MAP(index, kj::indices(bundle.bindings)) {
    return server::WorkerdApi::Global{
      .name = kj::str(bundle.bindings[index].name),
      .value =
          server::WorkerdApi::Global::Fetcher{
            .channel = static_cast<uint>(index + 1),
            .requiresHost = false,
            .isInHouse = true,
          },
    };
  };
}

}  // namespace

struct SandboxRuntime::Impl {
  Impl(const WorkerBundle& bundle,
      kj::Rc<FetchBroker> fetchBroker,
      kj::Rc<LogicalServiceHostChannel> logicalServiceHost)
      : errorReporter(kj::heap<ErrorReporter>()),
        config(buildConfig(configArena, bundle)),
        compatibilityFlags(buildCompatibilityFlags(compatibilityArena, bundle, *errorReporter)),
        io(kj::setupAsyncIo()),
        timer(kj::heap<Timer>(io.provider->getTimer())),
        timerChannel(newTimerChannel(newHyperlightTimerHostChannel(), io.provider->getTimer())),
        entropySource(kj::heap<EntropySource>()),
        threadContextHeaderBundle(headerTableBuilder),
        httpOverCapnpFactory(byteStreamFactory,
            capnp::HttpOverCapnpFactory::HeaderIdBundle(headerTableBuilder),
            capnp::HttpOverCapnpFactory::LEVEL_2),
        threadContext(*timer,
            *entropySource,
            threadContextHeaderBundle,
            httpOverCapnpFactory,
            byteStreamFactory),
        memoryCacheProvider(kj::heap<api::MemoryCacheProvider>(*timer)),
        api(kj::heap<server::WorkerdApi>(v8System,
            compatibilityFlags,
            capnp::List<server::config::Extension>::Reader{},
            kj::rc<IsolateLimitEnforcerImpl>()->getCreateParams(),
            v8::IsolateGroup::GetDefault(),
            kj::atomicRefcounted<JsgIsolateObserver>(),
            *memoryCacheProvider,
            api::pyodide::PythonConfig{
              .packageDiskCacheRoot = kj::none,
              .pyodideDiskCacheRoot = kj::none,
              .createSnapshot = false,
              .createBaselineSnapshot = false,
            })),
        isolate(kj::atomicRefcounted<Worker::Isolate>(kj::mv(api),
            kj::atomicRefcounted<IsolateObserver>(),
            SCRIPT_ID,
            kj::rc<IsolateLimitEnforcerImpl>().toOwn(),
            Worker::Isolate::InspectorPolicy::DISALLOW)),
        script(kj::atomicRefcounted<Worker::Script>(kj::atomicAddRef(*isolate),
            SCRIPT_ID,
            server::WorkerdApi::extractSource(
                bundle.mainModule, config, compatibilityFlags, *errorReporter),
            IsolateObserver::StartType::COLD,
            false,
            kj::none,
            kj::none,
            SpanParent(nullptr),
            buildFileSystem(bundle),
            kj::none)),
        worker(kj::atomicRefcounted<Worker>(kj::atomicAddRef(*script),
            kj::atomicRefcounted<WorkerObserver>(),
            [globals = buildGlobals(bundle)](jsg::Lock& lock,
                const Worker::Api& api,
                v8::Local<v8::Object> target,
                v8::Local<v8::Object>) {
              server::WorkerdApi::from(api).compileGlobals(lock, globals, target, 1);
            },
            IsolateObserver::StartType::COLD,
            SpanParent(nullptr),
            Worker::LockType(Worker::Lock::TakeSynchronously(kj::none)))),
        errorHandler(kj::heap<TaskErrorHandler>()),
        waitUntilTasks(*errorHandler),
        headerTable(headerTableBuilder.build()),
        channelFactory(kj::rc<ChannelFactory>(*timerChannel,
            kj::mv(fetchBroker),
            kj::mv(logicalServiceHost),
            *headerTable,
            bundle.bindings)) {}

  Response runRequest(kj::HttpMethod method,
      kj::StringPtr url,
      kj::ArrayPtr<const Header> requestHeaderList,
      kj::StringPtr requestBodyText) {
    kj::HttpHeaders requestHeaders(*headerTable);
    for (const auto& header: requestHeaderList) {
      requestHeaders.addPtrPtr(header.name, header.value);
    }
    HttpResponse response;
    auto requestBody = newMemoryInputStream(requestBodyText);
    auto context = kj::refcounted<IoContext>(
        threadContext, kj::atomicAddRef(*worker), kj::none, kj::heap<LimitEnforcerImpl>());
    auto incomingRequest = kj::heap<IoContext::IncomingRequest>(kj::addRef(*context),
        channelFactory.addRef(), kj::refcounted<RequestObserver>(), kj::none, kj::none);
    incomingRequest->delivered();
    incomingRequest->getContext()
        .run([&](Worker::Lock& lock) {
      auto& globalScope = lock.getGlobalScope();
      return globalScope.request(method, url, requestHeaders, *requestBody, response, "{}"_kj, lock,
          lock.getExportedHandler(kj::none, kj::none, {}, kj::none), kj::none);
    }).wait(io.waitScope);
    return {
      .statusCode = response.status,
      .headers = response.headers.releaseAsArray(),
      .body = response.body->str(),
    };
  }

  ScheduledResponse runScheduled(kj::Date scheduledTime, kj::StringPtr cron) {
    auto context = kj::refcounted<IoContext>(
        threadContext, kj::atomicAddRef(*worker), kj::none, kj::heap<LimitEnforcerImpl>());
    auto incomingRequest = kj::heap<IoContext::IncomingRequest>(kj::addRef(*context),
        channelFactory.addRef(), kj::refcounted<RequestObserver>(), kj::none, kj::none);
    incomingRequest->delivered();
    auto& requestContext = incomingRequest->getContext();
    requestContext.addWaitUntil(requestContext.run([scheduledTime, cron](Worker::Lock& lock) {
      lock.getGlobalScope().startScheduled(
          scheduledTime, cron, lock, lock.getExportedHandler(kj::none, kj::none, {}, kj::none));
    }));
    auto result = incomingRequest->finishScheduled(kj::mv(incomingRequest)).wait(io.waitScope);
    return {
      .retry = result.retry,
      .outcome = result.outcome,
    };
  }

  QueueResponse runQueue(QueueRequest request) {
    auto messages = KJ_MAP(message, request.messages) {
      return api::IncomingQueueMessage{
        .id = kj::mv(message.id),
        .timestamp = message.timestamp,
        .body = kj::mv(message.body),
        .contentType = kj::mv(message.contentType),
        .attempts = message.attempts,
      };
    };
    auto event = kj::refcounted<api::QueueCustomEvent>(api::QueueEvent::Params{
      .queueName = kj::mv(request.queueName),
      .messages = kj::mv(messages),
      .metadata =
          api::MessageBatchMetadata{
            .metrics =
                api::MessageBatchMetrics{
                  .backlogCount = request.backlogCount,
                  .backlogBytes = request.backlogBytes,
                  .oldestMessageTimestamp = request.oldestMessageTimestamp,
                },
          },
    });
    auto eventResult = kj::addRef(*event);
    auto context = kj::refcounted<IoContext>(
        threadContext, kj::atomicAddRef(*worker), kj::none, kj::heap<LimitEnforcerImpl>());
    auto incomingRequest = kj::heap<IoContext::IncomingRequest>(kj::addRef(*context),
        channelFactory.addRef(), kj::refcounted<RequestObserver>(), kj::none, kj::none);
    auto result = event->run(kj::mv(incomingRequest), kj::none, kj::none, {}, waitUntilTasks, false)
                      .wait(io.waitScope);
    auto retryBatch = eventResult->getRetryBatch();
    return {
      .outcome = result.outcome,
      .ackAll = eventResult->getAckAll(),
      .retryBatch = retryBatch.retry,
      .retryBatchDelaySeconds = retryBatch.delaySeconds,
      .explicitAcks = eventResult->getExplicitAcks(),
      .retryMessages =
          KJ_MAP(retry, eventResult->getRetryMessages()) {
      return QueueRetry{
        .messageId = kj::mv(retry.msgId),
        .delaySeconds = retry.delaySeconds,
      };
    },
    };
  }

  kj::Own<Worker::ValidationErrorReporter> errorReporter;
  capnp::MallocMessageBuilder configArena;
  server::config::Worker::Reader config;
  capnp::MallocMessageBuilder compatibilityArena;
  CompatibilityFlags::Reader compatibilityFlags;
  kj::AsyncIoContext io;
  kj::Own<kj::Timer> timer;
  kj::Own<TimerChannel> timerChannel;
  kj::Own<kj::EntropySource> entropySource;
  capnp::ByteStreamFactory byteStreamFactory;
  kj::HttpHeaderTable::Builder headerTableBuilder;
  ThreadContext::HeaderIdBundle threadContextHeaderBundle;
  capnp::HttpOverCapnpFactory httpOverCapnpFactory;
  ThreadContext threadContext;
  kj::Own<api::MemoryCacheProvider> memoryCacheProvider;
  kj::Own<Worker::Api> api;
  kj::Own<Worker::Isolate> isolate;
  kj::Own<Worker::Script> script;
  kj::Own<Worker> worker;
  kj::Own<kj::TaskSet::ErrorHandler> errorHandler;
  kj::TaskSet waitUntilTasks;
  kj::Own<kj::HttpHeaderTable> headerTable;
  kj::Rc<ChannelFactory> channelFactory;
};

SandboxRuntime::SandboxRuntime(const WorkerBundle& bundle, kj::Rc<FetchBroker> fetchBroker)
    : SandboxRuntime(bundle, kj::mv(fetchBroker), newHyperlightLogicalServiceHostChannel()) {}
SandboxRuntime::SandboxRuntime(const WorkerBundle& bundle,
    kj::Rc<FetchBroker> fetchBroker,
    kj::Rc<LogicalServiceHostChannel> logicalServiceHost)
    : impl(kj::heap<Impl>(bundle, kj::mv(fetchBroker), kj::mv(logicalServiceHost))) {}
SandboxRuntime::~SandboxRuntime() noexcept(false) {}

Response SandboxRuntime::runRequest(kj::HttpMethod method,
    kj::StringPtr url,
    kj::ArrayPtr<const Header> headers,
    kj::StringPtr body) {
  return impl->runRequest(method, url, headers, body);
}

ScheduledResponse SandboxRuntime::runScheduled(kj::Date scheduledTime, kj::StringPtr cron) {
  return impl->runScheduled(scheduledTime, cron);
}

QueueResponse SandboxRuntime::runQueue(QueueRequest request) {
  return impl->runQueue(kj::mv(request));
}

kj::Rc<TimerHostChannel> newHyperlightTimerHostChannel() {
  return kj::rc<HyperlightTimerHostChannel>();
}

kj::Own<TimerChannel> newTimerChannel(kj::Rc<TimerHostChannel> host, kj::Timer& pollTimer) {
  return kj::heap<TimerChannelImpl>(kj::mv(host), pollTimer);
}

}  // namespace workerd::server::sandbox_executor
