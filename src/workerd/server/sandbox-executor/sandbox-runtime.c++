// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-runtime.h"

#include "sandbox-fetch.h"

#include <workerd/api/global-scope.h>
#include <workerd/api/memory-cache.h>
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
#include <kj/async-io.h>

namespace workerd::server::sandbox_executor {
namespace {

constexpr kj::StringPtr SCRIPT_ID = "sandbox-executor"_kj;

jsg::V8System v8System({"--single-threaded"_kj, "--max-old-space-size=64"_kj,
  "--max-semi-space-size=4"_kj, "--no-concurrent-marking"_kj, "--no-concurrent-sweeping"_kj,
  "--no-concurrent-recompilation"_kj, "--no-parallel-scavenge"_kj});

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

class TimerChannelImpl final: public TimerChannel {
 public:
  explicit TimerChannelImpl(kj::Timer& timer): timer(timer) {}

  void syncTime() override {}
  kj::Date now(kj::Maybe<kj::Date>) override {
    return kj::systemPreciseCalendarClock().now();
  }
  kj::Promise<void> atTime(kj::Date when) override {
    auto now = kj::systemPreciseCalendarClock().now();
    if (when <= now) return kj::READY_NOW;
    return timer.afterDelay(when - now);
  }
  kj::Promise<void> afterLimitTimeout(kj::Duration delay) override {
    return timer.afterDelay(delay);
  }

 private:
  kj::Timer& timer;
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
  ChannelFactory(TimerChannel& timer, kj::Rc<FetchBroker> fetchBroker)
      : timer(timer),
        fetchBroker(kj::mv(fetchBroker)),
        nextRequestId(kj::rc<uint64_t>(1)) {}

  void abortIsolate(kj::StringPtr reason) override {
    KJ_FAIL_REQUIRE("isolate aborted", reason);
  }
  kj::Own<WorkerInterface> startSubrequest(uint channel, SubrequestMetadata metadata) override {
    KJ_REQUIRE(channel == 0, "only global outbound fetch is available");
    KJ_REQUIRE(*nextRequestId != kj::maxValue, "outbound fetch request ID space exhausted");
    return newOutboundFetchWorker(fetchBroker.addRef(), timer, kj::str((*nextRequestId)++));
  }
  kj::Own<SubrequestChannel> getSubrequestChannelResolved(uint channel,
      kj::Maybe<Frankenvalue> props,
      kj::Maybe<VersionRequest> versionRequest,
      Persistent persistent) override {
    KJ_REQUIRE(channel == 0 && props == kj::none && versionRequest == kj::none &&
            persistent == Persistent::NO,
        "only the global outbound fetch channel is available");
    return newOutboundFetchChannel(fetchBroker.addRef(), timer, nextRequestId.addRef());
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
  kj::Rc<uint64_t> nextRequestId;
};

class TaskErrorHandler final: public kj::TaskSet::ErrorHandler {
 public:
  void taskFailed(kj::Exception&&) override {}
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
    switch (input.type) {
      case ModuleType::ES_MODULE:
        output.setEsModule(input.source);
        break;
      case ModuleType::TEXT:
        output.setText(input.source);
        break;
      case ModuleType::JSON:
        output.setJson(input.source);
        break;
    }
  }
  return config;
}

CompatibilityFlags::Reader buildCompatibilityFlags(capnp::MallocMessageBuilder& arena,
    const WorkerBundle& bundle,
    Worker::ValidationErrorReporter& errorReporter) {
  auto flags = arena.initRoot<CompatibilityFlags>();
  compileCompatibilityFlags(bundle.compatibilityDate, bundle.compatibilityFlags, flags,
      errorReporter, false, CompatibilityDateValidation::CODE_VERSION, nullptr);
  return flags;
}

}  // namespace

struct SandboxRuntime::Impl {
  Impl(const WorkerBundle& bundle, kj::Rc<FetchBroker> fetchBroker)
      : errorReporter(kj::heap<ErrorReporter>()),
        config(buildConfig(configArena, bundle)),
        compatibilityFlags(buildCompatibilityFlags(compatibilityArena, bundle, *errorReporter)),
        io(kj::setupAsyncIo()),
        timer(kj::heap<Timer>(io.provider->getTimer())),
        timerChannel(kj::heap<TimerChannelImpl>(io.provider->getTimer())),
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
            newWorkerFileSystem(kj::heap<FsMap>(), getTmpDirectoryImpl()),
            kj::none)),
        worker(kj::atomicRefcounted<Worker>(kj::atomicAddRef(*script),
            kj::atomicRefcounted<WorkerObserver>(),
            [](jsg::Lock&, const Worker::Api&, v8::Local<v8::Object>, v8::Local<v8::Object>) {},
            IsolateObserver::StartType::COLD,
            SpanParent(nullptr),
            Worker::LockType(Worker::Lock::TakeSynchronously(kj::none)))),
        errorHandler(kj::heap<TaskErrorHandler>()),
        waitUntilTasks(*errorHandler),
        headerTable(headerTableBuilder.build()),
        channelFactory(kj::rc<ChannelFactory>(*timerChannel, kj::mv(fetchBroker))) {}

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
    : impl(kj::heap<Impl>(bundle, kj::mv(fetchBroker))) {}
SandboxRuntime::~SandboxRuntime() noexcept(false) {}

Response SandboxRuntime::runRequest(kj::HttpMethod method,
    kj::StringPtr url,
    kj::ArrayPtr<const Header> headers,
    kj::StringPtr body) {
  return impl->runRequest(method, url, headers, body);
}

}  // namespace workerd::server::sandbox_executor
