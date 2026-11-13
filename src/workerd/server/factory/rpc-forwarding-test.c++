// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include <workerd/io/frankenvalue.h>
#include <workerd/io/worker-interface.h>
#include <workerd/io/worker.h>

#include <capnp/compat/http-over-capnp.h>
#include <capnp/schema.h>
#include <kj/test.h>

namespace workerd::server {
namespace {

struct Calls {
  kj::String warmed;
  int64_t scheduled = 0;
  kj::String cron;
  int64_t alarm = 0;
  uint32_t retries = 0;
  int64_t abandoned = 0;
  bool queued = false;
};

// Unlike the published ordinary WorkerdBootstrap server, this test-only standard dispatcher
// accepts the scheduled/alarm methods. It qualifies client forwarding, not server support.
class Dispatcher final: public rpc::EventDispatcher::Server {
 public:
  explicit Dispatcher(Calls& calls): calls(calls) {}

  kj::Promise<void> prewarm(PrewarmContext context) override {
    calls.warmed = kj::str(context.getParams().getUrl());
    return kj::READY_NOW;
  }
  kj::Promise<void> runScheduled(RunScheduledContext context) override {
    calls.scheduled = context.getParams().getScheduledTime();
    calls.cron = kj::str(context.getParams().getCron());
    auto result = context.getResults().initResult();
    result.setRetry(true);
    result.setOutcome(EventOutcome::EXCEPTION);
    return kj::READY_NOW;
  }
  kj::Promise<void> runAlarm(RunAlarmContext context) override {
    calls.alarm = context.getParams().getScheduledTime();
    calls.retries = context.getParams().getRetryCount();
    auto result = context.getResults().initResult();
    result.setRetry(true);
    result.setRetryCountsAgainstLimit(false);
    result.setOutcome(EventOutcome::EXCEPTION);
    result.setErrorDescription("distinct alarm detail");
    return kj::READY_NOW;
  }
  kj::Promise<void> abandonAlarm(AbandonAlarmContext context) override {
    calls.abandoned = context.getParams().getScheduledTimeMs();
    context.getResults().setStoredAlarmTimeMs(987654);
    return kj::READY_NOW;
  }
  kj::Promise<void> queue(QueueContext context) override {
    calls.queued = true;
    context.getResults().initResult().setOutcome(EventOutcome::OK);
    return kj::READY_NOW;
  }

 private:
  Calls& calls;
};

class Event final: public WorkerInterface::CustomEvent {
 public:
  explicit Event(bool restore): restore(restore) {}

  kj::Promise<Result> run(kj::Own<IoContext_IncomingRequest>,
      kj::Maybe<kj::StringPtr>,
      kj::Maybe<Worker_VersionInfo>,
      Frankenvalue,
      kj::TaskSet&,
      bool) override {
    KJ_UNIMPLEMENTED("test event is only sent over RPC");
  }
  kj::Promise<Result> sendRpc(capnp::HttpOverCapnpFactory&,
      capnp::ByteStreamFactory&,
      FrankenvalueHandler& handler,
      rpc::EventDispatcher::Client dispatcher) override {
    if (restore) {
      auto request = dispatcher.restoreServiceRequest();
      auto params = Frankenvalue::fromJson(kj::str("{}"));
      handler.toCapnp(params, request.initParams());
      KJ_FAIL_REQUIRE("unsupported handler unexpectedly serialized restore params");
    }
    auto request = dispatcher.queueRequest();
    request.setQueueName("fixture");
    auto response = co_await request.send();
    co_return Result{.outcome = response.getResult().getOutcome()};
  }
  kj::Promise<Result> notSupported() override {
    KJ_UNIMPLEMENTED("test event unexpectedly refused locally");
  }
  uint16_t getType() override {
    return 0;
  }
  tracing::EventInfo getEventInfo() const override {
    return tracing::CustomEventInfo();
  }

 private:
  bool restore;
};

KJ_TEST("standard RPC client preserves all published event results and explicit restore refusal") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  capnp::ByteStreamFactory streams;
  kj::HttpHeaderTable::Builder headers;
  capnp::HttpOverCapnpFactory http(streams, headers, capnp::HttpOverCapnpFactory::LEVEL_2);
  auto table = headers.build();
  Calls calls;
  auto client = [&]() {
    return kj::heap<RpcWorkerInterface>(http, streams, getUnsupportedFrankenvalueHandler(),
        rpc::EventDispatcher::Client(kj::heap<Dispatcher>(calls)));
  };
  client()->prewarm("/warm").wait(waitScope);
  KJ_EXPECT(calls.warmed == "/warm");
  auto scheduled = client()
                       ->runScheduled(kj::UNIX_EPOCH + 123456 * kj::MILLISECONDS, "0 * * * *")
                       .wait(waitScope);
  KJ_EXPECT(calls.scheduled == 123);
  KJ_EXPECT(calls.cron == "0 * * * *");
  KJ_EXPECT(scheduled.retry && scheduled.outcome == EventOutcome::EXCEPTION);
  auto alarm = client()->runAlarm(kj::UNIX_EPOCH + 123456 * kj::MILLISECONDS, 7).wait(waitScope);
  KJ_EXPECT(calls.alarm == 123456 && calls.retries == 7);
  KJ_EXPECT(alarm.retry && !alarm.retryCountsAgainstLimit);
  KJ_EXPECT(alarm.outcome == EventOutcome::EXCEPTION);
  KJ_EXPECT(KJ_ASSERT_NONNULL(alarm.errorDescription) == "distinct alarm detail");
  auto abandoned =
      client()->abandonAlarm(kj::UNIX_EPOCH + 123456 * kj::MILLISECONDS).wait(waitScope);
  KJ_EXPECT(calls.abandoned == 123456);
  KJ_EXPECT(KJ_ASSERT_NONNULL(abandoned) == kj::UNIX_EPOCH + 987654 * kj::MILLISECONDS);
  auto queued = client()->customEvent(kj::heap<Event>(false)).wait(waitScope);
  KJ_EXPECT(calls.queued && queued.outcome == EventOutcome::OK);
  KJ_EXPECT_THROW_MESSAGE("Frankenvalues unsupported here",
      client()->customEvent(kj::heap<Event>(true)).wait(waitScope));
  auto schema = capnp::Schema::from<rpc::EventDispatcher>();
  KJ_EXPECT(schema.findMethodByName("test") == kj::none);
  KJ_EXPECT(schema.findMethodByName("bench") == kj::none);
}

}  // namespace
}  // namespace workerd::server
