// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include "sandbox-state.h"

#include <kj/array.h>
#include <kj/common.h>
#include <kj/memory.h>
#include <kj/string.h>
#include <kj/vector.h>

namespace workerd::server::sandbox_executor {

inline constexpr uint32_t ACTOR_BROKER_ABI_VERSION = 1;
inline constexpr auto ACTOR_ROUTE_HOST_CALL = "WorkerdActorV1Route";

using ActorIdentity = LogicalResourceIdentity;

struct ActorEvent {
  uint32_t protocolVersion;
  kj::String requestId;
  ActorIdentity actor;
  uint64_t routeGeneration;
  kj::Array<kj::byte> payload;
};

struct ActorRouteResult {
  kj::Array<kj::byte> response;
  uint64_t routeGeneration;
  uint64_t persistedVersion;
};

struct ActorInvocationResult {
  kj::Array<kj::byte> response;
  kj::Array<kj::byte> state;
  bool keepActive = true;
};

struct ActorPersistedState {
  kj::Array<kj::byte> state;
  uint64_t routeGeneration;
  uint64_t version;
};

enum class ActorLifecycleState {
  ABSENT,
  LOADING,
  ACTIVE,
  PERSISTING,
  EVICTED,
};

struct ActorLifecycleEvent {
  ActorLifecycleState from;
  ActorLifecycleState to;
  uint64_t routeGeneration;
  uint64_t persistedVersion;
  kj::String reason;
};

class ActorLifecycleObserver {
 public:
  virtual ~ActorLifecycleObserver() noexcept(false) {}
  virtual void record(const ActorIdentity& actor, const ActorLifecycleEvent& event) = 0;
};

class ActorPersistenceAdapter {
 public:
  virtual ~ActorPersistenceAdapter() noexcept(false) {}

  virtual kj::Maybe<ActorPersistedState> load(const ActorIdentity& actor) = 0;
  // Implementations reject stale generations and return a monotonically increasing version.
  virtual uint64_t save(
      const ActorIdentity& actor, uint64_t routeGeneration, kj::ArrayPtr<const kj::byte> state) = 0;
  virtual void erase(const ActorIdentity& actor) = 0;
};

class ActorBehavior {
 public:
  virtual ~ActorBehavior() noexcept(false) {}

  virtual ActorInvocationResult invoke(const ActorIdentity& actor,
      kj::ArrayPtr<const kj::byte> event,
      kj::ArrayPtr<const kj::byte> previousState) = 0;
};

struct ActorRouterLimits {
  size_t maxActiveActors = 64;
  size_t maxEventBytes = 1024 * 1024;
  size_t maxResponseBytes = 4 * 1024 * 1024;
  size_t maxStateBytesPerActor = 1024 * 1024;
};

class ActorRouter {
 public:
  virtual ~ActorRouter() noexcept(false) {}

  virtual ActorRouteResult route(ActorEvent event) = 0;
  virtual bool evict(const ActorIdentity& actor, kj::StringPtr reason) = 0;
};

class InMemoryActorPersistence final: public ActorPersistenceAdapter {
 public:
  explicit InMemoryActorPersistence(size_t maxActors, size_t maxTotalStateBytes);

  kj::Maybe<ActorPersistedState> load(const ActorIdentity& actor) override;
  uint64_t save(const ActorIdentity& actor,
      uint64_t routeGeneration,
      kj::ArrayPtr<const kj::byte> state) override;
  void erase(const ActorIdentity& actor) override;

  size_t size() const;

 private:
  struct Entry {
    kj::String key;
    ActorIdentity identity;
    kj::Array<kj::byte> state;
    uint64_t routeGeneration;
    uint64_t version;
  };

  kj::Maybe<size_t> find(kj::StringPtr key) const;

  const size_t maxActors;
  const size_t maxTotalStateBytes;
  size_t totalStateBytes = 0;
  kj::Vector<Entry> entries;
};

class BoundedInMemoryActorRouter final: public ActorRouter {
 public:
  BoundedInMemoryActorRouter(ActorBehavior& behavior,
      ActorPersistenceAdapter& persistence,
      ActorLifecycleObserver& observer,
      ActorRouterLimits limits = {});

  ActorRouteResult route(ActorEvent event) override;
  bool evict(const ActorIdentity& actor, kj::StringPtr reason) override;

  size_t activeActorCount() const;

 private:
  struct ActiveActor {
    kj::String key;
    ActorIdentity identity;
    kj::Array<kj::byte> state;
    uint64_t routeGeneration;
    uint64_t persistedVersion;
    uint64_t lastUsed;
  };

  kj::Maybe<size_t> find(kj::StringPtr key) const;
  void evictIndex(size_t index, kj::StringPtr reason);

  ActorBehavior& behavior;
  ActorPersistenceAdapter& persistence;
  ActorLifecycleObserver& observer;
  ActorRouterLimits limits;
  uint64_t sequence = 0;
  kj::Vector<ActiveActor> active;
};

}  // namespace workerd::server::sandbox_executor
