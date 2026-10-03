// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sandbox-actors.h"

#include <kj/debug.h>

namespace workerd::server::sandbox_executor {
namespace {

kj::Array<kj::byte> copyBytes(kj::ArrayPtr<const kj::byte> bytes) {
  return kj::heapArray(bytes);
}

ActorIdentity cloneActorIdentity(const ActorIdentity& actor) {
  return cloneLogicalResourceIdentity(actor);
}

void recordLifecycle(ActorLifecycleObserver& observer,
    const ActorIdentity& actor,
    ActorLifecycleState from,
    ActorLifecycleState to,
    uint64_t routeGeneration,
    uint64_t persistedVersion,
    kj::StringPtr reason) {
  ActorLifecycleEvent event{
    .from = from,
    .to = to,
    .routeGeneration = routeGeneration,
    .persistedVersion = persistedVersion,
    .reason = kj::str(reason),
  };
  observer.record(actor, event);
}

}  // namespace

InMemoryActorPersistence::InMemoryActorPersistence(size_t maxActors, size_t maxTotalStateBytes)
    : maxActors(maxActors),
      maxTotalStateBytes(maxTotalStateBytes) {
  KJ_REQUIRE(maxActors > 0, "in-memory actor persistence must retain at least one actor");
  KJ_REQUIRE(maxTotalStateBytes > 0, "in-memory actor persistence state quota must be non-zero");
}

kj::Maybe<size_t> InMemoryActorPersistence::find(kj::StringPtr key) const {
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].key == key) return i;
  }
  return kj::none;
}

kj::Maybe<ActorPersistedState> InMemoryActorPersistence::load(const ActorIdentity& actor) {
  auto key = actor.canonicalKey();
  KJ_IF_SOME(index, find(key)) {
    const auto& entry = entries[index];
    return ActorPersistedState{
      .state = copyBytes(entry.state),
      .routeGeneration = entry.routeGeneration,
      .version = entry.version,
    };
  }
  return kj::none;
}

uint64_t InMemoryActorPersistence::save(
    const ActorIdentity& actor, uint64_t routeGeneration, kj::ArrayPtr<const kj::byte> state) {
  KJ_REQUIRE(routeGeneration > 0, "actor route generation must be non-zero");
  auto key = actor.canonicalKey();
  KJ_IF_SOME(index, find(key)) {
    auto& entry = entries[index];
    KJ_REQUIRE(routeGeneration >= entry.routeGeneration,
        "actor persistence rejected a stale route generation");
    auto retainedBytes = totalStateBytes - entry.state.size();
    KJ_REQUIRE(
        state.size() <= maxTotalStateBytes && retainedBytes <= maxTotalStateBytes - state.size(),
        "in-memory actor persistence byte quota exceeded");
    totalStateBytes -= entry.state.size();
    entry.state = copyBytes(state);
    entry.routeGeneration = routeGeneration;
    totalStateBytes += state.size();
    KJ_REQUIRE(entry.version != kj::maxValue, "actor persistence version space exhausted");
    return ++entry.version;
  }

  KJ_REQUIRE(entries.size() < maxActors, "in-memory actor persistence actor quota exceeded");
  KJ_REQUIRE(
      state.size() <= maxTotalStateBytes && totalStateBytes <= maxTotalStateBytes - state.size(),
      "in-memory actor persistence byte quota exceeded");
  totalStateBytes += state.size();
  entries.add(Entry{
    .key = kj::mv(key),
    .identity = cloneActorIdentity(actor),
    .state = copyBytes(state),
    .routeGeneration = routeGeneration,
    .version = 1,
  });
  return 1;
}

void InMemoryActorPersistence::erase(const ActorIdentity& actor) {
  auto key = actor.canonicalKey();
  KJ_IF_SOME(index, find(key)) {
    totalStateBytes -= entries[index].state.size();
    if (index + 1 < entries.size()) {
      entries[index] = kj::mv(entries.back());
    }
    entries.removeLast();
  }
}

size_t InMemoryActorPersistence::size() const {
  return entries.size();
}

BoundedInMemoryActorRouter::BoundedInMemoryActorRouter(ActorBehavior& behavior,
    ActorPersistenceAdapter& persistence,
    ActorLifecycleObserver& observer,
    ActorRouterLimits limits)
    : behavior(behavior),
      persistence(persistence),
      observer(observer),
      limits(limits) {
  KJ_REQUIRE(limits.maxActiveActors > 0, "actor router must allow at least one active actor");
}

kj::Maybe<size_t> BoundedInMemoryActorRouter::find(kj::StringPtr key) const {
  for (size_t i = 0; i < active.size(); ++i) {
    if (active[i].key == key) return i;
  }
  return kj::none;
}

void BoundedInMemoryActorRouter::evictIndex(size_t index, kj::StringPtr reason) {
  auto& actor = active[index];
  recordLifecycle(observer, actor.identity, ActorLifecycleState::ACTIVE,
      ActorLifecycleState::EVICTED, actor.routeGeneration, actor.persistedVersion, reason);
  if (index + 1 < active.size()) {
    active[index] = kj::mv(active.back());
  }
  active.removeLast();
}

ActorRouteResult BoundedInMemoryActorRouter::route(ActorEvent event) {
  KJ_REQUIRE(event.protocolVersion == ACTOR_BROKER_ABI_VERSION, "unsupported actor broker ABI");
  KJ_REQUIRE(
      event.requestId.size() > 0 && event.requestId.size() <= 128, "invalid actor request ID");
  validateLogicalResourceIdentity(event.actor);
  KJ_REQUIRE(event.routeGeneration > 0, "actor route generation must be non-zero");
  KJ_REQUIRE(event.payload.size() <= limits.maxEventBytes, "actor event byte quota exceeded");

  auto key = event.actor.canonicalKey();
  auto existing = find(key);
  bool replaceGeneration = false;
  KJ_IF_SOME(index, existing) {
    KJ_REQUIRE(event.routeGeneration >= active[index].routeGeneration,
        "actor router rejected a stale route generation");
    if (event.routeGeneration > active[index].routeGeneration) {
      evictIndex(index, "generation");
      replaceGeneration = true;
    }
  }
  if (replaceGeneration) existing = kj::none;

  size_t index;
  KJ_IF_SOME(found, existing) {
    index = found;
  } else {
    if (active.size() == limits.maxActiveActors) {
      size_t oldest = 0;
      for (size_t i = 1; i < active.size(); ++i) {
        if (active[i].lastUsed < active[oldest].lastUsed) oldest = i;
      }
      evictIndex(oldest, "capacity");
    }

    recordLifecycle(observer, event.actor, ActorLifecycleState::ABSENT,
        ActorLifecycleState::LOADING, event.routeGeneration, 0, event.requestId);
    auto loaded = persistence.load(event.actor);
    auto state = kj::heapArray<kj::byte>(0);
    uint64_t persistedVersion = 0;
    KJ_IF_SOME(value, loaded) {
      KJ_REQUIRE(event.routeGeneration >= value.routeGeneration,
          "actor router rejected a stale persisted route generation");
      KJ_REQUIRE(value.state.size() <= limits.maxStateBytesPerActor,
          "persisted actor state byte quota exceeded");
      state = kj::mv(value.state);
      persistedVersion = value.version;
    }
    KJ_REQUIRE(sequence != kj::maxValue, "actor router sequence space exhausted");
    active.add(ActiveActor{
      .key = kj::mv(key),
      .identity = cloneActorIdentity(event.actor),
      .state = kj::mv(state),
      .routeGeneration = event.routeGeneration,
      .persistedVersion = persistedVersion,
      .lastUsed = ++sequence,
    });
    index = active.size() - 1;
    recordLifecycle(observer, active[index].identity, ActorLifecycleState::LOADING,
        ActorLifecycleState::ACTIVE, event.routeGeneration, persistedVersion, event.requestId);
  }

  auto& actor = active[index];
  KJ_REQUIRE(sequence != kj::maxValue, "actor router sequence space exhausted");
  actor.lastUsed = ++sequence;
  auto result = behavior.invoke(actor.identity, event.payload, actor.state);
  KJ_REQUIRE(
      result.response.size() <= limits.maxResponseBytes, "actor response byte quota exceeded");
  KJ_REQUIRE(
      result.state.size() <= limits.maxStateBytesPerActor, "actor state byte quota exceeded");
  recordLifecycle(observer, actor.identity, ActorLifecycleState::ACTIVE,
      ActorLifecycleState::PERSISTING, actor.routeGeneration, actor.persistedVersion,
      event.requestId);
  uint64_t persistedVersion;
  try {
    persistedVersion = persistence.save(actor.identity, actor.routeGeneration, result.state);
  } catch (kj::Exception& exception) {
    recordLifecycle(observer, actor.identity, ActorLifecycleState::PERSISTING,
        ActorLifecycleState::ACTIVE, actor.routeGeneration, actor.persistedVersion,
        "persistence-failed");
    throw kj::mv(exception);
  }
  actor.state = kj::mv(result.state);
  actor.persistedVersion = persistedVersion;
  recordLifecycle(observer, actor.identity, ActorLifecycleState::PERSISTING,
      ActorLifecycleState::ACTIVE, actor.routeGeneration, actor.persistedVersion, event.requestId);
  auto response = kj::mv(result.response);
  if (!result.keepActive) {
    evictIndex(index, "behavior");
  }
  return {
    .response = kj::mv(response),
    .routeGeneration = event.routeGeneration,
    .persistedVersion = persistedVersion,
  };
}

bool BoundedInMemoryActorRouter::evict(const ActorIdentity& actor, kj::StringPtr reason) {
  auto key = actor.canonicalKey();
  KJ_IF_SOME(index, find(key)) {
    evictIndex(index, reason);
    return true;
  }
  return false;
}

size_t BoundedInMemoryActorRouter::activeActorCount() const {
  return active.size();
}

}  // namespace workerd::server::sandbox_executor
