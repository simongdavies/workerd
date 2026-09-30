#include "messagechannel.h"

#include "blob.h"
#include "events.h"

#include <workerd/io/features.h>
#include <workerd/io/worker.h>
#include <workerd/jsg/ser.h>

namespace workerd::api {
namespace {

class MessagePortSerializerHandler final: public jsg::Serializer::ExternalHandler {
 public:
  void add(jsg::Ref<MessagePort> port) {
    ports.add(kj::mv(port));
  }

  uint32_t getIndex(MessagePort& port) {
    for (uint32_t i = 0; i < ports.size(); ++i) {
      if (ports[i].get() == &port) {
        return i;
      }
    }
    JSG_FAIL_REQUIRE(DOMDataCloneError, "MessagePort must be listed in the transfer list.");
  }

 private:
  kj::Vector<jsg::Ref<MessagePort>> ports;
};

class MessagePortDeserializerHandler final: public jsg::Deserializer::ExternalHandler {
 public:
  explicit MessagePortDeserializerHandler(kj::ArrayPtr<jsg::Ref<MessagePort>> ports)
      : ports(ports) {}

  jsg::Ref<MessagePort> get(uint32_t index) {
    JSG_REQUIRE(index < ports.size(), DOMDataCloneError,
        "MessagePort transfer index is outside the transfer list.");
    return ports[index].addRef();
  }

 private:
  kj::ArrayPtr<jsg::Ref<MessagePort>> ports;
};

struct PortTransfer {
  jsg::Ref<MessagePort> port;
  jsg::Ref<MessagePort> peer;
};

kj::Array<jsg::Ref<MessagePort>> addRefs(kj::ArrayPtr<jsg::Ref<MessagePort>> ports) {
  kj::Vector<jsg::Ref<MessagePort>> result;
  result.reserve(ports.size());
  for (auto& port: ports) {
    result.add(port.addRef());
  }
  return result.releaseAsArray();
}

}  // namespace

MessagePort::MessagePort(bool standardSemantics)
    : state(Pending()),
      standardSemantics(standardSemantics) {}

// Tracks 'message' listener registrations — both addEventListener() listeners and the
// onmessage attribute's trampoline — to transition the port between states: the first
// listener starts the port (delivering any queued messages), and removing the last one
// returns it to pending (queueing messages again). Counting every listener is technically
// not spec compliant (per spec only assigning onmessage enables the message queue), but it
// is what Node.js does.
void MessagePort::listenerCountChanged(jsg::Lock& js, kj::StringPtr type, size_t count) {
  if (standardSemantics || type != "message"_kj) {
    return;
  }
  KJ_SWITCH_ONEOF(state) {
    KJ_CASE_ONEOF(pending, Pending) {
      if (count > 0) {
        start(js);
      }
    }
    KJ_CASE_ONEOF(started, Started) {
      if (count == 0) {
        state = Pending();
      }
    }
    KJ_CASE_ONEOF(_, Closed) {
      // Closed is terminal: listener changes never restart the port.
    }
    KJ_CASE_ONEOF(_, Detached) {
      // Detached is terminal.
    }
  }
}

void MessagePort::dispatchMessage(
    jsg::Lock& js, const jsg::JsValue& value, kj::Array<jsg::Ref<MessagePort>> transferredPorts) {
  KJ_IF_SOME(detached, state.tryGet<Detached>()) {
    KJ_IF_SOME(replacement, detached.replacement) {
      KJ_IF_SOME(ref, replacement.tryAddRef(js)) {
        ref->dispatchMessage(js, value, kj::mv(transferredPorts));
      }
    }
    return;
  }
  if (standardSemantics && state.is<Closed>()) {
    return;
  }

  auto policy = standardSemantics ? DispatchExceptionPolicy::REPORT
                                  : effectiveExceptionPolicy(js, DispatchExceptionPolicy::REPORT);
  if (policy == DispatchExceptionPolicy::PROPAGATE) {
    // Compat path (spec_compliant_dispatch_exceptions disabled): the first throwing
    // listener ends the dispatch; the exception is swallowed and re-dispatched as a second
    // 'message' event carrying the exception as its data. (The spec path below uses a
    // 'messageerror' event instead; the 'message' type here is retained for compatibility.)
    // If that second dispatch throws, the exception propagates: the delivery microtask
    // fails.
    JSG_TRY(js) {
      dispatchEventImpl(js,
          js.alloc<MessageEvent>(
              js, value, kj::String(), JSG_THIS, kj::none, Trusted::YES, kj::mv(transferredPorts)));
    }
    JSG_CATCH(exception) {
      dispatchEventImpl(js,
          js.alloc<MessageEvent>(js, jsg::JsValue(exception.getHandle(js)), kj::String(), JSG_THIS,
              kj::none, Trusted::YES));
    }
    return;
  }

  kj::Maybe<jsg::Ref<MessagePort>> source;
  if (!standardSemantics) {
    source = JSG_THIS;
  }
  auto result = dispatchEventImpl(js,
      js.alloc<MessageEvent>(js, value, kj::String(), kj::mv(source), kj::none, Trusted::YES,
          kj::mv(transferredPorts)),
      policy);
  KJ_IF_SOME(exception, result.firstException) {
    if (!standardSemantics) {
      // Compatibility behavior: listener exceptions were historically re-dispatched as a
      // messageerror event carrying the exception.
      dispatchEventImpl(js,
          js.alloc<MessageEvent>(js, kj::str("messageerror"), exception.addRef(js), kj::String(),
              JSG_THIS, kj::none, Trusted::YES),
          DispatchExceptionPolicy::REPORT);
    }
  }
}

// Deliver the message to this port, buffering if necessary if the port
// has not been started. Buffered messages will be delivered when the
// port is started later.
void MessagePort::deliver(
    jsg::Lock& js, const jsg::JsValue& value, kj::Array<jsg::Ref<MessagePort>> transferredPorts) {
  KJ_SWITCH_ONEOF(state) {
    KJ_CASE_ONEOF(pending, Pending) {
      pending.add(QueuedMessage{
        .data = jsg::JsRef(js, value),
        .ports = kj::mv(transferredPorts),
      });
    }
    KJ_CASE_ONEOF(started, Started) {
      js.resolvedPromise().then(js,
          [self = JSG_THIS, value = jsg::JsRef(js, value), ports = kj::mv(transferredPorts)](
              jsg::Lock& js) mutable {
        self->dispatchMessage(js, value.getHandle(js), addRefs(ports));
      });
    }
    KJ_CASE_ONEOF(_, Closed) {
      // Drop the message.
    }
    KJ_CASE_ONEOF(_, Detached) {
      // Drop the message.
    }
  }
}

// Binds two ports to each other such that messages posted to one
// are delivered on the other.
void MessagePort::entangle(
    jsg::Lock& js, jsg::Ref<MessagePort>& port1, jsg::Ref<MessagePort>& port2) {
  port1->other = port2.getWeakRef(js);
  port2->other = port1.getWeakRef(js);
}

// Post a message to the entangled port.
void MessagePort::postMessage(jsg::Lock& js,
    jsg::Optional<jsg::JsRef<jsg::JsValue>> data,
    jsg::Optional<TransferListOrOptions> options) {
  TransferList transferList;
  KJ_SWITCH_ONEOF(kj::mv(options).orDefault(PostMessageOptions{})) {
    KJ_CASE_ONEOF(list, TransferList) {
      transferList = kj::mv(list);
    }
    KJ_CASE_ONEOF(opts, PostMessageOptions) {
      transferList = kj::mv(opts.transfer).orDefault({});
    }
  }

  if (!standardSemantics && transferList.size() > 0) {
    JSG_FAIL_REQUIRE(Error, "Transfer list is not supported");
  }

  MessagePortSerializerHandler portSerializer;
  kj::Vector<PortTransfer> portTransfers;
  kj::Vector<jsg::JsRef<jsg::JsValue>> arrayBufferTransfers;
  kj::Vector<jsg::JsRef<jsg::JsValue>> seen;

  for (auto& item: transferList) {
    auto value = item.getHandle(js);
    for (auto& previous: seen) {
      JSG_REQUIRE(!value.strictEquals(previous.getHandle(js)), DOMDataCloneError,
          "Transfer list contains duplicate entries.");
    }
    seen.add(item.addRef(js));

    KJ_IF_SOME(object, value.tryCast<jsg::JsObject>()) {
      KJ_IF_SOME(port, object.tryUnwrapAs<MessagePort>(js)) {
        JSG_REQUIRE(port.get() != this, DOMDataCloneError,
            "A MessagePort cannot transfer itself through postMessage().");
        JSG_REQUIRE(!port->isClosed(), DOMDataCloneError,
            "A closed or detached MessagePort cannot be transferred.");

        auto& peer = JSG_REQUIRE_NONNULL(
            port->other, DOMDataCloneError, "A disentangled MessagePort cannot be transferred.");
        auto peerRef = JSG_REQUIRE_NONNULL(peer.tryAddRef(js), DOMDataCloneError,
            "A disentangled MessagePort cannot be transferred.");

        portSerializer.add(port.addRef());
        portTransfers.add(PortTransfer{
          .port = kj::mv(port),
          .peer = kj::mv(peerRef),
        });
        continue;
      }
    }

    JSG_REQUIRE(
        value.isArrayBuffer(), DOMDataCloneError, "Transfer list entry is not transferable.");
    arrayBufferTransfers.add(item.addRef(js));
  }

  jsg::Serializer ser(js, {.externalHandler = portSerializer});
  for (auto& item: arrayBufferTransfers) {
    ser.transfer(js, item.getHandle(js));
  }
  KJ_IF_SOME(d, data) {
    ser.write(js, d.getHandle(js));
  } else {
    ser.write(js, js.undefined());
  }

  // Serialization can execute user code through getters. Revalidate every port before committing
  // any transfer so a nested close() or postMessage() cannot leave a partially-transferred list.
  for (auto& transfer: portTransfers) {
    JSG_REQUIRE(!transfer.port->isClosed(), DOMDataCloneError,
        "A closed or detached MessagePort cannot be transferred.");
    auto& peer = JSG_REQUIRE_NONNULL(transfer.port->other, DOMDataCloneError,
        "A disentangled MessagePort cannot be transferred.");
    auto peerRef = JSG_REQUIRE_NONNULL(
        peer.tryAddRef(js), DOMDataCloneError, "A disentangled MessagePort cannot be transferred.");
    JSG_REQUIRE(peerRef.get() == transfer.peer.get(), DOMDataCloneError,
        "MessagePort entanglement changed while serializing.");
  }

  auto released = ser.release();
  JSG_REQUIRE(released.sharedArrayBuffers.size() == 0, TypeError,
      "SharedArrayBuffer is unsupported with MessagePort");

  kj::Vector<jsg::Ref<MessagePort>> transferredPortBuilder;
  transferredPortBuilder.reserve(portTransfers.size());
  for (auto& transfer: portTransfers) {
    transferredPortBuilder.add(js.alloc<MessagePort>(transfer.port->standardSemantics));
  }
  for (uint32_t i = 0; i < portTransfers.size(); ++i) {
    auto& transfer = portTransfers[i];
    auto& clone = transferredPortBuilder[i];
    KJ_SWITCH_ONEOF(transfer.port->state) {
      KJ_CASE_ONEOF(pending, Pending) {
        Pending clonedPending;
        for (auto& message: pending) {
          clonedPending.add(QueuedMessage{
            .data = message.data.addRef(js),
            .ports = addRefs(message.ports),
          });
        }
        clone->state = kj::mv(clonedPending);
      }
      KJ_CASE_ONEOF(_, Started) {
        clone->state = Started{};
      }
      KJ_CASE_ONEOF(_, Closed) {
        KJ_UNREACHABLE;
      }
      KJ_CASE_ONEOF(_, Detached) {
        KJ_UNREACHABLE;
      }
    }
    transfer.port->state = Detached{clone.getWeakRef(js)};
    transfer.port->other = kj::none;
  }

  for (uint32_t i = 0; i < portTransfers.size(); ++i) {
    auto& transfer = portTransfers[i];
    auto& clone = transferredPortBuilder[i];
    jsg::Ref<MessagePort> peer = transfer.peer.addRef();
    bool peerWasTransferred = false;
    for (uint32_t j = 0; j < portTransfers.size(); ++j) {
      if (portTransfers[j].port.get() == transfer.peer.get()) {
        peer = transferredPortBuilder[j].addRef();
        peerWasTransferred = true;
        break;
      }
    }

    clone->other = peer.getWeakRef(js);
    if (!peerWasTransferred) {
      peer->other = clone.getWeakRef(js);
    }
  }
  auto transferredPorts = transferredPortBuilder.releaseAsArray();

  // If this port was closed by user code during serialization, the cloned message is dropped.
  if (isClosed()) {
    return;
  }

  KJ_IF_SOME(o, other) {
    KJ_IF_SOME(ref, o.tryAddRef(js)) {
      MessagePortDeserializerHandler portDeserializer(transferredPorts.asPtr());
      jsg::Deserializer deserializer(
          js, released, jsg::Deserializer::Options{.externalHandler = portDeserializer});
      auto clonedData = deserializer.readValue(js);
      ref->deliver(js, clonedData, kj::mv(transferredPorts));
    }
  }
}

void MessagePort::serialize(jsg::Lock& js, jsg::Serializer& serializer) {
  KJ_IF_SOME(externalHandler, serializer.getExternalHandler()) {
    auto handler = dynamic_cast<MessagePortSerializerHandler*>(&externalHandler);
    if (handler != nullptr) {
      serializer.writeRawUint32(handler->getIndex(*this));
      return;
    }
  }
  JSG_FAIL_REQUIRE(
      DOMDataCloneError, "MessagePort serialization requires a same-isolate transfer context.");
}

jsg::Ref<MessagePort> MessagePort::deserialize(
    jsg::Lock& js, rpc::SerializationTag tag, jsg::Deserializer& deserializer) {
  KJ_IF_SOME(externalHandler, deserializer.getExternalHandler()) {
    auto handler = dynamic_cast<MessagePortDeserializerHandler*>(&externalHandler);
    if (handler != nullptr) {
      return handler->get(deserializer.readRawUint32());
    }
  }
  JSG_FAIL_REQUIRE(
      DOMDataCloneError, "MessagePort deserialization requires a same-isolate transfer context.");
}

void MessagePort::closeImpl() {
  // Pending messages are dropped. Standard-semantics ports also cancel messages that were
  // already scheduled by start() or deliver(); legacy ports preserve that historical delivery.
  if (state.is<Closed>() || state.is<Detached>()) return;
  state = Closed{};
  KJ_IF_SOME(o, other) {
    // Use of tryGet here rather than tryAddRef is intentional. closeImpl
    // is called from the destructor, where we may or may not have the
    // isolate lock. Materializing a strong reference to the other port
    // requires the isolate lock. The other = kj::none line below will
    // ensure that the jsg::WeakRef is cleaned up under lock either
    // immediately or eventually.
    KJ_IF_SOME(ref, o.tryGet()) {
      ref.closeImpl();
    }
    other = kj::none;
  }
}

void MessagePort::close(jsg::Lock& js) {
  static constexpr kj::StringPtr name = "close"_kj;
  if (state.is<Closed>() || state.is<Detached>()) return;
  state = Closed{};

  if (standardSemantics) {
    KJ_IF_SOME(o, other) {
      KJ_IF_SOME(ref, o.tryAddRef(js)) {
        if (!ref->state.is<Closed>() && !ref->state.is<Detached>()) {
          ref->state = Closed{};
          ref->other = kj::none;
          auto closeEvent = js.alloc<Event>(name, Event::Init{}, Trusted::YES);
          ref->dispatchEventImpl(js, kj::mv(closeEvent));
        }
      }
      other = kj::none;
    }
    return;
  }

  KJ_IF_SOME(o, other) {
    KJ_IF_SOME(ref, o.tryAddRef(js)) {
      ref->close(js);
    }
    other = kj::none;
  }
  auto closeEvent = js.alloc<Event>(name, Event::Init{}, Trusted::YES);
  dispatchEventImpl(js, kj::mv(closeEvent));
}

// Start delivering messages on this port. Any messages that are
// buffered will be drained immediately.
void MessagePort::start(jsg::Lock& js) {
  KJ_SWITCH_ONEOF(state) {
    KJ_CASE_ONEOF(pending, Pending) {
      Pending list;
      for (auto& message: pending) {
        list.add(QueuedMessage{
          .data = message.data.addRef(js),
          .ports = addRefs(message.ports),
        });
      }
      state = Started{};
      // We're going to dispatch the messages using a microtask so that the actual
      // delivery is deferred to match Node.js' behavior as close as possible.
      js.resolvedPromise().then(js, [list = kj::mv(list), self = JSG_THIS](jsg::Lock& js) mutable {
        for (auto& item: list) {
          self->dispatchMessage(js, item.data.getHandle(js), addRefs(item.ports));
        }
      });
    }
    KJ_CASE_ONEOF(_, Started) {
      // Nothing to do in this case. We are already started!
    }
    KJ_CASE_ONEOF(_, Closed) {
      // Nothing to do in this case. Can't start after closing.
    }
    KJ_CASE_ONEOF(_, Detached) {
      // Nothing to do in this case. Can't start after transfer.
    }
  }
}

kj::Maybe<jsg::JsValue> MessagePort::getOnMessage(jsg::Lock& js) {
  return getEventHandlerAttribute(js, "message"_kj);
}

void MessagePort::setOnMessage(
    jsg::Lock& js, jsg::Optional<kj::OneOf<EventTarget::HandlerFunction, jsg::JsValue>> handler) {
  auto assignment = setEventHandlerAttribute(js, "message"_kj, kj::mv(handler));
  if (standardSemantics && assignment != EventHandlerAssignment::CLEARED) {
    start(js);
  }
}

kj::Maybe<jsg::JsValue> MessagePort::getOnMessageError(jsg::Lock& js) {
  return getEventHandlerAttribute(js, "messageerror"_kj);
}

void MessagePort::setOnMessageError(
    jsg::Lock& js, jsg::Optional<kj::OneOf<EventTarget::HandlerFunction, jsg::JsValue>> handler) {
  setEventHandlerAttribute(js, "messageerror"_kj, kj::mv(handler));
}

jsg::Ref<MessageChannel> MessageChannel::constructor(jsg::Lock& js) {
  auto flags = Worker::Isolate::from(js).getApi().getFeatureFlags();
  auto standardSemantics = flags.getMessagePortStandardSemantics();
  auto port1 = js.alloc<MessagePort>(standardSemantics);
  auto port2 = js.alloc<MessagePort>(standardSemantics);
  MessagePort::entangle(js, port1, port2);
  return js.alloc<MessageChannel>(kj::mv(port1), kj::mv(port2));
}

}  // namespace workerd::api
