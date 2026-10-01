#pragma once

#include <workerd/api/basics.h>
#include <workerd/io/io-context.h>
#include <workerd/jsg/jsg.h>
#include <workerd/jsg/modules-new.h>
#include <workerd/jsg/ser.h>
#include <workerd/jsg/url.h>

namespace workerd::api {

// An implementation of the Web platform MessagePort API for same-isolate channels.
// MessagePorts always come in pairs. When a message is posted to
// one it is delivered to the other, and vice versa. When one port
// is closed both ports are closed.
//
// MessagePort transfer is intentionally local to an isolate. Serializing a port for
// RPC or persistence remains unsupported because that would require an executor host
// channel rather than the same-isolate entanglement implemented here.
//
// Remaining differences from the HTML MessagePort definition:
// - We do not emit the close event on entangled ports when one of them is GC'd.
// - We do not prevent a MessagePort from being garbage collected while it has
//   messages queued up.
// - Unlike the implementation in Node.js, not closing a MessagePort does not
//   prevent anything from exiting. It's best to close MessagePorts manually
//   but the current implementation does not require it.
class MessagePort final: public EventTarget {
 public:
  using TransferList = kj::Array<jsg::JsRef<jsg::JsValue>>;
  struct PostMessageOptions {
    jsg::Optional<TransferList> transfer;
    JSG_STRUCT(transfer);
  };
  using TransferListOrOptions = kj::OneOf<TransferList, PostMessageOptions>;

  explicit MessagePort(bool standardSemantics);
  ~MessagePort() noexcept(false) {
    closeImpl();
  }

  // MessagePort instances cannot be created directly.
  // Use `new MessageChannel()`
  static jsg::Ref<MessagePort> constructor() = delete;

  void postMessage(jsg::Lock& js,
      jsg::Optional<jsg::JsRef<jsg::JsValue>> data = kj::none,
      jsg::Optional<TransferListOrOptions> options = kj::none);
  void closeImpl();
  void close(jsg::Lock& js);
  void start(jsg::Lock& js);

  // The onmessage event handler IDL attribute
  // (see EventTarget::setEventHandlerAttribute).
  kj::Maybe<jsg::JsValue> getOnMessage(jsg::Lock& js);
  void setOnMessage(
      jsg::Lock& js, jsg::Optional<kj::OneOf<EventTarget::HandlerFunction, jsg::JsValue>> handler);
  kj::Maybe<jsg::JsValue> getOnMessageError(jsg::Lock& js);
  void setOnMessageError(
      jsg::Lock& js, jsg::Optional<kj::OneOf<EventTarget::HandlerFunction, jsg::JsValue>> handler);

  JSG_RESOURCE_TYPE(MessagePort) {
    JSG_INHERIT(EventTarget);
    JSG_METHOD(postMessage);
    JSG_METHOD(close);
    JSG_METHOD(start);
    JSG_PROTOTYPE_PROPERTY(onmessage, getOnMessage, setOnMessage);
    JSG_PROTOTYPE_PROPERTY(onmessageerror, getOnMessageError, setOnMessageError);
  }

  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  static jsg::Ref<MessagePort> deserialize(
      jsg::Lock& js, rpc::SerializationTag tag, jsg::Deserializer& deserializer);
  JSG_SERIALIZABLE(rpc::SerializationTag::MESSAGE_PORT);

  jsg::Ref<MessagePort> addRef() {
    return JSG_THIS;
  }
  bool isClosed() const {
    return state.is<Closed>() || state.is<Detached>();
  }

  void deliver(jsg::Lock& js,
      const jsg::JsValue& data,
      kj::Array<jsg::Ref<MessagePort>> transferredPorts = {});

  // Bind two message ports together such that messages posted to
  // one are delivered to the other.
  static void entangle(jsg::Lock& js, jsg::Ref<MessagePort>& port1, jsg::Ref<MessagePort>& port2);

  // TODO(soon): Support serialization/deserialization to use MessagePort
  // with JSRPC. We'll need to implement a rpc mechanism for passing the
  // messages across the rpc boundary.

 private:
  struct QueuedMessage {
    jsg::JsRef<jsg::JsValue> data;
    kj::Array<jsg::Ref<MessagePort>> ports;

    void visitForGc(jsg::GcVisitor& visitor) {
      visitor.visit(data);
      visitor.visitAll(ports);
    }
  };

  // When the MessagePort is in the pending state, messages posted to it
  // will be buffered until the port is started. When the port is started,
  // the buffered messages will be delivered immediately.
  using Pending = kj::Vector<QueuedMessage>;
  struct Started {};
  struct Closed {};
  struct Detached {
    kj::Maybe<jsg::WeakRef<MessagePort>> replacement;
  };

  void dispatchMessage(
      jsg::Lock& js, const jsg::JsValue& value, kj::Array<jsg::Ref<MessagePort>> transferredPorts);

  kj::OneOf<Pending, Started, Closed, Detached> state;
  bool standardSemantics;

  // Two ports are entangled when they weakly reference each other.
  // Keep in mind that this is a weak reference! So if one of the
  // ports gets GC'd the other will will also end up being closed.
  // To keep them both alive, maintain strong references to both
  // ports!
  kj::Maybe<jsg::WeakRef<MessagePort>> other;

  void listenerCountChanged(jsg::Lock& js, kj::StringPtr type, size_t count) override;

  void visitForGc(jsg::GcVisitor& visitor) {
    KJ_IF_SOME(pending, state.tryGet<Pending>()) {
      for (auto& message: pending) {
        message.visitForGc(visitor);
      }
    }
  }
};

// MessageChannel is simple enough... create a couple of MessagePorts
// and entangle those so that they will exchange messages with each
// other.
class MessageChannel final: public jsg::Object {
 public:
  MessageChannel(jsg::Ref<MessagePort> port1, jsg::Ref<MessagePort> port2)
      : port1(kj::mv(port1)),
        port2(kj::mv(port2)) {}

  static jsg::Ref<MessageChannel> constructor(jsg::Lock& js);

  jsg::Ref<MessagePort> getPort1() {
    return port1.addRef();
  }
  jsg::Ref<MessagePort> getPort2() {
    return port2.addRef();
  }

  JSG_RESOURCE_TYPE(MessageChannel) {
    JSG_LAZY_READONLY_INSTANCE_PROPERTY(port1, getPort1);
    JSG_LAZY_READONLY_INSTANCE_PROPERTY(port2, getPort2);
  }

 private:
  jsg::Ref<MessagePort> port1;
  jsg::Ref<MessagePort> port2;

  void visitForGc(jsg::GcVisitor& visitor) {
    visitor.visit(port1, port2);
  }
};

// Module that exposes MessageChannel and MessagePort for internal use by
// built-in modules like node:worker_threads without requiring the global
// expose_global_message_channel compat flag.
class MessageChannelModule final: public jsg::Object {
 public:
  MessageChannelModule() = default;
  MessageChannelModule(jsg::Lock&, const jsg::Url&) {}

  JSG_RESOURCE_TYPE(MessageChannelModule) {
    JSG_NESTED_TYPE(MessageChannel);
    JSG_NESTED_TYPE(MessagePort);
  }
};

template <class Registry>
void registerMessageChannelModule(Registry& registry, auto featureFlags) {
  registry.template addBuiltinModule<MessageChannelModule>(
      "cloudflare-internal:messagechannel", workerd::jsg::ModuleRegistry::Type::INTERNAL);
}

template <typename TypeWrapper>
kj::Own<jsg::modules::ModuleBundle> getInternalMessageChannelModuleBundle(auto featureFlags) {
  jsg::modules::ModuleBundle::BuiltinBuilder builder(
      jsg::modules::ModuleBundle::BuiltinBuilder::Type::BUILTIN_ONLY);
  static const auto kSpecifier = "cloudflare-internal:messagechannel"_url;
  builder.addObject<MessageChannelModule, TypeWrapper>(kSpecifier);
  return builder.finish();
}

}  // namespace workerd::api

#define EW_MESSAGECHANNEL_ISOLATE_TYPES                                                            \
  api::MessagePort, api::MessageChannel, api::MessagePort::PostMessageOptions,                     \
      api::MessageChannelModule
