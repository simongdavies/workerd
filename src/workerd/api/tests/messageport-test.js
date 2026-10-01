// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert';

import { mock } from 'node:test';

export const simple1 = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    ok(port1 instanceof MessagePort);
    ok(port2 instanceof MessagePort);
    port1.postMessage(1);
    port2.postMessage(1);
    const { promise, resolve } = Promise.withResolvers();
    const handler = mock.fn((event) => {
      strictEqual(event.data, 1);
      strictEqual(event.isTrusted, true);
      resolve();
    });
    port2.onmessage = handler;
    port1.onmessage = handler;
    await promise;
    strictEqual(handler.mock.callCount(), 2);
  },
};

export const simple2 = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    ok(port1 instanceof MessagePort);
    ok(port2 instanceof MessagePort);
    const { promise, resolve } = Promise.withResolvers();
    const handler = mock.fn((event) => {
      strictEqual(event.data, 1);
      resolve();
    });
    port2.onmessage = handler;
    port1.onmessage = handler;
    port1.postMessage(1);
    port2.postMessage(1);
    await promise;
    strictEqual(handler.mock.callCount(), 2);
  },
};

export const simple3 = {
  async test() {
    const { port1, port2 } = new MessageChannel();

    const localCloseHandler = mock.fn();
    const remoteCloseHandler = mock.fn();
    port1.onclose = localCloseHandler;
    port2.onclose = remoteCloseHandler;

    port1.close();
    port2.onmessage = () => {
      throw new Error('should not be called');
    };
    port1.postMessage('nope');
    await scheduler.wait(10);
    strictEqual(localCloseHandler.mock.callCount(), 0);
    strictEqual(remoteCloseHandler.mock.callCount(), 1);
  },
};

export const simple4 = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    port2.close();
    port2.onmessage = () => {
      throw new Error('should not be called');
    };
    port1.onmessage = () => {
      throw new Error('should not be called');
    };
    port1.postMessage('nope');
    port2.postMessage('nope');
    await scheduler.wait(10);
  },
};

export const simple5 = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    throws(() => port1.postMessage(1, [1]), {
      code: 25,
      name: 'DataCloneError',
    });
    throws(() => port1.postMessage(1, { transfer: [1] }), {
      code: 25,
      name: 'DataCloneError',
    });
    // If the lists are empty it is ok.
    port1.postMessage(1, []);
    port1.postMessage(1, { transfer: [] });

    const handler = mock.fn((event) => {
      strictEqual(event.data, 1);
    });
    port2.onmessage = handler;
    await scheduler.wait(10);
    strictEqual(handler.mock.callCount(), 2);
  },
};

// The following are focused adaptations of the authoritative webmessaging WPT coverage.
// Refs: https://github.com/web-platform-tests/wpt/blob/master/webmessaging/Channel_postMessage_Blob.any.js
// Refs: https://github.com/web-platform-tests/wpt/blob/master/webmessaging/Channel_postMessage_DataCloneErr.any.js

export const postMessageBlob = {
  async test() {
    // Per the spec, Blob is a serializable object, so it round-trips through postMessage.
    const { port1, port2 } = new MessageChannel();
    const { promise, resolve } = Promise.withResolvers();
    port2.onmessage = (event) => resolve(event.data);
    port1.postMessage(new Blob(['hello'], { type: 'text/plain' }));
    const received = await promise;
    ok(received instanceof Blob);
    strictEqual(received.type, 'text/plain');
    strictEqual(await received.text(), 'hello');
  },
};

export const postMessageRpcTarget = {
  async test() {
    const { RpcTarget } = await import('cloudflare:workers');
    class Foo extends RpcTarget {}

    const { port1 } = new MessageChannel();
    throws(() => port1.postMessage(new Foo()), {
      code: 25, // DATA_CLONE_ERR,
      name: 'DataCloneError',
    });
  },
};

// Cross-global and cross-site transfer tests still require Worker/Window realms that this
// same-isolate implementation does not provide. Garbage-collected peers also do not yet emit
// close events:
// * https://github.com/web-platform-tests/wpt/blob/master/webmessaging/message-channels/close-event/garbage-collected.tentative.any.js

// The onmessage handler occupies a normal position in the listener list based on when it
// was first assigned, per HTML's event handler semantics.
export const onmessagePositionalOrdering = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const order = [];
    const { promise, resolve } = Promise.withResolvers();

    port2.addEventListener('message', () => order.push('a'));
    const b1 = () => order.push('b1');
    port2.onmessage = b1;
    strictEqual(port2.onmessage, b1);
    port2.addEventListener('message', () => order.push('c'));

    // Reassignment keeps the original position.
    port2.onmessage = () => order.push('b2');

    port2.addEventListener('message', () => resolve());
    port1.postMessage('hello');
    await promise;
    deepStrictEqual(order, ['a', 'b2', 'c']);
  },
};

// Clearing onmessage and assigning it again takes a fresh position at the end of the
// listener list.
export const onmessageClearedTakesFreshPosition = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const order = [];
    const { promise, resolve } = Promise.withResolvers();

    port2.onmessage = () => order.push('handler1');
    port2.addEventListener('message', () => order.push('listener'));

    port2.onmessage = null;
    strictEqual(port2.onmessage, null);
    port2.onmessage = () => order.push('handler2');

    port2.addEventListener('message', () => resolve());
    port1.postMessage('hello');
    await promise;
    deepStrictEqual(order, ['listener', 'handler2']);
  },
};

// Assigning a non-callable object to onmessage retains it as the attribute value and
// enables the port's message queue, but the object is never invoked: messages delivered
// while it is assigned are consumed and dropped.
export const onmessageNonCallableStartsPort = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const obj = {};
    port2.onmessage = obj;
    strictEqual(port2.onmessage, obj);
    port1.postMessage('lost');
    await scheduler.wait(10);

    const { promise, resolve } = Promise.withResolvers();
    port2.onmessage = (event) => resolve(event.data);
    port1.postMessage('kept');
    strictEqual(await promise, 'kept');
  },
};

// addEventListener() alone does not enable the port message queue.
export const addEventListenerDoesNotStartPort = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const handler = mock.fn();
    port2.addEventListener('message', handler);
    port1.postMessage('hello');
    await scheduler.wait(10);
    strictEqual(handler.mock.callCount(), 0);

    port2.start();
    await scheduler.wait(10);
    strictEqual(handler.mock.callCount(), 1);
    strictEqual(handler.mock.calls[0].arguments[0].data, 'hello');
  },
};

export const nullOnmessageDoesNotStartPort = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const handler = mock.fn();
    port2.addEventListener('message', handler);
    port1.postMessage('queued');
    port2.onmessage = null;
    await scheduler.wait(10);
    strictEqual(handler.mock.callCount(), 0);

    port2.start();
    await scheduler.wait(10);
    strictEqual(handler.mock.callCount(), 1);
  },
};

// Once enabled, a port remains enabled after its last listener is removed.
export const removingLastListenerDoesNotDisablePort = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const first = Promise.withResolvers();
    const handler = (event) => first.resolve(event.data);
    port2.addEventListener('message', handler);
    port2.start();
    port1.postMessage('one');
    strictEqual(await first.promise, 'one');

    port2.removeEventListener('message', handler);
    port1.postMessage('discarded');
    await scheduler.wait(10);

    const second = mock.fn();
    port2.addEventListener('message', second);
    await scheduler.wait(10);
    strictEqual(second.mock.callCount(), 0);

    port1.postMessage('two');
    await scheduler.wait(10);
    strictEqual(second.mock.callCount(), 1);
    strictEqual(second.mock.calls[0].arguments[0].data, 'two');
  },
};

// A once-listener's removal does not disable a port that was explicitly started.
export const onceListenerLeavesPortStarted = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const first = Promise.withResolvers();
    port2.addEventListener('message', (event) => first.resolve(event.data), {
      once: true,
    });
    port2.start();
    port1.postMessage('one');
    strictEqual(await first.promise, 'one');

    port1.postMessage('discarded');
    await scheduler.wait(10);

    const second = mock.fn();
    port2.addEventListener('message', second);
    await scheduler.wait(10);
    strictEqual(second.mock.callCount(), 0);
  },
};

// A closed port is terminal: attaching listeners or manipulating onmessage afterwards
// never restarts it, and no messages are delivered.
export const closedPortIsTerminal = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    port1.postMessage('queued');
    port2.close();

    const handler = mock.fn();
    port2.onmessage = null;
    port2.onmessage = handler;
    port2.addEventListener('message', handler);
    port1.postMessage('late');
    await scheduler.wait(10);
    strictEqual(handler.mock.callCount(), 0);
  },
};

// A throwing 'message' listener is reported and remaining listeners still run. messageerror is
// reserved for failures while deserializing a received message.
export const throwingMessageListener = {
  async test() {
    const order = [];
    const boom = new Error('port boom');
    const { port1, port2 } = new MessageChannel();
    const globalHandler = () => {
      order.push('global-error');
      // Injecting a message mid-report cannot jump the queue: delivery is always deferred
      // to a later microtask, so it arrives after the current event's remaining listeners
      // and after any messages queued before it.
      port1.postMessage('injected');
    };
    globalThis.addEventListener('error', globalHandler);
    try {
      const done = Promise.withResolvers();
      port2.addEventListener('message', (event) => {
        order.push(`l1:${event.data}`);
        if (event.data === 'bad') throw boom;
        if (event.data === 'injected') done.resolve();
      });
      port2.addEventListener('message', (event) =>
        order.push(`l2:${event.data}`)
      );
      port2.addEventListener('messageerror', (event) => {
        order.push('messageerror');
      });
      port2.start();

      port1.postMessage('bad');
      port1.postMessage('after');
      await done.promise;
      deepStrictEqual(order, [
        'l1:bad',
        'global-error',
        'l2:bad',
        'l1:after',
        'l2:after',
        'l1:injected',
        'l2:injected',
      ]);
    } finally {
      globalThis.removeEventListener('error', globalHandler);
    }
  },
};

export const onmessageerrorAttribute = {
  test() {
    const { port1 } = new MessageChannel();
    const handler = mock.fn();
    port1.onmessageerror = handler;
    strictEqual(port1.onmessageerror, handler);
    port1.dispatchEvent(
      new MessageEvent('messageerror', { data: 'bad clone' })
    );
    strictEqual(handler.mock.callCount(), 1);
    strictEqual(handler.mock.calls[0].arguments[0].data, 'bad clone');
  },
};

export const transferredPortIsClonedAndEntangled = {
  async test() {
    const channelA = new MessageChannel();
    const channelB = new MessageChannel();
    const original = channelB.port2;

    const received = Promise.withResolvers();
    channelA.port2.onmessage = (event) => received.resolve(event);
    channelA.port1.postMessage('ports', [original]);

    const event = await received.promise;
    strictEqual(event.data, 'ports');
    strictEqual(event.ports.length, 1);
    strictEqual(event.ports, event.ports);
    const clone = event.ports[0];
    ok(clone instanceof MessagePort);
    ok(clone !== original);
    throws(() => event.ports.push(new MessageChannel().port1), TypeError);

    const ping = Promise.withResolvers();
    clone.onmessage = (message) => ping.resolve(message.data);
    channelB.port1.postMessage('ping');
    strictEqual(await ping.promise, 'ping');

    const ignored = mock.fn();
    original.onmessage = ignored;
    channelB.port1.postMessage('clone-only');
    await scheduler.wait(10);
    strictEqual(ignored.mock.callCount(), 0);
  },
};

export const transferredPortInPayloadUsesClone = {
  async test() {
    const carrier = new MessageChannel();
    const transferred = new MessageChannel();
    const received = Promise.withResolvers();
    carrier.port2.onmessage = (event) => received.resolve(event);

    carrier.port1.postMessage({ port: transferred.port2 }, [transferred.port2]);
    const event = await received.promise;
    strictEqual(event.data.port, event.ports[0]);

    const response = Promise.withResolvers();
    transferred.port1.onmessage = (message) => response.resolve(message.data);
    event.data.port.postMessage({ nested: ['ok', 42] });
    deepStrictEqual(await response.promise, { nested: ['ok', 42] });
  },
};

export const transferValidationDataCloneErrors = {
  test() {
    const source = new MessageChannel();
    const transferable = new MessageChannel();

    throws(
      () =>
        source.port1.postMessage('duplicate', [
          transferable.port1,
          transferable.port1,
        ]),
      { code: 25, name: 'DataCloneError' }
    );
    throws(() => source.port1.postMessage('source', [source.port1]), {
      code: 25,
      name: 'DataCloneError',
    });
    throws(() => source.port1.postMessage('view', [new Uint8Array(4)]), {
      code: 25,
      name: 'DataCloneError',
    });
    throws(() => source.port1.postMessage({ port: transferable.port1 }), {
      code: 25,
      name: 'DataCloneError',
    });
    throws(() => structuredClone(transferable.port1), {
      code: 25,
      name: 'DataCloneError',
    });

    const closed = new MessageChannel();
    closed.port1.close();
    throws(() => source.port1.postMessage('closed', [closed.port1]), {
      code: 25,
      name: 'DataCloneError',
    });

    source.port1.postMessage('first transfer', [transferable.port1]);
    throws(() => source.port1.postMessage('detached', [transferable.port1]), {
      code: 25,
      name: 'DataCloneError',
    });
  },
};

export const arrayBufferTransferAndStructuredClone = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const buffer = new Uint8Array([1, 2, 3, 4]).buffer;
    const cyclic = { date: new Date(123456), map: new Map([['key', 7]]) };
    cyclic.self = cyclic;
    cyclic.buffer = buffer;

    const received = Promise.withResolvers();
    port2.onmessage = (event) => received.resolve(event.data);
    port1.postMessage(cyclic, [buffer]);
    strictEqual(buffer.byteLength, 0);

    const clone = await received.promise;
    strictEqual(clone.self, clone);
    strictEqual(clone.date.getTime(), 123456);
    strictEqual(clone.map.get('key'), 7);
    deepStrictEqual([...new Uint8Array(clone.buffer)], [1, 2, 3, 4]);
  },
};

export const queuedTransferredPortSurvivesStartAndGc = {
  async test() {
    const carrier = new MessageChannel();
    const payload = new MessageChannel();
    carrier.port1.postMessage('port', [payload.port2]);

    for (let i = 0; i < 10; ++i) gc();

    const received = Promise.withResolvers();
    carrier.port2.onmessage = (event) => received.resolve(event.ports[0]);
    const clone = await received.promise;

    const ping = Promise.withResolvers();
    clone.onmessage = (event) => ping.resolve(event.data);
    payload.port1.postMessage('alive');
    strictEqual(await ping.promise, 'alive');
  },
};

export const transferPreservesIncomingMessageOrder = {
  async test() {
    const channel1 = new MessageChannel();
    const channel2 = new MessageChannel();
    const channel3 = new MessageChannel();

    channel1.port2.postMessage('First');
    channel2.port1.postMessage('1', [channel1.port1]);
    const secondTransfer = Promise.withResolvers();
    channel2.port2.onmessage = (event) => {
      channel1.port2.postMessage('Second');
      channel1.port2.postMessage('Third');
      channel3.port2.postMessage('2', event.ports);
    };
    channel3.port1.onmessage = (event) =>
      secondTransfer.resolve(event.ports[0]);

    const port = await secondTransfer.promise;
    const messages = [];
    const done = Promise.withResolvers();
    port.onmessage = (event) => {
      messages.push(event.data);
      if (messages.length === 4) done.resolve();
    };
    channel1.port2.postMessage('Fourth');
    await done.promise;
    deepStrictEqual(messages, ['First', 'Second', 'Third', 'Fourth']);
  },
};

export const transferPreservesOutgoingMessageOrder = {
  async test() {
    const channel1 = new MessageChannel();
    const channel2 = new MessageChannel();
    const channel3 = new MessageChannel();

    channel2.port2.onmessage = (event) => {
      event.ports[0].postMessage('Second');
      event.ports[0].postMessage('Third');
      channel3.port2.postMessage('2', event.ports);
    };
    channel3.port1.onmessage = (event) => event.ports[0].postMessage('Fourth');

    channel1.port1.postMessage('First');
    channel2.port1.postMessage('1', [channel1.port1]);

    const messages = [];
    const done = Promise.withResolvers();
    channel1.port2.onmessage = (event) => {
      messages.push(event.data);
      if (messages.length === 4) done.resolve();
    };
    await done.promise;
    deepStrictEqual(messages, ['First', 'Second', 'Third', 'Fourth']);
  },
};

export const transferringEntangledPairRewiresBothClones = {
  async test() {
    const carrier = new MessageChannel();
    const pair = new MessageChannel();
    const received = Promise.withResolvers();
    carrier.port2.onmessage = (event) => received.resolve(event.ports);

    carrier.port1.postMessage('pair', [pair.port1, pair.port2]);
    const [port1, port2] = await received.promise;

    const message = Promise.withResolvers();
    port2.onmessage = (event) => message.resolve(event.data);
    port1.postMessage('still entangled');
    strictEqual(await message.promise, 'still entangled');
  },
};

export const separatelyTransferredEntangledPortsStayOrdered = {
  async test() {
    const pair = new MessageChannel();
    const carrier = new MessageChannel();
    pair.port1.postMessage(1);
    carrier.port1.postMessage('first', [pair.port1]);
    carrier.port1.postMessage('second', [pair.port2]);

    const transferred = [];
    const ready = Promise.withResolvers();
    carrier.port2.onmessage = (event) => {
      transferred.push(event.ports[0]);
      if (transferred.length === 2) ready.resolve();
    };
    await ready.promise;

    const [sender, receiver] = transferred;
    sender.postMessage(2);
    sender.postMessage(3);
    const messages = [];
    const done = Promise.withResolvers();
    receiver.onmessage = (event) => {
      messages.push(event.data);
      if (messages.length === 3) done.resolve();
    };
    await done.promise;
    deepStrictEqual(messages, [1, 2, 3]);
  },
};

export const closeCancelsScheduledDelivery = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const handler = mock.fn();
    port2.onmessage = handler;
    port1.postMessage('discarded');
    port2.close();
    await scheduler.wait(10);
    strictEqual(handler.mock.callCount(), 0);
  },
};

// User-constructed MessageEvents reflect the source and ports passed in their init.
export const messageEventSourceAndPorts = {
  test() {
    const { port1, port2 } = new MessageChannel();
    const event = new MessageEvent('message', {
      data: 'x',
      source: port1,
      ports: [port1, port2],
    });
    strictEqual(event.source, port1);
    const ports = event.ports;
    strictEqual(ports.length, 2);
    strictEqual(ports[0], port1);
    strictEqual(ports[1], port2);

    // Runtime-delivered MessagePort events have a null source and no transferred ports.
    const { promise, resolve } = Promise.withResolvers();
    port2.onmessage = (e) => resolve(e);
    port1.postMessage('hi');
    return promise.then((e) => {
      strictEqual(e.source, null);
      strictEqual(e.ports.length, 0);
      strictEqual(e.origin, null);
    });
  },
};
