// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
import { strictEqual, throws } from 'node:assert';

import { mock } from 'node:test';

export const transferListsRemainDisabled = {
  test() {
    const { port1 } = new MessageChannel();
    const transferred = new MessageChannel();
    throws(() => port1.postMessage('port', [transferred.port1]), {
      message: 'Transfer list is not supported',
    });
  },
};

export const addEventListenerStartsAndRemovingLastListenerStops = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const first = Promise.withResolvers();
    const listener = (event) => first.resolve(event.data);
    port2.addEventListener('message', listener);
    port1.postMessage('one');
    strictEqual(await first.promise, 'one');

    port2.removeEventListener('message', listener);
    port1.postMessage('two');
    await scheduler.wait(10);

    const second = Promise.withResolvers();
    port2.addEventListener('message', (event) => second.resolve(event.data));
    strictEqual(await second.promise, 'two');
  },
};

export const closeEventFiresOnBothPorts = {
  test() {
    const { port1, port2 } = new MessageChannel();
    const handler = mock.fn();
    port1.onclose = handler;
    port2.onclose = handler;
    port1.close();
    strictEqual(handler.mock.callCount(), 2);
  },
};

export const messageEventPortsRemainMutableSnapshots = {
  test() {
    const { port1 } = new MessageChannel();
    const event = new MessageEvent('message', { ports: [port1] });
    const first = event.ports;
    const second = event.ports;
    strictEqual(first === second, false);
    first.push(new MessageChannel().port1);
    strictEqual(first.length, 2);
    strictEqual(second.length, 1);
  },
};

export const runtimeMessageSourceRemainsReceivingPort = {
  async test() {
    const { port1, port2 } = new MessageChannel();
    const received = Promise.withResolvers();
    port2.onmessage = (event) => received.resolve(event);
    port1.postMessage('legacy source');
    strictEqual((await received.promise).source, port2);
  },
};
