// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

/* global onerror:writable, onunhandledrejection:writable, onrejectionhandled:writable */

import { deepStrictEqual, ok, strictEqual } from 'node:assert';

const enabled =
  Cloudflare.compatibilityFlags.worker_global_scope_event_handlers;

export const compatibilityGate = {
  test() {
    strictEqual('onerror' in globalThis, enabled);
    strictEqual('onunhandledrejection' in globalThis, enabled);
    strictEqual('onrejectionhandled' in globalThis, enabled);
    if (enabled) {
      strictEqual(globalThis.onerror, null);
      strictEqual(globalThis.onunhandledrejection, null);
      strictEqual(globalThis.onrejectionhandled, null);
    } else {
      strictEqual(globalThis.onerror, undefined);
      strictEqual(globalThis.onunhandledrejection, undefined);
      strictEqual(globalThis.onrejectionhandled, undefined);
    }
  },
};

export const compatibilityGatesCancellation = {
  async test() {
    let errorEvent;
    const errorListener = (event) => {
      errorEvent = event;
      event.preventDefault();
    };
    addEventListener('error', errorListener, { once: true });
    reportError(new Error('compatibility cancellation'));
    strictEqual(errorEvent.cancelable, enabled);
    strictEqual(errorEvent.defaultPrevented, enabled);

    const unhandled = Promise.withResolvers();
    addEventListener(
      'unhandledrejection',
      (event) => {
        event.preventDefault();
        unhandled.resolve(event);
      },
      { once: true }
    );
    Promise.reject('compatibility cancellation');
    const rejectionEvent = await unhandled.promise;
    strictEqual(rejectionEvent.cancelable, enabled);
    strictEqual(rejectionEvent.defaultPrevented, enabled);
  },
};

export const errorHandlerAttribute = {
  test() {
    if (!enabled) return;

    const order = [];
    let observedEvent;
    const before = () => {
      order.push('before');
    };
    const after = (event) => {
      order.push('after');
      observedEvent = event;
    };
    addEventListener('error', before);
    const first = () => {
      throw new Error('replaced handler must not run');
    };
    onerror = first;
    strictEqual(onerror, first);
    const replacement = function (message, source, lineno, colno, error) {
      order.push('attribute');
      strictEqual(this, globalThis);
      strictEqual(message, 'Uncaught Error: handler boom');
      ok(
        source === 'worker' || source.endsWith('global-event-handlers-test.js')
      );
      strictEqual(typeof lineno, 'number');
      strictEqual(typeof colno, 'number');
      strictEqual(error.message, 'handler boom');
      return true;
    };
    onerror = replacement;
    strictEqual(onerror, replacement);
    addEventListener('error', after);

    reportError(new Error('handler boom'));
    deepStrictEqual(order, ['before', 'attribute', 'after']);
    strictEqual(observedEvent.cancelable, true);
    strictEqual(observedEvent.defaultPrevented, true);

    const generic = new Event('error', { cancelable: true });
    onerror = function (event) {
      strictEqual(this, globalThis);
      strictEqual(arguments.length, 1);
      strictEqual(event, generic);
      return false;
    };
    strictEqual(dispatchEvent(generic), false);
    strictEqual(generic.defaultPrevented, true);

    onerror = null;
    strictEqual(onerror, null);
    order.length = 0;
    reportError(new Error('after clear'));
    deepStrictEqual(order, ['before', 'after']);

    onerror = () => {
      order.push('reassigned');
    };
    order.length = 0;
    reportError(new Error('after reassignment'));
    deepStrictEqual(order, ['before', 'after', 'reassigned']);
    strictEqual(observedEvent.defaultPrevented, false);
    onerror = null;

    const object = {};
    onerror = object;
    strictEqual(onerror, object);
    order.length = 0;
    reportError(new Error('object handler'));
    deepStrictEqual(order, ['before', 'after']);

    onerror = null;
    removeEventListener('error', before);
    removeEventListener('error', after);
  },
};

export const errorHandlerExceptionReporting = {
  test() {
    if (!enabled) return;

    const order = [];
    onerror = () => {
      order.push('attribute');
      throw new Error('onerror failure');
    };
    const after = () => {
      order.push('after');
    };
    addEventListener('error', after);
    reportError(new Error('reported'));
    deepStrictEqual(order, ['attribute', 'after']);
    onerror = null;
    removeEventListener('error', after);
  },
};

export const promiseRejectionHandlerAttributes = {
  async test() {
    if (!enabled) return;

    const order = [];
    const before = () => {
      order.push('before');
    };
    addEventListener('unhandledrejection', before);

    const replaced = () => {
      throw new Error('replaced handler must not run');
    };
    onunhandledrejection = replaced;
    strictEqual(onunhandledrejection, replaced);

    const unhandled = Promise.withResolvers();
    let rejectedPromise;
    const replacement = function (event) {
      order.push('attribute');
      strictEqual(this, globalThis);
      strictEqual(event.reason, 'unhandled');
      strictEqual(event.cancelable, true);
      event.preventDefault();
    };
    onunhandledrejection = replacement;
    strictEqual(onunhandledrejection, replacement);

    const after = (event) => {
      order.push('after');
      strictEqual(event.defaultPrevented, true);
      rejectedPromise = event.promise;
      unhandled.resolve();
    };
    addEventListener('unhandledrejection', after);
    Promise.reject('unhandled');
    await unhandled.promise;
    deepStrictEqual(order, ['before', 'attribute', 'after']);

    onunhandledrejection = null;
    strictEqual(onunhandledrejection, null);

    const handled = Promise.withResolvers();
    const handledHandler = function (event) {
      order.push('handled-attribute');
      strictEqual(this, globalThis);
      strictEqual(event.promise, rejectedPromise);
      strictEqual(event.reason, undefined);
      strictEqual(event.cancelable, false);
      handled.resolve();
    };
    onrejectionhandled = handledHandler;
    strictEqual(onrejectionhandled, handledHandler);
    rejectedPromise.catch(() => {});
    await handled.promise;

    onrejectionhandled = null;
    strictEqual(onrejectionhandled, null);
    removeEventListener('unhandledrejection', before);
    removeEventListener('unhandledrejection', after);
  },
};

export const promiseRejectionHandlerExceptionReporting = {
  async test() {
    if (!enabled) return;

    const done = Promise.withResolvers();
    onerror = () => true;
    onunhandledrejection = () => {
      throw new Error('unhandled handler failure');
    };
    const after = () => done.resolve();
    addEventListener('unhandledrejection', after);
    Promise.reject('exception');
    await done.promise;
    onunhandledrejection = null;
    onerror = null;
    removeEventListener('unhandledrejection', after);
  },
};
