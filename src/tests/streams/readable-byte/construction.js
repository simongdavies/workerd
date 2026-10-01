// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Byte-stream construction: type/strategy validation and defaults.

import { strictEqual, throws } from 'node:assert';
import { usingTsImpl } from 'which-impl';

// A size() function in a byte-stream strategy is rejected (spec parity).
export const sizeStrategyForBytes = {
  test() {
    throws(
      () =>
        new ReadableStream(
          { type: 'bytes' },
          {
            size() {
              return 1;
            },
            highWaterMark: 4,
          }
        ),
      {
        name: 'RangeError',
        message: usingTsImpl
          ? 'The strategy for a byte stream cannot have a size function'
          : 'The strategy for a byte stream cannot have a size function.',
      }
    );
  },
};

// autoAllocateChunkSize must be a positive integer: zero, negative, and
// NaN all throw TypeError on both sides (different messages).
export const autoAllocateChunkSizeValidated = {
  test() {
    for (const bad of [0, -1, NaN]) {
      throws(
        () => new ReadableStream({ type: 'bytes', autoAllocateChunkSize: bad }),
        {
          name: 'TypeError',
          message: usingTsImpl
            ? 'autoAllocateChunkSize must be a positive integer'
            : 'The autoAllocateChunkSize option cannot be zero.',
        }
      );
    }
  },
};

// A byte stream's default high-water mark is 0: desiredSize is 0 inside
// start() and — unlike a value stream's default hwm 1 — NO automatic
// pull follows (parity; the byte half of streams-js-test.js hwmDefault).
export const byteHwmDefaultIsZero = {
  async test() {
    let pulled = 0;
    let seen;
    new ReadableStream({
      type: 'bytes',
      start(c) {
        seen = c.desiredSize;
      },
      pull() {
        pulled++;
      },
    });
    strictEqual(seen, 0);
    await scheduler.wait(10);
    strictEqual(pulled, 0);
  },
};

// A synchronously throwing start() escapes the constructor (spec parity).
export const syncStartThrow = {
  test() {
    const err = new Error('start-throw');
    let caught;
    try {
      new ReadableStream({
        type: 'bytes',
        start() {
          throw err;
        },
      });
    } catch (e) {
      caught = e;
    }
    strictEqual(caught, err);
  },
};
