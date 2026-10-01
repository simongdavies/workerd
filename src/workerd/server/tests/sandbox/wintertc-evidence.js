const encoder = new TextEncoder();
const decoder = new TextDecoder();
const coreWasmModule = new WebAssembly.Module(
  new Uint8Array([
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60,
    0x00, 0x01, 0x7f, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0a, 0x01, 0x06, 0x61,
    0x6e, 0x73, 0x77, 0x65, 0x72, 0x00, 0x00, 0x0a, 0x06, 0x01, 0x04, 0x00,
    0x41, 0x2a, 0x0b,
  ])
);
const coreWasm = new WebAssembly.Instance(coreWasmModule);

let requestCount = 0;
let previousStateToken = null;

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function concat(chunks, size) {
  const output = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

async function sha256(bytes) {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  return [...digest]
    .map((value) => value.toString(16).padStart(2, '0'))
    .join('');
}

async function exerciseGlobalHandlers() {
  const observed = [];
  let resolveError;
  let resolveRejection;
  const errorObserved = new Promise((resolve) => {
    resolveError = resolve;
  });
  const rejectionObserved = new Promise((resolve) => {
    resolveRejection = resolve;
  });
  const previousError = globalThis.onerror;
  const previousUnhandledRejection = globalThis.onunhandledrejection;

  try {
    globalThis.onerror = (message, _source, _line, _column, error) => {
      observed.push(`error:${error?.message ?? message}`);
      resolveError();
      return true;
    };
    globalThis.onunhandledrejection = (event) => {
      observed.push(`rejection:${event.reason?.message ?? event.reason}`);
      event.preventDefault();
      resolveRejection();
    };

    queueMicrotask(() => {
      throw new Error('wintertc-global-error');
    });
    Promise.reject(new Error('wintertc-unhandled-rejection'));
    await Promise.all([errorObserved, rejectionObserved]);
  } finally {
    globalThis.onerror = previousError;
    globalThis.onunhandledrejection = previousUnhandledRejection;
  }

  observed.sort();
  return observed;
}

async function exerciseMessagePortTransfer() {
  const transport = new MessageChannel();
  const payload = new MessageChannel();

  try {
    const received = new Promise((resolve) => {
      payload.port1.onmessage = (event) => {
        resolve(event.data);
      };
      payload.port1.start();

      transport.port2.onmessage = (event) => {
        const transferred = event.ports[0] ?? event.data.port;
        transferred.postMessage('message-port-transfer-passed');
      };
      transport.port2.start();
    });

    transport.port1.postMessage({ port: payload.port2 }, [payload.port2]);
    return await received;
  } finally {
    transport.port1.close();
    transport.port2.close();
    payload.port1.close();
  }
}

async function exerciseMessagePortStage(stage) {
  const progress = [];
  if (stage === 'construct') {
    const channel = new MessageChannel();
    progress.push('constructed');
    channel.port1.close();
    channel.port2.close();
    return { stage, progress };
  }
  if (stage === 'listener-registration') {
    const channel = new MessageChannel();
    channel.port1.onmessage = () => {};
    progress.push('listener-registered');
    channel.port1.close();
    channel.port2.close();
    return { stage, progress };
  }
  if (stage === 'start') {
    const channel = new MessageChannel();
    channel.port1.start();
    progress.push('started');
    channel.port1.close();
    channel.port2.close();
    return { stage, progress };
  }
  if (stage === 'post-message') {
    const channel = new MessageChannel();
    channel.port1.postMessage('queued');
    progress.push('post-message-returned');
    channel.port1.close();
    channel.port2.close();
    return { stage, progress };
  }
  if (stage === 'queued-delivery') {
    const channel = new MessageChannel();
    try {
      const delivered = new Promise((resolve) => {
        channel.port2.onmessage = (event) => {
          progress.push(`delivered:${event.data}`);
          resolve();
        };
        progress.push('listener-registered');
        channel.port2.start();
        progress.push('started');
      });
      channel.port1.postMessage('queued');
      progress.push('posted');
      await delivered;
      return { stage, progress };
    } finally {
      channel.port1.close();
      channel.port2.close();
    }
  }
  if (stage === 'close') {
    const channel = new MessageChannel();
    channel.port1.close();
    progress.push('port1-closed');
    channel.port2.close();
    progress.push('port2-closed');
    return { stage, progress };
  }
  if (stage === 'transfer-reentanglement') {
    progress.push('transfer-started');
    const result = await exerciseMessagePortTransfer();
    progress.push(`transfer-delivered:${result}`);
    return { stage, progress };
  }
  if (stage === 'clone-failure') {
    const channel = new MessageChannel();
    try {
      let failure;
      try {
        channel.port1.postMessage({ unsupported: () => {} });
      } catch (error) {
        failure = {
          name: error?.name ?? 'Error',
          message: error?.message ?? String(error),
        };
      }
      if (failure === undefined) {
        throw new Error('MessagePort accepted an uncloneable function');
      }
      progress.push('clone-failure-caught');
      return { stage, progress, failure };
    } finally {
      channel.port1.close();
      channel.port2.close();
    }
  }
  throw new RangeError(`Unknown MessagePort stage: ${stage}`);
}

async function exerciseByob() {
  const expected = encoder.encode('byob-passed');
  const stream = new ReadableStream({
    type: 'bytes',
    pull(controller) {
      const request = controller.byobRequest;
      const amount = Math.min(request.view.byteLength, expected.byteLength);
      request.view.set(expected.subarray(0, amount));
      request.respond(amount);
      controller.close();
    },
  });
  const reader = stream.getReader({ mode: 'byob' });
  const { value, done } = await reader.read(new Uint8Array(64));
  return {
    done,
    value: decoder.decode(value),
    detachedInput: value.buffer.byteLength > 0,
  };
}

async function readAll(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let size = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(value);
    size += value.byteLength;
  }
  return decoder.decode(concat(chunks, size));
}

async function exerciseByteStreamTee() {
  const stream = new ReadableStream({
    type: 'bytes',
    start(controller) {
      controller.enqueue(encoder.encode('byte-stream-tee-passed'));
      controller.close();
    },
  });
  const [left, right] = stream.tee();
  const [leftText, rightText] = await Promise.all([
    readAll(left),
    readAll(right),
  ]);
  return { left: leftText, right: rightText };
}

function exerciseCoreWasm() {
  return coreWasm.exports.answer();
}

async function exerciseTimers() {
  return {
    start: await runSubtest('timer-start', () => {
      const handle = setTimeout(() => {}, 60000);
      clearTimeout(handle);
      return { handleType: typeof handle };
    }),
    immediateRead: await runSubtest(
      'timer-immediate-read',
      () =>
        new Promise((resolve) => {
          const started = performance.now();
          setTimeout(
            () => resolve({ elapsedMs: performance.now() - started }),
            0
          );
        })
    ),
    firedReadAndOrdering: await runSubtest(
      'timer-fired-read-ordering',
      () =>
        new Promise((resolve) => {
          const events = [];
          const started = performance.now();
          const canceled = setTimeout(() => events.push('canceled'), 1);
          clearTimeout(canceled);
          setTimeout(() => events.push('early'), 5);
          setTimeout(() => {
            events.push('late');
            resolve({
              elapsedMs: performance.now() - started,
              events,
              passed: events.join(',') === 'early,late',
            });
          }, 20);
        })
    ),
    cancel: await runSubtest('timer-cancel', () => {
      const handle = setTimeout(() => {}, 60000);
      clearTimeout(handle);
      return { canceled: true };
    }),
  };
}

async function runSubtest(id, test) {
  try {
    return { id, status: 'pass', result: await test() };
  } catch (error) {
    return {
      id,
      status: 'error',
      error: {
        name: error?.name ?? 'Error',
        message: error?.message ?? String(error),
        stack: error?.stack ?? null,
      },
    };
  }
}

function isolatedSubtest(id, endpoint) {
  return {
    id,
    status: 'isolated',
    endpoint,
    reason:
      'Run through the dedicated endpoint so a native failure cannot terminate the aggregate request.',
  };
}

async function exerciseCoreBehavior() {
  return {
    timers: isolatedSubtest('timers', '/evidence/timers'),
    globalHandlers: await runSubtest('global-handlers', exerciseGlobalHandlers),
    messagePort: isolatedSubtest('messageport', '/evidence/messageport'),
    byob: isolatedSubtest('byob', '/evidence/byob'),
    byteStreamTee: isolatedSubtest(
      'byte-stream-tee',
      '/evidence/byte-stream-tee'
    ),
    coreWasm: await runSubtest('core-wasm', exerciseCoreWasm),
    componentModel: {
      id: 'component-model',
      status: 'unsupported_boundary',
      result: {
        supported: false,
        boundary:
          'The executor accepts esModule, text, and json only; Workerd exposes core WebAssembly but no WASI Component Model runtime.',
      },
    },
  };
}

async function exerciseFetch(url) {
  const target = url.searchParams.get('upstream');
  if (target === null) {
    return Response.json(
      { error: 'The fetch demo requires an upstream query parameter.' },
      { status: 400 }
    );
  }

  let chunk = 0;
  const upload = new ReadableStream({
    async pull(controller) {
      if (chunk === 16) {
        controller.close();
        return;
      }
      await delay(5);
      const bytes = new Uint8Array(32768);
      bytes.fill(chunk);
      controller.enqueue(bytes);
      ++chunk;
    },
  });
  const response = await fetch(target, {
    method: 'POST',
    headers: { 'content-type': 'application/octet-stream' },
    body: upload,
  });
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(value);
    size += value.byteLength;
  }
  const body = concat(chunks, size);

  return Response.json({
    status: response.status,
    chunks: chunks.length,
    bytes: body.byteLength,
    sha256: await sha256(body),
    body: decoder.decode(body),
  });
}

export default {
  async fetch(request) {
    requestCount += 1;
    const url = new URL(request.url);

    if (url.pathname === '/evidence/core') {
      return Response.json(await exerciseCoreBehavior());
    }
    if (url.pathname === '/evidence/timers') {
      return Response.json(await runSubtest('timers', exerciseTimers));
    }
    if (url.pathname === '/evidence/global-handlers') {
      return Response.json(
        await runSubtest('global-handlers', exerciseGlobalHandlers)
      );
    }
    if (url.pathname === '/evidence/messageport') {
      return Response.json(
        await runSubtest('messageport', () => {
          const stage = url.searchParams.get('stage');
          return stage === null
            ? {
                stages: [
                  'construct',
                  'listener-registration',
                  'start',
                  'post-message',
                  'queued-delivery',
                  'close',
                  'transfer-reentanglement',
                  'clone-failure',
                ],
              }
            : exerciseMessagePortStage(stage);
        })
      );
    }
    if (url.pathname === '/evidence/byob') {
      return Response.json(await runSubtest('byob', exerciseByob));
    }
    if (url.pathname === '/evidence/byte-stream-tee') {
      return Response.json(
        await runSubtest('byte-stream-tee', exerciseByteStreamTee)
      );
    }
    if (url.pathname === '/evidence/core-wasm') {
      return Response.json(await runSubtest('core-wasm', exerciseCoreWasm));
    }
    if (url.pathname === '/evidence/fetch') {
      return exerciseFetch(url);
    }
    if (url.pathname === '/evidence/state') {
      const stateToken = url.searchParams.get('token');
      const result = {
        requestCount,
        stateToken,
        previousStateToken,
      };
      previousStateToken = stateToken;
      return Response.json(result);
    }

    return Response.json(
      {
        endpoints: [
          '/evidence/core',
          '/evidence/timers',
          '/evidence/global-handlers',
          '/evidence/messageport',
          '/evidence/byob',
          '/evidence/byte-stream-tee',
          '/evidence/core-wasm',
          '/evidence/fetch?upstream=https://host-policy-allowed-echo.example/',
          '/evidence/state?token=<unique-token>',
        ],
      },
      { status: 404 }
    );
  },
};
