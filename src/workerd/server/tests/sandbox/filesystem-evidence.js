import {
  closeSync,
  existsSync,
  openSync,
  readFileSync,
  readSync,
  writeFileSync,
} from 'node:fs';

function capture(operation) {
  try {
    return { ok: true, value: operation() };
  } catch (error) {
    return {
      ok: false,
      code: error?.code ?? null,
      message: error?.message ?? String(error),
    };
  }
}

function readBytes(path, length) {
  const fd = openSync(path, 'r');
  try {
    const bytes = Buffer.alloc(length);
    const count = readSync(fd, bytes, 0, bytes.length, 0);
    return bytes.subarray(0, count);
  } finally {
    closeSync(fd);
  }
}

function pinnedVfsEvidence() {
  const marker = '/tmp/request-marker';
  const tmpWasPresent = existsSync(marker);
  writeFileSync(marker, 'request-local');

  const bundleRead = readFileSync('/bundle/worker.js', 'utf8');
  const bundleWrite = capture(() =>
    writeFileSync('/bundle/worker.js', 'mutated')
  );

  writeFileSync('/dev/null', 'discarded');
  const nullRead = readBytes('/dev/null', 16);
  const zeroRead = readBytes('/dev/zero', 32);
  const randomRead = readBytes('/dev/random', 32);

  return {
    surface: 'workerd-vfs',
    bundle: {
      readable: bundleRead.includes("surface: 'workerd-vfs'"),
      immutable: !bundleWrite.ok,
      writeError: bundleWrite.code,
    },
    tmp: {
      readWrite: readFileSync(marker, 'utf8') === 'request-local',
      freshRequest: !tmpWasPresent,
    },
    dev: {
      nullDiscardThenEof: nullRead.length === 0,
      zeroReadLength: zeroRead.length,
      zeroOnly: zeroRead.every((value) => value === 0),
      randomReadLength: randomRead.length,
      randomNontrivial:
        randomRead.some((value) => value !== 0) &&
        randomRead.some((value) => value !== randomRead[0]),
    },
  };
}

function hostFsEvidence(url) {
  const writeValue = url.searchParams.get('value') ?? 'bounded-write';
  const quotaBytes = Number(url.searchParams.get('quotaBytes') ?? 0);
  const allowedRead = capture(() =>
    readFileSync('/storage/readonly/allowed.txt', 'utf8')
  );
  const readOnlyWrite = capture(() =>
    writeFileSync('/storage/readonly/denied.txt', 'must-not-write')
  );
  const boundedWrite = capture(() => {
    writeFileSync('/storage/scratch/bounded.txt', writeValue);
    return readFileSync('/storage/scratch/bounded.txt', 'utf8');
  });
  const traversal = capture(() =>
    readFileSync('/storage/readonly/../../etc/passwd', 'utf8')
  );
  const unlisted = capture(() =>
    readFileSync('/storage/unlisted/anything', 'utf8')
  );
  const quota =
    quotaBytes > 0
      ? capture(() =>
          writeFileSync('/storage/scratch/quota.bin', Buffer.alloc(quotaBytes, 0x71))
        )
      : {
          ok: null,
          code: null,
          message: 'set quotaBytes to exercise host EDQUOT',
        };

  return {
    surface: 'hyperlight-hostfs',
    allowedRead,
    readOnlyWrite,
    boundedWrite,
    traversal,
    unlisted,
    quota,
  };
}

export default {
  fetch(request) {
    const url = new URL(request.url);
    const result =
      url.pathname === '/evidence/workerd-vfs'
        ? pinnedVfsEvidence()
        : url.pathname === '/evidence/hostfs'
          ? hostFsEvidence(url)
          : {
              error: 'not_found',
              routes: ['/evidence/workerd-vfs', '/evidence/hostfs'],
            };
    return Response.json(result, { status: result.error ? 404 : 200 });
  },
};
