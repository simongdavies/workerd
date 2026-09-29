export default {
  async fetch(request) {
    const url = new URL(request.url);
    const urlPattern = new URLPattern({ pathname: '/wintertc-*' });
    const headers = new Headers(request.headers);
    const clonedRequest = new Request(request);
    const formData = new FormData();
    formData.append('smoke', 'yes');
    const blob = new Blob(['abc'], { type: 'text/plain' });
    const encoded = new TextEncoder().encode('abc');
    const decoded = new TextDecoder().decode(encoded);
    const digest = new Uint8Array(
      await crypto.subtle.digest('SHA-256', encoded)
    );
    const digestHex = [...digest]
      .map((value) => value.toString(16).padStart(2, '0'))
      .join('');
    const random = crypto.getRandomValues(new Uint8Array(8));
    const readable = new ReadableStream({
      start(controller) {
        controller.enqueue(encoded);
        controller.close();
      },
    });
    const transformed = readable.pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          controller.enqueue(
            new TextEncoder().encode(
              new TextDecoder().decode(chunk).toUpperCase()
            )
          );
        },
      })
    );
    const transformedText = await new Response(transformed).text();
    const compressed = await new Response(
      blob.stream().pipeThrough(new CompressionStream('gzip'))
    ).arrayBuffer();
    const decompressed = await new Response(
      new Blob([compressed])
        .stream()
        .pipeThrough(new DecompressionStream('gzip'))
    ).text();
    let webAssembly = 'unavailable';
    try {
      const wasm = await WebAssembly.instantiate(
        new Uint8Array([
          0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01,
          0x60, 0x00, 0x01, 0x7f, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0a, 0x01,
          0x06, 0x61, 0x6e, 0x73, 0x77, 0x65, 0x72, 0x00, 0x00, 0x0a, 0x06,
          0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
        ])
      );
      webAssembly = wasm.instance.exports.answer() === 42 ? 'passes' : 'failed';
    } catch {
      webAssembly = 'blocked by executor embedder policy';
    }

    return Response.json({
      smoke: 'WinterTC minimum-common API smoke (not conformance)',
      url: url.pathname === '/wintertc-smoke',
      urlPattern: urlPattern.test(url),
      request: clonedRequest.method === 'POST',
      response: typeof Response === 'function',
      headers: headers.get('x-smoke') === 'yes',
      formData: formData.get('smoke') === 'yes',
      blob: (await blob.text()) === 'abc',
      textCodec: decoded === 'abc',
      cryptoDigest:
        digestHex ===
        'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
      cryptoRandom: random.length === 8 && random.some((value) => value !== 0),
      readableStream: transformedText === 'ABC',
      transformStream: transformedText === 'ABC',
      compression: decompressed === 'abc',
      performance: typeof performance.now() === 'number',
      webAssembly,
      timers: 'not exercised: executor timer channel is disabled',
      capabilityBackedUnavailable: [
        'outbound fetch',
        'WebSocket',
        'connect',
        'bindings',
        'actors',
      ],
    });
  },
};
