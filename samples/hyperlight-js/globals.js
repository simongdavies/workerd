// Sample handler: a tour of the WinterTC globals baked into the custom guest
// runtime, all running inside the Hyperlight + QuickJS micro-VM. Returns a JSON
// summary so you can see each API working in-guest.
//
//   curl -s "localhost:8080/api/v1/items?page=1&sort=name" | python3 -m json.tool
async function handler(event) {
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const bytes = enc.encode("hello from the micro-VM \u26a1");

  // URL + URLSearchParams: parse the request URL and its query.
  const u = new URL(event.url);
  const params = {};
  for (const [k, v] of u.searchParams) params[k] = v;
  const resolved = new URL("../v2/items?page=2", event.url).href;

  // Headers: case-insensitive, combine-on-append.
  const h = new Headers({ "Content-Type": "application/json" });
  h.append("X-Trace", "abc");
  h.append("X-Trace", "def");

  // Event / AbortController: wire a listener, abort, observe it fire in-guest.
  const ac = new AbortController();
  let abortFired = false;
  ac.signal.addEventListener("abort", () => {
    abortFired = true;
  });
  ac.abort();

  // structuredClone + queueMicrotask (await so it drains) + Blob.
  const cloned = structuredClone({ nested: { n: 7 }, list: [1, 2] });
  const microOrder = [];
  queueMicrotask(() => microOrder.push("micro"));
  microOrder.push("sync");
  await Promise.resolve();
  const blob = new Blob(["hyper", "light \u26a1"], { type: "text/plain" });
  const blobText = await blob.text();

  // URLPattern: match the request pathname into a named route.
  const route = new URLPattern({ pathname: "/api/:version/items" });
  const routeMatch = route.exec(event.url);

  // Request / Response data shells.
  const req = new Request(event.url, { method: "POST", headers: { "X-Demo": "1" }, body: "ping" });
  const resp = new Response("pong", { status: 201, headers: { "X-Out": "2" } });

  return {
    textEncoder: { byteLength: bytes.length, decoded: dec.decode(bytes) },
    base64: { btoaHello: btoa("hello"), atobWorkerd: atob("d29ya2VyZA==") },
    url: { href: u.href, origin: u.origin, pathname: u.pathname, params, resolved },
    headers: {
      contentType: h.get("content-type"),
      xTrace: h.get("x-trace"),
      all: Array.from(h.entries()),
    },
    events: {
      domExceptionCode: new DOMException("x", "DataCloneError").code,
      abortFired,
      reason: ac.signal.reason ? ac.signal.reason.name : null,
    },
    util: {
      cloned,
      microOrder,
      blob: { size: blob.size, type: blob.type, text: blobText },
    },
    urlPattern: {
      matched: route.test(event.url),
      version: routeMatch ? routeMatch.pathname.groups.version : null,
    },
    request: { method: req.method, body: await req.text() },
    response: { status: resp.status, ok: resp.ok, body: await resp.text() },
    globals: {
      TextEncoder: typeof TextEncoder,
      TextDecoder: typeof TextDecoder,
      atob: typeof atob,
      btoa: typeof btoa,
      URL: typeof URL,
      URLSearchParams: typeof URLSearchParams,
      Headers: typeof Headers,
      DOMException: typeof DOMException,
      Event: typeof Event,
      EventTarget: typeof EventTarget,
      AbortController: typeof AbortController,
      AbortSignal: typeof AbortSignal,
      structuredClone: typeof structuredClone,
      queueMicrotask: typeof queueMicrotask,
      Blob: typeof Blob,
      URLPattern: typeof URLPattern,
      Request: typeof Request,
      Response: typeof Response,
    },
  };
}

export { handler };
