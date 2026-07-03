// Sample handler for the Hyperlight + QuickJS micro-VM backend (the `hyperlightJs`
// service type). It runs inside a hardware-isolated micro-VM -- no V8, no JSG on
// this path.
//
// This handler demonstrates returning a real HTTP Response: the returned Response's
// status, headers and body are mapped to the actual HTTP response by the workerd host.
//
// The handler receives a single `event` argument:
//   event = { method, url, headers: [[name, value], ...], body, bodyEncoding }
// where bodyEncoding is "utf-8" | "base64" | "none".
//
//   curl -i localhost:8080/           -> 200 text/plain
//   curl -i localhost:8080/json       -> 201 application/json
//   curl -i localhost:8080/redirect   -> 302 Location
//   curl -i localhost:8080/teapot     -> 418
//   curl -i -X POST --data hi localhost:8080/json   (body is echoed back)
async function handler(event) {
  const url = new URL(event.url);

  if (url.pathname === "/json") {
    // Response.json sets Content-Type: application/json and serializes the value.
    return Response.json(
      { ok: true, method: event.method, echo: event.body },
      { status: 201, headers: { "X-Api": "v2" } },
    );
  }
  if (url.pathname === "/redirect") {
    return Response.redirect("https://example.com/", 302);
  }
  if (url.pathname === "/teapot") {
    return new Response("no coffee here", { status: 418, headers: { "X-Brew": "tea" } });
  }

  // Default: a plain-text 200 with a custom response header.
  return new Response(`hello ${event.method} ${url.pathname}`, {
    status: 200,
    headers: { "Content-Type": "text/plain; charset=utf-8", "X-Custom": "yes" },
  });
}

export { handler };
