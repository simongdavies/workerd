# Hyperlight micro-VM JavaScript backend — sample

This sample runs a JavaScript Worker inside a **Hyperlight + QuickJS micro-VM**
using the `hyperlightJs` service type. A request to this backend never enters V8
or JSG: it is marshalled to a JSON event and handed to a handler running inside a
**hardware-isolated micro-VM**, which returns the response.

For the full build/run/test walkthrough (and how it works), see
[`docs/hyperlight-js.md`](../../docs/hyperlight-js.md). For the API coverage and
design analysis, see [`docs/hyperlight-js-wintertc.md`](../../docs/hyperlight-js-wintertc.md).

## Requirements

- **Linux / x86-64** with access to **`/dev/kvm`** (be a member of the `kvm`
  group, or have read/write on `/dev/kvm`).
- A `workerd` binary that includes this backend (see "Build" below).

## Build

The custom guest runtime is already committed (as a prebuilt blob), so the common
case is just building `workerd`:

```sh
just build
# or:
bazel build //src/workerd/server:workerd
```

The binary is then at `bazel-bin/src/workerd/server/workerd`. (You only need to
rebuild the guest runtime blob if you change the in-guest globals — see the doc.)

## Run

From the repository root:

```sh
bazel-bin/src/workerd/server/workerd serve samples/hyperlight-js/config.capnp
```

This serves on http://localhost:8080. In another terminal:

```sh
curl -i localhost:8080/            # 200 text/plain,  X-Custom: yes
curl -i localhost:8080/json        # 201 application/json,  X-Api: v2
curl -i localhost:8080/redirect    # 302,  Location: https://example.com/
curl -i localhost:8080/teapot      # 418,  X-Brew: tea
curl -i -X POST --data hi localhost:8080/json   # the body is echoed back
```

## The three handlers

Each has its own config so it runs out of the box:

| Config | Handler | Shows |
| --- | --- | --- |
| `config.capnp` | `router.js` | routing + returning a real `Response` (status/headers/body) |
| `globals.capnp` | `globals.js` | the WinterTC globals working in-guest (`URL`, `Headers`, `Blob`, `structuredClone`, `URLPattern`, `Request`/`Response`, …) |
| `echo.capnp` | `echo.js` | what the host marshals into the `event` (method, url, headers, body) |

```sh
workerd serve samples/hyperlight-js/globals.capnp
curl -s "localhost:8080/api/v1/items?page=1&sort=name" | python3 -m json.tool

workerd serve samples/hyperlight-js/echo.capnp
curl -X POST -H "X-Custom: hi" --data "the body" "localhost:8080/echo?q=1"
```

## The handler contract

Each handler module exports a `handler` function (sync or `async`) that receives
one `event`:

```js
// event = { method, url, headers: [[name, value], ...], body, bodyEncoding }
//   bodyEncoding: "utf-8" | "base64" | "none"
export function handler(event) {
  // Return a Response -> its status/headers/body become the HTTP response.
  // Return any other value -> served as 200 with application/json.
  return new Response("hi", { status: 200, headers: { "X-Demo": "1" } });
}
```

## Tuning

- `WORKERD_HYPERLIGHT_POOL_SIZE=N` — number of warm micro-VMs (default `1`, max
  `64`). This is the concurrency ceiling; each VM serves requests serially,
  rewinding to a clean snapshot between them.

```sh
WORKERD_HYPERLIGHT_POOL_SIZE=8 workerd serve samples/hyperlight-js/config.capnp
```
