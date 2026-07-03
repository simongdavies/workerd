# Building, running, and testing the Hyperlight JS backend

This is a step-by-step guide to build, run, and test **workerd's Hyperlight +
QuickJS micro-VM JavaScript backend** — the `hyperlightJs` service type. A
request to this backend never enters V8 or JSG: it is marshalled to a JSON event
and run by a handler inside a **hardware-isolated micro-VM**, which returns the
response.

For *what* the backend can and cannot do (the WinterTC API coverage and the
execution-model limits), see [`hyperlight-js-wintertc.md`](./hyperlight-js-wintertc.md).
This document is the *how-to-run-it* companion.

```
curl ─HTTP─▶ workerd hyperlightJs service (C++)          [no V8, no JSG here]
                   │  marshal request (method, url, headers, body) → JSON event
                   ▼
            adapter crate (Rust)  ── run on a warm micro-VM ──▶
                   │
                   ▼
            QuickJS guest in a Hyperlight micro-VM  ── handler(event) ──▶ result
                   │  (rewinds to a clean snapshot before each request)
                   ▼
            Response / value → workerd → HTTP response
```

---

## 1. Prerequisites

**To build and run** (the common case):

- **Linux / x86-64.** The backend is target-gated to `x86_64-linux`; it is not
  built on other platforms.
- **`/dev/kvm` access.** The guest runs in a KVM micro-VM. Be a member of the
  `kvm` group (or have read/write on `/dev/kvm`). Check with:
  ```sh
  ls -l /dev/kvm && id -nG | tr ' ' '\n' | grep -qx kvm && echo "kvm: ok"
  ```
- The **standard workerd build toolchain** (Bazel via `just`, a C/C++ compiler).
  See [`development.md`](./development.md).

The custom guest runtime is **committed as a prebuilt blob**
(`deps/rust/hyperlight-js-runtime/jsruntime.bin`), so a normal build does **not**
need any Hyperlight guest toolchain. You only need that to *change* the in-guest
globals — see [§8](#8-modifying-the-in-guest-globals-advanced).

---

## 2. Build workerd

```sh
just build
# or, targeting just the server binary:
bazel build //src/workerd/server:workerd
```

The binary is then at `bazel-bin/src/workerd/server/workerd`.

> **Low-memory machines:** the V8 build is memory-hungry. If you hit OOM, cap the
> build, e.g.:
> ```sh
> bazel build //src/workerd/server:workerd \
>   --jobs=8 --local_resources=memory=HOST_RAM*0.5
> ```

---

## 3. Run a sample

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
curl -i -X POST --data hi localhost:8080/json   # the request body is echoed back
```

There are three ready samples (each with its own config) under
[`samples/hyperlight-js/`](../samples/hyperlight-js/) — see that directory's
`README.md`:

| Config | Handler | Shows |
| --- | --- | --- |
| `config.capnp` | `router.js` | routing + returning a real `Response` |
| `globals.capnp` | `globals.js` | the WinterTC globals working in-guest |
| `echo.capnp` | `echo.js` | what the host marshals into the `event` |

---

## 4. The handler contract

A handler module exports a `handler` function (sync **or** `async`) that receives
a single `event`:

```js
// event = {
//   method,                       // e.g. "GET", "POST"
//   url,                          // absolute request URL string
//   headers,                      // [[name, value], ...] (duplicates preserved)
//   body,                         // string, or null when there is no body
//   bodyEncoding,                 // "utf-8" | "base64" | "none"
// }
export async function handler(event) {
  // Return a Response  -> its status / headers / body become the HTTP response.
  // Return anything else -> served as 200 with Content-Type application/json.
  return new Response("hi", { status: 200, headers: { "X-Demo": "1" } });
}
```

- A **binary** request body arrives base64-encoded (`bodyEncoding: "base64"`);
  decode it in-guest with `atob`. A UTF-8 body arrives as a plain string.
- **`async` handlers are supported** — the runtime drains the job queue and
  resolves a returned promise before serializing the result.

### Available globals

Baked into the guest runtime (all run in-guest, no host round-trip):

`TextEncoder`, `TextDecoder`, `atob`, `btoa`, `URL`, `URLSearchParams`,
`Headers`, `DOMException`, `Event`, `EventTarget`, `AbortController`,
`AbortSignal`, `structuredClone`, `queueMicrotask`, `Blob`, `URLPattern`,
`Request`, `Response` — plus the runtime built-ins (`console`, `crypto`
Node-style, `require`).

---

## 5. Configuration

The service is declared in a Cap'n Proto config with the `hyperlightJs` service
type, embedding the handler module:

```capnp
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    ( name = "js", hyperlightJs = ( handler = embed "router.js" ) ),
  ],
  sockets = [ ( name = "http", address = "*:8080", service = "js" ) ],
);
```

### Environment variables

- **`WORKERD_HYPERLIGHT_POOL_SIZE`** — number of warm micro-VMs kept in the pool
  (default `1`, max `64`). This is the effective concurrency ceiling: each VM
  serves requests serially, rewinding to a clean post-init snapshot between them.

  ```sh
  WORKERD_HYPERLIGHT_POOL_SIZE=8 \
    bazel-bin/src/workerd/server/workerd serve samples/hyperlight-js/config.capnp
  ```

---

## 6. Testing / smoke-checking

Start `config.capnp` and confirm the routes map correctly:

```sh
# status line only
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/json     # 201
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/teapot   # 418

# request body round-trips into the handler and back out
curl -s -X POST --data "ping" localhost:8080/json                # {"...","echo":"ping"}
```

Start `globals.capnp` and confirm every global is live in-guest:

```sh
curl -s "localhost:8080/api/v1/items?page=1&sort=name" | python3 -m json.tool
# -> "globals" object with every entry "function", url params parsed,
#    URLPattern "version": "v1", Blob/structuredClone/etc. all working.
```

---

## 7. Performance (indicative)

Measured on loopback with a keep-alive `curl` client (not a tuned load
generator), handler = `router.js` (parses the URL, returns a `Response`):

| Scenario | Result |
| --- | --- |
| Warm latency, pool=1, 1 client | **p50 ≈ 0.9 ms**, p99 ≈ 1.5 ms |
| Throughput, pool=1, sequential | ≈ 700 req/s |
| Throughput, pool=8, 16 clients | ≈ 3,000 req/s |

The ~0.9 ms median is the *full* round trip including rewinding the micro-VM to a
clean snapshot per request. Throughput plateaus because the per-request host-side
marshalling (header iteration, JSON build/parse, response mapping) runs on
workerd's single event-loop thread — the VMs parallelize, that marshalling does
not.

---

## 8. Modifying the in-guest globals (advanced)

The globals are baked into a custom guest runtime that is **cross-compiled to a
prebuilt blob** and embedded into `workerd`. To change them you need the
Hyperlight guest toolchain (`cargo-hyperlight` + `clang`).

1. Edit the runtime source:
   `deps/rust/hyperlight-js-runtime-custom/src/main.rs`
   (Rust classes via `#[rquickjs::class]`, or JS polyfills via `ctx.eval`).
2. Rebuild the guest blob:
   ```sh
   cd deps/rust/hyperlight-js-runtime-custom
   ./build-runtime.sh   # cross-compiles + installs ../hyperlight-js-runtime/jsruntime.bin
   ```
3. Rebuild `workerd` — Bazel re-embeds the updated blob:
   ```sh
   bazel build //src/workerd/server:workerd
   ```

You can iterate on the runtime **natively** (much faster than the guest build)
because the crate also builds as a CLI:

```sh
cd deps/rust/hyperlight-js-runtime-custom
cargo run -- path/to/handler.js '{}'
```

---

## 9. Where the code lives

| Path | Role |
| --- | --- |
| `src/workerd/server/workerd.capnp` | the `hyperlightJs` service type + `HyperlightJsServer` config struct |
| `src/workerd/server/server.{h,c++}` | `HyperlightJsService` — wires the config to the adapter |
| `src/rust/hyperlight-js/lib.rs` | Rust adapter: warm micro-VM pool, request → event marshalling, response mapping |
| `src/rust/hyperlight-js/ffi.rs` | the C++ ↔ Rust `cxx` bridge |
| `src/rust/hyperlight-js/bridge.h` | C++ `WorkerInterface` — reads headers+body, calls the adapter, writes the response |
| `deps/rust/hyperlight-js-runtime-custom/` | the custom guest runtime source (the WinterTC globals) + `build-runtime.sh` |
| `deps/rust/hyperlight-js-runtime/jsruntime.bin` | the prebuilt guest runtime blob that `workerd` embeds |
| `deps/rust/Cargo.toml`, `build/deps/rust.MODULE.bazel` | how the `hyperlight-js` dependency is vendored (crate_universe) |

---

## 10. Limitations

This backend is an excellent fit for **synchronous, compute-bound request →
response handlers**. It is **not** a drop-in for streaming proxies,
concurrent-subrequest workers, or WebSocket. In particular:

- **Host functions** — `fetch` (→ workerd) and `crypto.subtle` are **not yet
  wired**; handlers run purely in-guest today.
- **Streaming, concurrent `fetch`, background timers, WebSocket** are blocked by
  the current Hyperlight execution model (sync host calls, no suspend/resume).

See [`hyperlight-js-wintertc.md`](./hyperlight-js-wintertc.md) §3–§8 for the full
tier map and the reasons.
