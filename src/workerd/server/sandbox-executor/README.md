# Sandbox executor capability contracts

The sandbox executor preserves Workerd's native event and binding semantics while adapting
Hyperlight host calls at the I/O-channel boundary. WASI compatibility code should reuse these
surfaces rather than define duplicate fetch, scheduled, queue, or logical-service ingress modules.

## HTTP and WASI adapter

Worker `fetch` remains the stable HTTP ingress. Global outbound `fetch()` uses subrequest channel 0
and the `FetchBroker` v2 streaming protocol, including bounded upload/download chunks, poll-driven
backpressure, cancellation, and classified failures. Raw `connect()` is denied.

The `wasi:http@0.2.12` proxy target should translate its incoming request to the standard Worker
`Request`/`Response` path and use global `fetch()` for HTTP(S) egress. The `wasi:http@0.3.1`
service/middleware target should use the same adapter and compose middleware before invoking the
Worker handler; it should not register a second ingress or host function.

Current HTTP limits are:

| Resource                        |      Limit |
| ------------------------------- | ---------: |
| Ingress request body            |     32 KiB |
| Ingress response body           |     32 KiB |
| Ingress headers                 |         64 |
| Ingress aggregate header bytes  |      8 KiB |
| Ingress URL                     |      8 KiB |
| Outbound request body           |      1 MiB |
| Outbound response body          |      4 MiB |
| Outbound headers                |        128 |
| Outbound aggregate header bytes |     64 KiB |
| Outbound URL                    |     16 KiB |
| Outbound request ID             |  256 bytes |
| Concurrent outbound fetches     |         16 |
| Outbound deadline               | 10 seconds |

Outbound failures are classified as `denied`, `timeout`, `canceled`, `malformed_response`,
`host_failure`, `overloaded`, or `size_limit`. Non-tunneled host errors surface to JavaScript as an
`Error` prefixed `outbound fetch failed`; host details are not exposed.

## Event ingress

Scheduled ingress uses Workerd's native `startScheduled()`, `waitUntil`, and
`finishScheduled()` path, preserving module and service-worker handlers, event outcome, and
`noRetry()`. Queue ingress uses `QueueCustomEvent`, preserving Workerd message decoding,
acknowledgement, retry, batch retry, compatibility flags, and `waitUntil`.

Fetch invocations drain their `IncomingRequest` before returning to Hyperlight. Queue invocations
also wait for the background drain task set, including
`queue_consumer_no_wait_for_wait_until`; the compatibility flag still determines the native queue
handler outcome and acknowledgement semantics. Rejected background promises are reported by the
native I/O context. Expired work is cancelled, and timer/fetch cleanup must finish before a
snapshot-safe boundary can be acknowledged.
An aborted I/O context, including a never-settling promise with no pending I/O, is a failed
completion rather than a successful drain. It cannot acknowledge a snapshot-safe boundary.
`AbortSignal.timeout()` creates an internal request deadline, not application background work.
These deadline timers remain live throughout handler execution, open transport and tracked work,
then retire through normal timer cancellation after that lifetime finishes. User timers,
intervals and scheduler waits retain their application lifetime; they are not cleared to
manufacture quiescence. A tracked promise awaiting a deadline must complete or fail under the
admitted lifetime before any deadline timer can retire.

## Negotiated runtime extensions

The shared [runtime contract](../../../../docs/workerd-runtime-contract-v1.json) defines the additive
`runtime_capabilities`, `invoke`, `ingress_stream`, and `checkpoint` exports. Buffered v1 envelopes
and their LF-terminated responses remain unchanged. A host must query `runtime_capabilities` before
using an extension.

Initialization v4 is an explicit immutable-module loader. Its manifest carries the host-verified
canonical bundle identity and per-module raw byte length/SHA-256 descriptors rather than inline
source. `WorkerdBundleV1Read` returns exactly the requested bytes in chunks of at most 16 KiB,
bound to worker version, bundle digest, module name, and offset. The guest validates all descriptors
before reading, limits each module to 1 MiB and aggregate source to 8 MiB, and verifies every source
digest before constructing V8 state. String modules contain UTF-8; Wasm modules contain raw binary,
not base64. The callback is available only during initialization. Legacy v1/v2/v3 source and
envelope limits remain unchanged.

`invoke` v2 accepts fetch, scheduled, and queue envelopes with an admitted remaining
`lifetime_budget_ms`. One elapsed wall deadline covers handler execution, response transport, and
tracked work; it does not restart at drain. The host watchdog also covers blocked host callbacks and
JavaScript CPU execution. Legacy v1 has native soft limits of 30 seconds for drain and 15 minutes for
scheduled/queue work, subordinate to the host's invocation deadline.

`ingress_stream` adapts native request/response streams and inbound `WebSocketPair` connections to
`WorkerdIngressV1Send` and `WorkerdIngressV1Receive`. The initial request body is empty; the receive
channel supplies the body incrementally. HTTP writes are split into at most 16 KiB decoded frames.
GET/HEAD ingress certifies an absent body, preserving `Request.body === null` and both Request
cloning paths even when `Content-Length: 0` is present. Headers are not stripped. Positive body
lengths or transfer encoding on GET/HEAD are rejected; POST retains incremental body streaming.
WebSocket messages are bounded to 16 KiB, and close frames to 125 bytes. Text and close reasons must
be valid UTF-8. The host validates the original HTTP upgrade and client framing, and synthesizes the
wire handshake when the worker actually accepts an upgrade.

All callbacks run on the isolate's owner thread. The adapter retains at most one HTTP input frame
and one header block, with no unbounded callback queue. Negotiated ingress also bounds the native
JavaScript WebSocket queue to four data messages plus the in-flight message and terminal close;
oversized messages or queue exhaustion throw `RangeError`. Other Workerd transports retain their
native defaults. Receive `pending` and send `backpressure`
yield to a native 1 ms poll timer without consuming a sequence. A backpressured frame must not have
been queued or partially committed by the host. Other rejections and cancellation terminate the
transport; the native tracked-work lifetime is drained before dispatch returns.

Response headers, data, and HTTP `end` can reach the host before tracked work finishes.
`CallDone`, not response `end`, ends invocation ownership. An open WebSocket remains active until
its close handshake and tracked work finish. `checkpoint` acknowledges only an idle runtime with
no outstanding timer/fetch handles. A failed ingress transport or uncertain host cleanup makes
checkpoint fail; the host must retire that VM rather than park or silently reuse it.
An otherwise-completed invocation with an untracked live application timer or fetch is explicitly
rejected as unsupported; checkpoint never cancels such work to manufacture an idle state.

`--self-test` exercises real JavaScript delayed/rejected/expired tracked work, both queue
compatibility branches, budgeted scheduled/queue invocations, incremental HTTP bodies larger than
the buffered v1 limit, inbound text/binary WebSocket echo and close, nonblocking polling,
backpressure retry, cancellation, active-stream checkpoint rejection, and shared negative vectors.
It also loads and executes a 757,597-byte immutable JavaScript module, checks raw Wasm transport,
and rejects corrupted digests, short reads, oversized descriptors, invalid UTF-8, and invalid Wasm.

## Logical service bindings

Worker bundle init protocol v3 adds a sorted `bindings` manifest containing only `{name,kind}`.
Names are globally unique and kinds are `kv`, `cache`, `d1`, `durable_object`, or `webhook`.
KV, Cache, D1, and Durable Object bindings are exposed through a standard Workerd `Fetcher`-style
service channel. Calls are `POST` requests whose
body is the canonical composite-router v2 envelope. Workerd rejects a binding-name or operation-kind
mismatch before invoking the host.

All bindings share the single historical `WorkerdLogicalServiceV1Invoke` host registration.
Canonical envelope version selects frozen dotted v1 or composite v2; no aliases, second
registration, or guest downgrade path exists. Host identity is fixed out of band, and guest
`request_id` is correlation only. The host registration is available only on restored request VMs,
never in the initialized snapshot.

Typed KV, Cache, D1, and Durable Object lanes should translate their existing Workerd API calls onto
these channels. WebSockets and TCP/TLS/UDP remain separate typed Workerd I/O adapters but must reuse
the same restored-VM lifecycle, request budgets, and host-owned identity rules rather than add
logical-service registrations.

### Host-only webhook verification

A `webhook` binding exposes a native `verify(rawBody, signature)` method rather than a generic
Fetcher or guest-held credential. `rawBody` is a UTF-8 string, ArrayBuffer, or ArrayBuffer view;
verification preserves its exact bytes and limits it to 32 KiB. The signature is limited to
1 KiB. The existing logical host transport carries the typed `webhook_verify` operation.

The host alone resolves the authorized operator-private signing key and checks the signature and
timestamp. No signing key, token, or private-file path is supplied to the guest binding, module,
environment, or checkpoint. Invalid signatures return `{valid:false}`; verified events may return
`{valid:true,event:...}`. Policy denial, host failure, mismatched correlation, and malformed or
unverified event results reject with an error. An application adapter may map the canonical
lowercase binding name to `env.WEBHOOK` without introducing a secret into guest state.

## Scoped authenticated provider WebSockets

An additive `provider_websocket` binding exposes `connect(publicUrl, subprotocols)` after
`authenticated-egress-websocket-v1` negotiation. The returned native WebSocket starts in
`CONNECTING` and uses Workerd's standard open, text/binary message, error and close events.
Only secure public endpoint URLs and bounded subprotocols are accepted; no guest authentication
headers or credential-reference parameters exist. The operator-owned host policy authorizes
the exact destination and injects credentials privately. The public URL is validated, not ignored.

The four versioned JSON callbacks are `WorkerdWebSocketV1Open`, `WorkerdWebSocketV1Send`,
`WorkerdWebSocketV1Receive`, and `WorkerdWebSocketV1Close`. Pending receives and backpressured sends
do not consume a sequence. The scoped native queue retains at most four data messages, including
the in-flight message; decoded messages are limited to 16 KiB and cumulative session data to 8 MiB.
`bufferedAmount` includes queued and in-flight data until acceptance. Provider compression is not
negotiated. Host rules may impose lower bounds and always retain the admitted invocation deadline.
Provider `close()` without a code uses normal close code 1000; the frozen callback has no
representation for an empty wire close frame. Ordinary and inbound WebSocket defaults are unchanged.

Connecting, open and closing sessions block successful checkpoint and park. A close event is
terminal only after the host has closed both wire halves, joined the worker and released the handle.
Cancellation can remain pending; this latches non-quiescent guest state, and host teardown must
finish the bounded join before releasing VM capacity. No TLS socket or provider handle is
serialized into a parked instance. A later session redials with reconstructed host authority.

## Native build and qualification

`crypto.randomUUID()` and `crypto.getRandomValues()` obtain fresh host OS CSPRNG bytes on every
call through `WorkerdEntropyV1Read`, with exact reads of 1..16,384 bytes. Larger WebCrypto requests
are chunked, empty requests are no-ops, and provider errors or short reads fail closed. No entropy
pool or deterministic generator is retained in initialized or parked guest state. Native tests
use the OS `getrandom()` source. BoringSSL-backed key generation requires separate clone/resume
qualification; UUID freshness alone does not establish every cryptographic path.

Canonical packaged executors use the pinned Clang/LLD/libc++ 22 toolchain with
`--config=opt --strip=always --//:io_backend=cxx`, matching the Hyperlight setup recipe.
FASTBUILD/Rust-I/O executors are development artifacts, not substitutes for this release profile.
Record the source tree, exact build invocation, executor SHA-256, ELF load footprint, and rootfs
closure separately; stripping produces a distinct derived artifact and does not reduce mapped
`PT_LOAD` segments.

Keep the workspace's existing Bazel output base and local action cache across development and
release builds. Share downloads and portable action results explicitly with
`--repository_cache="$BAZEL_SHARED_CACHE/repos/v1" --disk_cache="$BAZEL_SHARED_CACHE"`; native and
container builders should mount the same content caches with compatible permissions. The disk
cache's `ac`/`cas` entries are separate from repository downloads and initially empty until populated.
Profile changes legitimately invalidate compiler action keys; never copy mismatched object files
or clear existing caches to force a rebuild. Coordinate heavy builds with `flock --close` so a
persistent Bazel server does not inherit the lane lock.

Run the real executor `--self-test` on the exact packaged artifact and qualify its event,
tracked-work, streaming, WebSocket, and changed-heap checkpoint behavior in Hyperlight separately.
Native tests do not establish a guest VM memory profile or cross-host checkpoint compatibility.
Guest-driver exception diagnostics use address-only stack traces; symbolization must not spawn
subprocesses inside the single-address-space kernel.
