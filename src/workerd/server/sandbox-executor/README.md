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

| Resource | Limit |
| --- | ---: |
| Ingress request body | 32 KiB |
| Ingress response body | 32 KiB |
| Ingress headers | 64 |
| Ingress aggregate header bytes | 8 KiB |
| Ingress URL | 8 KiB |
| Outbound request body | 1 MiB |
| Outbound response body | 4 MiB |
| Outbound headers | 128 |
| Outbound aggregate header bytes | 64 KiB |
| Outbound URL | 16 KiB |
| Outbound request ID | 256 bytes |
| Concurrent outbound fetches | 16 |
| Outbound deadline | 10 seconds |

Outbound failures are classified as `denied`, `timeout`, `canceled`, `malformed_response`,
`host_failure`, `overloaded`, or `size_limit`. Non-tunneled host errors surface to JavaScript as an
`Error` prefixed `outbound fetch failed`; host details are not exposed.

## Event ingress

Scheduled ingress uses Workerd's native `startScheduled()`, `waitUntil`, and
`finishScheduled()` path, preserving module and service-worker handlers, event outcome, and
`noRetry()`. Queue ingress uses `QueueCustomEvent`, preserving Workerd message decoding,
acknowledgement, retry, batch retry, compatibility flags, and `waitUntil`.

## Logical service bindings

Worker bundle init protocol v3 adds a sorted `bindings` manifest containing only `{name,kind}`.
Names are globally unique and kinds are `kv`, `cache`, `d1`, or `durable_object`. Each binding is
exposed through a standard Workerd `Fetcher`-style service channel. Calls are `POST` requests whose
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
