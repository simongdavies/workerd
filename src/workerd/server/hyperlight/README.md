# Hyperlight guest executor

These optional Linux x86-64 targets run the existing Workerd engine in a
Hyperlight guest. They use standard Workerd configuration, Worker interfaces
and application globals. There is no new application API or product control
plane.

The guest call verifier uses published `hyperlight-common` 0.18.0. The host
owns VM admission, image measurement, devices, clocks, entropy, socket policy,
deadlines and final retirement. This target does not create VMs or confer
network or caller authority.

## Build and host tests

```sh
bazel build //src/workerd/server/hyperlight:workerd-hyperlight-selected-executor
bazel test //src/workerd/server/hyperlight:worker-selection_test \
  //src/workerd/server/hyperlight:executor_test \
  //src/workerd/server/server:workerd-server_test \
  //src/workerd/server/factory:rpc-forwarding-test@
bazel test //...
```

The host tests use fake call inputs and owned local streams; they do not
require KVM or a running Hyperlight VM. Hardware execution is a separately
identified qualification lane, not implied by a host test pass.

## Selected process entry

The selected executable supports:

```text
workerd-hyperlight-selected-executor --serve-artifact <config-bytes> <read-budget> <sha256> <socket-name>
```

The host supplies a measured image containing the standard serialized
`Workerd.Config` at `/workerd/config.bin`. The executor validates the exact
regular-file length, independent read budget and SHA-256 before engine
configuration. Supplied paths, symlink traversal, trailing configuration bytes
and a failed configuration retry are not accepted.

The selected entry requires one explicit HTTP socket with a nonempty standard
`capnpConnectHost`, targeting a configured Worker. Its exact Config service
index, entrypoint and opaque properties are retained. The CONNECT host is a
routing selector, not authentication.

Admission is installed before constructing Workers or isolates. An admitted
factory creates one application isolate; it does not first construct and then
discard other configured Workers or a startup-snapshot zygote. Ordinary
unrestricted factories retain their existing behavior.

The unselected executable, `workerd-hyperlight-executor`, remains available for
an embedder that owns the complete service graph. Neither executable's absence
of arguments changes its existing named-call entry behavior.

## Standard call and configuration boundary

The published Hyperlight call codec carries the executor operations
`configure`, `configure_artifact`, `fetch` and `serve_artifact`. Configuration
is a standard Workerd Config, not a provider manifest. Finite `fetch` accepts
one exact HTTP/1 message and returns the raw response within the host-supplied
reply budget. Invalid framing, oversized output and uncertain failures do not
produce success-shaped responses or automatic retries.

Finite configuration accepts Worker services and explicitly supplied channels.
The injected default `internet` channel denies requests. A configured service
cannot replace a supplied channel by reusing its name.

Terminal ingress consumes an unconfigured executor and runs until its owned
drain completes. It admits one explicit unicast IPv4 address and nonzero port
using cleartext HTTP or TCP. The ingress service remains a Worker; existing
standard ExternalServer services may be explicit leafs. Network/disk services,
TLS ingress and wildcard/ephemeral selectors are refused. Native transport
access still requires separate host admission.

HTTP, WebSocket and TCP behavior uses the release's normal listener and
stream interfaces. The bounded owned pumps preserve backpressure and
half-close; read EOF is not treated as full disconnect or original-task join.
Background work and endpoint retirement are settled before executor
destruction, but call completion alone is not a snapshot-safe declaration.

## Embedding services and placement

`Executor::new(channels)` and the one-shot service-factory variant accept
already-admitted Workerd channels. Credentials, product translation and
authorization remain in the embedding host.

`with_worker_endpoints()` supplies every other configured Worker's endpoint
when one Worker is selected. Missing, foreign, duplicate and selected-name
replacements fail before local startup; there is no local fallback.
`with_dynamic_worker_loader()` similarly allows an embedder to host dynamic
descendants without compiling a local substitute. Returned source, bindings
and opaque properties retain their existing Workerd meanings.

An optional actor-storage provider returns the existing namespace Stage
capability. It does not replace durable storage with a local database.
Unsupported namespace-wide or facet operations fail explicitly rather than
silently creating an in-memory namespace.

Trusted terminal connection admission may attach an opaque immutable local
request value before HTTP or RPC dispatch. That value is captured through the
actual async frame. It is not exposed to application JavaScript or serialized,
and is not proof of a later capability caller or a permission grant.

## Owned standard RPC

`UnscopedRpcWorkerEndpoint` accepts an embedding connector returning a fresh,
exclusively owned, already-admitted Cap'n Proto stream per request. A connector
using HTTP must complete the standard CONNECT handshake before returning it.
There is no pool, reconnect/replay, implicit endpoint selection or authority
inference.

The existing RPC client retains its owned stream and original metadata.
Factory retirement retains cleanup outside cancellable ordinary task sets
and waits for the actual local producer to finish or cancel. Local cleanup,
EOF and counters are not an original native or JSG task join.

Published client forwarding and published server support are different.
Ordinary Bootstrap server refusals for prewarm, scheduled, alarm and queue
remain unchanged; abandonAlarm delivery is not implemented there. The wire
has no test or bench method, so the endpoint refuses unsupported test before
dialing. Test-only standard dispatchers qualify client forwarding separately.

## Explicit remaining boundaries

This code supplies embedding seams, not a VM scheduler or completed
cross-owner security protocol. All-three-mode physical placement, remote
actor/dynamic capability transport, effective per-call authority, original
native/JSG joins and remote scheduled/alarm/queue/abandonAlarm delivery remain
separate implementation and qualification obligations. They are not removed
from the intended scope by being unimplemented here.

No product adapter, proprietary binding protocol, deployment controller,
cloud service or native runtime successor is imported by these targets.
Published guest codec compatibility does not assert that every required host
transport capability has already been released.
