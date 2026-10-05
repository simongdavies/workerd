# Logical service broker protocols

This package defines the Workerd side of two explicitly-dispatched logical-service envelopes behind
one historical Hyperlight host-call registration. It deliberately contains no backing-store
implementation.

Signed dotted protocol v1 remains byte-for-byte frozen. It contains KV, Cache API, policy, and
identity operations using the canonical fixtures under `fixtures/`. The Hyperlight host supplies
`RequestIdentity` out of band, authorizes the parsed request with an explicit `Policy`, charges
host-owned quotas, and only then dispatches to a logical service. An empty grant set denies every
request. The strict schema rejects unknown fields, so host paths, live database names, credentials,
trust roots, and storage policy cannot be smuggled into the ABI.

Composite protocol v2 uses the canonical top-level request order
`version,request_id,binding,operation`; `operation.kind` is the first operation field and is a
closed enum:

The coordinating contract revision is SHA-256
`0b54afbe94ebd68189769aa0959bb761706385c85bba81c36373242913a80926`.

- KV: `kv_get`, `kv_put`, `kv_delete`, `kv_list`
- Cache API: `cache_match`, `cache_put`, `cache_delete`
- D1: `d1_batch`
- Durable Objects: `do_deliver`, `do_alarm`, `do_storage_get`, `do_storage_put`,
  `do_storage_delete`, `do_storage_list`, `do_set_alarm`, `do_delete_alarm`, `do_passivate`

The common v2 response fields are ordered `version,request_id,status,code`, followed only by fields
from the selected central capability schema. Unknown versions, binding kinds, and operation kinds
are rejected before adapter execution. There are no aliases and no guest negotiation or downgrade.

The host function remains named `WorkerdLogicalServiceV1Invoke` for ABI stability. Its name does not
select the wire protocol: the first canonical envelope field selects signed v1 or composite v2. A
second v2 host function must not be registered. The host function is available only in restored
request VMs, never during initialized snapshot capture.

Worker bundle init protocol v3 adds a sorted, globally unique `bindings` manifest after `storage`.
Each manifest entry contains exactly `{name,kind}`, where `kind` is `kv`, `cache`, `d1`, or
`durable_object`. It contains no host path, database identifier, credentials, quotas, policy,
identity, actor placement, or routing generation. Workerd exposes each entry as a service binding
on subrequest channels starting at 1; channel 0 remains global outbound fetch. The service binding
accepts only `POST` with a canonical v2 envelope, requires its envelope binding and operation kind
to match the manifest entry, and returns the canonical router response as JSON. Typed KV, Cache,
D1, and Durable Object API lanes can translate onto this one channel surface without registering
additional host functions.

Identity is not part of the v2 manifest. Trusted
`RequestIdentity { workload_id, snapshot_id, attempt }` remains fixed out of band and exact-matched
by the host. Guest `request_id` is correlation only. Signed v1 identity behavior remains unchanged.
Persistent backing state and schema versions are host-owned and survive request-VM teardown;
request budgets, concurrency, handles, counters, and audit buffers reset for every restored VM.

`Dispatcher` is the trusted adapter contract. It requires canonical request bytes, exact host-owned
identity authorization, a successful request-scoped budget reservation, and a typed service adapter
before dispatch. It validates and canonicalizes the adapter response, settles each reservation once,
and emits payload-free audit events containing only opaque identity, binding, operation, decision,
and byte counts.
