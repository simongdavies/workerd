# Logical service broker protocol

This package defines the Workerd side of one versioned host-call envelope for logical KV, Cache
API, policy, and identity bindings. It deliberately contains no backing-store implementation.

The Hyperlight host supplies `RequestIdentity` out of band, authorizes the parsed request with an
explicit `Policy`, charges host-owned quotas, and only then dispatches to a logical service. An empty
grant set denies every request. The strict schema rejects unknown fields, so host paths, live
database names, credentials, trust roots, and storage policy cannot be smuggled into the ABI.

`fixtures/` contains canonical deterministic envelopes for coordinating the eventual executor and
host adapters. The remaining integration step is to register `WorkerdLogicalServiceV1Invoke` with
the sandbox executor and implement the corresponding trusted Hyperlight host dispatcher.

`Dispatcher` is the trusted adapter contract. It requires canonical request bytes, exact host-owned
identity authorization, a successful request-scoped budget reservation, and a typed service adapter
before dispatch. It validates and canonicalizes the adapter response, settles each reservation once,
and emits payload-free audit events containing only opaque identity, binding, operation, decision,
and byte counts.
