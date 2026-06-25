# WinterTC coverage in a workerd Hyperlight JS backend — how it works & where it hurts

> Status: design analysis for a **new workerd service type** that runs a JavaScript Worker inside a
> Hyperlight + QuickJS micro-VM — distinct from the V8/JSG path used by normal `worker` services. It
> maps the WinterTC ("Minimum Common API") surface onto what the current hyperlight execution model
> can and cannot do, and calls out the hard blockers.

---

## 1. The architecture (and what it is *not*)

This is **not** a replacement for workerd's V8 isolates. workerd keeps V8 + JSG + its WinterTC C++
layer for normal `worker` services, untouched. The hyperlight JS backend is a **distinct service
type**: a request to it never enters V8 or JSG — it crosses into a **hardware-isolated micro-VM**
that runs its own JS engine (QuickJS via `hyperlight-js`) with its **own** API surface.

> **In-process, not out-of-process.** Hyperlight is an *embedded* VMM — `hyperlight-host` is a Rust
> library linked into workerd. The guest runs in a hardware-virtualized **micro-VM** (its own vCPU
> and guest memory, isolated in non-root mode), but that VM is created and driven **in-process** by a
> workerd thread that *enters* the vCPU and runs the guest until it VM-exits back to the host. There
> is **no separate OS process, no fork/exec, no daemon** — the isolation boundary is the
> **CPU-virtualization boundary**, not a process boundary. (This in-process embedding is Hyperlight's
> differentiator vs a separate-process VMM like Firecracker.)
>
> **Hyperlight abstracts over the host hypervisor**, so none of this is hypervisor specific. It runs on
> **KVM** (Linux), **mshv** (Microsoft Hypervisor / Hyper-V, e.g. Azure Linux), and **WHP** (Windows
> Hypervisor Platform) today, with **AArch64** across those — plus **Apple's Virtualization
> framework** — on the roadmap. workerd just enters the vCPU through whichever driver the platform
> provides (`KVM_RUN`, `WHvRunVirtualProcessor`, …); the execution model above is identical regardless.

```
curl ─HTTP─▶ workerd HyperlightJsService (C++)        [no V8, no JSG on this path]
                   │  marshal request → JSON event
                   ▼
            hyperlight-js host (Rust)  ── handle_event(handler, event) ──▶ blocking call
                   │                                                          │
                   ▼                                                          ▼
            QuickJS guest in a Hyperlight micro-VM  —— runs handler(event) ——▶ result JSON
                   │  (rewinds to a clean snapshot before each call = isolation)
                   ▼
            response JSON → workerd → HTTP 200
```

Key consequences of the boundary:
- The micro-VM boundary is a **hard memory boundary** (that *is* the isolation). There is **no shared
  object graph** — everything crosses as **JSON strings**, with a **binary sidecar** for
  `Uint8Array`/`Buffer`. workerd's zero-copy buffer passing does **not** apply across it.
- Per-request isolation is via **snapshot/restore** (the guest rewinds to a clean post-init snapshot
  before each call), not via a fresh V8 context.

## 2. The extension mechanisms (this is how we add APIs)

`hyperlight-js` gives three first-class ways to extend the guest,
plus host functions. **This is the toolkit we build the WinterTC surface with.**

| Mechanism | What it does | Runs where | Use it for |
| --- | --- | --- | --- |
| **`custom_globals!`** | Install globals on `globalThis` — Rust classes (`#[rquickjs::class]`) **or** JS polyfills (`ctx.eval`) | in-guest | `TextEncoder`, `URL`, `atob`/`btoa`, `Headers`, … (the doc's own example is `TextEncoder`) |
| **`native_modules!`** | Custom Rust-implemented importable modules; can override built-ins | in-guest | perf-critical bits (compression, hashing) |
| **user modules** | Host-registered reusable ES modules (`import … from 'user:x'`) | in-guest | pure-JS polyfill bundles |
| **host functions** | `hostModule(name).register(fn, cb)` → guest `import * as m from 'host:m'` | **calls back to host** | anything needing the host: `fetch`→workerd, crypto, clock |

Built-ins already present in the guest: `io`, `crypto` (Node-style), `console`, `require`. Bazel
integration uses **`HYPERLIGHT_JS_RUNTIME_PATH`** to embed a prebuilt *custom* runtime (with our
globals baked in) — no nested cross-compile in the build.

## 3. WinterTC coverage map

The WinterTC "Minimum Common API" mapped onto this model. Three tiers:

### ✅ Tier 1 — doable now, in-guest, no host needed (pure-JS polyfill or small Rust class)
| API | How |
| --- | --- |
| `console` | built-in |
| `TextEncoder` / `TextDecoder` | `custom_globals!` (Rust class — example in the runtime docs) |
| `atob` / `btoa` | pure-JS polyfill |
| `URL` / `URLSearchParams` | pure-JS polyfill (well-trodden) |
| `URLPattern` | pure-JS polyfill |
| `Headers` | pure-JS |
| `Request` / `Response` (data shells) | pure-JS — *minus* streaming bodies (see Tier 3) |
| `Event` / `EventTarget` | pure-JS |
| `AbortController` / `AbortSignal` (objects) | pure-JS — *minus* actually aborting async work |
| `Blob` (in-memory) | pure-JS over a byte backing |
| `structuredClone` (JSON-ish) | pure-JS — complex/cyclic types harder |
| `DOMException` | pure-JS |
| `queueMicrotask` | maps to QuickJS's job queue |

> **Not yet: in-guest time.** `performance.now()`/`Date.now()` are **not** an in-guest read today.
> [hyperlight PR #1422](https://github.com/hyperlight-dev/hyperlight/pull/1422) (currently **open/
> unmerged**, behind the **off-by-default** `enable_guest_clock` feature) adds a paravirtualized guest
> clock; **once it merges, the feature is enabled, and hyperlight-js wires QuickJS's clock to it**,
> those become cheap in-guest shared-memory reads (no VM exit) and promote from Tier 2 → Tier 1. Until
> all three happen, time needs the host (Tier 2). The clock is **read-only time**; a *separate* flag,
> **`hw-interrupts`**, adds a host-injected periodic timer IRQ (a preemption primitive) — but neither
> makes the VM run **between** calls, so neither adds a background event loop (see Tier 3's timer note / §4).

### ⚠️ Tier 2 — works but host-backed and **blocking** (a host function into workerd)
| API | How | Caveat |
| --- | --- | --- |
| `fetch` (single, whole-body) | host fn → workerd HTTP + network policy | blocks the VM until the **entire** response is materialized; no streaming body |
| `crypto.subtle` | host fn → workerd WebCrypto (or an in-guest crypto lib) | subtle is async-by-spec; the sync bridge fakes the promise |
| `performance.now()` / `Date.now()` | host clock **today** | in-guest & near-free **once PR #1422 lands + is enabled + wired** (see note above); not in-guest today |
| hashing / non-stream compression | `native_modules!` (Rust) or host fn | fine for one-shot buffers |

### ❌ Tier 3 — hard / blocked by the current hyperlight execution model
| API | Why it's blocked |
| --- | --- |
| `ReadableStream`/`WritableStream`/`TransformStream` | async streaming + backpressure across a hard **copy** boundary; no zero-copy; each chunk is a blocking copy |
| streaming `fetch` request/response bodies | same — body can't stream, only arrive whole |
| **concurrent** `fetch` (parallel subrequests) | the sync host call serializes them |
| `setTimeout`/`setInterval` firing in the **background** (after the response) | the vCPU is **parked between calls**, so nothing fires once the handler returns — *not* for lack of a timer primitive (this branch has `hw-interrupts`), but because the host doesn't keep the vCPU scheduled. **In-handler** timers are tractable; **background** ones need §6.2. See the timer note below |
| `WebSocket` | long-lived bidirectional async — doesn't fit request/response + sync at all |
| real async event loop / top-level `await` of live I/O | no macrotask loop between host calls |

> **On timers, precisely (it's subtler than "no clock").** The `enable-guest-time` branch carries
> *two separate* feature flags, and neither is the blocker people assume:
>
> - **`enable_guest_clock`** — a paravirtualized monotonic clock the guest *reads* from a shared page
>   (no VM exit). Pure timekeeping; promotes `Date.now()`/`performance.now()` (Tier 1). Not a scheduler.
> - **`hw-interrupts`** — a host `TimerThread` injects a periodic timer IRQ (vector `0x20`) into the
>   running guest's LAPIC (KVM `eventfd` / mshv `request_virtual_interrupt` / WHP `WHvRequestInterrupt`),
>   the period armed by the guest via a PV-timer IO port. That **is** a real preemption primitive.
>
> So a timer primitive *exists*; here's what it can and can't do:
>
> - ✅ **In-handler timers** — while the vCPU is running, a `setTimeout` callback can fire (preempt the
>   loop / drain a tiny in-guest timer wheel over QuickJS's job queue). Tractable.
> - ❌ **Background timers** — between requests the host **parks the vCPU**, so injected IRQs just sit
>   pending in the LAPIC IRR until the host re-enters; nothing fires *after the response*. That needs
>   the host to keep the vCPU scheduled / re-enter on a host timer (§6.2), plus a VM-lifetime/billing
>   decision for idle VMs.
>
> Building it **in-guest with tokio is a non-starter**: the guest target (`x86_64-hyperlight-none`) is
> freestanding — no OS, no epoll/threads/syscalls — so tokio's reactor has nothing to stand on. The
> right shape is a tiny `no_std` timer wheel over QuickJS's existing microtask queue, ticked by the
> guest clock — *not* tokio. (Distinct from `InterruptHandle::kill()`, which is **cancellation**: it
> only *stops* a running guest, it never schedules one.)

## 4. The core pain point: **sync host calls vs async WinterTC**

This is the crux, and it's structural.

- **hyperlight-host's host-function type is synchronous** — `Box<dyn Fn(String) -> Result<String>>`.
  When the guest calls a host function, the **QuickJS VM freezes** — it's a blocking `Fn` call, not a
  `Future`. There is **no suspend/resume mechanism** in hyperlight-host/hyperlight-common today.
- **QuickJS *does* have a microtask/job queue**, so `Promise`s resolve **within a single handler
  run** (in-handler `.then`/`await` of already-available values works fine).
- **But there is no macrotask/event loop driven by host I/O.** Async I/O (`fetch`, timers, streams)
  is *faked* by blocking host calls: the guest "`await fetch()`" really means *the host function
  blocks the entire VM until the response is ready, then returns it synchronously* and the promise
  resolves immediately.

What that costs us:
- **No concurrency inside a handler** — two parallel `await`s of host work serialize.
- **No streaming** — you get whole bodies, not chunked async streams.
- **No background timers** — the VM isn't running between calls, so nothing fires `setTimeout` *after
  the response returns*. (The `enable-guest-time` branch adds a guest **clock** (`enable_guest_clock`)
  to *read* time and **`hw-interrupts`** to *preempt the running guest* — enough for **in-handler**
  timers, but neither makes a **parked** vCPU run a callback later; that needs the host to keep the
  vCPU scheduled, §6.2. See §3's timer note.)
- **No WebSocket** — there's no place for long-lived async bidirectional I/O to live.

WinterTC fundamentally assumes an **async event loop**; the micro-VM runs **synchronously to
completion per call**. That mismatch is the whole story.

## 5. Streaming, specifically (workerd's superpower vs our boundary)

workerd's defining strength is **zero-copy streaming** — `ReadableStream`s piped through the C++ I/O
layer without copying. Our boundary is the opposite: a **hard copy boundary** with no shared memory.
The best achievable today is a **pull-based, synchronous, copy-per-chunk** emulation:

```
guest: const chunk = host.readChunk(streamId)   // BLOCKS the VM
host:  returns the next chunk (a copy across the boundary)
guest: ...process...; repeat until done
```

That is functional for "read a body in pieces," but it **blocks the VM per chunk**, has **no real
backpressure**, **no zero-copy**, and **no async concurrency**. It is not `ReadableStream` semantics
— it's a synchronous iterator with a copy tax. Streaming proxies (the thing workers are great at)
are therefore the *worst* fit for this backend.

## 6. What would actually unblock the hard parts

In rough dependency order:

1. **Async host functions with guest suspend/resume** in `hyperlight-host`/`hyperlight-common`.
   Today host calls are blocking `Fn`s. If the guest could **yield** (suspend the VM) while the host
   performs async work, and the host could **drive an event loop and resume** the guest, then
   concurrent `fetch`, timers, and async streams all become possible. **This is the single biggest
   lever — and it's a core capability that does not exist today.**
2. **Event-loop integration** — bridge the guest's job queue + a host timer/IO loop to workerd's KJ
   event loop. Depends on (1).
3. **A streaming protocol** over the boundary (chunked + backpressure signalling). Still copy-per-
   chunk, but *async* once (1) lands.

Without (1), Tier 3 stays blocked no matter how many polyfills we write — it's an execution-model
limit, not an API-authoring limit.

## 7. Honest scope ladder

| Horizon | What ships | Effort |
| --- | --- | --- |
| **Now** | QuickJS handler + `console` + a few `custom_globals!` (`TextEncoder`, `URL`, base64) + a blocking `fetch` host fn → compute/transform handlers with a basic WinterTC-ish surface | days |
| **Medium** | Fuller pure-JS data classes (`Headers`, `Request`/`Response` shells, `URLSearchParams`, `URLPattern`, `structuredClone`, `Event`/`AbortController`) + host-backed `crypto` + whole-body `fetch` → **most of the non-streaming Minimum Common API** | weeks |
| **Hard** | Real async **streams**, **concurrent** `fetch`, **timers**/event loop, **WebSocket**, streaming bodies → **true async WinterTC** | months — **blocked on the suspend/resume core capability (§6.1)** |

## 8. Bottom line

The Hyperlight JS backend is an excellent fit for **synchronous, compute-bound, request → response
handlers** — data transformation, validation, generation, single-shot `fetch`-then-respond. That is
exactly the proven HyperAgent use case (run an LLM-written JS handler that computes and returns).

It is **not** a drop-in for **streaming proxies, concurrent-subrequest workers, or WebSocket** — those
require an async execution model that hyperlight-host does not provide yet. Closing that gap is a
**hyperlight-core** project (async host calls + suspend/resume), not a polyfill exercise.

So WinterTC coverage splits cleanly: **the data-class half is tractable now** via the guest
extension toolkit; **the async/streaming half is gated** on a core capability we'd have to build.
