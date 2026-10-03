# Sandboxed Worker process-boundary smoke test

Build workerd with the C++ I/O backend:

```sh
bazel build //src/workerd/server:workerd --//:io_backend=cxx
```

Then start the one-Worker guest:

```sh
bazel-bin/src/workerd/server/workerd serve \
  src/workerd/server/tests/sandbox/guest.capnp
```

In another shell, start the host coordinator:

```sh
bazel-bin/src/workerd/server/workerd serve --experimental \
  src/workerd/server/tests/sandbox/host.capnp
```

Send a streamed request through the shared host listener:

```sh
curl --data payload http://127.0.0.1:8787/hello
```

The response is `guest:/hello:payload`. This proves the request boundary only; the guest process is
not a security boundary and has not yet been replaced by Hyperlight.

## Hyperlight executor bundle protocol

The Hyperlight executor keeps the `init(string)` and `fetch(string)` calls. Protocol v1 `init`
accepts one canonical compact JSON object:

```json
{"protocol_version":1,"worker_version":"hello-v1","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default { fetch() { return new Response(\"hello\") } }"}]}
```

The top-level and module field order shown above is required. Compatibility flags are sorted and
unique. The main module is first; all other modules are sorted by name. Supported module types are
`esModule`, `commonJsModule`, `text`, and `json`. The envelope is limited to 60 KiB, with at most 32
modules, 32 KiB per source, and 48 KiB of aggregate source.

Before compiling CommonJS modules, the executor resolves static string-literal `require()` edges
against this bounded module list. Relative specifiers support explicit files, `.js`, `.cjs`, and
`index` resolution. Bare packaged specifiers use the registered package's bundled `package.json`
`main`; they never consult a host path. Resolution rewrites edges to canonical relative module
names, while traversal and unregistered relative modules reject initialization.

Protocol v2 appends a `storage` field after `modules`. It contains at most eight entries, sorted by
unique logical name:

```json
{"protocol_version":2,"worker_version":"storage-v1","compatibility_date":"2025-12-31","compatibility_flags":["enable_nodejs_fs_module","enable_web_file_system","nodejs_compat"],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"..."}],"storage":[{"name":"readonly","mode":"ro"},{"name":"scratch","mode":"rw"}]}
```

Names contain only lowercase ASCII letters, digits, and hyphens and are limited to 64 bytes. The
manifest never contains a host path. For each entry, the executor opens the fixed guest directory
`/mnt/workerd-storage/<name>` without following symlinks and exposes it at `/storage/<name>`.
`mode` is `ro` or `rw`; the Hyperlight launcher remains responsible for mounting the corresponding
guest path with equal or stricter hostfs permissions and quota.

`fetch` emits one compact protocol-v1 response object followed by one LF. The executor validates the
complete response before emitting any bytes and writes it in chunks of at most 1 KiB because the
Hyperlight console path truncates larger writes. Intermediate chunks never contain LF.

Generate the canonical sample bundles with:

```sh
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py helloworld
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py web-streams
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py wintertc-smoke
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py wintertc-evidence \
  --output /tmp/wintertc-evidence.json
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py filesystem-evidence \
  --output /tmp/filesystem-evidence.json
```

The filesystem evidence bundle has two deliberately separate routes. `/evidence/workerd-vfs`
probes Workerd's pinned `/bundle`, request-local `/tmp`, and `/dev/{null,zero,random}` behavior.
Call it twice on one executor instance; both responses must report `tmp.freshRequest: true`.
`/evidence/hostfs` probes the Hyperlight-backed `/storage/ro` and `/storage/rw` mounts, including
allowed read, read-only denial, bounded write, traversal, and unlisted-name errors. Pass
`?quotaBytes=<host-quota-exceeding-size>` to require the launcher-enforced quota path; the returned
error code must be `EDQUOT`.

`ecma-429-support-matrix.json` records the representative executor API smoke and the corresponding
upstream Workerd WPT baseline. The executor probe is a smoke test, not a WinterTC or ECMA-429
conformance suite. It distinguishes pure Web API behavior from APIs unavailable by sandbox policy.

## Hyperlight outbound fetch protocol

Global `fetch()` uses Workerd subrequest channel 0 and a typed Hyperlight host-call adapter. The
guest supplies the HTTP method, URL, ordered header block, and request body. Network policy remains
host-only and is never represented in the guest protocol. Other subrequest channels, raw
connections, WebSockets, actors, and capability-backed networking remain unavailable.

Protocol v2 is the default. `WorkerdFetchV2Start`, `Write`, `Finish`, `Poll`, `Read`, and `Cancel`
form a bounded streaming operation. Start negotiates write and read payload chunks, currently
preferring 32 KiB and never exceeding the 64 KiB host-call ABI. Write transmits the JSON header block
before body bytes and applies host backpressure. Poll can expose response metadata before upload
finishes. Read returns a pending, data, or EOF tag and streams response bytes directly into
Workerd's native HTTP response. Dropping the Workerd request cancels the host operation.

Protocol v1 remains a non-streaming bring-up fallback. It buffers at most 1 MiB of request body and
4 MiB of response body, permits at most 16 concurrent operations, and has a 10 second total
deadline. It is not fetch-parity and should not be selected where v2 is available.

## WinterTC evidence bundle

`wintertc-evidence.js` is the behavior probe used after loading the generated
`wintertc-evidence.json` through the coordinator's existing `init(string)` call. Send these requests
through `fetch(string)`:

```json
{"protocol_version":1,"request_id":"core-1","method":"GET","url":"https://evidence.test/evidence/core","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"timers-1","method":"GET","url":"https://evidence.test/evidence/timers","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"handlers-1","method":"GET","url":"https://evidence.test/evidence/global-handlers","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"messageport-1","method":"GET","url":"https://evidence.test/evidence/messageport","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"byob-1","method":"GET","url":"https://evidence.test/evidence/byob","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"tee-1","method":"GET","url":"https://evidence.test/evidence/byte-stream-tee","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"wasm-1","method":"GET","url":"https://evidence.test/evidence/core-wasm","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"fetch-1","method":"GET","url":"https://evidence.test/evidence/fetch?upstream=https%3A%2F%2FHOST-POLICY-ALLOWED-ECHO%2F","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"state-a1","method":"GET","url":"https://evidence.test/evidence/state?token=tenant-a-first","headers":[],"body_base64":""}
{"protocol_version":1,"request_id":"state-a2","method":"GET","url":"https://evidence.test/evidence/state?token=tenant-a-second","headers":[],"body_base64":""}
```

The aggregate core response runs global `error` and `unhandledrejection` handlers and the cached
core Wasm fixture through independent pass/error wrappers. Timers, MessagePort, BYOB, and
byte-stream tee are represented as isolated results and run only through their dedicated endpoints,
so a native failure cannot hide the other results by terminating the aggregate request. The
global-handler and MessagePort probes use no timers. The timer endpoint reports separate
`timer-start`, `timer-immediate-read`, `timer-fired-read-ordering`, and `timer-cancel` stages.
The MessagePort endpoint accepts a `stage` query parameter with `construct`,
`listener-registration`, `start`, `post-message`, `queued-delivery`, `close`,
`transfer-reentanglement`, and `clone-failure` values; omitting it returns that inventory.
The bundle explicitly enables `worker_global_scope_event_handlers` and
`message_port_standard_semantics`. Core Wasm compilation and instantiation happen once during
permitted module startup; requests only call the cached instance's export. The fetch probe uploads
16 32-KiB chunks paced by 5-ms timers. Its response records status, response chunk count, byte
count, body, and SHA-256 while the request and response traverse the streaming host-call adapter.
Replace `HOST-POLICY-ALLOWED-ECHO` with a deterministic host-controlled echo endpoint; network
policy stays outside the guest.

The state requests provide the isolation check. Two requests on one deliberately reused instance
must report monotonically increasing `requestCount`, and the second must report
`previousStateToken: "tenant-a-first"`. A newly acquired instance for another isolation key must
start with `requestCount: 1` and `previousStateToken: null`. Any other result is a state leak or pool
classification error.

`wintertc-evidence-manifest.json` is the machine-readable scenario inventory. Its
`known_deviations` list intentionally retains the remaining
`readable-byte-streams/patched-global.any.js` deviation: Workerd does not synchronously invoke a
patched `Object.prototype.then` getter during `controller.enqueue()`. This must remain a reported
deviation rather than being hidden by a passing evidence expectation.

Validate the manifest, generated bundle limits, explicit deviations, and metrics fixture with:

```sh
python3 src/workerd/server/tests/sandbox/validate-wintertc-evidence.py
```

### Component Model and WASI boundary

This executor cannot load a WebAssembly Component Model/WASI component. Its bundle parser accepts
only `esModule`, `commonJsModule`, `text`, and `json`, and requires the main module to be an ES
module. Workerd exposes V8's core `WebAssembly` API, but `node:wasi` is explicitly a non-functional
stub whose constructor and lifecycle methods throw `ERR_METHOD_NOT_IMPLEMENTED`. There is no
component linker, canonical ABI implementation, or WASI Preview 2 host in this path.

The deterministic fallback is the core Wasm module embedded in `wintertc-evidence.js`. It needs no
WASI imports, is compiled and instantiated during module startup, and its cached instance is used
by the `/evidence/core` request. A component demo becomes possible only after the executor protocol
gains a component module type and the runtime gains a Component Model/WASI host; adding only a
`.wasm` bundle type would not cross that boundary.

## Optimized executor package

Do not run this while another Workerd validation owns the heavy build slot. From Ubuntu WSL, after
the tee commit has been integrated, package with:

```sh
cd /mnt/c/Users/sdavies/.copilot/repos/copilot-worktrees/workerd/simongdavies-workerd-compliance-integration
WORKERD_BAZEL_JOBS=16 \
  python3 src/workerd/server/tests/sandbox/package-optimized-executor.py \
  --tee-commit "$TEE_COMMIT" \
  --clean-checkout /absolute/path/to/clean/workerd \
  --clean-checkout-sha "$CLEAN_WORKERD_SHA" \
  /home/simon/workerd-wintertc-package
```

Add `--dry-run` to print the exact Docker invocation without starting the build.
Add `--preflight-only` to validate the expected base plus all four signed source commits without
starting Docker. The script fails before build if a commit is missing or unsigned, HEAD is not the
adapter base, the integration tree is clean, or `git diff --check` fails. After build it fails
explicitly if the artifact is missing, non-executable, not an ELF static PIE, over the 128 MiB
page-aligned PT_LOAD gate, dynamically interpreted, or contains NEEDED/RPATH/RUNPATH entries.
It also rejects source trees missing the frozen fetch v1/v2 or WorkerdTimerV1 adapter names. A
successful package writes `hyperlight-handoff.json` with the executor absolute path, SHA-256, GNU
BuildID, page-aligned PT_LOAD span, static-PIE checks, clean checkout path/SHA, source commits,
protocol inventory, and lower-memory eligibility.

Use `--finalize-existing --evidence-bundle <path>` to refresh evidence inputs without rebuilding an
unchanged executor. If the output directory already contains a handoff, finalize-only packaging
preserves its executor build provenance rather than attributing the binary to the current dirty
tree. A colocated `wintertc-expected-rows.json` is included in the archive with the evidence bundle.

The script pins image
`sha256:2c84806bb370a129dd1bec8bc0242591ec20defa78b5ffc7d04a13aafe6d6237`,
shares `/home/sdavies/.cache/workerd-bazel` for the output, action, and repository caches, mounts the
validated `/home/simon/.cache/workerd-libcxx22` read-only, and uses only the required host
link/rpath and action environment flags. It intentionally supplies no target `--linkopt`. The
optimized static PIE is rejected if its page-aligned PT_LOAD span exceeds 128 MiB, has PT_INTERP,
or contains NEEDED, RPATH, or RUNPATH entries. The package includes the binary, SHA-256, ELF
inspection output, and a compressed archive.

## Load and pool metrics

The host pool should write one JSON object per completed request. Required fields are:

```json
{"request_id":"cold-1","mode":"cold","started_ns":0,"completed_ns":1000000,"pool_acquire_ms":0.1,"init_ms":10.0,"request_ms":2.0,"total_ms":12.1,"cpu_ms":8.0,"peak_rss_bytes":134217728,"error_code":null,"isolate_id":"isolate-1","state_token":"tenant-a-first","observed_previous_token":null,"expected_clean_state":true}
```

Use `mode` values `cold`, `prewarmed`, and `offline_restore`. Measure monotonic timestamps around
pool acquisition, guest init, request dispatch, and complete response consumption. Record process
CPU time deltas and peak guest/host RSS at the same boundaries. The load driver should use fixed
concurrency and duration for each mode, report the attempted request count, and retain every error
code rather than retrying invisibly.

Summarize the JSONL with:

```sh
python3 src/workerd/server/tests/sandbox/summarize-executor-metrics.py \
  src/workerd/server/tests/sandbox/wintertc-metrics.fixture.jsonl \
  --output /tmp/workerd-wintertc-summary.json
```

The summary reports request count, success count, error rate and codes, throughput, min/mean/p50/p95/
p99/max for acquisition, init, request, total, and CPU time, peak RSS, distinct isolate count, and
state-leak failures. Compare cold versus prewarmed/offline restore using the same bundle, request
mix, host, concurrency, and sample duration.
