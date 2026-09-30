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

Build and validate the static executor from the repository root:

```sh
docker build --platform=linux/amd64 -t workerd-dev -f .devcontainer/Dockerfile .devcontainer
mkdir -p "$HOME/.cache/workerd-bazel" "$HOME/workerd-artifacts"
docker run --rm --platform=linux/amd64 \
  -v "$PWD:/workspace" \
  -v "$HOME/.cache/workerd-bazel:/root/.cache/bazel" \
  -v "$HOME/workerd-artifacts:/artifacts" \
  -w /workspace \
  workerd-dev \
  bash -lc '
    set -eux
    bazel build //src/workerd/server:workerd-sandbox-executor \
      --//:io_backend=cxx \
      --workspace_status_command=/bin/true \
      --jobs="$(nproc)" \
      --repo_env=CC=/usr/lib/llvm-22/bin/clang \
      --repo_env=AR=/usr/lib/llvm-22/bin/llvm-ar \
      --linkopt=--ld-path=/usr/lib/llvm-22/bin/ld.lld \
      --host_linkopt=--ld-path=/usr/lib/llvm-22/bin/ld.lld

    executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
    "$executor" --self-test
    /usr/lib/llvm-22/bin/llvm-strip "$executor"
    file_output=$(file "$executor")
    printf "%s\n" "$file_output"
    printf "%s\n" "$file_output" | grep -Eq "x86-64|Advanced Micro Devices X86-64"
    ! readelf -l "$executor" | grep -q INTERP
    ldd "$executor" 2>&1 | grep -q "statically linked"
    cp "$executor" /artifacts/workerd-sandbox-executor
    chmod 0755 /artifacts/workerd-sandbox-executor
    sha256sum /artifacts/workerd-sandbox-executor
  '
```

The Hyperlight executor keeps the `init(string)` and `fetch(string)` calls. `init` accepts one
canonical compact JSON object:

```json
{"protocol_version":1,"worker_version":"hello-v1","compatibility_date":"2023-02-28","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default { fetch() { return new Response(\"hello\") } }"}]}
```

The top-level and module field order shown above is required. Compatibility flags are sorted and
unique. The main module is first; all other modules are sorted by name. Supported module types are
`esModule`, `text`, and `json`. The envelope is limited to 60 KiB, with at most 32 modules, 32 KiB
per source, and 48 KiB of aggregate source.

`fetch` emits one compact protocol-v1 response object followed by one LF. The executor validates the
complete response before emitting any bytes and writes it in chunks of at most 1 KiB because the
Hyperlight console path truncates larger writes. Intermediate chunks never contain LF.

Generate the canonical sample bundles with:

```sh
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py helloworld
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py web-streams
python3 src/workerd/server/tests/sandbox/make-executor-bundle.py wintertc-smoke
```

`ecma-429-support-matrix.json` records the representative executor API smoke and the corresponding
upstream Workerd WPT baseline. The executor probe is a smoke test, not a WinterTC or ECMA-429
conformance suite. It distinguishes pure Web API behavior from APIs unavailable by sandbox policy.
