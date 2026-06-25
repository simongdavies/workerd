#!/usr/bin/env bash
# Build the custom hyperlight-js guest runtime (this crate) for the Hyperlight target and install
# it as the prebuilt blob workerd embeds (//deps/rust/hyperlight-js-runtime:jsruntime.bin).
#
# Requires `cargo-hyperlight` (cargo install cargo-hyperlight) and clang. Run from anywhere:
#   deps/rust/hyperlight-js-runtime-custom/build-runtime.sh
set -euo pipefail
cd "$(dirname "$0")"

# QuickJS needs the libc stub headers from hyperlight-js-runtime/include and -D__wasi__=1 (disables
# pthreads). Locate the include dir from the resolved hyperlight-js-runtime git dependency.
runtime_manifest=$(cargo metadata --format-version 1 | python3 -c \
  "import json,sys; m=json.load(sys.stdin); print(next(p['manifest_path'] for p in m['packages'] if p['name']=='hyperlight-js-runtime'))")
runtime_dir=$(dirname "$runtime_manifest")
export HYPERLIGHT_CFLAGS="-I${runtime_dir}/include -D__wasi__=1 -D_POSIX_MONOTONIC_CLOCK"

echo "HYPERLIGHT_CFLAGS=${HYPERLIGHT_CFLAGS}"
cargo hyperlight build --profile release --target-dir target

blob="target/x86_64-hyperlight-none/release/hyperlight-js-runtime-custom"
dest="../hyperlight-js-runtime/jsruntime.bin"
cp "$blob" "$dest"
echo "Installed $(stat -c%s "$dest") byte runtime -> $(cd "$(dirname "$dest")" && pwd)/$(basename "$dest")"
