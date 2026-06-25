load("//:build/wd_cc_library.bzl", "wd_cc_library")
load("//:build/wd_rust_crate.bzl", "wd_rust_crate")

wd_rust_crate(
    name = "hyperlight-js",
    cxx_bridge_deps = {
        "ffi.rs": [
            "@workerd//src/workerd/io:worker-interface",
            "@workerd//src/rust/kj",
        ],
    },
    cxx_bridge_srcs = ["ffi.rs"],
    proc_macro_deps = [
        "@crates_vendor//:async-trait",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":bridge",
        "@crates_vendor//:anyhow",
        "@crates_vendor//:futures",
        "@crates_vendor//:hyperlight-js",
        "@crates_vendor//:serde_json",
        "@workerd//src/rust/kj",
        "@workerd//src/rust/worker",
    ],
)

wd_cc_library(
    name = "bridge",
    hdrs = ["bridge.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":ffi.rs@cxx",
        "@capnp-cpp//src/kj",
        "@capnp-cpp//src/kj/compat:kj-http",
        "@workerd//src/workerd/io:worker-interface",
        "@workerd//src/workerd/util",
    ],
)
