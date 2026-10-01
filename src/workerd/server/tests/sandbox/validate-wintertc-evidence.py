#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load_bundle_builder():
    path = ROOT / "make-executor-bundle.py"
    spec = importlib.util.spec_from_file_location("make_executor_bundle", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest",
        type=Path,
        default=ROOT / "wintertc-evidence-manifest.json",
    )
    parser.add_argument(
        "--metrics-fixture",
        type=Path,
        default=ROOT / "wintertc-metrics.fixture.jsonl",
    )
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    bundle = load_bundle_builder().bundle("wintertc-evidence")

    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported evidence manifest schema")
    for field in (
        "worker_version",
        "compatibility_date",
        "compatibility_flags",
        "main_module",
    ):
        if manifest["bundle"][field] != bundle[field]:
            raise ValueError(f"manifest bundle mismatch: {field}")
    expected_flags = [
        "worker_global_scope_event_handlers",
        "message_port_standard_semantics",
    ]
    if bundle["compatibility_flags"] != expected_flags:
        raise ValueError("evidence compatibility flags must be explicitly enabled")
    expected_adapters = {
        "timer_adapter": {
            "version": 1,
            "host_calls": [
                "WorkerdTimerV1Start",
                "WorkerdTimerV1Read",
                "WorkerdTimerV1Cancel",
            ],
        },
        "fetch_adapter": {
            "version": 2,
            "host_calls": [
                "WorkerdFetchV2Start",
                "WorkerdFetchV2Write",
                "WorkerdFetchV2Finish",
                "WorkerdFetchV2Poll",
                "WorkerdFetchV2Read",
                "WorkerdFetchV2Cancel",
            ],
        },
        "core_wasm_fixture": {
            "initialization": "module-startup",
            "cached_request_result": 42,
        },
    }
    if manifest.get("executor_expectations") != expected_adapters:
        raise ValueError("executor adapter expectations are incomplete")
    if {scenario["id"] for scenario in manifest["scenarios"]} != {
        "core-aggregate",
        "timers",
        "global-handlers",
        "messageport",
        "byob",
        "byte-stream-tee",
        "core-wasm",
        "streaming-fetch",
        "state-reuse",
    }:
        raise ValueError("evidence scenario set is incomplete")
    expected_endpoints = {
        "/evidence/core",
        "/evidence/timers",
        "/evidence/global-handlers",
        "/evidence/messageport",
        "/evidence/byob",
        "/evidence/byte-stream-tee",
        "/evidence/core-wasm",
        "/evidence/fetch",
        "/evidence/state",
    }

    deviations = {
        deviation["id"]: deviation for deviation in manifest["known_deviations"]
    }
    patched_global = deviations["byob-patched-global-synchronous-then-getter"]
    if patched_global["status"] != "expected-deviation":
        raise ValueError("patched-global BYOB deviation must remain explicit")
    if (
        "do not convert it into a passing expectation"
        not in patched_global["evidence_policy"]
    ):
        raise ValueError("patched-global evidence policy is missing")
    if set(deviations) != {"byob-patched-global-synchronous-then-getter"}:
        raise ValueError("unexpected evidence deviation")

    encoded = json.dumps(bundle, ensure_ascii=False, separators=(",", ":")).encode()
    source_text = bundle["modules"][0]["source"]
    source = source_text.encode()
    wasm_module_setup = source_text.find(
        "const coreWasmModule = new WebAssembly.Module("
    )
    wasm_instance_setup = source_text.find(
        "const coreWasm = new WebAssembly.Instance(coreWasmModule);"
    )
    fetch_handler = source_text.find("async fetch(request)")
    if (
        wasm_module_setup < 0
        or wasm_instance_setup < wasm_module_setup
        or fetch_handler < wasm_instance_setup
    ):
        raise ValueError(
            "core Wasm must be synchronously constructed during module startup"
        )
    if "WebAssembly.instantiate(" in source_text:
        raise ValueError("core Wasm must not use asynchronous instantiation")
    missing_endpoints = {
        endpoint
        for endpoint in expected_endpoints
        if f"url.pathname === '{endpoint}'" not in source_text
    }
    if missing_endpoints:
        raise ValueError(f"evidence endpoints are missing: {sorted(missing_endpoints)}")
    if source_text.count("await runSubtest(") < 10:
        raise ValueError("evidence subtests must report independent pass/error results")
    for isolated_endpoint in (
        "/evidence/timers",
        "/evidence/messageport",
        "/evidence/byob",
        "/evidence/byte-stream-tee",
    ):
        if (
            "isolatedSubtest(" not in source_text
            or isolated_endpoint not in source_text
        ):
            raise ValueError("native-risk subtests must be isolated from the aggregate")
    handler_start = source_text.find("async function exerciseGlobalHandlers()")
    messageport_start = source_text.find("async function exerciseMessagePortTransfer()")
    byob_start = source_text.find("async function exerciseByob()")
    if min(handler_start, messageport_start, byob_start) < 0:
        raise ValueError("evidence function inventory is incomplete")
    if "setTimeout(" in source_text[handler_start:messageport_start]:
        raise ValueError("global-handler evidence must not depend on timers")
    if "setTimeout(" in source_text[messageport_start:byob_start]:
        raise ValueError("MessagePort evidence must not depend on timers")
    for timer_stage in (
        "timer-start",
        "timer-immediate-read",
        "timer-fired-read-ordering",
        "timer-cancel",
    ):
        if timer_stage not in source_text:
            raise ValueError(f"timer evidence stage is missing: {timer_stage}")
    for messageport_stage in (
        "construct",
        "listener-registration",
        "start",
        "post-message",
        "queued-delivery",
        "close",
        "transfer-reentanglement",
        "clone-failure",
    ):
        if f"'{messageport_stage}'" not in source_text:
            raise ValueError(
                f"MessagePort evidence stage is missing: {messageport_stage}"
            )
    if len(encoded) + 1 > 60 * 1024:
        raise ValueError("evidence bundle exceeds executor envelope limit")
    if len(source) > 32 * 1024:
        raise ValueError("evidence worker exceeds executor module limit")

    modes = set()
    with args.metrics_fixture.open(encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, 1):
            event = json.loads(line)
            mode = event.get("mode")
            if mode not in {"cold", "prewarmed", "offline_restore"}:
                raise ValueError(f"metrics line {line_number}: invalid mode")
            modes.add(mode)
    if modes != {"cold", "prewarmed", "offline_restore"}:
        raise ValueError("metrics fixture does not cover all acquisition modes")

    print(
        json.dumps(
            {
                "status": "ok",
                "bundle_bytes": len(encoded) + 1,
                "source_bytes": len(source),
                "known_deviations": sorted(deviations),
                "metrics_modes": sorted(modes),
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
