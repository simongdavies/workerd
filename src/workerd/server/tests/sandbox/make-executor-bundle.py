#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]


def module(name: str, source: Path) -> dict[str, str]:
    return {
        "name": name,
        "type": "esModule",
        "source": source.read_text(encoding="utf-8"),
    }


def bundle(name: str) -> dict[str, object]:
    if name == "helloworld":
        main = ROOT / "samples/helloworld_esm/worker.js"
        return {
            "protocol_version": 1,
            "worker_version": "helloworld-v1",
            "compatibility_date": "2023-02-28",
            "compatibility_flags": [],
            "main_module": "worker.js",
            "modules": [module("worker.js", main)],
        }
    if name == "web-streams":
        sample = ROOT / "samples/web-streams"
        return {
            "protocol_version": 1,
            "worker_version": "web-streams-v1",
            "compatibility_date": "2025-12-31",
            "compatibility_flags": [],
            "main_module": "worker.js",
            "modules": [
                module("worker.js", sample / "worker.js"),
                module("streams-util", sample / "streams-util.js"),
            ],
        }
    if name == "wintertc-smoke":
        source = ROOT / "src/workerd/server/sandbox-executor/wintertc-smoke.js"
        return {
            "protocol_version": 1,
            "worker_version": "wintertc-smoke-v1",
            "compatibility_date": "2025-12-31",
            "compatibility_flags": [],
            "main_module": "worker.js",
            "modules": [module("worker.js", source)],
        }
    if name == "wintertc-evidence":
        source = ROOT / "src/workerd/server/tests/sandbox/wintertc-evidence.js"
        return {
            "protocol_version": 1,
            "worker_version": "wintertc-evidence-v1",
            "compatibility_date": "2025-12-31",
            "compatibility_flags": [
                "worker_global_scope_event_handlers",
                "message_port_standard_semantics",
            ],
            "main_module": "worker.js",
            "modules": [module("worker.js", source)],
        }
    if name == "filesystem-evidence":
        source = ROOT / "src/workerd/server/tests/sandbox/filesystem-evidence.js"
        return {
            "protocol_version": 2,
            "worker_version": "filesystem-evidence-v1",
            "compatibility_date": "2025-12-31",
            "compatibility_flags": [
                "enable_nodejs_fs_module",
                "enable_web_file_system",
                "nodejs_compat",
            ],
            "main_module": "worker.js",
            "modules": [module("worker.js", source)],
            "storage": [
                {"name": "readonly", "mode": "ro"},
                {"name": "scratch", "mode": "rw"},
            ],
        }
    raise ValueError(f"unknown bundle: {name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "bundle",
        choices=[
            "helloworld",
            "web-streams",
            "wintertc-smoke",
            "wintertc-evidence",
            "filesystem-evidence",
        ],
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    encoded = (
        json.dumps(bundle(args.bundle), ensure_ascii=False, separators=(",", ":"))
        + "\n"
    )
    if args.output:
        with args.output.open("w", encoding="utf-8", newline="\n") as output:
            output.write(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
