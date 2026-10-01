#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path

MODES = ("cold", "prewarmed", "offline_restore")
TIMINGS = (
    "pool_acquire_ms",
    "init_ms",
    "request_ms",
    "total_ms",
    "cpu_ms",
)


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize_values(values: list[float]) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "mean": statistics.fmean(values) if values else None,
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values) if values else None,
    }


def load_events(path: Path) -> list[dict[str, object]]:
    events = []
    with path.open(encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, 1):
            if not line.strip():
                continue
            event = json.loads(line)
            mode = event.get("mode")
            if mode not in MODES:
                raise ValueError(f"line {line_number}: invalid mode {mode!r}")
            events.append(event)
    if not events:
        raise ValueError("metrics input is empty")
    return events


def summarize(events: list[dict[str, object]]) -> dict[str, object]:
    by_mode = defaultdict(list)
    for event in events:
        by_mode[event["mode"]].append(event)

    mode_summaries = {}
    for mode in MODES:
        samples = by_mode[mode]
        errors = Counter(
            str(sample.get("error_code"))
            for sample in samples
            if sample.get("error_code") is not None
        )
        start_ns = [int(sample["started_ns"]) for sample in samples]
        end_ns = [int(sample["completed_ns"]) for sample in samples]
        wall_seconds = (max(end_ns) - min(start_ns)) / 1_000_000_000 if samples else 0
        successful = sum(sample.get("error_code") is None for sample in samples)

        mode_summaries[mode] = {
            "requests": len(samples),
            "successful": successful,
            "errors": dict(sorted(errors.items())),
            "error_rate": (len(samples) - successful) / len(samples)
            if samples
            else None,
            "throughput_rps": successful / wall_seconds if wall_seconds > 0 else None,
            "timings_ms": {
                timing: summarize_values(
                    [
                        float(sample[timing])
                        for sample in samples
                        if sample.get(timing) is not None
                    ]
                )
                for timing in TIMINGS
            },
            "peak_rss_bytes": max(
                (int(sample.get("peak_rss_bytes", 0)) for sample in samples),
                default=None,
            ),
        }

    isolation_failures = []
    first_token_by_isolate = {}
    for event in events:
        isolate_id = event.get("isolate_id")
        expected_clean = event.get("expected_clean_state")
        observed_previous = event.get("observed_previous_token")
        if expected_clean and observed_previous is not None:
            isolation_failures.append(
                {
                    "request_id": event.get("request_id"),
                    "isolate_id": isolate_id,
                    "observed_previous_token": observed_previous,
                }
            )
        if isolate_id is not None and isolate_id not in first_token_by_isolate:
            first_token_by_isolate[isolate_id] = event.get("state_token")

    return {
        "schema": {
            "required": [
                "request_id",
                "mode",
                "started_ns",
                "completed_ns",
                "pool_acquire_ms",
                "init_ms",
                "request_ms",
                "total_ms",
                "cpu_ms",
                "peak_rss_bytes",
            ],
            "modes": list(MODES),
        },
        "modes": mode_summaries,
        "isolation": {
            "passed": not isolation_failures,
            "failures": isolation_failures,
            "distinct_isolates": len(first_token_by_isolate),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input", type=Path, help="JSONL measurements from the host pool"
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    encoded = json.dumps(summarize(load_events(args.input)), indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(encoded + "\n", encoding="utf-8")
    else:
        print(encoded)


if __name__ == "__main__":
    main()
