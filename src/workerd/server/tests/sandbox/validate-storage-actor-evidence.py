#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXECUTOR = ROOT.parent.parent / "sandbox-executor"


def main() -> None:
    manifest = json.loads(
        (ROOT / "storage-actor-evidence.json").read_text(encoding="utf-8")
    )
    fixtures = json.loads(
        (ROOT / "storage-actor-canonical-fixtures.json").read_text(encoding="utf-8")
    )
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported storage/actor evidence schema")

    identity = manifest["shared_identity"]
    if identity["fields"] != [
        "tenant_id",
        "namespace_id",
        "resource_id",
        "worker_version",
        "snapshot_id",
    ]:
        raise ValueError("logical identity fields are not deterministic")
    forbidden = {"path", "mount", "credential", "connection_string"}
    if set(identity["forbidden_fields"]) != forbidden:
        raise ValueError("host storage secrets are not explicitly excluded")

    sqlite = manifest["sqlite_broker"]
    if (
        sqlite["abi_version"] != 1
        or sqlite["host_call"] != "WorkerdSqliteV1Transaction"
        or sqlite["persistence_owner"] != "host"
    ):
        raise ValueError("SQLite host ownership contract is incomplete")
    if set(sqlite["transaction_modes"]) != {"read_only", "read_write"}:
        raise ValueError("SQLite transaction modes are incomplete")
    if sqlite["transaction_outcomes"] != ["committed", "rolled_back"]:
        raise ValueError("SQLite transaction outcomes are incomplete")
    if set(sqlite["audit_outcomes"]) != {
        "committed",
        "rolled_back",
        "denied",
        "failed",
    }:
        raise ValueError("SQLite audit outcomes are incomplete")
    if set(sqlite["policy_denials"]) != {
        "attach",
        "detach",
        "vacuum",
        "pragma",
        "load_extension",
        "transaction_control",
    }:
        raise ValueError("SQLite policy denial set is incomplete")
    if set(sqlite["quota_dimensions"]) != {
        "statements",
        "sql_bytes",
        "parameters",
        "parameter_bytes",
        "rows",
        "rows_read",
        "result_bytes",
        "rows_written",
    }:
        raise ValueError("SQLite quota dimensions are incomplete")

    actor = manifest["actor_router"]
    if (
        actor["abi_version"] != 1
        or actor["host_call"] != "WorkerdActorV1Route"
        or actor["request_vm_parity"] is not False
    ):
        raise ValueError("actor seam must remain separate from request-VM parity")
    if actor["lifecycle"] != [
        "absent",
        "loading",
        "active",
        "persisting",
        "evicted",
    ]:
        raise ValueError("actor lifecycle order is not deterministic")
    if actor["route_generation"] != "monotonic-per-logical-actor":
        raise ValueError("actor route generation policy is incomplete")

    expected_scenarios = {
        "opaque-identity-rejects-host-path",
        "snapshot-scoped-routing",
        "transaction-policy-before-persistence",
        "transaction-input-output-quotas",
        "sqlite-host-failure-audit",
        "actor-state-reuse",
        "actor-capacity-eviction",
        "actor-persistence-reload",
        "actor-generation-upgrade",
        "actor-stale-generation-rejection",
        "actor-persistence-failure-lifecycle",
        "cross-runtime-canonical-fixtures",
    }
    if set(manifest["scenarios"]) != expected_scenarios:
        raise ValueError("storage/actor evidence scenarios are incomplete")

    public_headers = {
        "state": (EXECUTOR / "sandbox-state.h").read_text(encoding="utf-8"),
        "storage": (EXECUTOR / "sandbox-storage.h").read_text(encoding="utf-8"),
        "actors": (EXECUTOR / "sandbox-actors.h").read_text(encoding="utf-8"),
    }
    for forbidden_field in forbidden:
        if any(forbidden_field in source.lower() for source in public_headers.values()):
            raise ValueError(
                f"public broker ABI exposes forbidden field: {forbidden_field}"
            )
    if "SQLITE_BROKER_ABI_VERSION = 1" not in public_headers["storage"]:
        raise ValueError("SQLite ABI version does not match evidence")
    if sqlite["host_call"] not in public_headers["storage"]:
        raise ValueError("SQLite host call does not match evidence")
    if (
        "SqliteTransactionOutcome" not in public_headers["storage"]
        or "SqliteAuditEvent" not in public_headers["storage"]
        or "SqliteAuditOutcome" not in public_headers["storage"]
    ):
        raise ValueError("SQLite transaction or audit shapes do not match evidence")
    if "ACTOR_BROKER_ABI_VERSION = 1" not in public_headers["actors"]:
        raise ValueError("actor ABI version does not match evidence")
    if actor["host_call"] not in public_headers["actors"]:
        raise ValueError("actor host call does not match evidence")
    if (
        "ActorRouteResult" not in public_headers["actors"]
        or "ActorLifecycleEvent" not in public_headers["actors"]
        or "routeGeneration" not in public_headers["actors"]
    ):
        raise ValueError("actor route or lifecycle shapes do not match evidence")

    if fixtures.get("schema_version") != 1:
        raise ValueError("unsupported canonical fixture schema")
    fixture_identity = fixtures["identity"]
    expected_key = "\n".join(
        fixture_identity[field]
        for field in (
            "tenant_id",
            "namespace_id",
            "resource_id",
            "worker_version",
            "snapshot_id",
        )
    )
    if fixture_identity["canonical_key"] != expected_key:
        raise ValueError("canonical logical identity key does not match its fields")

    sqlite_fixtures = fixtures["sqlite"]
    if sqlite_fixtures["host_call"] != sqlite["host_call"]:
        raise ValueError("canonical SQLite host call does not match evidence")
    committed = sqlite_fixtures["committed"]
    if committed["response"] != {
        "protocol_version": 1,
        "request_id": "sqlite-commit-1",
        "outcome": "committed",
        "logical_commit": 7,
        "failed_statement": None,
        "error_code": "",
        "result_count": 2,
    }:
        raise ValueError("canonical committed transaction shape changed")
    rolled_back = sqlite_fixtures["rolled_back"]
    if (
        rolled_back["logical_commit"] is not None
        or rolled_back["failed_statement"] != 1
        or rolled_back["result_count"] != 0
        or rolled_back["audit_outcome"] != "rolled_back"
    ):
        raise ValueError("canonical rollback must not expose partial results")
    if sqlite_fixtures["host_failure"] != {
        "request_id": "sqlite-host-failure-1",
        "audit_outcome": "failed",
        "detail": "host persistence failed",
    }:
        raise ValueError("canonical SQLite host failure audit shape changed")
    forbidden_ids = {case["id"] for case in sqlite_fixtures["forbidden"]}
    if forbidden_ids != {
        "attach-host-path",
        "pragma",
        "transaction-control",
        "multiple-statements",
        "comment-obfuscation",
        "unparsed-common-table-expression",
    }:
        raise ValueError("canonical forbidden SQL evidence is incomplete")
    if any(
        case["audit_outcome"] != "denied"
        for case in sqlite_fixtures["forbidden"]
    ):
        raise ValueError("forbidden SQL must produce denied audit evidence")

    actor_fixtures = fixtures["actors"]
    if actor_fixtures["host_call"] != actor["host_call"]:
        raise ValueError("canonical actor host call does not match evidence")
    routes = actor_fixtures["routes"]
    if [route["route_generation"] for route in routes] != [1, 2]:
        raise ValueError("canonical actor route generations are not monotonic")
    if [route["persisted_version"] for route in routes] != [1, 2]:
        raise ValueError("canonical actor persistence versions are not monotonic")
    if routes[1]["response"] != routes[0]["response"] + routes[1]["input"]:
        raise ValueError("canonical actor state continuity is invalid")
    lifecycle = actor_fixtures["lifecycle"]
    expected_transitions = [
        ("absent", "loading", 1, 0),
        ("loading", "active", 1, 0),
        ("active", "persisting", 1, 0),
        ("persisting", "active", 1, 1),
        ("active", "evicted", 1, 1),
        ("absent", "loading", 2, 0),
        ("loading", "active", 2, 1),
        ("active", "persisting", 2, 1),
        ("persisting", "active", 2, 2),
    ]
    actual_transitions = [
        (
            event["from"],
            event["to"],
            event["route_generation"],
            event["persisted_version"],
        )
        for event in lifecycle
    ]
    if actual_transitions != expected_transitions:
        raise ValueError("canonical actor lifecycle evidence changed")
    stale_route = actor_fixtures["stale_route"]
    if (
        stale_route["rejected_generation"] >= stale_route["persisted_generation"]
        or stale_route["outcome"] != "denied"
    ):
        raise ValueError("canonical stale actor route evidence is invalid")
    persistence_failure = actor_fixtures["persistence_failure"]
    if persistence_failure != {
        "route_generation": 1,
        "persisted_version": 0,
        "transitions": [
            ["active", "persisting"],
            ["persisting", "active"],
        ],
        "reason": "persistence-failed",
    }:
        raise ValueError("canonical actor persistence failure lifecycle changed")

    print(
        json.dumps(
            {
                "status": "ok",
                "schema_version": manifest["schema_version"],
                "scenario_count": len(expected_scenarios),
                "forbidden_identity_fields": sorted(forbidden),
                "fixture_counts": {
                    "forbidden_sql": len(sqlite_fixtures["forbidden"]),
                    "actor_routes": len(routes),
                    "actor_lifecycle_events": len(lifecycle),
                },
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
