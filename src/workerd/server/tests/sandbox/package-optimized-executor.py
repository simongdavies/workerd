#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import tarfile
from pathlib import Path

IMAGE = "sha256:2c84806bb370a129dd1bec8bc0242591ec20defa78b5ffc7d04a13aafe6d6237"
BASE_COMMIT = "039b00382b21d0de328359f68e1b26e266c48826"
INTEGRATED_COMMITS = (
    "4bd4e3feede891e043dbd6d1ed76e3d21b66d322",
    "8010bd8a7e9c0dc8d90266359329b86d99fa6df7",
    "7c2ccee563100310f489feebe827d9833b0d1c42",
)


CONTAINER_SCRIPT = r"""
set -euo pipefail
bazel --output_base=/root/.cache/bazel/workerd-wintertc-output \
  build \
  --config=opt \
  --strip=always \
  --//:io_backend=cxx \
  --workspace_status_command=/bin/true \
  --disk_cache=/root/.cache/bazel/action-cache \
  --repository_cache=/root/.cache/bazel/repository-cache \
  --jobs="$WORKERD_BAZEL_JOBS" \
  --repo_env=CC=/usr/lib/llvm-22/bin/clang \
  --host_linkopt=-L/opt/libcxx22 \
  --host_linkopt=-Wl,-rpath,/opt/libcxx22 \
  --action_env=LD_LIBRARY_PATH=/opt/libcxx22 \
  --host_action_env=LD_LIBRARY_PATH=/opt/libcxx22 \
  --noshow_progress \
  --noshow_loading_progress \
  //src/workerd/server:workerd-sandbox-executor

executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
python3 src/workerd/server/tests/sandbox/package-optimized-executor.py \
  --verify-artifact "$executor" \
  --inspection-output /package

"$executor" --self-test
install -m 0755 "$executor" /package/workerd-sandbox-executor
sha256sum /package/workerd-sandbox-executor > /package/workerd-sandbox-executor.sha256
chown "$(stat -c '%u:%g' /package)" \
  /package/workerd-sandbox-executor \
  /package/workerd-sandbox-executor.sha256 \
  /package/file.txt \
  /package/program-headers.txt \
  /package/dynamic-section.txt \
  /package/notes.txt
"""


def run_output(command: list[str], cwd: Path | None = None) -> str:
    return subprocess.check_output(command, text=True, cwd=cwd).strip()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def select_git(checkout: Path) -> str:
    if re.match(r"^/mnt/[A-Za-z]/", checkout.resolve().as_posix()):
        git_exe = shutil.which("git.exe")
        if git_exe is None:
            raise SystemExit(
                "Windows-backed WSL checkout requires git.exe for ref preflight"
            )
        return git_exe
    git_file = checkout / ".git"
    if git_file.is_file():
        pointer = git_file.read_text(encoding="utf-8", errors="replace")
        if re.search(r"gitdir:\s*[A-Za-z]:[/\\]", pointer):
            git_exe = shutil.which("git.exe")
            if git_exe is None:
                raise SystemExit(
                    "Windows-backed WSL worktree requires git.exe for ref preflight"
                )
            return git_exe
    return "git"


def verify_commit(git: str, checkout: Path, commit: str) -> None:
    try:
        subprocess.run(
            [git, "cat-file", "-e", f"{commit}^{{commit}}"],
            check=True,
            cwd=checkout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"missing required commit: {commit}") from error
    try:
        subprocess.run(
            [git, "verify-commit", commit],
            check=True,
            cwd=checkout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"required commit is not validly signed: {commit}") from error


def validate_executor_protocol_sources(worktree: Path) -> None:
    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            worktree / "src/workerd/server/sandbox-executor/sandbox-executor.c++",
            worktree / "src/workerd/server/sandbox-executor/sandbox-runtime.c++",
        )
    )
    required = (
        "WorkerdFetchV1Start",
        "WorkerdFetchV2Start",
        "WorkerdTimerV1Start",
        "WorkerdTimerV1Read",
        "WorkerdTimerV1Cancel",
    )
    missing = [name for name in required if name not in sources]
    if missing:
        raise SystemExit(
            "executor source is missing required host-call adapters: "
            + ", ".join(missing)
        )


def validate_clean_checkout(path: Path, expected_sha: str) -> str:
    if not path.is_absolute() or not path.is_dir():
        raise SystemExit(f"clean checkout path is not an absolute directory: {path}")
    git = select_git(path)
    actual_sha = run_output([git, "rev-parse", "HEAD"], cwd=path)
    if actual_sha != expected_sha:
        raise SystemExit(
            f"clean checkout SHA mismatch: {actual_sha}; expected {expected_sha}"
        )
    if run_output([git, "status", "--porcelain=v1"], cwd=path):
        raise SystemExit(f"clean checkout has local changes: {path}")
    return actual_sha


def build_source_identity(head: str, patch_sha256: str) -> str:
    return hashlib.sha256(f"{head}\n{patch_sha256}\n".encode("ascii")).hexdigest()


def capture_worktree_patch(git: str, worktree: Path) -> bytes:
    patch = bytearray(
        subprocess.check_output(
            [git, "diff", "--binary", "--full-index", "HEAD"], cwd=worktree
        )
    )
    untracked = subprocess.check_output(
        [git, "ls-files", "--others", "--exclude-standard", "-z"], cwd=worktree
    ).split(b"\0")
    for encoded_path in filter(None, untracked):
        path = encoded_path.decode("utf-8")
        result = subprocess.run(
            [
                git,
                "diff",
                "--binary",
                "--full-index",
                "--no-index",
                "--",
                "/dev/null",
                path,
            ],
            cwd=worktree,
            stdout=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 1:
            raise SystemExit(f"failed to capture untracked source file: {path}")
        patch.extend(result.stdout)
    return bytes(patch)


def capture_build_source(
    worktree: Path, output_directory: Path, source_patch: Path | None
) -> dict[str, object]:
    git = select_git(worktree)
    head = run_output([git, "rev-parse", "HEAD"], cwd=worktree)
    branch = run_output([git, "branch", "--show-current"], cwd=worktree)
    status = run_output([git, "status", "--porcelain=v1"], cwd=worktree).splitlines()
    patch_path = output_directory / "workerd-build-source.patch"
    if source_patch is None:
        patch_path.write_bytes(capture_worktree_patch(git, worktree))
    else:
        shutil.copyfile(source_patch, patch_path)
    patch_sha256 = sha256_file(patch_path)
    identity = build_source_identity(head, patch_sha256)
    return {
        "worktree_path": str(worktree),
        "branch": branch,
        "head": head,
        "dirty": True,
        "dirty_path_count": len(status),
        "tracked_binary_patch": {
            "path": str(patch_path.resolve()),
            "sha256": patch_sha256,
            "bytes": patch_path.stat().st_size,
        },
        "head_and_patch_identity_sha256": identity,
    }


def preflight(worktree: Path, tee_commit: str) -> None:
    if tee_commit == BASE_COMMIT or tee_commit in INTEGRATED_COMMITS:
        raise SystemExit(
            "tee commit must be distinct from the base and integrated commits"
        )
    git = select_git(worktree)
    head = run_output([git, "rev-parse", "HEAD"], cwd=worktree)
    if head != BASE_COMMIT:
        raise SystemExit(f"unexpected HEAD: {head}; expected base {BASE_COMMIT}")
    for commit in (*INTEGRATED_COMMITS, tee_commit):
        verify_commit(git, worktree, commit)
    subprocess.run([git, "diff", "--check"], check=True, cwd=worktree)
    changed = set(
        run_output([git, "status", "--porcelain=v1"], cwd=worktree).splitlines()
    )
    if not changed:
        raise SystemExit("integration worktree has no uncommitted changes")
    validate_executor_protocol_sources(worktree)


def verify_artifact(path: Path, inspection_output: Path | None) -> dict[str, object]:
    if not path.is_file():
        raise SystemExit(f"executor artifact is missing: {path}")
    if not os.access(path, os.X_OK):
        raise SystemExit(f"executor artifact is not executable: {path}")

    file_output = run_output(["file", str(path)])
    if "ELF" not in file_output or "static-pie linked" not in file_output:
        raise SystemExit(f"executor is not an ELF static PIE: {file_output}")

    headers = run_output(["readelf", "-W", "-l", str(path)])
    segments = [
        (int(address, 16), int(memory_size, 16))
        for address, memory_size in re.findall(
            r"^\s*LOAD\s+0x[0-9a-f]+\s+0x([0-9a-f]+)\s+0x[0-9a-f]+\s+"
            r"0x[0-9a-f]+\s+0x([0-9a-f]+)",
            headers,
            re.MULTILINE | re.IGNORECASE,
        )
    ]
    if not segments:
        raise SystemExit("executor has no PT_LOAD segments")
    low = min(address & ~0xFFF for address, _ in segments)
    high = max((address + size + 0xFFF) & ~0xFFF for address, size in segments)
    span = high - low
    print(f"PT_LOAD_PAGE_SPAN=0x{span:08x} ({span} bytes)")
    if span > 0x08000000:
        raise SystemExit("executor exceeds the 128 MiB Hyperlight PT_LOAD gate")
    if "INTERP" in headers:
        raise SystemExit("executor unexpectedly has PT_INTERP")

    dynamic = run_output(["readelf", "-W", "-d", str(path)])
    for forbidden in ("NEEDED", "RPATH", "RUNPATH"):
        if forbidden in dynamic:
            raise SystemExit(f"executor unexpectedly has {forbidden}")

    notes = run_output(["readelf", "-W", "-n", str(path)])
    build_id_match = re.search(r"Build ID:\s*([0-9a-f]+)", notes, re.IGNORECASE)
    if build_id_match is None:
        raise SystemExit("executor is missing a GNU BuildID")
    sha256 = hashlib.sha256(path.read_bytes()).hexdigest()

    if inspection_output is not None:
        inspection_output.mkdir(parents=True, exist_ok=True)
        (inspection_output / "file.txt").write_text(
            file_output + "\n", encoding="utf-8"
        )
        (inspection_output / "program-headers.txt").write_text(
            headers + "\n", encoding="utf-8"
        )
        (inspection_output / "dynamic-section.txt").write_text(
            dynamic + "\n", encoding="utf-8"
        )
        (inspection_output / "notes.txt").write_text(notes + "\n", encoding="utf-8")

    return {
        "executor_path": str(path.resolve()),
        "sha256": sha256,
        "build_id": build_id_match.group(1).lower(),
        "pt_load_page_span_bytes": span,
        "pt_load_page_span_hex": f"0x{span:08x}",
        "static_pie": True,
        "has_pt_interp": False,
        "has_needed": False,
        "has_rpath": False,
        "has_runpath": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_directory", type=Path, nargs="?")
    parser.add_argument(
        "--jobs",
        type=int,
        default=int(os.environ.get("WORKERD_BAZEL_JOBS", "16")),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the exact Docker invocation without starting the build",
    )
    parser.add_argument("--tee-commit")
    parser.add_argument("--clean-checkout", type=Path)
    parser.add_argument("--clean-checkout-sha")
    parser.add_argument(
        "--source-patch",
        type=Path,
        help="frozen git diff --binary captured from the source state used for the build",
    )
    parser.add_argument(
        "--finalize-existing",
        action="store_true",
        help="verify and package an existing executor without invoking Bazel",
    )
    parser.add_argument(
        "--evidence-bundle",
        type=Path,
        help="canonical wintertc-evidence bundle to include in the handoff package",
    )
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--verify-artifact", type=Path)
    parser.add_argument("--inspection-output", type=Path)
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("run this script from Ubuntu WSL")

    if args.verify_artifact is not None:
        print(
            json.dumps(
                verify_artifact(args.verify_artifact, args.inspection_output),
                separators=(",", ":"),
            )
        )
        return
    if args.tee_commit is None:
        parser.error("--tee-commit is required")
    worktree = Path.cwd().resolve()
    preflight(worktree, args.tee_commit)
    if args.preflight_only:
        print("integration_preflight_ok")
        return
    if args.clean_checkout is None or args.clean_checkout_sha is None:
        parser.error("--clean-checkout and --clean-checkout-sha are required")
    if args.output_directory is None:
        parser.error("output_directory is required unless --preflight-only is used")

    output_directory = args.output_directory.resolve()
    clean_checkout = args.clean_checkout.resolve()
    clean_checkout_sha = validate_clean_checkout(
        clean_checkout, args.clean_checkout_sha
    )
    cache_root = Path(
        os.environ.get("WORKERD_BAZEL_CACHE_ROOT", "/home/sdavies/.cache/workerd-bazel")
    )
    libcxx22_root = Path(
        os.environ.get("WORKERD_LIBCXX22_ROOT", "/home/simon/.cache/workerd-libcxx22")
    )

    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        "workerd-wintertc-optimized-build",
        "-e",
        f"WORKERD_BAZEL_JOBS={args.jobs}",
        "-v",
        f"{worktree}:/workerd",
        "-v",
        f"{cache_root}:/root/.cache/bazel",
        "-v",
        "/home/simon/.local/bin/bazel:/usr/local/bin/bazel:ro",
        "-v",
        f"{libcxx22_root}:/opt/libcxx22:ro",
        "-v",
        f"{output_directory}:/package",
        "-w",
        "/workerd",
        IMAGE,
        "/bin/bash",
        "-lc",
        CONTAINER_SCRIPT,
    ]
    if args.dry_run:
        print(shlex.join(command))
    else:
        output_directory.mkdir(parents=True, exist_ok=True)
        executor = output_directory / "workerd-sandbox-executor"
        if not args.finalize_existing:
            subprocess.run(command, check=True)
        verification = verify_artifact(executor, output_directory)
        handoff_path = output_directory / "hyperlight-handoff.json"
        existing_handoff = None
        if args.finalize_existing and handoff_path.exists():
            existing_handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
        build_source = (
            existing_handoff["build_source"]
            if existing_handoff is not None
            else capture_build_source(worktree, output_directory, args.source_patch)
        )
        if existing_handoff is not None:
            patch_path = output_directory / "workerd-build-source.patch"
            patch_sha256 = sha256_file(patch_path)
            build_source["tracked_binary_patch"] = {
                "path": str(patch_path.resolve()),
                "sha256": patch_sha256,
                "bytes": patch_path.stat().st_size,
            }
            build_source["head_and_patch_identity_sha256"] = build_source_identity(
                build_source["head"], patch_sha256
            )
        evidence_bundle = None
        if args.evidence_bundle is not None:
            bundle_path = output_directory / "wintertc-evidence.bundle.json"
            if args.evidence_bundle.resolve() != bundle_path.resolve():
                shutil.copyfile(args.evidence_bundle, bundle_path)
            bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
            source_path = (
                worktree / "src/workerd/server/tests/sandbox/wintertc-evidence.js"
            )
            manifest_path = (
                worktree
                / "src/workerd/server/tests/sandbox/wintertc-evidence-manifest.json"
            )
            evidence_bundle = {
                "path": str(bundle_path.resolve()),
                "sha256": sha256_file(bundle_path),
                "bytes": bundle_path.stat().st_size,
                "worker_version": bundle["worker_version"],
                "compatibility_date": bundle["compatibility_date"],
                "compatibility_flags": bundle["compatibility_flags"],
                "source_sha256": sha256_file(source_path),
                "manifest_sha256": sha256_file(manifest_path),
            }
        handoff = {
            "schema_version": 2,
            "executor": verification,
            "build_source": build_source,
            "upstream_comparison": existing_handoff["upstream_comparison"]
            if existing_handoff is not None
            else {
                "path": str(clean_checkout),
                "sha": clean_checkout_sha,
                "clean": True,
            },
            "build": existing_handoff["build"]
            if existing_handoff is not None
            else {
                "container_image": IMAGE,
                "jobs": args.jobs,
                "target": "//src/workerd/server:workerd-sandbox-executor",
                "configuration": ["--config=opt", "--strip=always"],
                "host_libcxx_root": "/opt/libcxx22",
                "target_linkopts": [],
            },
            "source_commits": existing_handoff["source_commits"]
            if existing_handoff is not None
            else {
                "base": BASE_COMMIT,
                "integrated": list(INTEGRATED_COMMITS),
                "tee": args.tee_commit,
            },
            "protocols": {
                "entrypoints": ["init(string)", "fetch(string)"],
                "fetch": ["v1", "v2"],
                "timer": "WorkerdTimerV1",
            },
            "memory_policy": {
                "lower_memory_pt_load_limit_bytes": 0x08000000,
                "lower_memory_eligible": verification["pt_load_page_span_bytes"]
                <= 0x08000000,
                "default_vm_memory_mib": 768,
            },
        }
        if evidence_bundle is not None:
            handoff["evidence_bundle"] = evidence_bundle
        handoff_path.write_text(
            json.dumps(handoff, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        archive_path = output_directory / "workerd-sandbox-executor.tar.gz"
        package_files = [
            "workerd-sandbox-executor",
            "workerd-sandbox-executor.sha256",
            "file.txt",
            "program-headers.txt",
            "dynamic-section.txt",
            "notes.txt",
            "hyperlight-handoff.json",
            "workerd-build-source.patch",
        ]
        if evidence_bundle is not None:
            package_files.append("wintertc-evidence.bundle.json")
        expected_rows = output_directory / "wintertc-expected-rows.json"
        if expected_rows.exists():
            package_files.append(expected_rows.name)
        with tarfile.open(archive_path, "w:gz") as archive:
            for name in package_files:
                archive.add(output_directory / name, arcname=name)


if __name__ == "__main__":
    main()
