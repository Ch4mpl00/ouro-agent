#!/usr/bin/env python3
# Runs the workspace's tests with the test binaries side by side, instead of
# `cargo test`'s one binary after another. Builds once (`cargo test --no-run`),
# then runs every test binary — each from its crate's directory, as cargo
# does — at most JOBS at a time, longest first by the previous run's times.
# Each binary's output goes to target/test-logs/<name>.log; a failure prints
# its log. Exit code is non-zero if any binary failed.
#
#   scripts/test.py                  # everything
#   scripts/test.py -p mcp-tools     # passed through to cargo (packages, …)
#   TEST_JOBS=4 scripts/test.py      # how many binaries at once (default 6)
#   TARGET_LIMIT_GB=40 scripts/test.py  # clean target/ above this (default 25)
#
# Every rebuild leaves the previous test binaries behind in target/debug/deps,
# so target/ only grows; past the limit the run starts with `cargo clean`.
#
# Isolation: each test file takes its own fresh database in the shared test
# Postgres (crates/mcp/tests/support/container.rs), and servers bind free
# ports, so binaries don't share state. Doc-tests are not run.

import concurrent.futures
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "target" / "test-logs"
TIMES = LOGS / "times.json"
JOBS = int(os.environ.get("TEST_JOBS", "6"))
TARGET_LIMIT_GB = float(os.environ.get("TARGET_LIMIT_GB", "25"))


def keep_target_small():
    target = ROOT / "target"
    if not target.exists():
        return
    du = subprocess.run(["du", "-sk", str(target)], stdout=subprocess.PIPE, text=True)
    size_gb = int(du.stdout.split()[0]) / 1024 / 1024 if du.returncode == 0 else 0
    if size_gb > TARGET_LIMIT_GB:
        print(f"[test] target/ is {size_gb:.0f} GB (limit {TARGET_LIMIT_GB:.0f}); cargo clean", flush=True)
        subprocess.run(["cargo", "clean"], cwd=ROOT, check=True)


def build(cargo_args):
    cmd = ["cargo", "test", "--no-run", "--message-format=json-render-diagnostics", *cargo_args]
    if not any(a in ("-p", "--package", "--workspace") or a.startswith("--package=") for a in cargo_args):
        cmd.append("--workspace")
    proc = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        sys.exit(proc.returncode)
    binaries = []
    for line in proc.stdout.splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("reason") == "compiler-artifact" and msg.get("profile", {}).get("test") and msg.get("executable"):
            kind = msg["target"]["kind"][0]
            name = f'{msg["package_id"].split("#")[0].rsplit("/", 1)[-1]}::{kind}:{msg["target"]["name"]}'
            binaries.append((name, msg["executable"], str(Path(msg["manifest_path"]).parent)))
    return binaries


def run(name, exe, cwd):
    log = LOGS / (name.replace("::", "__").replace(":", "_") + ".log")
    env = dict(os.environ, CARGO_MANIFEST_DIR=cwd)
    started = time.monotonic()
    with open(log, "w") as out:
        code = subprocess.run([exe], cwd=cwd, env=env, stdout=out, stderr=subprocess.STDOUT).returncode
    return name, code, time.monotonic() - started, log


def main():
    # Read before the clean below, which takes target/test-logs with it.
    try:
        previous = json.loads(TIMES.read_text())
    except (OSError, ValueError):
        previous = {}
    keep_target_small()
    LOGS.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    binaries = build(sys.argv[1:])
    built = time.monotonic() - started
    # Longest first, so the slow ones don't start last and stretch the run.
    binaries.sort(key=lambda b: -previous.get(b[0], 0))
    print(f"[test] built in {built:.0f}s; running {len(binaries)} test binaries, {JOBS} at a time", flush=True)
    return finish(binaries, previous, started, built)


def finish(binaries, previous, started, built):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=JOBS) as pool:
        futures = [pool.submit(run, *b) for b in binaries]
        for future in concurrent.futures.as_completed(futures):
            name, code, took, log = future.result()
            results.append((name, code, took, log))
            print(f"[test] {'ok  ' if code == 0 else 'FAIL'} {took:6.1f}s  {name}", flush=True)
    failed = [r for r in results if r[1] != 0]
    for name, code, took, log in failed:
        print(f"\n===== {name} (exit {code}) — {log} =====")
        print(log.read_text()[-20000:])
    previous.update({name: took for name, code, took, log in results})
    TIMES.write_text(json.dumps(previous, indent=1, sort_keys=True))
    total = time.monotonic() - started
    print(f"\n[test] {len(results) - len(failed)} passed, {len(failed)} failed — {total:.0f}s ({built:.0f}s build)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
