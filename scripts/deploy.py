#!/usr/bin/env python3
"""Build off-host, deliver changed binaries, apply one native deployment.

Deploys never send a Docker image. No previous release is kept.
"""
import argparse
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import uuid

PREFIX = Path("/opt/ouro")
STATE = Path("/var/lib/ouro")
CONFIG = Path("/etc/ouro")
BINARIES = {"mcp": "mcp-tools", "agent": "agent-supervisor"}
NATIVE = {"mcp", "mcp-tunnel", "agent"}
ASSETS = ["skills.default", "crates/mcp/gateway.config.json", "deploy", "docker-compose.yml", "scripts/deploy.py"]


def run(args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, **kwargs)


def output(args, **kwargs):
    return run(args, stdout=subprocess.PIPE, text=True, **kwargs).stdout.strip()


def digest(path):
    result = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def plan(paths, bootstrap=False, requested=()):
    """Pure change selection: tests/docs cannot trigger a build or restart."""
    binaries, services = set(), set()
    infrastructure = bootstrap
    if bootstrap:
        binaries.update(BINARIES.values())
        services.update(NATIVE | {"tunnel-client"})
    for path in paths:
        if path in {"Cargo.toml", "Cargo.lock"}:
            binaries.update(BINARIES.values())
            services.update(NATIVE)
        for crate, binary, affected in [
            ("mcp", "mcp-tools", {"mcp", "mcp-tunnel", "agent"}),
            ("agent", "agent-supervisor", {"agent"}),
        ]:
            root = f"crates/{crate}/"
            if path == root + "Cargo.toml" or path.startswith((root + "src/", root + "migrations/")):
                binaries.add(binary)
                services.update(affected)
        if path.startswith("skills.default/"):
            services.update(NATIVE)
        if path == "crates/mcp/gateway.config.json":
            services.update({"mcp", "agent"})
        if path == "docker-compose.yml" or path.startswith("deploy/systemd/"):
            infrastructure = True
            services.update(NATIVE | {"tunnel-client"})
        if path == "deploy/probe.py":
            services.update(NATIVE | {"tunnel-client"})
    for name in requested:
        services.add(name)
        if name in BINARIES:
            binaries.add(BINARIES[name])
        if name == "mcp-tunnel":
            binaries.add("mcp-tools")
        if name == "mcp":
            services.add("agent")
    # The two MCP processes share one executable, so both must switch it.
    if "mcp-tools" in binaries:
        services.update(NATIVE)
    return {"binaries": sorted(binaries), "services": sorted(services), "infrastructure": infrastructure}


def ssh(host, args, **kwargs):
    return run(["ssh", host, shlex.join([str(a) for a in args])], **kwargs)


def remote_info(host):
    code = (
        "import json,pathlib,hashlib; p=pathlib.Path('/opt/ouro'); "
        "r=p/'revision'; "
        "print(json.dumps({'revision':r.read_text().strip() if r.exists() else None,"
        "'hashes':{f.name:hashlib.sha256(f.read_bytes()).hexdigest() "
        "for f in (p/'bin').glob('*') if f.is_file()}}))"
    )
    return json.loads(ssh(host, ["python3", "-c", code], stdout=subprocess.PIPE, text=True).stdout)


def extract_archive(archive, payload):
    payload = Path(payload).resolve()
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        # git archive contains only the requested, committed runtime files.
        # Resolve macOS /var -> /private/var before containment checks.
        for member in source.getmembers():
            if member.issym() or member.islnk() or not (payload / member.name).resolve().is_relative_to(payload):
                raise RuntimeError("unexpected archive entry: " + member.name)
        source.extractall(payload)


def local(args):
    started = time.monotonic()
    root = Path(output(["git", "rev-parse", "--show-toplevel"]))
    os.chdir(root)
    run(["git", "diff", "--quiet"])
    run(["git", "diff", "--cached", "--quiet"])
    revision = output(["git", "rev-parse", "HEAD"])
    run(["git", "fetch", "-q", "origin"])
    if not output(["git", "branch", "-r", "--contains", revision]):
        raise RuntimeError("commit must be pushed before deployment")
    info = remote_info(args.host)
    bootstrap = info["revision"] is None
    paths = [] if bootstrap else output(["git", "diff", "--name-only", info["revision"], revision]).splitlines()
    selection = plan(paths, bootstrap, args.services)
    print(f"[deploy] {revision[:12]}: {json.dumps(selection)}", flush=True)
    if not selection["services"] and not bootstrap:
        print("[deploy] no runtime changes; no build, upload or restart")
        return
    with tempfile.TemporaryDirectory(prefix="ouro-deploy-") as directory:
        payload = Path(directory).resolve()
        archive = run(["git", "archive", revision, *ASSETS], stdout=subprocess.PIPE).stdout
        extract_archive(archive, payload)
        binary_dir = payload / "bin"
        binary_dir.mkdir()
        if selection["binaries"]:
            run([
                "docker", "buildx", "build", "--platform", "linux/amd64", "--target", "native",
                "--build-arg", "BINARIES=" + " ".join(selection["binaries"]),
                "--output", f"type=local,dest={binary_dir}", ".",
            ])
        unchanged = []
        for binary in list(binary_dir.iterdir()):
            if info["hashes"].get(binary.name) == digest(binary):
                unchanged.append(binary.name)
                binary.unlink()
        if unchanged:
            print("[deploy] unchanged binaries omitted: " + ", ".join(sorted(unchanged)), flush=True)
        manifest = {
            **selection, "revision": revision, "base_revision": info["revision"], "bootstrap": bootstrap,
            "hashes": {str(p.relative_to(payload)): digest(p) for p in payload.rglob("*") if p.is_file()},
        }
        (payload / "deployment.json").write_text(json.dumps(manifest, indent=2) + "\n")
        remote_payload = PREFIX / (".incoming-" + uuid.uuid4().hex)
        ssh(args.host, ["mkdir", "-p", remote_payload])
        run(["rsync", "-az", "--stats", str(payload) + "/", f"{args.host}:{remote_payload}/"])
        ssh(args.host, ["python3", remote_payload / "scripts/deploy.py", "--apply", remote_payload,
                        "--repository", args.repository])
    print(f"[deploy] complete in {time.monotonic() - started:.1f}s", flush=True)


def atomic_write(path, contents, mode=0o644):
    path = Path(path)
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(contents)
    temporary.chmod(mode)
    temporary.replace(path)


def write_env(path, values):
    def quote(value):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    atomic_write(path, "".join(f"{key}={quote(value)}\n" for key, value in sorted(values.items())), 0o600)


def container(name):
    return json.loads(output(["docker", "inspect", "agent-helper-" + name + "-1"]))[0]


def environment(container_config):
    return dict(item.split("=", 1) for item in container_config["Config"]["Env"])


def credential_environment(repository, filename, container_config):
    keys = set()
    for line in (repository / filename).read_text().splitlines():
        if re.match(r"^[A-Za-z_][A-Za-z_0-9]*=", line.strip()):
            keys.add(line.strip().split("=", 1)[0])
    effective = environment(container_config)
    return {key: effective[key] for key in keys if key in effective}


def bootstrap_configuration(repository):
    # Read the effective container environment, not a second dotenv parser.
    # Never print credentials; the server's files remain authoritative.
    if (CONFIG / "agent.env").exists():
        return
    mcp = container("mcp")
    agent = container("agent")
    postgres = environment(container("postgres"))
    pg_url = "postgresql://{}:{}@127.0.0.1:5432/{}".format(*[
        urllib.parse.quote(postgres[key], safe="") for key in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]
    ])
    common = {
        "MALLOC_ARENA_MAX": "2", "MALLOC_MMAP_THRESHOLD_": "131072", "MALLOC_TRIM_THRESHOLD_": "131072",
    }
    mcp_env = {**credential_environment(repository, ".env.mcp", mcp), **common,
               "DATABASE_URL": pg_url, "MCP_HOST": "127.0.0.1", "MCP_TRANSPORT": "http",
               "MCP_PORT": "3000", "MCP_ALLOWED_HOSTS": "localhost,127.0.0.1,localhost:3000,127.0.0.1:3000",
               "MCP_MEMORY_ACTOR": "supervisor", "MCP_NO_POLLERS": "0", "MCP_TOOLSETS": "",
               "STORAGE_DIR": str(STATE / "storage")}
    write_env(CONFIG / "mcp.env", mcp_env)
    write_env(CONFIG / "mcp-tunnel.env", {
        **mcp_env, "MCP_PORT": "3001", "MCP_ALLOWED_HOSTS": "*", "MCP_MEMORY_ACTOR": "chatgpt",
        "MCP_NO_POLLERS": "1", "MCP_TOOLSETS": "news-read,telegram-send,skills,memory",
    })
    write_env(CONFIG / "agent.env", {
        **credential_environment(repository, ".env.agent", agent), **common,
        "MCP_TRANSPORT": "http", "MCP_URL": "http://127.0.0.1:3000/mcp",
        "AGENT_ROOT": str(PREFIX / "app"), "AGENT_DB_PATH": str(STATE / "agent/agent.db"),
    })


def import_state():
    if (STATE / ".imported").exists():
        return
    legacy = ["agent-helper-" + name + "-1" for name in ["agent", "mcp", "mcp-tunnel"]]
    # Graceful stop closes sqlite/WAL and prevents a second Telegram poller.
    run(["docker", "stop", "--time", "180", legacy[0]])
    run(["docker", "stop", "--time", "90", *legacy[1:]])
    for volume, destination in [("agent-data", "agent"), ("agent-skills", "skills"), ("mcp-storage", "storage")]:
        mount = output(["docker", "volume", "inspect", "agent-helper_" + volume, "--format", "{{.Mountpoint}}"])
        shutil.copytree(mount, STATE / destination, dirs_exist_ok=True)
    with sqlite3.connect("file:" + str(STATE / "agent/agent.db") + "?mode=ro", uri=True) as database:
        if database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("imported agent.db failed integrity check")
        count = database.execute("SELECT COUNT(*) FROM traces").fetchone()[0]
        print(f"[deploy] agent.db integrity=ok, traces={count}", flush=True)
    owner = pwd.getpwnam("ouro")
    for path in [STATE, *STATE.rglob("*")]:
        os.chown(path, owner.pw_uid, owner.pw_gid, follow_symlinks=False)
    atomic_write(STATE / ".imported", "native state imported\n", 0o600)
    # Remove the stopped containers, so Docker cannot resurrect old pollers.
    run(["docker", "rm", *legacy])


def install_files(payload, repository, affected):
    (PREFIX / "bin").mkdir(exist_ok=True)
    app = PREFIX / "app"
    app.mkdir(exist_ok=True)
    for binary in (payload / "bin").iterdir():
        temporary = PREFIX / "bin" / (binary.name + ".new")
        shutil.copyfile(binary, temporary)
        temporary.chmod(0o755)
        temporary.replace(PREFIX / "bin" / binary.name)
    # This directory is the shipped fallback only; live skills are elsewhere.
    if affected & NATIVE:
        defaults = app / "skills.default"
        def tree_hashes(directory):
            return {str(p.relative_to(directory)): digest(p) for p in directory.rglob("*") if p.is_file()}
        shipped = payload / "skills.default"
        if not defaults.exists() or tree_hashes(defaults) != tree_hashes(shipped):
            if defaults.exists():
                shutil.rmtree(defaults)
            shutil.copytree(shipped, defaults)
        gateway_dir = app / "crates/mcp"
        gateway_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(payload / "crates/mcp/gateway.config.json", gateway_dir / "gateway.config.json")
    shutil.copyfile(payload / "deploy/probe.py", app / "probe.py")
    for name in ["skills", "storage"]:
        link = app / name
        if not link.is_symlink():
            link.symlink_to(STATE / name)
    for unit in (payload / "deploy/systemd").glob("*.service"):
        atomic_write(Path("/etc/systemd/system") / unit.name, unit.read_text().replace("__REPOSITORY__", str(repository)))
    run(["systemd-analyze", "verify", *Path("/etc/systemd/system").glob("ouro-*.service")])
    run(["systemctl", "daemon-reload"])


def wait_agent(timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pid = output(["systemctl", "show", "ouro-agent", "--property=MainPID", "--value"])
        if pid != "0":
            journal = output(["journalctl", "-u", "ouro-agent", "_PID=" + pid, "--since", "-5min", "--no-pager", "-o", "cat"])
            if "mcp connected; skill validation passed" in journal:
                print("[health] supervisor connected and skills validated", flush=True)
                return
        time.sleep(1)
    raise RuntimeError("supervisor did not connect and validate skills")


def apply(payload, repository):
    payload, repository = Path(payload).resolve(), Path(repository).resolve()
    if not re.fullmatch(r"/[A-Za-z0-9/_-]+", str(repository)):
        raise RuntimeError("repository path must be an absolute path without shell/systemd special characters")
    PREFIX.mkdir(exist_ok=True)
    with open(PREFIX / "deploy.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = json.loads((payload / "deployment.json").read_text())
        marker = PREFIX / "revision"
        current = marker.read_text().strip() if marker.exists() else None
        if current != manifest["base_revision"]:
            raise RuntimeError("another deploy changed the server; rerun deployment")
        for filename, expected in manifest["hashes"].items():
            path = (payload / filename).resolve()
            if not path.is_relative_to(payload) or digest(path) != expected:
                raise RuntimeError("payload checksum mismatch: " + filename)
        run(["git", "diff", "--quiet"], cwd=repository)
        run(["git", "diff", "--cached", "--quiet"], cwd=repository)
        run(["git", "fetch", "-q", "origin"], cwd=repository)
        if not output(["git", "branch", "-r", "--contains", manifest["revision"]], cwd=repository):
            raise RuntimeError("deployment commit is not on origin")
        try:
            pwd.getpwnam("ouro")
        except KeyError:
            run(["useradd", "--system", "--home-dir", str(STATE), "--shell", "/usr/sbin/nologin", "ouro"])
        CONFIG.mkdir(mode=0o700, exist_ok=True)
        for name in ["agent", "skills", "storage"]:
            path = STATE / name
            path.mkdir(parents=True, exist_ok=True)
            shutil.chown(path, "ouro", "ouro")
        if manifest["bootstrap"]:
            bootstrap_configuration(repository)
        affected = set(manifest["services"])
        if not manifest["bootstrap"]:
            # Stop the supervisor before replacing shared skills or its binary.
            for name in ["agent", "tunnel-client", "mcp-tunnel", "mcp"]:
                if name in affected:
                    run(["systemctl", "stop", f"ouro-{name}.service"])
        else:
            import_state()
        install_files(payload, repository, affected)
        run(["git", "checkout", "-q", "--detach", manifest["revision"]], cwd=repository)
        # Check real credentials and paths before starting the new supervisor.
        run(["systemd-run", "--quiet", "--wait", "--pipe", "--collect",
             "--property=User=ouro", "--property=WorkingDirectory=" + str(PREFIX / "app"),
             "--property=EnvironmentFile=" + str(CONFIG / "agent.env"),
             str(PREFIX / "bin/agent-supervisor"), "--check-config"])
        if manifest["infrastructure"]:
            run(["systemctl", "restart", "ouro-containers.service"])
        else:
            run(["systemctl", "start", "ouro-containers.service"])
        if "mcp-tunnel" in affected:
            affected.add("tunnel-client")
        units = [f"ouro-{name}.service" for name in ["mcp", "mcp-tunnel", "agent", "tunnel-client"] if name in affected]
        run(["systemctl", "enable", "ouro-containers.service", *units])
        run(["systemctl", "start", *units])
        for name in ["mcp", "mcp-tunnel"]:
            run(["python3", PREFIX / "app/probe.py", name])
        run(["systemctl", "is-active", "ouro-agent", "ouro-mcp", "ouro-mcp-tunnel", "ouro-tunnel-client"])
        if "agent" in affected:
            wait_agent()
        atomic_write(marker, manifest["revision"] + "\n")
        shutil.rmtree(payload)
        print("[deploy] healthy; revision recorded; staging removed; no previous release retained", flush=True)


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("services", nargs="*", metavar="SERVICE")
    parser.add_argument("--host", default=os.environ.get("DEPLOY_HOST", "ouro"))
    parser.add_argument("--repository", default=os.environ.get("DEPLOY_DIR", "/root/agent-helper"))
    parser.add_argument("--apply", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    unknown = set(args.services) - (NATIVE | {"tunnel-client"})
    if unknown:
        parser.error("unknown services: " + ", ".join(sorted(unknown)))
    return args


def main():
    args = arguments()
    if args.apply:
        apply(args.apply, args.repository)
    else:
        local(args)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError, BlockingIOError) as error:
        print(f"[deploy] failed: {error}", file=sys.stderr)
        sys.exit(1)
