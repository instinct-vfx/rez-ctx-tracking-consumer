"""Capture real rez context-tracking messages as test fixtures.

Run with a Python that has rez installed (it is not a dependency of this project):

    python tools/capture_rez_fixtures.py tests/fixtures/rez-<version>

rez's own ``stdout`` tracking mode prints a Python repr, not the JSON it puts on
the wire, so this script wraps ``rez.utils.amqp._publish_message`` and records
``json.dumps(data)``, which is exactly the body rez publishes. No broker is
needed. Host, user and local paths are replaced with neutral placeholders.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

PLACEHOLDER_HOST = "ws01.example.com"
PLACEHOLDER_USER = "artist"
PLACEHOLDER_REPO = "/studio/packages"
PLACEHOLDER_REZ_PATH = "/opt/rez/lib/python3/site-packages/rez"

PACKAGES = {
    "bar/2.1.0": 'name = "bar"\nversion = "2.1.0"\n',
    "baz/1.2": 'name = "baz"\nversion = "1.2"\n',
    "foo/1.0.0": (
        'name = "foo"\nversion = "1.0.0"\nrequires = ["bar-2"]\nvariants = [["baz-1"]]\n'
    ),
}

ALL_CONTEXT_FIELDS = [
    "resolved_packages", "resolved_ephemerals", "serialize_version", "patch_locks",
    "package_orderers", "package_filter", "graph", "timestamp", "requested_timestamp",
    "building", "testing", "caching", "implicit_packages", "package_requests",
    "package_paths", "append_sys_path", "package_caching", "package_cache_async",
    "default_patch_lock", "rez_version", "rez_path", "user", "host", "platform", "arch",
    "os", "created", "parent_suite_path", "suite_context_name", "status",
    "failure_description", "from_cache", "solve_time", "load_time", "num_loaded_packages",
]  # fmt: skip


def main(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="rez-fixtures-"))
    repo = work / "packages"
    for rel, content in PACKAGES.items():
        (repo / rel).mkdir(parents=True)
        (repo / rel / "package.py").write_text(content)

    os.environ["REZ_CONTEXT_TRACKING_HOST"] = "stdout"
    os.environ["REZ_PACKAGES_PATH"] = str(repo)

    import rez
    import rez.utils.amqp as amqp
    from rez.config import config
    from rez.resolved_context import ResolvedContext

    captured: list[dict[str, str]] = []

    def fake_publish(host: str, amqp_settings: Any, routing_key: str, data: Any) -> bool:
        captured.append({"routing_key": routing_key, "body": json.dumps(data)})
        return True

    amqp._publish_message = fake_publish
    scrub = _scrubber(repo=str(repo), rez_path=os.path.dirname(rez.__file__))

    def flush(name: str) -> None:
        for _ in range(200):  # publish_message(block=False) uses a worker thread
            if amqp._num_pending == 0:
                break
            time.sleep(0.05)
        if len(captured) != 1:
            raise RuntimeError(f"{name}: expected 1 message, got {len(captured)}")
        msg = captured.pop()
        fixture = {
            "rez_version": rez.__version__,
            "routing_key": msg["routing_key"],
            "payload": scrub(json.loads(msg["body"])),
        }
        path = out_dir / f"{name}.json"
        path.write_text(json.dumps(fixture, indent=2) + "\n")
        print(f"wrote {path} ({msg['routing_key']})")
        ResolvedContext.context_tracking_payload = None  # reset cached base payload

    rxt = work / "foo.rxt"
    ResolvedContext(["foo"]).save(str(rxt))
    flush("created_default")

    ResolvedContext.load(str(rxt))
    flush("sourced_default")

    ResolvedContext(["foo", "!bar"])  # conflicting request -> status "failed"
    flush("created_failed")

    # rez drops extra fields that still contain an unexpanded env var.
    config.override("context_tracking_extra_fields", {"show": "demo", "site": "${NO_SUCH_VAR}"})
    ResolvedContext(["bar"])
    flush("created_extra_fields")
    config.remove_override("context_tracking_extra_fields")

    config.override("context_tracking_context_fields", ALL_CONTEXT_FIELDS)
    ResolvedContext(["foo"])
    flush("created_all_fields")


def _scrubber(repo: str, rez_path: str) -> Any:
    repo_re = re.compile(re.escape(os.path.normpath(repo)), re.IGNORECASE)
    rez_re = re.compile(re.escape(os.path.normpath(rez_path)), re.IGNORECASE)
    host_re = re.compile(re.escape(_hostname()), re.IGNORECASE)

    def scrub(value: Any, key: str | None = None) -> Any:
        if isinstance(value, dict):
            return {k: scrub(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [scrub(v) for v in value]
        if isinstance(value, str):
            if key == "user":
                return PLACEHOLDER_USER
            if key == "host":
                return PLACEHOLDER_HOST
            value = repo_re.sub(PLACEHOLDER_REPO, value)
            value = rez_re.sub(PLACEHOLDER_REZ_PATH, value)
            return host_re.sub(PLACEHOLDER_HOST, value)
        return value

    return scrub


def _hostname() -> str:
    import socket

    return socket.gethostname()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} OUT_DIR")
    main(Path(sys.argv[1]))
