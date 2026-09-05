#!/usr/bin/env python3
"""Bootstrap only Cronos infrastructure; secrets travel through stdin, never files/logs.

Run: python3 scripts/bootstrap_infra.py [--wait]
Context/namespace are intentionally fixed. Existing credentials are preserved; this is
not a credential-rotation command. Namespace creation is the sole cluster-scoped write.
"""

from __future__ import annotations

import argparse
import base64
import json
import secrets
import subprocess
import sys
from pathlib import Path

CONTEXT = "yc-gradius"
NAMESPACE = "cronos-bot"
ROOT = Path(__file__).resolve().parents[1]


def kubectl(
    *args: str, payload: dict | None = None, check: bool = True
) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl", "--context", CONTEXT, "--namespace", NAMESPACE, *args],
        input=json.dumps(payload) if payload is not None else None,
        text=True,
        capture_output=True,
        check=check,
    )


def validate_manifest(path: Path) -> None:
    result = kubectl("create", "--dry-run=client", "-f", str(path), "-o", "json")
    docs = []
    remaining = result.stdout.strip()
    decoder = json.JSONDecoder()
    while remaining:
        obj, end = decoder.raw_decode(remaining)
        docs.extend(obj.get("items", [obj]))
        remaining = remaining[end:].strip()
    for doc in docs:
        metadata = doc["metadata"]
        if doc["kind"] == "Namespace":
            if metadata["name"] != NAMESPACE:
                raise ValueError(f"Unexpected namespace in {path}")
        elif metadata.get("namespace") != NAMESPACE:
            raise ValueError(f"Unexpected resource scope in {path}: {doc['kind']}")
        if doc["kind"] not in {
            "Namespace",
            "ResourceQuota",
            "LimitRange",
            "ServiceAccount",
            "NetworkPolicy",
            "ConfigMap",
            "Service",
            "StatefulSet",
            "Deployment",
        }:
            raise ValueError(f"Unexpected infrastructure kind in {path}: {doc['kind']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", action="store_true", help="Wait up to 5 minutes per datastore")
    args = parser.parse_args()
    manifests = sorted((ROOT / "k8s" / "infra").glob("*.yaml"))
    for path in manifests:
        validate_manifest(path)
    print(f"Validated {len(manifests)} files for {CONTEXT}/{NAMESPACE}.", flush=True)
    namespace = manifests[0]
    print(kubectl("apply", "-f", str(namespace)).stdout.strip(), flush=True)
    existing = kubectl("get", "secret", "cronos-infra", "--ignore-not-found", "-o", "json")
    if existing.stdout.strip():
        data = json.loads(existing.stdout)["data"]
        required = {"ADMIN_DATABASE_URL", "DATABASE_URL", "RABBITMQ_URL", "REDIS_URL"}
        if not required <= data.keys():
            raise ValueError("Existing cronos-infra is incomplete; refusing to rotate credentials")
        print("Existing cronos-infra credentials preserved.", flush=True)
    else:
        pg_admin, pg_app, rabbit, redis = [secrets.token_urlsafe(36) for _ in range(4)]
        strings = {
            "POSTGRES_PASSWORD": pg_admin,
            "POSTGRES_APP_PASSWORD": pg_app,
            "RABBITMQ_PASSWORD": rabbit,
            "REDIS_PASSWORD": redis,
            "ADMIN_DATABASE_URL": f"postgresql://cronos_admin:{pg_admin}@postgres:5432/cronos",
            "DATABASE_URL": f"postgresql://cronos_app:{pg_app}@postgres:5432/cronos",
            "RABBITMQ_URL": f"amqp://cronos:{rabbit}@rabbitmq:5672/cronos",
            "REDIS_URL": f"redis://:{redis}@redis:6379/0",
        }
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {"name": "cronos-infra", "namespace": NAMESPACE},
            "data": {
                key: base64.b64encode(value.encode()).decode() for key, value in strings.items()
            },
        }
        # create (not apply) avoids retaining a second credential copy in annotations.
        kubectl("create", "-f", "-", payload=secret)
        print("Created cronos-infra with newly generated credentials (values hidden).", flush=True)
    for path in manifests[1:]:
        print(kubectl("apply", "-f", str(path)).stdout.strip(), flush=True)
    if args.wait:
        for name in ("statefulset/postgres", "statefulset/rabbitmq", "deployment/redis"):
            print(kubectl("rollout", "status", name, "--timeout=300s").stdout.strip(), flush=True)
    print(
        "Bootstrap applied. NetworkPolicy enforcement requires an enforcing cluster CNI.",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        # Never dump payload or arbitrary stderr: API errors may echo rejected objects.
        print(
            f"kubectl failed (exit {exc.returncode}); inspect scoped resource events.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
