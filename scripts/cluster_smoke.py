"""Create or inspect one bounded cluster smoke Job; persist only public summaries."""

import argparse
import hashlib
import json
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

CONTEXT = "yc-gradius"
NAMESPACE = "cronos-bot"
IMAGE = "cr.yandex/crpe8jmbmklfe4l31c68/cronos"
ROOT = Path(__file__).resolve().parents[1]
LABELS = {
    "app.kubernetes.io/name": "cronos-smoke",
    "app.kubernetes.io/part-of": "cronos",
    "app.kubernetes.io/component": "application",
}


def commit_sha(value):
    if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
        raise argparse.ArgumentTypeError("SHA must contain exactly 40 hexadecimal characters")
    return value.lower()


def manifest(sha, name, source):
    env = [
        {"name": key, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}
        for key, secret in (
            ("DATABASE_URL", "cronos-infra"),
            ("ALLTOKENS_API_KEY", "cronos-app"),
            ("ALLTOKENS_BASE_URL", "cronos-app"),
        )
    ]
    env.append({"name": "ARTIFACTS_DIR", "value": "/tmp/artifacts"})
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": LABELS,
            "annotations": {"cronos/source-sha256": hashlib.sha256(source.encode()).hexdigest()},
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 600,
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": LABELS},
                "spec": {
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 30,
                    "serviceAccountName": "cronos-runtime",
                    "automountServiceAccountToken": False,
                    "imagePullSecrets": [{"name": "image-pull-secret"}],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                        "fsGroupChangePolicy": "OnRootMismatch",
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "smoke",
                            "image": f"{IMAGE}:{sha}",
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "-c", source],
                            "env": env,
                            "resources": {
                                "requests": {
                                    "cpu": "100m",
                                    "memory": "256Mi",
                                    "ephemeral-storage": "64Mi",
                                },
                                "limits": {
                                    "cpu": "500m",
                                    "memory": "768Mi",
                                    "ephemeral-storage": "256Mi",
                                },
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                        }
                    ],
                    "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}}],
                },
            },
        },
    }


class Kubectl:
    def __init__(self):
        self.deadline = time.monotonic() + 60

    def call(self, *args, data=None):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Inspection time limit reached")
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "--context",
                    CONTEXT,
                    "--namespace",
                    NAMESPACE,
                    "--request-timeout=8s",
                    *args,
                ],
                input=json.dumps(data) if data is not None else None,
                capture_output=True,
                text=True,
                timeout=min(10, remaining),
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise TimeoutError("Kubernetes request timed out") from None
        if result.returncode:
            # Neither API stderr nor pod logs are printed: they may include connection details.
            raise RuntimeError(f"kubectl {args[0]} failed with exit code {result.returncode}")
        return result.stdout


def job_status(job):
    conditions = job.get("status", {}).get("conditions", [])
    for condition in conditions:
        if condition.get("status") == "True" and condition.get("type") in {"Complete", "Failed"}:
            return "completed" if condition["type"] == "Complete" else "failed"
    return "running" if job.get("status", {}).get("active") else "pending"


def public_logs(logs):
    scenarios = {}
    for line in logs.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict) or row.get("scenario") not in {"memory", "search", "file"}:
            continue
        tools = row.get("tools", [])
        kinds = [tool.get("kind") for tool in tools if isinstance(tool, dict)]
        scenario = row["scenario"]
        required = {"memory": "memory_write", "search": "web_search", "file": "file_create"}[
            scenario
        ]
        item = {"scenario": scenario, "required_tool_observed": required in kinds}
        frames = row.get("preview_frames")
        if isinstance(frames, int) and not isinstance(frames, bool):
            item["preview_frames"] = frames
        cost = str(row.get("cost_rub", ""))
        if re.fullmatch(r"\d+(?:\.\d+)?", cost):
            item["cost_rub"] = cost
        scenarios[scenario] = item
    return {
        "pass_marker": "LIVE_AGENT_SMOKE_PASS" in logs.splitlines(),
        "scenarios": list(scenarios.values()),
    }


def save(summary):
    directory = ROOT / ".local" / "qa"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{summary['job']}.json"
    summary["updated_at"] = datetime.now(UTC).isoformat()
    summary["summary_path"] = str(target)
    target.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sha", type=commit_sha, help="Full commit SHA of an already built image")
    parser.add_argument("--job", help="Inspect an existing smoke Job without creating another one")
    parser.add_argument(
        "--wait-seconds", type=int, choices=range(0, 51), default=30, metavar="0..50"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and save a preparation summary without Kubernetes calls",
    )
    args = parser.parse_args(argv)
    prefix = f"cronos-smoke-{args.sha[:12]}-"
    if args.job and not re.fullmatch(re.escape(prefix) + r"[a-f0-9]{8}", args.job):
        parser.error("--job must belong to this SHA and follow the generated smoke Job name")
    name = args.job or prefix + uuid4().hex[:8]
    source = (ROOT / "scripts" / "live_smoke.py").read_text()
    compile(source, "live_smoke.py", "exec")
    definition = manifest(args.sha, name, source)
    summary = {
        "job": name,
        "context": CONTEXT,
        "namespace": NAMESPACE,
        "image": f"{IMAGE}:{args.sha}",
        "sha": args.sha,
        "source_sha256": definition["metadata"]["annotations"]["cronos/source-sha256"],
        "status": "prepared",
        "created": False,
    }
    if args.dry_run:
        save(summary)
        return 0
    kubectl = Kubectl()
    try:
        if not args.job:
            existing = json.loads(
                kubectl.call(
                    "get", "jobs", "-l", "app.kubernetes.io/name=cronos-smoke", "-o", "json"
                )
            )
            active = [
                job["metadata"]["name"]
                for job in existing.get("items", [])
                if job_status(job) not in {"completed", "failed"}
            ]
            if active:
                summary.update(
                    status="not_started",
                    reason="Another smoke Job is active; inspect it before starting another test",
                    active_jobs=active,
                )
                save(summary)
                return 2
            kubectl.call("create", "-f", "-", data=definition)
            summary["created"] = True
        until = min(kubectl.deadline - 10, time.monotonic() + args.wait_seconds)
        while True:
            job = json.loads(kubectl.call("get", "job", name, "-o", "json"))
            pod = job["spec"]["template"]["spec"]
            if (
                pod["containers"][0]["image"] != summary["image"]
                or job["metadata"].get("labels", {}).get("app.kubernetes.io/name") != "cronos-smoke"
            ):
                raise RuntimeError("Job identity does not match requested smoke image")
            summary["source_sha256"] = (
                job["metadata"].get("annotations", {}).get("cronos/source-sha256")
            )
            summary["status"] = job_status(job)
            if summary["status"] in {"completed", "failed"} or time.monotonic() >= until:
                break
            time.sleep(min(3, max(0, until - time.monotonic())))
        try:
            logs = kubectl.call(
                "logs",
                f"job/{name}",
                "--tail=1000",
                "--limit-bytes=262144",
                "--pod-running-timeout=1s",
            )
            summary.update(public_logs(logs))
        except RuntimeError, TimeoutError:
            summary["logs_available"] = False
        if summary["status"] == "completed":
            summary["status"] = "passed" if summary.get("pass_marker") else "failed"
        summary["inspect_command"] = (
            f"python3 scripts/cluster_smoke.py {args.sha} --job {name} --wait-seconds 30"
        )
    except (RuntimeError, TimeoutError, json.JSONDecodeError, OSError) as error:
        summary.update(status="inspection_error", error_type=type(error).__name__)
        summary["inspect_command"] = (
            f"python3 scripts/cluster_smoke.py {args.sha} --job {name} --wait-seconds 30"
        )
    save(summary)
    return 1 if summary["status"] in {"failed", "inspection_error"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
