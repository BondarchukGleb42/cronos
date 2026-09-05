#!/usr/bin/env python3
"""Read Cronos GitHub Actions state without printing or changing credentials.

Examples:
    python scripts/github_status.py latest
    python scripts/github_status.py status 123456789
    python scripts/github_status.py jobs 123456789
    python scripts/github_status.py logs 123456789 --max-lines 50

The token is read from git's configured credential helper into memory. Only GET
requests are made; no secret-management or workflow-mutation endpoint is used.
"""

import argparse
import io
import json
import os
import re
import ssl
import subprocess
import sys
from collections import deque
from urllib.parse import urlparse
from zipfile import BadZipFile, ZipFile

import certifi
import httpx

REPOSITORY = "BondarchukGleb42/cronos"
API_ROOT = "https://api.github.com"
ERROR_PATTERN = (
    r"##\[error\]|\b(?:error|failed|failure|exception|traceback|fatal|denied|forbidden)\b"
)


class GitHubReadError(Exception):
    """A safe error message which contains no response body or credential values."""


def credential_token() -> str:
    environment = os.environ | {"GIT_TERMINAL_PROMPT": "0"}
    try:
        result = subprocess.run(
            ["git", "-c", "credential.interactive=false", "credential", "fill"],
            input=f"protocol=https\nhost=github.com\npath={REPOSITORY}.git\n\n",
            capture_output=True,
            text=True,
            timeout=15,
            env=environment,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise GitHubReadError("Git credential helper is unavailable") from exc
    if result.returncode:
        raise GitHubReadError("Git credential helper did not provide GitHub credentials")
    fields = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    token = fields.get("password")
    if not token:
        raise GitHubReadError("Git credential helper returned no GitHub token")
    return token


def redact(text: str, token: str) -> str:
    text = text.replace(token, "[REDACTED]") if token else text
    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
    text = re.sub(r"(?:github_pat_|gh[pousr]_)[A-Za-z0-9_]+", "[REDACTED]", text)
    text = re.sub(
        r"(?i)\b(postgres(?:ql)?|amqps?|rediss?|https?)://[^\s/@]+@",
        r"\1://[REDACTED]@",
        text,
    )
    text = re.sub(
        r"(?i)((?:authorization|x-api-key|api[_-]?key|password|private[_-]?key|access[_-]?token)\s*[\"']?\s*[:=]\s*).*$",
        r"\1[REDACTED]",
        text,
    )
    return text


def run_summary(run: dict) -> dict:
    keys = (
        "id",
        "name",
        "event",
        "status",
        "conclusion",
        "head_branch",
        "head_sha",
        "run_number",
        "run_attempt",
        "created_at",
        "updated_at",
        "html_url",
    )
    return {key: run.get(key) for key in keys}


def job_summary(job: dict) -> dict:
    return {
        **{
            key: job.get(key)
            for key in (
                "id",
                "name",
                "status",
                "conclusion",
                "started_at",
                "completed_at",
                "html_url",
            )
        },
        "steps": [
            {key: step.get(key) for key in ("number", "name", "status", "conclusion")}
            for step in job.get("steps", [])
        ],
    }


class GitHubReader:
    def __init__(self, token: str):
        self.token = token
        self.tls = ssl.create_default_context(cafile=certifi.where())
        self.client = httpx.Client(
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "cronos-readonly-actions-status",
            },
            verify=self.tls,
            timeout=30,
            follow_redirects=False,
        )

    def close(self):
        self.client.close()

    def _response(self, path: str, **params) -> httpx.Response:
        try:
            response = self.client.get(f"{API_ROOT}/repos/{REPOSITORY}/{path}", params=params)
        except httpx.HTTPError as exc:
            raise GitHubReadError("GitHub API request failed before receiving a response") from exc
        if response.status_code >= 400:
            details = ""
            if response.status_code == 403:
                details = "; token access or rate limit does not allow this read"
            elif response.status_code == 404:
                details = "; resource may not exist or may be unavailable to this token"
            raise GitHubReadError(f"GitHub API returned HTTP {response.status_code}{details}")
        return response

    def latest(self, branch: str = "main") -> dict | None:
        runs = self._response("actions/runs", branch=branch, per_page=1).json()["workflow_runs"]
        return runs[0] if runs else None

    def run(self, run_id: int) -> dict:
        return self._response(f"actions/runs/{run_id}").json()

    def jobs(self, run_id: int) -> list[dict]:
        jobs, page = [], 1
        while True:
            response = self._response(
                f"actions/runs/{run_id}/jobs",
                per_page=100,
                page=page,
                filter="latest",
            ).json()
            batch = response["jobs"]
            jobs.extend(job_summary(job) for job in batch)
            if not batch or len(jobs) >= response["total_count"]:
                return jobs
            page += 1

    def logs(self, run_id: int, pattern: str, max_lines: int) -> dict:
        match = re.compile(pattern, re.IGNORECASE)
        response = self._response(f"actions/runs/{run_id}/logs")
        if response.is_redirect:
            location = response.headers.get("location", "")
            if urlparse(location).scheme != "https":
                raise GitHubReadError("GitHub logs redirect did not use HTTPS")
            # GitHub credentials must never reach the external signed blob URL.
            try:
                with httpx.Client(verify=self.tls, timeout=60, follow_redirects=True) as download:
                    response = download.get(location)
            except httpx.HTTPError as exc:
                raise GitHubReadError("GitHub logs download failed") from exc
            if response.status_code != 200:
                raise GitHubReadError(f"GitHub logs download returned HTTP {response.status_code}")
        lines, count = deque(maxlen=max_lines), 0
        try:
            with ZipFile(io.BytesIO(response.content)) as archive:
                for item in sorted(archive.infolist(), key=lambda item: item.filename):
                    if item.is_dir():
                        continue
                    with archive.open(item) as stream:
                        for number, raw in enumerate(stream, start=1):
                            line = raw.decode("utf-8", errors="replace").rstrip()
                            if match.search(line):
                                count += 1
                                lines.append(
                                    {
                                        "file": item.filename,
                                        "line": number,
                                        "text": redact(line, self.token),
                                    }
                                )
        except BadZipFile as exc:
            raise GitHubReadError("GitHub did not return a valid log archive") from exc
        return {
            "run_id": run_id,
            "pattern": pattern,
            "matched_lines": count,
            "truncated": count > max_lines,
            "lines": list(lines),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    latest = commands.add_parser("latest", help="Latest run metadata")
    latest.add_argument("--branch", default="main")
    for name in ("status", "jobs", "runjobs"):
        command = commands.add_parser(name)
        command.add_argument("run_id", type=int, nargs="?")
        command.add_argument("--branch", default="main")
    logs = commands.add_parser("logs", help="Read matching error lines from completed run logs")
    logs.add_argument("run_id", type=int)
    logs.add_argument("--pattern", default=ERROR_PATTERN)
    logs.add_argument("--max-lines", type=int, default=80)
    args = parser.parse_args()
    if args.command == "logs" and args.max_lines < 1:
        parser.error("--max-lines must be positive")
    if getattr(args, "run_id", None) is not None and args.run_id < 1:
        parser.error("run_id must be positive")

    reader = None
    try:
        reader = GitHubReader(credential_token())
        if args.command == "logs":
            result = reader.logs(args.run_id, args.pattern, args.max_lines)
        else:
            run_id = getattr(args, "run_id", None)
            run = reader.run(run_id) if run_id is not None else reader.latest(args.branch)
            if run is None:
                result = {"repository": REPOSITORY, "branch": args.branch, "run": None}
            elif args.command == "latest":
                result = run_summary(run)
            elif args.command in {"jobs", "runjobs"}:
                result = {"run_id": run["id"], "jobs": reader.jobs(run["id"])}
            else:
                result = {"run": run_summary(run), "jobs": reader.jobs(run["id"])}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (GitHubReadError, re.PatternError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    except KeyError, TypeError, ValueError:
        print('{"error": "GitHub returned an unexpected response format"}', file=sys.stderr)
        return 1
    finally:
        if reader is not None:
            reader.close()


if __name__ == "__main__":
    raise SystemExit(main())
