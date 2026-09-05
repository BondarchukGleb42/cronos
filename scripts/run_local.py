"""Run a local check with runtime env loaded without printing credentials."""

import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


def environment():
    env = dict(os.environ)
    result = subprocess.run(
        [
            "kubectl",
            "--context",
            "yc-gradius",
            "-n",
            "cronos-bot",
            "get",
            "secret",
            "cronos-infra",
            "-o",
            "json",
        ],
        capture_output=True,
        check=True,
    )
    data = json.loads(result.stdout)["data"]
    for key in ["DATABASE_URL", "ADMIN_DATABASE_URL", "RABBITMQ_URL", "REDIS_URL"]:
        env[key] = base64.b64decode(data[key]).decode()
    app_secret = subprocess.run(
        [
            "kubectl",
            "--context",
            "yc-gradius",
            "-n",
            "cronos-bot",
            "get",
            "secret",
            "cronos-app",
            "-o",
            "json",
        ],
        capture_output=True,
        check=True,
    )
    for key, value in json.loads(app_secret.stdout).get("data", {}).items():
        if key in {
            "TELEGRAM_PROXY",
            "TELEGRAM_BOT_TOKEN",
            "ALLTOKENS_API_KEY",
            "ALLTOKENS_BASE_URL",
        }:
            env[key] = base64.b64decode(value).decode()
    for key in ["DATABASE_URL", "ADMIN_DATABASE_URL"]:
        u = urlsplit(env[key])
        env[key] = urlunsplit(
            (u.scheme, u.netloc.rsplit("@", 1)[0] + "@127.0.0.1:15432", u.path, u.query, u.fragment)
        )
    path = Path(__file__).resolve().parents[1] / "credentials"
    names = {
        "bot_token": "TELEGRAM_BOT_TOKEN",
        "telegram_proxy": "TELEGRAM_PROXY",
        "all_tokens_api_key": "ALLTOKENS_API_KEY",
        "all_tokens_url": "ALLTOKENS_BASE_URL",
    }
    for line in path.read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            continue
        if key.strip() in names:
            env[names[key.strip()]] = value.strip().strip("\"'")
    env["PROVIDER_TLS12"] = "true"
    env["ARTIFACTS_DIR"] = str(Path(__file__).resolve().parents[1] / ".local" / "artifacts")
    return env


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--":
        args = args[1:]
    raise SystemExit(subprocess.call(args, env=environment()))
