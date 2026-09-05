"""Select the verified Cronos cluster without relying on a kubeconfig context name."""

import base64
import hashlib
import json
import os
import ssl
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

# Verified from the local yc-gradius context. Updating these pins requires checking
# the intended cluster, rather than accepting whichever context a secret selects.
EXPECTED_SERVER = "https://158.160.206.91"
EXPECTED_CA_SHA256 = "ed33146e7c913bb47bca091df5b63fd9c686a6937e76034e0d509d5a830ad914"
TARGET_CONTEXT = "yc-gradius"


def decode_input(raw: str) -> bytes:
    raw = raw.strip()
    if not raw:
        raise ValueError("KUBE_CONFIG secret is empty")
    if "apiVersion:" in raw or raw.startswith("{"):
        return raw.encode()
    try:
        return base64.b64decode("".join(raw.split()), validate=True)
    except ValueError:
        raise ValueError("KUBE_CONFIG is neither raw YAML/JSON nor valid base64") from None


def ca_fingerprint(cluster: dict) -> str | None:
    data = cluster.get("certificate-authority-data")
    if not isinstance(data, str) or cluster.get("insecure-skip-tls-verify"):
        return None
    try:
        certificate = base64.b64decode("".join(data.split()), validate=True)
        if b"-----BEGIN CERTIFICATE-----" in certificate:
            certificate = ssl.PEM_cert_to_DER_cert(certificate.decode("ascii"))
        return hashlib.sha256(certificate).hexdigest()
    except ValueError, UnicodeError:
        return None


def context_metadata(config: dict) -> list[dict[str, str]]:
    """Only context names and credential-free endpoint labels are loggable."""
    clusters = {item["name"]: item["cluster"] for item in config.get("clusters", [])}
    result = []
    for item in config.get("contexts", []):
        cluster = clusters.get(item["context"].get("cluster"), {})
        try:
            url = urlsplit(cluster.get("server", ""))
            endpoint = f"{url.scheme}://{url.hostname or '<missing>'}"
            if url.port:
                endpoint += f":{url.port}"
        except ValueError, TypeError:
            endpoint = "<invalid>"
        result.append({"context": item["name"], "endpoint": endpoint})
    return result


def normalize_config(
    config: dict, *, expected_server: str, expected_ca_sha256: str
) -> tuple[dict, bool, str]:
    cluster_items = config.get("clusters", [])
    clusters = {item["name"]: item["cluster"] for item in cluster_items}
    if len(clusters) != len(cluster_items):
        raise ValueError("KUBE_CONFIG contains duplicate cluster names")
    candidates = []
    for item in config.get("contexts", []):
        cluster = clusters.get(item["context"].get("cluster"), {})
        if (
            cluster.get("server", "").rstrip("/") == expected_server.rstrip("/")
            and ca_fingerprint(cluster) == expected_ca_sha256
        ):
            candidates.append((item, cluster))
    if not candidates:
        raise ValueError(
            "KUBE_CONFIG has no context matching the verified Cronos endpoint and CA; "
            "embed certificate-authority-data and verify the target cluster"
        )
    if len(candidates) != 1:
        raise ValueError(
            "KUBE_CONFIG has multiple contexts matching Cronos; provide one unambiguous context"
        )
    context, cluster = candidates[0]
    user_items = [
        item for item in config.get("users", []) if item["name"] == context["context"].get("user")
    ]
    if len(user_items) != 1:
        raise ValueError("The verified Cronos context has a missing or ambiguous user")
    user = user_items[0]["user"]
    plugin = user.get("exec")
    needs_token = False
    if plugin:
        if Path(plugin.get("command", "")).name != "yc" or "create-token" not in plugin.get(
            "args", []
        ):
            raise ValueError(
                "KUBE_CONFIG uses an unsupported credential plugin; provide static credentials or YC exec"
            )
        # Never run a credential plugin supplied by a kubeconfig secret.
        user = {}
        needs_token = True
    normalized = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{"name": TARGET_CONTEXT, "cluster": cluster}],
        "users": [{"name": "cronos-ci", "user": user}],
        "contexts": [
            {
                "name": TARGET_CONTEXT,
                "context": {
                    "cluster": TARGET_CONTEXT,
                    "user": "cronos-ci",
                    "namespace": "cronos-bot",
                },
            }
        ],
        "current-context": TARGET_CONTEXT,
    }
    return normalized, needs_token, context["name"]


def main() -> None:
    path = Path(os.environ["RUNNER_TEMP"], "cronos-kubeconfig")
    try:
        data = decode_input(os.environ.get("CRONOS_KUBE_CONFIG", ""))
        path.touch(mode=0o600)
        path.chmod(0o600)
        path.write_bytes(data)
        # config view parses locally and does not execute credential plugins.
        result = subprocess.run(
            ["kubectl", "--kubeconfig", str(path), "config", "view", "--raw", "-o", "json"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise ValueError(
                "kubectl could not parse KUBE_CONFIG; credential-bearing parser output was suppressed"
            )
        config = json.loads(result.stdout)
        print("Kubeconfig contexts: " + json.dumps(context_metadata(config)))
        normalized, needs_token, source = normalize_config(
            config, expected_server=EXPECTED_SERVER, expected_ca_sha256=EXPECTED_CA_SHA256
        )
        path.write_text(json.dumps(normalized))
        print(
            "Verified Cronos context: "
            + json.dumps(
                {"source": source, "normalized": TARGET_CONTEXT, "endpoint": EXPECTED_SERVER}
            )
        )
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"needs_yc_token={str(needs_token).lower()}\n")
        with open(os.environ["GITHUB_ENV"], "a") as env:
            env.write(f"KUBECONFIG={path}\n")
    except ValueError as exc:
        raise SystemExit(str(exc)) from None
    except KeyError, TypeError, AttributeError:
        raise SystemExit("KUBE_CONFIG has an invalid configuration structure") from None


if __name__ == "__main__":
    main()
