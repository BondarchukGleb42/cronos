"""Download the checked CI image archive without forwarding GitHub credentials."""

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import urlparse
from zipfile import ZipFile

import httpx
from github_status import GitHubReader, GitHubReadError, credential_token


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id", type=int)
    args = parser.parse_args()
    reader = GitHubReader(credential_token())
    try:
        run = reader._response(f"actions/runs/{args.run_id}").json()
        sha = run["head_sha"]
        artifacts = reader._response(f"actions/runs/{args.run_id}/artifacts").json()["artifacts"]
        artifact = next(
            (
                item
                for item in artifacts
                if item["name"] == f"cronos-image-{sha}" and not item["expired"]
            ),
            None,
        )
        if not artifact:
            raise GitHubReadError("The image artifact is not available yet")
        root = Path(__file__).resolve().parents[1] / ".local" / "images"
        root.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(root).free < artifact["size_in_bytes"] * 3:
            raise GitHubReadError("Insufficient local disk space for the image archive")
        response = reader._response(f"actions/artifacts/{artifact['id']}/zip")
        location = response.headers.get("location", "")
        if not response.is_redirect or urlparse(location).scheme != "https":
            raise GitHubReadError("Expected an HTTPS artifact download redirect")
        zipped = root / f"{sha}.zip"
        digest = hashlib.sha256()
        downloaded = 0
        try:
            with httpx.Client(verify=reader.tls, timeout=60, follow_redirects=True) as public:
                with public.stream("GET", location) as download:
                    if download.status_code != 200:
                        raise GitHubReadError("Artifact download was rejected")
                    with zipped.open("wb") as output:
                        for chunk in download.iter_bytes(1024 * 1024):
                            output.write(chunk)
                            digest.update(chunk)
                            downloaded += len(chunk)
            expected = artifact.get("digest")
            if expected and expected != "sha256:" + digest.hexdigest():
                raise GitHubReadError("Artifact checksum mismatch")
            target = root / f"{sha}.tar"
            with (
                ZipFile(zipped) as archive,
                archive.open("cronos-image.tar") as source,
                target.open("wb") as output,
            ):
                shutil.copyfileobj(source, output, 1024 * 1024)
            zipped.unlink()
            print(
                json.dumps(
                    {
                        "run_id": args.run_id,
                        "sha": sha,
                        "archive": str(target),
                        "bytes": target.stat().st_size,
                        "verified_digest": expected,
                    },
                    ensure_ascii=False,
                )
            )
        except httpx.HTTPError:
            raise GitHubReadError(
                "Artifact network download failed; credentials were not forwarded"
            ) from None
    finally:
        reader.close()


if __name__ == "__main__":
    try:
        main()
    except (GitHubReadError, OSError, KeyError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        raise SystemExit(1) from None
