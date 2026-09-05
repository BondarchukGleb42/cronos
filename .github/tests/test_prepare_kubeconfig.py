import base64
import copy
import hashlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "prepare_kubeconfig.py"
SPEC = importlib.util.spec_from_file_location("prepare_kubeconfig", SCRIPT)
loader = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(loader)

TEST_CA = b"sanitized-public-test-ca"
TEST_CA_HASH = hashlib.sha256(TEST_CA).hexdigest()
TOKEN = "fixture-credential-never-log"


def fixture():
    return {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": "exported-cluster",
                "cluster": {
                    "server": loader.EXPECTED_SERVER,
                    "certificate-authority-data": base64.b64encode(TEST_CA).decode(),
                },
            },
            {"name": "other-cluster", "cluster": {"server": "https://192.0.2.123"}},
        ],
        "contexts": [
            {
                "name": "user-exported-context",
                "context": {"cluster": "exported-cluster", "user": "exported-user"},
            },
            {
                "name": "unrelated-current",
                "context": {"cluster": "other-cluster", "user": "other-user"},
            },
        ],
        "users": [
            {"name": "exported-user", "user": {"token": TOKEN}},
            {"name": "other-user", "user": {"token": "unrelated-credential"}},
        ],
        "current-context": "unrelated-current",
    }


def normalize(config):
    return loader.normalize_config(
        config, expected_server=loader.EXPECTED_SERVER, expected_ca_sha256=TEST_CA_HASH
    )


class KubeconfigTests(unittest.TestCase):
    def test_raw_yaml_json_and_base64(self):
        for raw in ("apiVersion: v1\nkind: Config\n", json.dumps(fixture())):
            self.assertEqual(loader.decode_input(raw), raw.strip().encode())
            self.assertEqual(
                loader.decode_input(base64.b64encode(raw.encode()).decode()), raw.encode()
            )
        for raw in ("", "not valid base64!"):
            with self.assertRaises(ValueError):
                loader.decode_input(raw)

    def test_renames_verified_context_and_ignores_unrelated_current(self):
        result, needs_token, source = normalize(fixture())
        self.assertEqual(source, "user-exported-context")
        self.assertEqual(result["current-context"], "yc-gradius")
        self.assertEqual(result["contexts"][0]["context"]["namespace"], "cronos-bot")
        self.assertEqual(result["users"][0]["user"]["token"], TOKEN)
        self.assertEqual(len(result["contexts"]), 1)
        self.assertFalse(needs_token)

    def test_wrong_endpoint_ca_missing_ca_and_insecure_tls_are_rejected(self):
        changes = [
            {"server": "https://192.0.2.12"},
            {"certificate-authority-data": base64.b64encode(b"wrong-ca").decode()},
            {"certificate-authority-data": None},
            {"insecure-skip-tls-verify": True},
        ]
        for change in changes:
            config = fixture()
            config["clusters"][0]["cluster"].update(change)
            with (
                self.subTest(change=change),
                self.assertRaisesRegex(ValueError, "no context matching"),
            ):
                normalize(config)

    def test_matching_contexts_are_ambiguous_even_when_one_is_current(self):
        config = fixture()
        extra = copy.deepcopy(config["contexts"][0])
        extra["name"] = "second-correct-context"
        config["contexts"].append(extra)
        config["current-context"] = extra["name"]
        with self.assertRaisesRegex(ValueError, "multiple contexts"):
            normalize(config)

    def test_duplicate_clusters_and_missing_user_are_rejected(self):
        config = fixture()
        config["clusters"].append(config["clusters"][0])
        with self.assertRaisesRegex(ValueError, "duplicate cluster"):
            normalize(config)
        config = fixture()
        config["users"] = []
        with self.assertRaisesRegex(ValueError, "missing or ambiguous user"):
            normalize(config)

    def test_yc_exec_replaced_without_execution_and_other_plugins_rejected(self):
        config = fixture()
        config["users"][0]["user"] = {
            "exec": {"command": "/local/bin/yc", "args": ["managed-kubernetes", "create-token"]}
        }
        result, needs_token, _ = normalize(config)
        self.assertEqual(result["users"][0]["user"], {})
        self.assertTrue(needs_token)
        config["users"][0]["user"]["exec"]["command"] = "untrusted-plugin"
        with self.assertRaisesRegex(ValueError, "unsupported credential plugin"):
            normalize(config)

    def test_metadata_excludes_tokens_and_url_credentials(self):
        config = fixture()
        config["clusters"][1]["cluster"]["server"] = (
            "https://username:password@192.0.2.123:443/path?token=hidden"
        )
        metadata = json.dumps(loader.context_metadata(config))
        for secret in (TOKEN, "username", "password", "hidden", "unrelated-credential"):
            self.assertNotIn(secret, metadata)
        self.assertIn("https://192.0.2.123:443", metadata)

    def test_entrypoint_writes_normalized_private_file_and_only_safe_diagnostics(self):
        config = fixture()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory, "output")
            env_file = Path(directory, "env")
            environment = {
                "RUNNER_TEMP": directory,
                "CRONOS_KUBE_CONFIG": json.dumps(config),
                "GITHUB_OUTPUT": str(output),
                "GITHUB_ENV": str(env_file),
            }
            capture = io.StringIO()
            with (
                patch.dict(os.environ, environment),
                patch.object(loader, "EXPECTED_CA_SHA256", TEST_CA_HASH),
                patch.object(
                    loader.subprocess,
                    "run",
                    return_value=Mock(returncode=0, stdout=json.dumps(config)),
                ) as run,
                redirect_stdout(capture),
            ):
                loader.main()
            command = run.call_args.args[0]
            self.assertNotIn("--minify", command)
            self.assertNotIn("--context", command)
            path = Path(directory, "cronos-kubeconfig")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(path.read_text())["current-context"], "yc-gradius")
            self.assertIn("needs_yc_token=false", output.read_text())
            self.assertIn(str(path), env_file.read_text())
            self.assertNotIn(TOKEN, capture.getvalue())
            self.assertIn("user-exported-context", capture.getvalue())

    def test_parser_failure_does_not_print_credential_bearing_stderr(self):
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.dict(
                    os.environ,
                    {"RUNNER_TEMP": directory, "CRONOS_KUBE_CONFIG": json.dumps(fixture())},
                ),
                patch.object(
                    loader.subprocess, "run", return_value=Mock(returncode=1, stderr=TOKEN)
                ),
            ):
                with self.assertRaises(SystemExit) as caught:
                    loader.main()
                self.assertNotIn(TOKEN, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
