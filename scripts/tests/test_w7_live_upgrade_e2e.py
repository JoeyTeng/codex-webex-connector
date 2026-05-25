from __future__ import annotations

import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import BytesIO, StringIO
from pathlib import Path

from scripts import w7_live_upgrade_e2e as harness


class W7LiveUpgradeE2ETest(unittest.TestCase):
    def test_parse_token_file_supports_env_shape_without_leaking_bearer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "token.txt"
            path.write_text(
                "\n".join(
                    [
                        "email=User@Example.com",
                        "bearer=Bearer secret-token",
                        "ord_id=ord-123",
                    ]
                ),
                encoding="utf-8",
            )

            token = harness.parse_token_file(path)

            self.assertEqual(token.email, "user@example.com")
            self.assertEqual(token.bearer, "secret-token")
            self.assertEqual(
                token.redacted_summary,
                {"email": "user@example.com", "bearer_len": 12, "ord_id_len": 7},
            )
            self.assertNotIn("secret-token", json.dumps(token.redacted_summary))

    def test_parse_token_file_supports_json_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "token.json"
            path.write_text(
                json.dumps(
                    {
                        "WEBEX_DEVELOPER_EMAIL": "dev@example.com",
                        "WEBEX_DEVELOPER_BEARER": "json-secret",
                        "WEBEX_DEVELOPER_ORD_ID": "ord-json",
                    }
                ),
                encoding="utf-8",
            )

            token = harness.parse_token_file(path)

            self.assertEqual(token.email, "dev@example.com")
            self.assertEqual(token.bearer, "json-secret")
            self.assertEqual(token.ord_id, "ord-json")

    def test_parse_token_file_rejects_json_null_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "token.json"
            path.write_text(
                json.dumps(
                    {
                        "email": "dev@example.com",
                        "bearer": None,
                        "ord_id": "ord-json",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(harness.HarnessError, "bearer.*string"):
                harness.parse_token_file(path)

    def test_parse_bot_env_file_rejects_json_null_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bot.json"
            path.write_text(
                json.dumps(
                    {
                        "WEBEX_BOT_TOKEN": None,
                        "WEBEX_BOT_EMAIL": "bot@example.com",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(harness.HarnessError, "WEBEX_BOT_TOKEN.*string"):
                harness.parse_bot_env_file(path)

    def test_record_writes_developer_summary_as_json_without_secret(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.RunState(
                args=harness.build_parser().parse_args([]),
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-TEST",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            developer = harness.DeveloperToken(
                email="dev@example.com",
                bearer=harness.SecretText("secret-token"),
                ord_id="ord-123",
            )

            state.record("developer_token", developer.redacted_summary)

            manifest_text = state.manifest_path.read_text(encoding="utf-8")
            self.assertEqual(
                json.loads(manifest_text)["developer_token"],
                {"email": "dev@example.com", "bearer_len": 12, "ord_id_len": 7},
            )
            self.assertNotIn("secret-token", manifest_text)

    def test_record_does_not_pollute_manifest_after_json_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.RunState(
                args=harness.build_parser().parse_args([]),
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-TEST",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            state.record("ok", {"value": True})

            with self.assertRaises(TypeError):
                state.record("bad", object())

            self.assertNotIn("bad", state.manifest)
            self.assertNotIn("bad", json.loads(state.manifest_path.read_text(encoding="utf-8")))

    def test_fnv1a_matches_offset_basis_for_empty_string(self) -> None:
        self.assertEqual(harness.fnv1a_hex(""), "cbf29ce484222325")

    def test_socket_paths_are_release_scoped(self) -> None:
        state_dir = Path("/tmp/wxcd-state")

        first = harness.worker_ingress_socket_path("instance", "release-a", state_dir)
        second = harness.worker_ingress_socket_path("instance", "release-b", state_dir)

        self.assertNotEqual(first, second)
        self.assertEqual(first.parent, Path("/tmp"))
        self.assertTrue(first.name.startswith("wxcd-ingress-"))

    def test_person_email_validation_rejects_mismatched_token_owner(self) -> None:
        with self.assertRaises(harness.HarnessError):
            harness.ensure_person_email_matches(
                {"emails": ["actual@example.com"]},
                "configured@example.com",
                "bot",
            )

    def test_create_membership_retries_transient_invalid_room(self) -> None:
        class FakeResponse:
            status = 200

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"id":"membership"}'

        calls = 0
        sleeps: list[float] = []
        original_urlopen = harness.urllib.request.urlopen
        original_sleep = harness.time.sleep

        def fake_urlopen(request: object, timeout: int) -> FakeResponse:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise harness.urllib.error.HTTPError(
                    url="https://webexapis.com/v1/memberships",
                    code=400,
                    msg="Bad Request",
                    hdrs={},
                    fp=BytesIO(b'{"message":"Invalid roomId"}'),
                )
            return FakeResponse()

        harness.urllib.request.urlopen = fake_urlopen
        harness.time.sleep = sleeps.append
        try:
            response = harness.WebexApi("token").create_membership("room", "bot@example.com")
        finally:
            harness.urllib.request.urlopen = original_urlopen
            harness.time.sleep = original_sleep

        self.assertEqual(response["id"], "membership")
        self.assertEqual(calls, 2)
        self.assertEqual(sleeps, [harness.WEBEX_TRANSIENT_ROOM_RETRY_BASE_SECONDS])

    def test_create_message_retries_transient_invalid_room(self) -> None:
        class FakeResponse:
            status = 200

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"id":"message"}'

        calls = 0
        original_urlopen = harness.urllib.request.urlopen
        original_sleep = harness.time.sleep

        def fake_urlopen(request: object, timeout: int) -> FakeResponse:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise harness.urllib.error.HTTPError(
                    url="https://webexapis.com/v1/messages",
                    code=400,
                    msg="Bad Request",
                    hdrs={},
                    fp=BytesIO(b'{"message":"Invalid roomId"}'),
                )
            return FakeResponse()

        harness.urllib.request.urlopen = fake_urlopen
        harness.time.sleep = lambda seconds: None
        try:
            response = harness.WebexApi("token").create_message("room", "hello")
        finally:
            harness.urllib.request.urlopen = original_urlopen
            harness.time.sleep = original_sleep

        self.assertEqual(response["id"], "message")
        self.assertEqual(calls, 2)

    def test_webex_path_segment_escapes_complete_resource_id(self) -> None:
        self.assertEqual(
            harness.webex_path_segment("room/with+plus="),
            "room%2Fwith%2Bplus%3D",
        )

    def test_webex_resource_id_paths_encode_slashes(self) -> None:
        class CapturingApi(harness.WebexApi):
            def __init__(self) -> None:
                self.calls: list[tuple[str, str, dict[str, object]]] = []

            def request(
                self,
                method: str,
                path: str,
                body: dict[str, object] | None = None,
                **kwargs: object,
            ) -> dict[str, object]:
                self.calls.append((method, path, kwargs))
                return {}

        api = CapturingApi()

        api.get_room("room/with+plus=")
        api.delete_room("room/with+plus=")
        api.delete_membership("membership/with+plus=")

        self.assertEqual(
            api.calls,
            [
                ("GET", "/rooms/room%2Fwith%2Bplus%3D", {}),
                ("DELETE", "/rooms/room%2Fwith%2Bplus%3D", {"allow_empty": True}),
                (
                    "DELETE",
                    "/memberships/membership%2Fwith%2Bplus%3D",
                    {"allow_empty": True},
                ),
            ],
        )

    def test_history_page_needles_do_not_match_adjacent_page_navigation(self) -> None:
        page_two_needles = harness.history_page_needles(2, "thread-123")
        page_one_navigation = "Thread `thread-123` history page 1 of 2.\nUse `/history page 2` for older turns."

        self.assertFalse(all(needle in page_one_navigation for needle in page_two_needles))
        self.assertTrue(
            all(needle in "Thread `thread-123` history page 2 of 2." for needle in page_two_needles)
        )

    def test_history_page_alternatives_accept_missing_page_response(self) -> None:
        alternatives = harness.history_page_response_alternatives(2, "thread-123", allow_missing=True)
        missing_page = "No history on page 2 for thread `thread-123`. Last available page is 1."

        self.assertTrue(
            any(all(needle in missing_page for needle in needles) for needles in alternatives)
        )

    def test_history_page_alternatives_require_existing_seeded_page(self) -> None:
        alternatives = harness.history_page_response_alternatives(2, "thread-123", allow_missing=False)
        missing_page = "No history on page 2 for thread `thread-123`. Last available page is 1."

        self.assertFalse(
            any(all(needle in missing_page for needle in needles) for needles in alternatives)
        )

    def test_live_requires_explicit_opt_in(self) -> None:
        old_value = os.environ.pop("WXCD_LIVE_E2E", None)
        try:
            with self.assertRaises(harness.BlockedError):
                harness.ensure_opt_in(True)
        finally:
            if old_value is not None:
                os.environ["WXCD_LIVE_E2E"] = old_value

    def test_live_main_blocks_before_reading_missing_token_file(self) -> None:
        old_value = os.environ.pop("WXCD_LIVE_E2E", None)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                with redirect_stdout(StringIO()):
                    code = harness.main(
                        [
                            "--live",
                            "--test-root",
                            str(Path(tmp) / "run"),
                            "--token-file",
                            str(Path(tmp) / "missing-token.txt"),
                            "--cbth-bin",
                            str(Path(tmp) / "missing-cbth"),
                        ]
                    )
            self.assertEqual(code, 78)
        finally:
            if old_value is not None:
                os.environ["WXCD_LIVE_E2E"] = old_value

    def test_live_main_preflights_missing_cbth_service_upgrade_smoke_before_token_file(self) -> None:
        old_live = os.environ.get("WXCD_LIVE_E2E")
        old_upgrade = os.environ.pop("WXCD_E2E_CBTH_UPGRADE_CMD", None)
        os.environ["WXCD_LIVE_E2E"] = "1"
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = StringIO()
                with redirect_stdout(output):
                    code = harness.main(
                        [
                            "--live",
                            "--test-root",
                            str(Path(tmp) / "run"),
                            "--token-file",
                            str(Path(tmp) / "missing-token.txt"),
                            "--cbth-bin",
                            str(Path(tmp) / "missing-cbth"),
                        ]
                    )
            self.assertEqual(code, 78)
            self.assertIn("cbth C8 service upgrade-smoke executable", output.getvalue())
        finally:
            if old_live is None:
                os.environ.pop("WXCD_LIVE_E2E", None)
            else:
                os.environ["WXCD_LIVE_E2E"] = old_live
            if old_upgrade is not None:
                os.environ["WXCD_E2E_CBTH_UPGRADE_CMD"] = old_upgrade

    def test_validate_test_root_blocks_repo_internal_secret_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)

            with self.assertRaisesRegex(harness.BlockedError, r"\.codex-tmp"):
                harness.validate_test_root(repo_root / "w7-live-run", repo_root, explicit=True)

    def test_validate_test_root_allows_repo_codex_tmp_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)

            harness.validate_test_root(repo_root / ".codex-tmp" / "w7-live-run", repo_root, explicit=True)

    def test_isolated_child_env_removes_webex_wxcd_and_cbth_values(self) -> None:
        old_values = {
            "WEBEX_BOT_TOKEN": os.environ.get("WEBEX_BOT_TOKEN"),
            "WXCD_CONFIG_PATH": os.environ.get("WXCD_CONFIG_PATH"),
            "CBTH_HOME": os.environ.get("CBTH_HOME"),
        }
        os.environ["WEBEX_BOT_TOKEN"] = "prod-token"
        os.environ["WXCD_CONFIG_PATH"] = "/prod/wxcd.toml"
        os.environ["CBTH_HOME"] = "/prod/cbth"
        try:
            env = harness.isolated_child_env({"WXCD_CONFIG_PATH": "/tmp/test/wxcd.toml"})
        finally:
            for key, value in old_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertNotIn("WEBEX_BOT_TOKEN", env)
        self.assertNotIn("CBTH_HOME", env)
        self.assertEqual(env["WXCD_CONFIG_PATH"], "/tmp/test/wxcd.toml")

    def test_ensure_untracked_scrubs_git_subprocess_env(self) -> None:
        old_values = {
            "WEBEX_BOT_TOKEN": os.environ.get("WEBEX_BOT_TOKEN"),
            "WXCD_CONFIG_PATH": os.environ.get("WXCD_CONFIG_PATH"),
            "CBTH_HOME": os.environ.get("CBTH_HOME"),
        }
        os.environ["WEBEX_BOT_TOKEN"] = "prod-token"
        os.environ["WXCD_CONFIG_PATH"] = "/prod/wxcd.toml"
        os.environ["CBTH_HOME"] = "/prod/cbth"
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            path = repo_root / "token.txt"
            path.write_text("secret", encoding="utf-8")
            calls: list[dict[str, object]] = []
            original_run = harness.subprocess.run

            def fake_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
                calls.append({"command": command, **kwargs})
                raise harness.subprocess.CalledProcessError(1, command)

            harness.subprocess.run = fake_run
            try:
                harness.ensure_untracked(path, repo_root)
            finally:
                harness.subprocess.run = original_run
                for key, value in old_values.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        self.assertEqual(len(calls), 1)
        env = calls[0]["env"]
        self.assertIsInstance(env, dict)
        self.assertNotIn("WEBEX_BOT_TOKEN", env)
        self.assertNotIn("WXCD_CONFIG_PATH", env)
        self.assertNotIn("CBTH_HOME", env)

    def test_upgrade_command_env_uses_task_scoped_cbth_home(self) -> None:
        old_values = {
            "WEBEX_BOT_TOKEN": os.environ.get("WEBEX_BOT_TOKEN"),
            "WXCD_CONFIG_PATH": os.environ.get("WXCD_CONFIG_PATH"),
            "CBTH_HOME": os.environ.get("CBTH_HOME"),
        }
        os.environ["WEBEX_BOT_TOKEN"] = "prod-token"
        os.environ["WXCD_CONFIG_PATH"] = "/prod/wxcd.toml"
        os.environ["CBTH_HOME"] = "/prod/cbth"
        try:
            env = harness.upgrade_command_env(Path("/tmp/w7-cbth"))
        finally:
            for key, value in old_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertNotIn("WEBEX_BOT_TOKEN", env)
        self.assertNotIn("WXCD_CONFIG_PATH", env)
        self.assertEqual(env["CBTH_HOME"], "/tmp/w7-cbth")

    def test_redact_command_covers_split_sensitive_arguments(self) -> None:
        redacted = harness.redact_command(
            [
                "cbth",
                "plugin",
                "upgrade",
                "--token",
                "secret-token",
                "--bearer=secret-bearer",
                "WEBEX_BOT_TOKEN=env-secret",
                "Authorization: Bearer header-secret",
                "token",
                "next-secret",
                "secret-token-value",
            ]
        )

        self.assertEqual(
            redacted,
            [
                "cbth",
                "plugin",
                "upgrade",
                "--token",
                "<redacted>",
                "--bearer=<redacted>",
                "WEBEX_BOT_TOKEN=<redacted>",
                "<redacted>",
                "token",
                "<redacted>",
                "<redacted>",
            ],
        )
        for secret in [
            "secret-token",
            "secret-bearer",
            "env-secret",
            "header-secret",
            "next-secret",
            "secret-token-value",
        ]:
            self.assertNotIn(secret, json.dumps(redacted))

    def test_cbth_service_upgrade_smoke_preflight_uses_help_command(self) -> None:
        calls: list[dict[str, object]] = []
        original_run = harness.subprocess.run
        old_values = {
            "WEBEX_BOT_TOKEN": os.environ.get("WEBEX_BOT_TOKEN"),
            "WXCD_CONFIG_PATH": os.environ.get("WXCD_CONFIG_PATH"),
            "CBTH_HOME": os.environ.get("CBTH_HOME"),
        }

        def fake_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
            calls.append({"command": command, **kwargs})
            return harness.subprocess.CompletedProcess(command, 0)

        args = harness.build_parser().parse_args(["--cbth-bin", "/bin/echo"])
        harness.subprocess.run = fake_run
        os.environ["WEBEX_BOT_TOKEN"] = "prod-token"
        os.environ["WXCD_CONFIG_PATH"] = "/prod/wxcd.toml"
        os.environ["CBTH_HOME"] = "/prod/cbth"
        try:
            harness.preflight_cbth_service_upgrade_smoke(args, Path("/tmp"))
        finally:
            harness.subprocess.run = original_run
            for key, value in old_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(calls[0]["command"], ["/bin/echo", "service", "upgrade-smoke", "--help"])
        self.assertEqual(calls[0]["stdin"], harness.subprocess.DEVNULL)
        self.assertEqual(calls[0]["timeout"], harness.UPGRADE_CHECK_TIMEOUT_SECONDS)
        env = calls[0]["env"]
        self.assertIsInstance(env, dict)
        self.assertNotIn("WEBEX_BOT_TOKEN", env)
        self.assertNotIn("WXCD_CONFIG_PATH", env)
        self.assertNotIn("CBTH_HOME", env)

    def test_run_cbth_service_upgrade_smoke_records_c8_report(self) -> None:
        args = harness.build_parser().parse_args(["--cbth-bin", "/bin/echo"])
        with tempfile.TemporaryDirectory() as tmp:
            old_values = {
                "WEBEX_BOT_TOKEN": os.environ.get("WEBEX_BOT_TOKEN"),
                "WXCD_CONFIG_PATH": os.environ.get("WXCD_CONFIG_PATH"),
                "CBTH_HOME": os.environ.get("CBTH_HOME"),
            }
            test_root = Path(tmp) / "run"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
            )
            report = {
                "ok": True,
                "run_id": "run-1",
                "system_mutation_performed": False,
                "release_upgrade": {
                    "handoff_performed": True,
                    "events": [
                        "prepare_shadow:release-2",
                        "quiesce:active-1",
                        "drain:active-1",
                        "handoff_export:active-1",
                        "handoff_import:shadow-1",
                        "promote:active-1->shadow-1",
                        "shutdown:active-1",
                    ],
                },
            }
            calls: list[dict[str, object]] = []
            original_run = harness.subprocess.run

            def fake_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
                calls.append({"command": command, **kwargs})
                return harness.subprocess.CompletedProcess(
                    command,
                    0,
                    stdout=json.dumps({"service_upgrade_smoke": report}),
                    stderr="",
                )

            harness.subprocess.run = fake_run
            os.environ["WEBEX_BOT_TOKEN"] = "prod-token"
            os.environ["WXCD_CONFIG_PATH"] = "/prod/wxcd.toml"
            os.environ["CBTH_HOME"] = "/prod/cbth"
            try:
                result = harness.run_cbth_service_upgrade_smoke(state)
            finally:
                harness.subprocess.run = original_run
                for key, value in old_values.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

            self.assertEqual(result["run_id"], "run-1")
            self.assertEqual(state.manifest["cbth_service_upgrade_smoke"]["status"], "passed")
            self.assertEqual(
                state.manifest["cbth_service_upgrade_smoke"]["cbth_merge_commit"],
                harness.CBTH_C8_MERGE_COMMIT,
            )
            self.assertIn("upgrade-smoke", calls[0]["command"])
            self.assertEqual(calls[0]["stdin"], harness.subprocess.DEVNULL)
            env = calls[0]["env"]
            self.assertIsInstance(env, dict)
            self.assertNotIn("WEBEX_BOT_TOKEN", env)
            self.assertNotIn("WXCD_CONFIG_PATH", env)
            self.assertNotIn("CBTH_HOME", env)

    def test_cbth_service_upgrade_smoke_rejects_out_of_order_events(self) -> None:
        report = {
            "ok": True,
            "system_mutation_performed": False,
            "release_upgrade": {
                "handoff_performed": True,
                "events": [
                    "prepare_shadow:release-2",
                    "promote:active-1->shadow-1",
                    "quiesce:active-1",
                    "drain:active-1",
                    "handoff_export:active-1",
                    "handoff_import:shadow-1",
                    "shutdown:active-1",
                ],
            },
        }

        with self.assertRaisesRegex(harness.HarnessError, "release events do not match"):
            harness.validate_cbth_service_upgrade_smoke_report(report)

    def test_preflight_upgrade_command_rejects_missing_executable(self) -> None:
        args = harness.build_parser().parse_args(
            ["--live", "--cbth-upgrade-command", "/definitely/missing/w7-upgrade"]
        )

        with self.assertRaises(harness.BlockedError):
            harness.preflight_upgrade_command(args)

    def test_preflight_upgrade_command_rejects_missing_cbth_upgrade_subcommand(self) -> None:
        old_check = os.environ.pop("WXCD_E2E_CBTH_UPGRADE_CHECK_CMD", None)
        with tempfile.TemporaryDirectory() as tmp:
            try:
                cbth = Path(tmp) / "cbth"
                cbth.write_text("#!/usr/bin/env sh\nexit 2\n", encoding="utf-8")
                cbth.chmod(0o700)
                args = harness.build_parser().parse_args(
                    [
                        "--live",
                        "--cbth-upgrade-command",
                        f"{cbth} --home {{cbth_home}} plugin upgrade {{plugin}}",
                    ]
                )

                with self.assertRaises(harness.BlockedError):
                    harness.preflight_upgrade_command(
                        args,
                        Path(tmp) / "release-a",
                        Path(tmp) / "release-b",
                        Path(tmp) / "cbth-home",
                        "WXCD-W7-E2E-20260525-abc123xy",
                        Path(tmp),
                    )
            finally:
                if old_check is not None:
                    os.environ["WXCD_E2E_CBTH_UPGRADE_CHECK_CMD"] = old_check

    def test_preflight_upgrade_command_resolves_repo_relative_executable_with_cwd(self) -> None:
        old_check = os.environ.pop("WXCD_E2E_CBTH_UPGRADE_CHECK_CMD", None)
        with tempfile.TemporaryDirectory() as tmp:
            try:
                upgrade = Path(tmp) / "tools" / "w7-upgrade"
                upgrade.parent.mkdir()
                upgrade.write_text("#!/usr/bin/env sh\nexit 0\n", encoding="utf-8")
                upgrade.chmod(0o700)
                args = harness.build_parser().parse_args(
                    [
                        "--live",
                        "--cbth-upgrade-command",
                        "tools/w7-upgrade plugin upgrade {plugin}",
                    ]
                )

                harness.preflight_upgrade_command(
                    args,
                    Path(tmp) / "release-a",
                    Path(tmp) / "release-b",
                    Path(tmp) / "cbth-home",
                    "WXCD-W7-E2E-20260525-abc123xy",
                    Path(tmp),
                )
            finally:
                if old_check is not None:
                    os.environ["WXCD_E2E_CBTH_UPGRADE_CHECK_CMD"] = old_check

    def test_developer_email_override_must_match_token_owner(self) -> None:
        args = harness.build_parser().parse_args(["--developer-email", "other@example.com"])
        developer = harness.DeveloperToken(
            email="dev@example.com",
            bearer=harness.SecretText("secret"),
            ord_id="ord-123",
        )

        with self.assertRaises(harness.HarnessError):
            harness.resolve_developer_email(args, developer)

    def test_prepare_release_dirs_requires_release_pair(self) -> None:
        args = harness.build_parser().parse_args(["--live", "--release-a", "/tmp/release-a"])
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-TEST",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            with self.assertRaises(harness.BlockedError):
                harness.prepare_release_dirs(state)

    def test_prepare_release_dirs_scrubs_build_subprocess_env(self) -> None:
        args = harness.build_parser().parse_args(["--live"])
        old_values = {
            "WEBEX_BOT_TOKEN": os.environ.get("WEBEX_BOT_TOKEN"),
            "WXCD_CONFIG_PATH": os.environ.get("WXCD_CONFIG_PATH"),
            "CBTH_HOME": os.environ.get("CBTH_HOME"),
        }
        os.environ["WEBEX_BOT_TOKEN"] = "prod-token"
        os.environ["WXCD_CONFIG_PATH"] = "/prod/wxcd.toml"
        os.environ["CBTH_HOME"] = "/prod/cbth"
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-TEST",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            calls: list[dict[str, object]] = []
            original_ensure = harness.ensure_sidecar_dependencies
            original_run = harness.subprocess.run
            original_check_output = harness.subprocess.check_output
            original_stage_release = harness.stage_release

            def fake_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
                calls.append({"command": command, **kwargs})
                return harness.subprocess.CompletedProcess(command, 0)

            def fake_check_output(command: list[str], **kwargs: object) -> str:
                calls.append({"command": command, **kwargs})
                return json.dumps({"target_directory": str(Path(tmp) / "target")})

            def fake_stage_release(
                state: harness.RunState, target_dir: Path, label: str
            ) -> Path:
                return state.test_root / "releases" / label

            harness.ensure_sidecar_dependencies = lambda state: None
            harness.subprocess.run = fake_run
            harness.subprocess.check_output = fake_check_output
            harness.stage_release = fake_stage_release
            try:
                harness.prepare_release_dirs(state)
            finally:
                harness.ensure_sidecar_dependencies = original_ensure
                harness.subprocess.run = original_run
                harness.subprocess.check_output = original_check_output
                harness.stage_release = original_stage_release
                for key, value in old_values.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        self.assertEqual(len(calls), 2)
        for call in calls:
            env = call["env"]
            self.assertIsInstance(env, dict)
            self.assertNotIn("WEBEX_BOT_TOKEN", env)
            self.assertNotIn("WXCD_CONFIG_PATH", env)
            self.assertNotIn("CBTH_HOME", env)

    def test_ensure_sidecar_dependencies_scrubs_pnpm_install_env(self) -> None:
        args = harness.build_parser().parse_args(["--live"])
        old_values = {
            "WEBEX_BOT_TOKEN": os.environ.get("WEBEX_BOT_TOKEN"),
            "WXCD_CONFIG_PATH": os.environ.get("WXCD_CONFIG_PATH"),
            "CBTH_HOME": os.environ.get("CBTH_HOME"),
        }
        os.environ["WEBEX_BOT_TOKEN"] = "prod-token"
        os.environ["WXCD_CONFIG_PATH"] = "/prod/wxcd.toml"
        os.environ["CBTH_HOME"] = "/prod/cbth"
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            sidecar_dir = repo_root / "sidecars" / "webex-ws-sidecar"
            sidecar_dir.mkdir(parents=True)
            state = harness.RunState(
                args=args,
                repo_root=repo_root,
                test_root=repo_root / "run",
                prefix="WXCD-W7-TEST",
                logs_dir=repo_root / "run" / "logs",
                manifest_path=repo_root / "run" / "manifest.json",
            )
            calls: list[dict[str, object]] = []
            original_which = harness.shutil.which
            original_run = harness.subprocess.run

            def fake_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
                calls.append({"command": command, **kwargs})
                (sidecar_dir / "node_modules" / "@webex" / "webex-core").mkdir(parents=True)
                return harness.subprocess.CompletedProcess(command, 0)

            harness.shutil.which = lambda name: "/usr/bin/pnpm" if name == "pnpm" else original_which(name)
            harness.subprocess.run = fake_run
            try:
                harness.ensure_sidecar_dependencies(state)
            finally:
                harness.shutil.which = original_which
                harness.subprocess.run = original_run
                for key, value in old_values.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        self.assertEqual(len(calls), 1)
        env = calls[0]["env"]
        self.assertIsInstance(env, dict)
        self.assertNotIn("WEBEX_BOT_TOKEN", env)
        self.assertNotIn("WXCD_CONFIG_PATH", env)
        self.assertNotIn("CBTH_HOME", env)

    def test_validate_release_dir_requires_sidecar_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            release = Path(tmp) / "release"
            for path in [
                release / "bin" / "wxcd-worker",
                release / "bin" / "wxcd-supervisor",
                release / "plugin" / "manifest.json",
                release / "sidecars" / "webex-ws-sidecar" / "index.cjs",
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("", encoding="utf-8")

            with self.assertRaises(harness.HarnessError):
                harness.validate_release_dir(release)

    def test_sidecar_dependencies_present_accepts_webex_core_module(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sidecar = Path(tmp) / "sidecar"
            module = sidecar / "node_modules" / "@webex" / "webex-core"
            module.mkdir(parents=True)

            self.assertTrue(harness.sidecar_dependencies_present(sidecar))

    def test_socket_accepts_connections_detects_live_unix_socket(self) -> None:
        class FakeSocket:
            def __enter__(self) -> "FakeSocket":
                return self

            def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
                return None

            def settimeout(self, timeout: int) -> None:
                return None

            def connect(self, path: str) -> None:
                if path != "/tmp/live.sock":
                    raise OSError("connection refused")

        original_socket = harness.socket.socket
        harness.socket.socket = lambda family, kind: FakeSocket()
        try:
            self.assertTrue(harness.socket_accepts_connections(Path("/tmp/live.sock")))
            self.assertFalse(harness.socket_accepts_connections(Path("/tmp/missing.sock")))
        finally:
            harness.socket.socket = original_socket

    def test_private_file_writer_creates_temp_file_with_owner_only_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "secret.env"
            calls: list[tuple[int, int]] = []
            original_open = harness.os.open

            def recording_open(file: str, flags: int, mode: int = 0o777) -> int:
                calls.append((flags, mode))
                return original_open(file, flags, mode)

            harness.os.open = recording_open
            try:
                harness.write_private_text(path, "WEBEX_BOT_TOKEN=secret\n")
            finally:
                harness.os.open = original_open

            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertFalse(path.with_suffix(path.suffix + ".tmp").exists())
            self.assertEqual(
                calls,
                [(harness.os.O_WRONLY | harness.os.O_CREAT | harness.os.O_EXCL, 0o600)],
            )

    def test_cbth_registry_pins_plugin_identity_environment(self) -> None:
        args = harness.build_parser().parse_args([])
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "run"
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
            )

            registry_path = harness.write_cbth_registry(
                state,
                test_root / "cbth-home",
                Path("/release-a"),
                Path("/tmp/wxcd.toml"),
                Path("/tmp/wxcd.env"),
                "w7-instance",
                "w7-a",
            )

            registry = json.loads(registry_path.read_text(encoding="utf-8"))
            environment = registry["plugins"][0]["environment"]
            self.assertEqual(environment["WXCD_PLUGIN_INSTANCE_ID"], "w7-instance")
            self.assertEqual(environment["WXCD_PLUGIN_RELEASE_ID"], "w7-a")
            self.assertEqual(
                environment["WXCD_PLUGIN_HOME"],
                str(test_root / "cbth-home" / "plugins" / harness.PLUGIN_NAME),
            )

    def test_run_live_expanded_preflight_happens_before_parsing_credentials(self) -> None:
        old_live = os.environ.get("WXCD_LIVE_E2E")
        os.environ["WXCD_LIVE_E2E"] = "1"
        with tempfile.TemporaryDirectory() as tmp:
            token_file = Path(tmp) / "token.txt"
            token_file.write_text("email=dev@example.com\nbearer=secret\nord_id=ord\n", encoding="utf-8")
            bot_file = Path(tmp) / "bot.env"
            bot_file.write_text(
                "WEBEX_BOT_TOKEN=bot-secret\nWEBEX_BOT_EMAIL=bot@example.com\n",
                encoding="utf-8",
            )
            args = harness.build_parser().parse_args(
                [
                    "--live",
                    "--token-file",
                    str(token_file),
                    "--bot-env-file",
                    str(bot_file),
                    "--cbth-upgrade-command",
                    "{cbth_home}/missing-upgrade",
                ]
            )
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            original_prepare = harness.prepare_release_dirs
            original_parse_token = harness.parse_token_file
            original_parse_bot = harness.parse_bot_env_file

            def fake_prepare_release_dirs(state: harness.RunState) -> tuple[Path, Path]:
                return Path(tmp) / "release-a", Path(tmp) / "release-b"

            def fail_parse_token_file(path: Path) -> harness.DeveloperToken:
                raise AssertionError("token file should not be parsed before expanded preflight")

            def fail_parse_bot_env_file(path: Path) -> harness.BotCredentials:
                raise AssertionError("bot env file should not be parsed before expanded preflight")

            harness.prepare_release_dirs = fake_prepare_release_dirs
            harness.parse_token_file = fail_parse_token_file
            harness.parse_bot_env_file = fail_parse_bot_env_file
            try:
                with self.assertRaises(harness.BlockedError):
                    harness.run_live(state)
            finally:
                harness.prepare_release_dirs = original_prepare
                harness.parse_token_file = original_parse_token
                harness.parse_bot_env_file = original_parse_bot
                if old_live is None:
                    os.environ.pop("WXCD_LIVE_E2E", None)
                else:
                    os.environ["WXCD_LIVE_E2E"] = old_live

    def test_verify_command_executable_returns_expanded_user_path(self) -> None:
        old_home = os.environ.get("HOME")
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            tool = home / "bin" / "cbth"
            tool.parent.mkdir(parents=True)
            tool.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            tool.chmod(0o755)
            os.environ["HOME"] = str(home)
            try:
                resolved = harness.verify_command_executable("~/bin/cbth", Path(tmp), "cbth")
            finally:
                if old_home is None:
                    os.environ.pop("HOME", None)
                else:
                    os.environ["HOME"] = old_home

            self.assertEqual(resolved, str(tool))

    def test_cleanup_does_not_parse_unverified_token_file(self) -> None:
        args = harness.build_parser().parse_args(["--token-file", "/tmp/tracked-token.txt"])
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            original = harness.parse_token_file

            def fail_parse_token_file(path: Path) -> harness.DeveloperToken:
                raise AssertionError("parse_token_file should not be called by cleanup")

            harness.parse_token_file = fail_parse_token_file
            try:
                harness.cleanup_live(state)
            finally:
                harness.parse_token_file = original

    def test_cleanup_missing_owner_token_marks_known_room_failure(self) -> None:
        args = harness.build_parser().parse_args([])
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "run"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
                owns_test_root=True,
            )
            state.add_room("control", "known", "WXCD-W7-E2E-20260525-abc123xy control")
            state.record("result", "passed")

            cleanup_ok = harness.cleanup_live(state)

            self.assertFalse(cleanup_ok)
            self.assertTrue(test_root.exists())
            self.assertTrue(state.cleanup_failed)
            self.assertEqual(
                state.manifest["cleanup_skip_control"],
                "no token available for room owner",
            )

    def test_cleanup_error_keeps_passed_run_root_for_diagnostics(self) -> None:
        class FakeApi:
            def __init__(self, bearer: str) -> None:
                self.bearer = bearer

            def delete_room(self, room_id: str) -> None:
                raise harness.HarnessError(f"delete failed for {room_id}")

            def list_rooms(self, max_items: int = 100) -> list[dict[str, str]]:
                return []

        args = harness.build_parser().parse_args([])
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "run"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
                developer_token=harness.DeveloperToken(
                    email="dev@example.com",
                    bearer=harness.SecretText("secret"),
                    ord_id="ord-123",
                ),
            )
            state.add_room("control", "known", "WXCD-W7-E2E-20260525-abc123xy control")
            state.record("result", "passed")
            original = harness.WebexApi
            harness.WebexApi = FakeApi
            try:
                cleanup_ok = harness.cleanup_live(state)
            finally:
                harness.WebexApi = original

            self.assertFalse(cleanup_ok)
            self.assertTrue(test_root.exists())
            self.assertIn("cleanup_error_control", state.manifest)

    def test_cleanup_skip_for_known_room_keeps_root_for_diagnostics(self) -> None:
        class FakeApi:
            def __init__(self, bearer: str) -> None:
                self.bearer = bearer

            def delete_room(self, room_id: str) -> None:
                raise AssertionError(f"unsafe room must not be deleted: {room_id}")

            def list_rooms(self, max_items: int = 100) -> list[dict[str, str]]:
                return []

        args = harness.build_parser().parse_args([])
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "run"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
                owns_test_root=True,
                developer_token=harness.DeveloperToken(
                    email="dev@example.com",
                    bearer=harness.SecretText("secret"),
                    ord_id="ord-123",
                ),
            )
            state.add_room("control", "known", "unexpected title")
            state.record("result", "passed")
            original = harness.WebexApi
            harness.WebexApi = FakeApi
            try:
                cleanup_ok = harness.cleanup_live(state)
            finally:
                harness.WebexApi = original

            self.assertFalse(cleanup_ok)
            self.assertTrue(test_root.exists())
            self.assertTrue(state.cleanup_failed)
            self.assertEqual(
                state.manifest["cleanup_skip_control"],
                "room title does not match prefix",
            )

    def test_cleanup_deletes_only_owned_passed_run_root(self) -> None:
        args = harness.build_parser().parse_args([])
        with tempfile.TemporaryDirectory() as tmp:
            owned_root = Path(tmp) / "owned"
            explicit_root = Path(tmp) / "explicit"
            owned_root.mkdir()
            explicit_root.mkdir()
            owned_state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=owned_root,
                prefix="WXCD-W7-E2E-20260525-owned123",
                logs_dir=owned_root / "logs",
                manifest_path=owned_root / "manifest.json",
                owns_test_root=True,
            )
            explicit_state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=explicit_root,
                prefix="WXCD-W7-E2E-20260525-explic1",
                logs_dir=explicit_root / "logs",
                manifest_path=explicit_root / "manifest.json",
            )

            owned_state.record("result", "passed")
            explicit_state.record("result", "passed")
            self.assertTrue(harness.cleanup_live(owned_state))
            self.assertTrue(harness.cleanup_live(explicit_state))

            self.assertFalse(owned_root.exists())
            self.assertTrue(explicit_root.exists())
            self.assertNotEqual(owned_state.manifest_path, owned_root / "manifest.json")
            self.assertTrue(owned_state.manifest_path.exists())
            preserved = json.loads(owned_state.manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(preserved["success_manifest_preserved"])
            self.assertEqual(preserved["deleted_test_root"], str(owned_root))

    def test_cleanup_root_delete_error_marks_failure_and_preserves_diagnostics(self) -> None:
        args = harness.build_parser().parse_args([])
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "owned"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-owned123",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
                owns_test_root=True,
            )
            state.record("result", "passed")
            original_rmtree = harness.shutil.rmtree

            def fail_rmtree(path: Path) -> None:
                raise OSError(f"cannot delete {path}")

            harness.shutil.rmtree = fail_rmtree
            try:
                cleanup_ok = harness.cleanup_live(state)
            finally:
                harness.shutil.rmtree = original_rmtree

            self.assertFalse(cleanup_ok)
            self.assertTrue(test_root.exists())
            self.assertTrue(state.cleanup_failed)
            self.assertIn("cleanup_error_test_root", state.manifest)

    def test_main_success_reports_preserved_manifest_for_owned_root(self) -> None:
        original_run_live = harness.run_live
        original_cleanup_live = harness.cleanup_live
        original_mkdtemp = harness.tempfile.mkdtemp
        old_live = os.environ.get("WXCD_LIVE_E2E")
        os.environ["WXCD_LIVE_E2E"] = "1"

        with tempfile.TemporaryDirectory() as tmp:
            owned_root = Path(tmp) / "owned-run"

            def fake_mkdtemp(prefix: str) -> str:
                owned_root.mkdir()
                return str(owned_root)

            def fake_run_live(state: harness.RunState) -> None:
                state.record("result", "passed")

            def fake_cleanup_live(state: harness.RunState) -> bool:
                harness.cleanup_owned_test_root(state)
                return not state.cleanup_failed

            harness.tempfile.mkdtemp = fake_mkdtemp
            harness.run_live = fake_run_live
            harness.cleanup_live = fake_cleanup_live
            try:
                output = StringIO()
                with redirect_stdout(output):
                    code = harness.main(["--live", "--repo-root", str(Path(tmp)), "--cbth-bin", "/bin/echo"])
            finally:
                harness.tempfile.mkdtemp = original_mkdtemp
                harness.run_live = original_run_live
                harness.cleanup_live = original_cleanup_live
                if old_live is None:
                    os.environ.pop("WXCD_LIVE_E2E", None)
                else:
                    os.environ["WXCD_LIVE_E2E"] = old_live

            self.assertEqual(code, 0)
            payload = json.loads(output.getvalue())
            manifest_path = Path(payload["manifest"])
            self.assertTrue(manifest_path.exists())
            self.assertFalse(owned_root.exists())
            self.assertNotEqual(manifest_path, owned_root / "manifest.json")
            self.assertTrue(json.loads(manifest_path.read_text(encoding="utf-8"))["success_manifest_preserved"])

    def test_main_cleans_up_after_keyboard_interrupt(self) -> None:
        original_run_live = harness.run_live
        original_cleanup_live = harness.cleanup_live
        old_live = os.environ.get("WXCD_LIVE_E2E")
        os.environ["WXCD_LIVE_E2E"] = "1"

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            test_root = Path(tmp) / "run"
            repo_root.mkdir()
            cleanup_calls: list[str] = []

            def fake_run_live(state: harness.RunState) -> None:
                state.record("created_before_interrupt", True)
                raise KeyboardInterrupt

            def fake_cleanup_live(state: harness.RunState) -> bool:
                cleanup_calls.append(str(state.test_root))
                state.record("cleanup_called_after_interrupt", True)
                return True

            harness.run_live = fake_run_live
            harness.cleanup_live = fake_cleanup_live
            try:
                output = StringIO()
                with redirect_stdout(output):
                    code = harness.main(
                        [
                            "--live",
                            "--repo-root",
                            str(repo_root),
                            "--test-root",
                            str(test_root),
                            "--cbth-bin",
                            "/bin/echo",
                        ]
                    )
            finally:
                harness.run_live = original_run_live
                harness.cleanup_live = original_cleanup_live
                if old_live is None:
                    os.environ.pop("WXCD_LIVE_E2E", None)
                else:
                    os.environ["WXCD_LIVE_E2E"] = old_live

            self.assertEqual(code, 130)
            self.assertEqual(cleanup_calls, [str(test_root.resolve())])
            payload = json.loads(output.getvalue())
            self.assertEqual(payload["status"], "failed")
            self.assertEqual(payload["reason"], "interrupted")
            manifest = json.loads(Path(payload["manifest"]).read_text(encoding="utf-8"))
            self.assertTrue(manifest["cleanup_called_after_interrupt"])

    def test_cleanup_uses_bot_api_for_session_room(self) -> None:
        class FakeApi:
            deleted: list[tuple[str, str]] = []

            def __init__(self, bearer: str) -> None:
                self.bearer = str(bearer)

            def delete_room(self, room_id: str) -> None:
                self.deleted.append((self.bearer, room_id))

            def list_rooms(self, max_items: int = 100) -> list[dict[str, str]]:
                return []

        args = harness.build_parser().parse_args([])
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "run"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
                developer_token=harness.DeveloperToken(
                    email="dev@example.com",
                    bearer=harness.SecretText("dev-secret"),
                    ord_id="ord-123",
                ),
                bot_credentials=harness.BotCredentials(
                    token=harness.SecretText("bot-secret"),
                    email="bot@example.com",
                    display_name="Test Bot",
                ),
            )
            state.add_room("control", "control-room", "WXCD-W7-E2E-20260525-abc123xy control")
            state.add_room("session", "session-room", "WXCD-W7-E2E-20260525-abc123xy session")
            original = harness.WebexApi
            harness.WebexApi = FakeApi
            try:
                cleanup_ok = harness.cleanup_live(state)
            finally:
                harness.WebexApi = original

            self.assertTrue(cleanup_ok)
            self.assertIn(("dev-secret", "control-room"), FakeApi.deleted)
            self.assertIn(("bot-secret", "session-room"), FakeApi.deleted)

    def test_cleanup_prefix_scan_deletes_untracked_prefixed_rooms_with_safe_prefix(self) -> None:
        class FakeApi:
            def __init__(self) -> None:
                self.deleted: list[str] = []

            def list_rooms(self, max_items: int = 100) -> list[dict[str, str]]:
                return [
                    {"id": "known", "title": "WXCD-W7-E2E-20260525-abc123xy control"},
                    {"id": "untracked", "title": "WXCD-W7-E2E-20260525-abc123xy session"},
                    {"id": "other", "title": "Other room"},
                ]

            def delete_room(self, room_id: str) -> None:
                self.deleted.append(room_id)

        with tempfile.TemporaryDirectory() as tmp:
            args = harness.build_parser().parse_args([])
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            state.add_room("control", "known", "WXCD-W7-E2E-20260525-abc123xy control")
            api = FakeApi()

            harness.cleanup_untracked_prefix_rooms(state, api)

            self.assertEqual(api.deleted, ["untracked"])
            self.assertEqual(
                state.manifest["cleanup_prefix_scan_deleted_rooms_developer"],
                [{"id": "untracked", "title": "WXCD-W7-E2E-20260525-abc123xy session"}],
            )

    def test_cleanup_prefix_scan_skips_unsafe_custom_prefix(self) -> None:
        class FakeApi:
            def __init__(self) -> None:
                self.deleted: list[str] = []

            def list_rooms(self, max_items: int = 100) -> list[dict[str, str]]:
                return [{"id": "untracked", "title": "WXCD production"}]

            def delete_room(self, room_id: str) -> None:
                self.deleted.append(room_id)

        with tempfile.TemporaryDirectory() as tmp:
            args = harness.build_parser().parse_args([])
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            api = FakeApi()

            harness.cleanup_untracked_prefix_rooms(state, api)

            self.assertEqual(api.deleted, [])
            self.assertEqual(state.manifest["cleanup_prefix_scan_skipped_developer"]["prefix"], "WXCD")

    def test_create_local_thread_records_thread_id_before_history_seed(self) -> None:
        class FakeAppServer:
            def __init__(self, codex_bin: str, log_path: Path) -> None:
                self.codex_bin = codex_bin
                self.log_path = log_path

            def initialize(self) -> None:
                return None

            def create_thread(self, cwd: Path, developer_instructions: str) -> str:
                return "thread-123"

            def turn_start_and_wait(
                self,
                thread_id: str,
                cwd: Path,
                text: str,
                timeout_seconds: int,
            ) -> None:
                raise harness.HarnessError("history seed failed")

        args = harness.build_parser().parse_args(["--history-turns", "1"])
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=Path(tmp) / "run",
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=Path(tmp) / "run" / "logs",
                manifest_path=Path(tmp) / "run" / "manifest.json",
            )
            original = harness.CodexAppServer
            harness.CodexAppServer = FakeAppServer
            try:
                with self.assertRaises(harness.HarnessError):
                    harness.create_local_codex_thread(state)
            finally:
                harness.CodexAppServer = original

            self.assertEqual(state.thread_id, "thread-123")
            self.assertEqual(state.manifest["thread_id"], "thread-123")

    def test_upgrade_command_expands_placeholders_as_argv(self) -> None:
        command = harness.expand_upgrade_command(
            'cbth --home "{cbth_home}" plugin upgrade {plugin} --from {release_a_id} --to {release_b_id} --release-dir "{release_b}"',
            Path("/old release"),
            Path("/new release"),
            "w7-a",
            "w7-b",
            Path("/tmp/cbth-home"),
            "WXCD-W7",
        )

        self.assertEqual(
            command,
            [
                "cbth",
                "--home",
                "/tmp/cbth-home",
                "plugin",
                "upgrade",
                "webex-connector",
                "--from",
                "w7-a",
                "--to",
                "w7-b",
                "--release-dir",
                "/new release",
            ],
        )

    def test_upgrade_check_preflight_disables_stdin_and_uses_timeout(self) -> None:
        args = harness.build_parser().parse_args(
            ["--cbth-upgrade-command", "/bin/echo plugin upgrade {plugin}"]
        )
        calls: list[dict[str, object]] = []
        original_run = harness.subprocess.run

        def fake_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
            calls.append({"command": command, **kwargs})
            return harness.subprocess.CompletedProcess(command, 0)

        with tempfile.TemporaryDirectory() as tmp:
            harness.subprocess.run = fake_run
            try:
                harness.preflight_upgrade_command(
                    args,
                    Path(tmp) / "release-a",
                    Path(tmp) / "release-b",
                    Path(tmp) / "cbth-home",
                    "WXCD-W7-E2E-20260525-abc123xy",
                    Path(tmp),
                )
            finally:
                harness.subprocess.run = original_run

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["stdin"], harness.subprocess.DEVNULL)
        self.assertEqual(calls[0]["timeout"], harness.UPGRADE_CHECK_TIMEOUT_SECONDS)

    def test_upgrade_smoke_bounds_external_command(self) -> None:
        args = harness.build_parser().parse_args(
            [
                "--cbth-upgrade-command",
                "cbth plugin upgrade {plugin}",
                "--upgrade-timeout-seconds",
                "7",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "run"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
            )
            calls: list[dict[str, object]] = []
            original_run = harness.subprocess.run
            original_active_check = harness.active_check
            original_wait_until = harness.wait_until

            def fake_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
                calls.append({"command": command, **kwargs})
                return harness.subprocess.CompletedProcess(command, 0)

            def fake_wait_until(name: str, timeout_seconds: int, probe: object) -> object:
                self.assertEqual(timeout_seconds, args.startup_timeout_seconds)
                if name == "worker health after cbth upgrade":
                    return {"healthy": True}
                return True

            harness.subprocess.run = fake_run
            harness.active_check = lambda socket_path: {"healthy": True}
            harness.wait_until = fake_wait_until
            try:
                harness.run_upgrade_smoke_or_block(
                    state,
                    Path(tmp) / "release-a",
                    Path(tmp) / "release-b",
                    Path(tmp) / "cbth-home",
                    Path(tmp) / "lifecycle-a.sock",
                    Path(tmp) / "ingress-a.sock",
                    Path(tmp) / "lifecycle-b.sock",
                    Path(tmp) / "ingress-b.sock",
                )
            finally:
                harness.subprocess.run = original_run
                harness.active_check = original_active_check
                harness.wait_until = original_wait_until

            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["stdin"], harness.subprocess.DEVNULL)
            self.assertEqual(calls[0]["timeout"], 7)
            self.assertEqual(state.manifest["webex_release_upgrade"]["status"], "passed")

    def test_upgrade_smoke_timeout_unwinds_as_harness_error(self) -> None:
        args = harness.build_parser().parse_args(
            [
                "--cbth-upgrade-command",
                "cbth plugin upgrade {plugin}",
                "--upgrade-timeout-seconds",
                "7",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            test_root = Path(tmp) / "run"
            test_root.mkdir()
            state = harness.RunState(
                args=args,
                repo_root=Path(tmp),
                test_root=test_root,
                prefix="WXCD-W7-E2E-20260525-abc123xy",
                logs_dir=test_root / "logs",
                manifest_path=test_root / "manifest.json",
            )
            original_run = harness.subprocess.run
            original_active_check = harness.active_check

            def timeout_run(command: list[str], **kwargs: object) -> harness.subprocess.CompletedProcess[str]:
                raise harness.subprocess.TimeoutExpired(command, kwargs["timeout"])

            harness.subprocess.run = timeout_run
            harness.active_check = lambda socket_path: {"healthy": True}
            try:
                with self.assertRaisesRegex(harness.HarnessError, "timed out after 7s"):
                    harness.run_upgrade_smoke_or_block(
                        state,
                        Path(tmp) / "release-a",
                        Path(tmp) / "release-b",
                        Path(tmp) / "cbth-home",
                        Path(tmp) / "lifecycle-a.sock",
                        Path(tmp) / "ingress-a.sock",
                        Path(tmp) / "lifecycle-b.sock",
                        Path(tmp) / "ingress-b.sock",
                    )
            finally:
                harness.subprocess.run = original_run
                harness.active_check = original_active_check


if __name__ == "__main__":
    unittest.main()
