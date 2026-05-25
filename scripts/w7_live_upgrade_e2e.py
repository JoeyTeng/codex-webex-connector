#!/usr/bin/env python3
"""Opt-in W7 live Webex/cbth upgrade smoke harness.

The default path is intentionally dry-run only. Live mode requires
WXCD_LIVE_E2E=1 and local credentials. The harness owns Webex-specific setup,
assertions, diagnostics, and cleanup. Generic plugin upgrade orchestration must
come from cbth C8 `service upgrade-smoke` or an explicit operator-provided
Webex release upgrade command.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import queue
import random
import re
import secrets
import shlex
import shutil
import signal
import socket
import string
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


WEBEX_BASE_URL = "https://webexapis.com/v1"
UTC = dt.timezone.utc
REQUIRED_TOKEN_KEYS = ("email", "bearer", "ord_id")
RUN_MANIFEST = "manifest.json"
PLUGIN_NAME = "webex-connector"
CHILD_ENV_REMOVE_PREFIXES = ("WEBEX_", "WXCD_", "CBTH_")
SAFE_PREFIX_SCAN_RE = re.compile(r"^WXCD-W7-E2E-\d{8}-[a-z0-9]{8}$")
CBTH_C8_MERGE_COMMIT = "ee76fdd5937ca57e8156631c32509be12d3cf4c2"
CBTH_C8_PR_URL = "https://github.com/JoeyTeng/codex-background-task-handler/pull/99"
UPGRADE_CHECK_TIMEOUT_SECONDS = 30
WEBEX_TRANSIENT_ROOM_RETRY_ATTEMPTS = 5
WEBEX_TRANSIENT_ROOM_RETRY_BASE_SECONDS = 0.25


class HarnessError(RuntimeError):
    """Base class for expected harness failures."""


class BlockedError(HarnessError):
    """Raised when an external prerequisite is missing."""


class SecretText(str):
    """Marker type for values that must not be printed."""


def webex_path_segment(value: str) -> str:
    return urllib.parse.quote(value, safe="")


@dataclass(frozen=True)
class DeveloperToken:
    email: str
    bearer: SecretText
    ord_id: str

    @property
    def redacted_summary(self) -> dict[str, Any]:
        return {
            "email": self.email,
            "bearer_len": len(self.bearer),
            "ord_id_len": len(self.ord_id),
        }


@dataclass(frozen=True)
class BotCredentials:
    token: SecretText
    email: str
    display_name: str | None


@dataclass
class ProcessHandle:
    name: str
    process: subprocess.Popen[str]


@dataclass
class RunState:
    args: argparse.Namespace
    repo_root: Path
    test_root: Path
    prefix: str
    logs_dir: Path
    manifest_path: Path
    owns_test_root: bool = False
    manifest: dict[str, Any] = field(default_factory=dict)
    processes: list[ProcessHandle] = field(default_factory=list)
    created_rooms: dict[str, str] = field(default_factory=dict)
    session_id: str | None = None
    session_room_id: str | None = None
    thread_id: str | None = None
    codex_app_server: CodexAppServer | None = None
    developer_token: DeveloperToken | None = None
    bot_credentials: BotCredentials | None = None
    cleanup_failed: bool = False

    def record(self, key: str, value: Any) -> None:
        candidate = dict(self.manifest)
        candidate[key] = value
        write_private_json(self.manifest_path, candidate)
        self.manifest = candidate

    def add_room(self, label: str, room_id: str, title: str) -> None:
        self.created_rooms[label] = room_id
        rooms = self.manifest.setdefault("rooms", {})
        rooms[label] = {"id": room_id, "title": title, "deleted": False}
        write_private_json(self.manifest_path, self.manifest)

    def mark_room_deleted(self, label: str) -> None:
        rooms = self.manifest.setdefault("rooms", {})
        if label in rooms:
            rooms[label]["deleted"] = True
            write_private_json(self.manifest_path, self.manifest)

    def record_cleanup_error(self, key: str, error: Exception | str) -> None:
        self.cleanup_failed = True
        self.record(key, str(error))


class WebexApi:
    def __init__(self, bearer: str, timeout_seconds: int = 30) -> None:
        token = bearer.removeprefix("Bearer ").strip()
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._timeout_seconds = timeout_seconds

    def get_me(self) -> dict[str, Any]:
        return self.request("GET", "/people/me")

    def create_room(self, title: str) -> dict[str, Any]:
        return self.request("POST", "/rooms", {"title": title})

    def get_room(self, room_id: str) -> dict[str, Any]:
        return self.request("GET", f"/rooms/{webex_path_segment(room_id)}")

    def delete_room(self, room_id: str) -> None:
        self.request("DELETE", f"/rooms/{webex_path_segment(room_id)}", allow_empty=True)

    def list_rooms(self, max_items: int = 100) -> list[dict[str, Any]]:
        response = self.request("GET", f"/rooms?max={max_items}")
        return list(response.get("items", []))

    def create_membership(self, room_id: str, person_email: str) -> dict[str, Any]:
        return self.request(
            "POST",
            "/memberships",
            {
                "roomId": room_id,
                "personEmail": person_email,
            },
            allowed_statuses={200, 409},
            retry_transient_room=True,
        )

    def list_memberships(self, room_id: str) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"roomId": room_id, "max": "100"})
        response = self.request("GET", f"/memberships?{query}")
        return list(response.get("items", []))

    def delete_membership(self, membership_id: str) -> None:
        self.request(
            "DELETE",
            f"/memberships/{webex_path_segment(membership_id)}",
            allow_empty=True,
        )

    def create_message(self, room_id: str, text: str, markdown: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"roomId": room_id, "text": text}
        if markdown is not None:
            body["markdown"] = markdown
        return self.request("POST", "/messages", body, retry_transient_room=True)

    def list_messages(self, room_id: str, max_items: int = 50) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"roomId": room_id, "max": str(max_items)})
        response = self.request("GET", f"/messages?{query}")
        return list(response.get("items", []))

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        allow_empty: bool = False,
        allowed_statuses: set[int] | None = None,
        retry_transient_room: bool = False,
    ) -> dict[str, Any]:
        allowed_statuses = allowed_statuses or set(range(200, 300))
        url = f"{WEBEX_BASE_URL}{path}"
        data = None if body is None else json.dumps(body).encode("utf-8")
        attempts = WEBEX_TRANSIENT_ROOM_RETRY_ATTEMPTS if retry_transient_room else 1
        delay = WEBEX_TRANSIENT_ROOM_RETRY_BASE_SECONDS
        for attempt in range(attempts):
            request = urllib.request.Request(url, data=data, method=method, headers=self._headers)
            try:
                with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                    payload = response.read()
                    if response.status not in allowed_statuses:
                        raise HarnessError(f"Webex {method} {path} returned HTTP {response.status}")
            except urllib.error.HTTPError as error:
                payload = error.read()
                message = payload.decode("utf-8", errors="replace")
                if (
                    retry_transient_room
                    and attempt + 1 < attempts
                    and is_transient_invalid_room_error(error.code, message)
                ):
                    time.sleep(delay)
                    delay *= 2
                    continue
                if error.code in allowed_statuses:
                    if allow_empty or not payload:
                        return {}
                    return json.loads(payload.decode("utf-8"))
                raise HarnessError(f"Webex {method} {path} returned HTTP {error.code}: {message}") from error
            if allow_empty or not payload:
                return {}
            return json.loads(payload.decode("utf-8"))
        raise HarnessError(f"Webex {method} {path} retry loop exhausted")


def is_transient_invalid_room_error(status_code: int, response_body: str) -> bool:
    return status_code == 400 and "Invalid roomId" in response_body


class CodexAppServer:
    def __init__(self, codex_bin: str, log_path: Path) -> None:
        self._next_id = 1
        self._events: queue.Queue[dict[str, Any]] = queue.Queue()
        self._responses: dict[int, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._closed = threading.Event()
        stderr = log_path.open("w", encoding="utf-8")
        self._process = subprocess.Popen(
            [codex_bin, "app-server", "--listen", "stdio://"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            env=isolated_child_env(),
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self._stderr = stderr
        if self._process.stdin is None or self._process.stdout is None:
            raise HarnessError("codex app-server stdio pipes are unavailable")
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader.start()

    def initialize(self) -> None:
        self.request(
            "initialize",
            {
                "clientInfo": {"name": "wxcd-w7-live-e2e", "version": "0.1.0"},
                "capabilities": {"experimentalApi": True},
            },
        )

    def create_thread(self, cwd: Path, developer_instructions: str) -> str:
        response = self.request(
            "thread/start",
            {
                "cwd": str(cwd),
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "developerInstructions": developer_instructions,
                "serviceName": "wxcd-w7-live-e2e",
                "experimentalRawEvents": False,
                "persistExtendedHistory": True,
            },
        )
        return json_pointer_str(response, "/thread/id")

    def turn_start_and_wait(self, thread_id: str, cwd: Path, text: str, timeout_seconds: int) -> None:
        self.request(
            "turn/start",
            {
                "threadId": thread_id,
                "cwd": str(cwd),
                "input": [{"type": "text", "text": text, "text_elements": []}],
            },
        )
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                event = self._events.get(timeout=min(1.0, remaining))
            except queue.Empty:
                continue
            if (
                event.get("method") == "turn/completed"
                and json_pointer(event.get("params", {}), "/threadId") == thread_id
            ):
                return
        raise HarnessError(f"timed out waiting for local Codex turn completion for {thread_id}")

    def archive(self, thread_id: str) -> None:
        self.request("thread/archive", {"threadId": thread_id})

    def request(self, method: str, params: dict[str, Any], timeout_seconds: int = 120) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        frame = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        assert self._process.stdin is not None
        self._process.stdin.write(json.dumps(frame) + "\n")
        self._process.stdin.flush()
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            with self._lock:
                response = self._responses.pop(request_id, None)
            if response is not None:
                if "error" in response:
                    raise HarnessError(f"codex app-server {method} failed: {response['error']}")
                result = response.get("result")
                if not isinstance(result, dict):
                    return {"value": result}
                return result
            if self._closed.is_set():
                raise HarnessError(f"codex app-server closed while waiting for {method}")
            time.sleep(0.05)
        raise HarnessError(f"timed out waiting for codex app-server {method}")

    def close(self) -> None:
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        self._stderr.close()

    def _read_stdout(self) -> None:
        assert self._process.stdout is not None
        for line in self._process.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            response_id = payload.get("id")
            if isinstance(response_id, int) and ("result" in payload or "error" in payload):
                with self._lock:
                    self._responses[response_id] = payload
                continue
            if "method" in payload:
                self._events.put(payload)
        self._closed.set()


def parse_token_file(path: Path) -> DeveloperToken:
    content = path.read_text(encoding="utf-8")
    values = parse_json_or_env(content)
    normalized = normalize_keys(values)
    missing = [key for key in REQUIRED_TOKEN_KEYS if not normalized.get(key)]
    if missing:
        raise HarnessError(f"token file is missing fields: {', '.join(missing)}")
    return DeveloperToken(
        email=str(normalized["email"]).strip().lower(),
        bearer=SecretText(str(normalized["bearer"]).strip().removeprefix("Bearer ").strip()),
        ord_id=str(normalized["ord_id"]).strip(),
    )


def parse_bot_env_file(path: Path) -> BotCredentials:
    values = parse_json_or_env(path.read_text(encoding="utf-8"))
    token = values.get("WEBEX_BOT_TOKEN")
    email = values.get("WEBEX_BOT_EMAIL")
    if not token or not email:
        raise HarnessError(f"{path} must contain WEBEX_BOT_TOKEN and WEBEX_BOT_EMAIL")
    display_name = values.get("WEBEX_BOT_DISPLAY_NAME")
    return BotCredentials(
        token=SecretText(str(token).strip().removeprefix("Bearer ").strip()),
        email=str(email).strip().lower(),
        display_name=str(display_name).strip() if display_name else None,
    )


def parse_json_or_env(content: str) -> dict[str, str]:
    stripped = content.strip()
    if not stripped:
        return {}
    if stripped.startswith("{"):
        loaded = json.loads(stripped)
        if not isinstance(loaded, dict):
            raise HarnessError("JSON credential file must contain an object")
        values: dict[str, str] = {}
        for key, value in loaded.items():
            if not isinstance(value, str):
                raise HarnessError(f"JSON credential value for {key} must be a string")
            values[str(key)] = value
        return values
    values: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            continue
        values[key.strip()] = strip_env_quotes(value.strip())
    return values


def strip_env_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def normalize_keys(values: dict[str, str]) -> dict[str, str]:
    aliases = {
        "WEBEX_DEVELOPER_EMAIL": "email",
        "WXCD_E2E_DEVELOPER_EMAIL": "email",
        "EMAIL": "email",
        "email": "email",
        "WEBEX_DEVELOPER_BEARER": "bearer",
        "WXCD_E2E_DEVELOPER_BEARER": "bearer",
        "BEARER": "bearer",
        "bearer": "bearer",
        "WEBEX_DEVELOPER_ORD_ID": "ord_id",
        "WXCD_E2E_DEVELOPER_ORD_ID": "ord_id",
        "ORD_ID": "ord_id",
        "ord_id": "ord_id",
    }
    normalized: dict[str, str] = {}
    for key, value in values.items():
        target = aliases.get(key)
        if target:
            normalized[target] = value
    return normalized


def ensure_opt_in(live: bool) -> None:
    if live and os.environ.get("WXCD_LIVE_E2E") != "1":
        raise BlockedError("live mode requires WXCD_LIVE_E2E=1")


def upgrade_command_template(args: argparse.Namespace) -> str | None:
    return args.cbth_upgrade_command or os.environ.get("WXCD_E2E_CBTH_UPGRADE_CMD")


def upgrade_check_command_template(args: argparse.Namespace) -> str | None:
    return args.cbth_upgrade_check_command or os.environ.get("WXCD_E2E_CBTH_UPGRADE_CHECK_CMD")


def preflight_cbth_service_upgrade_smoke(args: argparse.Namespace, cwd: Path) -> None:
    command = [args.cbth_bin, "service", "upgrade-smoke", "--help"]
    verify_command_executable(command[0], cwd, "cbth C8 service upgrade-smoke executable")
    try:
        subprocess.run(
            command,
            cwd=cwd,
            env=isolated_child_env(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=UPGRADE_CHECK_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise BlockedError(
            "cbth C8 service upgrade-smoke command is unavailable; "
            f"requires cbth PR #99 merged at {CBTH_C8_MERGE_COMMIT}"
        ) from error


def preflight_upgrade_command(
    args: argparse.Namespace,
    release_a: Path | None = None,
    release_b: Path | None = None,
    cbth_home: Path | None = None,
    prefix: str | None = None,
    cwd: Path | None = None,
) -> None:
    template = upgrade_command_template(args)
    if not template:
        return
    try:
        command = shlex.split(template)
    except ValueError as error:
        raise BlockedError(f"Webex release upgrade command is invalid: {error}") from error
    if not command:
        raise BlockedError("Webex release upgrade command is empty")
    if release_a is not None and release_b is not None and cbth_home is not None and prefix is not None:
        command = expand_upgrade_command(
            template,
            release_a,
            release_b,
            args.release_a_id,
            args.release_b_id,
            cbth_home,
            prefix,
        )
    verify_command_executable(command[0], cwd or Path.cwd(), "Webex release upgrade executable")
    if release_a is not None and release_b is not None and cbth_home is not None and prefix is not None:
        verify_upgrade_command_semantics(args, command, release_a, release_b, cbth_home, prefix, cwd or Path.cwd())


def verify_command_executable(
    executable: str,
    cwd: Path | None = None,
    label: str = "executable",
) -> None:
    if "{" in executable or "}" in executable:
        return
    expanded = Path(executable).expanduser()
    if expanded.parent != Path(".") or any(separator in executable for separator in ("/", os.sep)):
        candidate = expanded if expanded.is_absolute() else (cwd or Path.cwd()) / expanded
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return
        raise BlockedError(f"{label} is unavailable: {executable}")
    if shutil.which(executable) is None:
        raise BlockedError(f"{label} is unavailable: {executable}")


def verify_upgrade_command_semantics(
    args: argparse.Namespace,
    command: list[str],
    release_a: Path,
    release_b: Path,
    cbth_home: Path,
    prefix: str,
    cwd: Path,
) -> None:
    check_template = upgrade_check_command_template(args)
    if check_template:
        check_command = expand_upgrade_command(
            check_template,
            release_a,
            release_b,
            args.release_a_id,
            args.release_b_id,
            cbth_home,
            prefix,
        )
    else:
        check_command = inferred_cbth_plugin_upgrade_check(command)
        if check_command is None:
            raise BlockedError("custom Webex release upgrade command requires WXCD_E2E_CBTH_UPGRADE_CHECK_CMD")
    try:
        subprocess.run(
            check_command,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            env=upgrade_command_env(cbth_home),
            timeout=UPGRADE_CHECK_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise BlockedError(f"Webex release upgrade check failed: {redact_command(check_command)}") from error


def inferred_cbth_plugin_upgrade_check(command: list[str]) -> list[str] | None:
    try:
        plugin_index = command.index("plugin")
        upgrade_index = command.index("upgrade", plugin_index + 1)
    except ValueError:
        return None
    if upgrade_index != plugin_index + 1:
        return None
    return command[: upgrade_index + 1] + ["--help"]


def ensure_untracked(path: Path, repo_root: Path) -> None:
    if not path.exists():
        raise HarnessError(f"credential file does not exist: {path}")
    pathspec = str(path)
    try:
        pathspec = str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        pass
    try:
        subprocess.run(
            ["git", "ls-files", "--error-unmatch", pathspec],
            cwd=repo_root,
            env=isolated_child_env(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    except subprocess.CalledProcessError:
        return
    raise HarnessError(f"credential file is tracked by git and cannot be used: {path}")


def validate_test_root(test_root: Path, repo_root: Path, explicit: bool) -> None:
    if not explicit:
        return
    try:
        relative = test_root.resolve().relative_to(repo_root.resolve())
    except ValueError:
        return
    if relative.parts and relative.parts[0] == ".codex-tmp":
        return
    raise BlockedError(
        "--test-root inside the repository must be under ignored .codex-tmp/ "
        "because live diagnostics include secret-bearing wxcd.env"
    )


def default_prefix() -> str:
    date = dt.datetime.now(UTC).strftime("%Y%m%d")
    suffix = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8))
    return f"WXCD-W7-E2E-{date}-{suffix}"


def write_private_json(path: Path, value: dict[str, Any]) -> None:
    write_private_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_private_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(value)
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        if fd != -1:
            os.close(fd)


def fnv1a_hex(value: str) -> str:
    hash_value = 0xCBF29CE484222325
    for byte in value.encode("utf-8"):
        hash_value ^= byte
        hash_value = (hash_value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{hash_value:016x}"


def worker_ingress_socket_path(plugin_instance_id: str, plugin_release_id: str, state_dir: Path) -> Path:
    key = f"{plugin_instance_id}\n{plugin_release_id}\n{state_dir}"
    return Path("/tmp") / f"wxcd-ingress-{fnv1a_hex(key)}.sock"


def lifecycle_socket_path(plugin_instance_id: str, plugin_release_id: str, plugin_home: Path) -> Path:
    key = f"{plugin_instance_id}\n{plugin_release_id}\n{plugin_home}"
    return Path("/tmp") / f"wxcd-lifecycle-{fnv1a_hex(key)}.sock"


def json_pointer(value: Any, pointer: str) -> Any:
    current = value
    for part in pointer.strip("/").split("/"):
        if part == "":
            continue
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def json_pointer_str(value: Any, pointer: str) -> str:
    result = json_pointer(value, pointer)
    if not isinstance(result, str) or not result:
        raise HarnessError(f"missing JSON string at {pointer}")
    return result


def ensure_person_email_matches(person: dict[str, Any], expected_email: str, label: str) -> None:
    emails = [str(item).lower() for item in person.get("emails", [])]
    if expected_email.lower() not in emails:
        raise HarnessError(f"{label} token email does not match credential file")


def resolve_developer_email(args: argparse.Namespace, developer: DeveloperToken) -> str:
    if args.developer_email is None:
        args.developer_email = developer.email
        return developer.email
    override = str(args.developer_email).strip().lower()
    if override != developer.email:
        raise HarnessError("--developer-email must match the developer token email")
    args.developer_email = override
    return override


def history_page_needles(page: int, thread_id: str) -> list[str]:
    return [f"history page {page} of", thread_id]


def history_page_response_alternatives(page: int, thread_id: str, allow_missing: bool) -> list[list[str]]:
    alternatives = [history_page_needles(page, thread_id)]
    if allow_missing:
        alternatives.append([f"No history on page {page} for thread `{thread_id}`."])
    return alternatives


def wait_until(name: str, timeout_seconds: int, probe: Any) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = probe()
            if result:
                return result
        except Exception as error:  # noqa: BLE001
            last_error = error
        time.sleep(1)
    if last_error is not None:
        raise HarnessError(f"timed out waiting for {name}: {last_error}") from last_error
    raise HarnessError(f"timed out waiting for {name}")


def send_worker_ingress(socket_path: Path, envelope: dict[str, Any], timeout_seconds: int = 30) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout_seconds)
        client.connect(str(socket_path))
        client.sendall(json.dumps(envelope).encode("utf-8") + b"\n")
        data = b""
        while not data.endswith(b"\n"):
            chunk = client.recv(4096)
            if not chunk:
                break
            data += chunk
    if not data:
        raise HarnessError(f"worker socket {socket_path} returned no data")
    return json.loads(data.decode("utf-8"))


def health_check(socket_path: Path) -> dict[str, Any]:
    return send_worker_ingress(socket_path, {"kind": "health_check"}, timeout_seconds=5)


def active_check(socket_path: Path) -> dict[str, Any]:
    return send_worker_ingress(socket_path, {"kind": "active_check"}, timeout_seconds=5)


def socket_accepts_connections(socket_path: Path) -> bool:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(1)
            client.connect(str(socket_path))
        return True
    except OSError:
        return False


def start_process(
    state: RunState,
    name: str,
    argv: list[str],
    env: dict[str, str] | None = None,
) -> None:
    stdout = (state.logs_dir / f"{name}.out.log").open("w", encoding="utf-8")
    stderr = (state.logs_dir / f"{name}.err.log").open("w", encoding="utf-8")
    process_env = isolated_child_env(env)
    process = subprocess.Popen(
        argv,
        cwd=state.repo_root,
        env=process_env,
        stdout=stdout,
        stderr=stderr,
        text=True,
        start_new_session=True,
    )
    state.processes.append(ProcessHandle(name=name, process=process))
    state.record("processes", [{"name": item.name, "pid": item.process.pid} for item in state.processes])


def isolated_child_env(overrides: dict[str, str] | None = None) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not any(key.startswith(prefix) for prefix in CHILD_ENV_REMOVE_PREFIXES)
    }
    if overrides:
        env.update(overrides)
    return env


def upgrade_command_env(cbth_home: Path) -> dict[str, str]:
    return isolated_child_env({"CBTH_HOME": str(cbth_home)})


def run_cbth_service_upgrade_smoke(state: RunState) -> dict[str, Any]:
    smoke_root = (
        Path(state.args.cbth_service_upgrade_smoke_root).expanduser().resolve()
        if state.args.cbth_service_upgrade_smoke_root
        else state.test_root / "cbth-c8-service-upgrade-smoke"
    )
    startup_timeout_ms = state.args.cbth_service_upgrade_smoke_timeout_seconds * 1000
    command = [
        state.args.cbth_bin,
        "service",
        "upgrade-smoke",
        "--allow-task-scoped-mutation",
        "--smoke-root",
        str(smoke_root),
        "--startup-timeout-ms",
        str(startup_timeout_ms),
        "--json",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=state.repo_root,
            env=isolated_child_env(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            timeout=state.args.cbth_service_upgrade_smoke_timeout_seconds + 10,
        )
    except subprocess.TimeoutExpired as error:
        raise HarnessError(
            "cbth C8 service upgrade-smoke timed out after "
            f"{state.args.cbth_service_upgrade_smoke_timeout_seconds}s"
        ) from error
    except (OSError, subprocess.CalledProcessError) as error:
        stderr = getattr(error, "stderr", "") or ""
        raise HarnessError(f"cbth C8 service upgrade-smoke failed: {stderr.strip()}") from error

    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise HarnessError(f"cbth C8 service upgrade-smoke returned invalid JSON: {completed.stdout}") from error
    report = payload.get("service_upgrade_smoke")
    if not isinstance(report, dict):
        raise HarnessError("cbth C8 service upgrade-smoke JSON missing service_upgrade_smoke")
    validate_cbth_service_upgrade_smoke_report(report)
    state.record(
        "cbth_service_upgrade_smoke",
        {
            "status": "passed",
            "cbth_pr": CBTH_C8_PR_URL,
            "cbth_merge_commit": CBTH_C8_MERGE_COMMIT,
            "command": redact_command(command),
            "report": report,
        },
    )
    return report


def validate_cbth_service_upgrade_smoke_report(report: dict[str, Any]) -> None:
    if report.get("ok") is not True:
        raise HarnessError("cbth C8 service upgrade-smoke did not report ok=true")
    if report.get("system_mutation_performed") is not False:
        raise HarnessError("cbth C8 service upgrade-smoke reported system mutation")
    release_upgrade = report.get("release_upgrade")
    if not isinstance(release_upgrade, dict):
        raise HarnessError("cbth C8 service upgrade-smoke missing release_upgrade")
    if release_upgrade.get("handoff_performed") is not True:
        raise HarnessError("cbth C8 service upgrade-smoke did not perform handoff")
    events = release_upgrade.get("events")
    required_events = [
        "prepare_shadow:release-2",
        "quiesce:active-1",
        "drain:active-1",
        "handoff_export:active-1",
        "handoff_import:shadow-1",
        "promote:active-1->shadow-1",
        "shutdown:active-1",
    ]
    if not isinstance(events, list) or not has_ordered_subsequence(
        list(map(str, events)),
        required_events,
    ):
        raise HarnessError("cbth C8 service upgrade-smoke release events do not match C8 contract")


def has_ordered_subsequence(events: list[str], required_events: list[str]) -> bool:
    next_required = 0
    for event in events:
        if next_required < len(required_events) and event == required_events[next_required]:
            next_required += 1
    return next_required == len(required_events)


def stop_processes(state: RunState) -> None:
    for handle in reversed(state.processes):
        if handle.process.poll() is not None:
            continue
        try:
            os.killpg(handle.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        try:
            handle.process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(handle.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            handle.process.wait(timeout=5)


def prepare_release_dirs(state: RunState) -> tuple[Path, Path]:
    release_a = Path(state.args.release_a).expanduser().resolve() if state.args.release_a else None
    release_b = Path(state.args.release_b).expanduser().resolve() if state.args.release_b else None
    if (release_a is None) != (release_b is None):
        raise BlockedError("--release-a and --release-b must be provided together")
    if release_a and release_b:
        validate_release_dir(release_a)
        validate_release_dir(release_b)
        return release_a, release_b
    if state.args.no_build_release:
        raise BlockedError("--release-a and --release-b are required when --no-build-release is set")

    print("Building release binaries for isolated live smoke.")
    ensure_sidecar_dependencies(state)
    build_env = isolated_child_env()
    subprocess.run(
        ["cargo", "build", "--release", "--package", "wxcd-worker", "--package", "wxcd-supervisor"],
        cwd=state.repo_root,
        env=build_env,
        check=True,
    )
    target_dir = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=state.repo_root,
        env=build_env,
        text=True,
    )
    target_path = Path(json.loads(target_dir)["target_directory"])
    release_a = stage_release(state, target_path, "release-a")
    release_b = stage_release(state, target_path, "release-b")
    return release_a, release_b


def validate_release_dir(path: Path) -> None:
    required = [
        path / "bin" / "wxcd-worker",
        path / "bin" / "wxcd-supervisor",
        path / "plugin" / "manifest.json",
        path / "sidecars" / "webex-ws-sidecar" / "index.cjs",
        path / "sidecars" / "webex-ws-sidecar" / "node_modules" / "@webex" / "webex-core",
    ]
    missing = [item for item in required if not item.exists()]
    if missing:
        formatted = ", ".join(str(item) for item in missing)
        raise HarnessError(f"release dir is missing required files: {formatted}")


def ensure_sidecar_dependencies(state: RunState) -> None:
    sidecar_dir = state.repo_root / "sidecars" / "webex-ws-sidecar"
    if sidecar_dependencies_present(sidecar_dir):
        return
    pnpm_bin = shutil.which("pnpm")
    if pnpm_bin is None:
        raise BlockedError(
            "sidecar runtime dependencies are missing; install pnpm and run pnpm install --dir sidecars/webex-ws-sidecar"
        )
    print("Installing sidecar runtime dependencies for isolated live smoke.")
    subprocess.run(
        [pnpm_bin, "--dir", str(sidecar_dir), "install", "--frozen-lockfile"],
        cwd=state.repo_root,
        env=isolated_child_env(),
        check=True,
    )
    if not sidecar_dependencies_present(sidecar_dir):
        raise HarnessError("sidecar runtime dependencies are still missing after pnpm install")


def sidecar_dependencies_present(sidecar_dir: Path) -> bool:
    return (sidecar_dir / "node_modules" / "@webex" / "webex-core").exists()


def stage_release(state: RunState, target_dir: Path, label: str) -> Path:
    release_dir = state.test_root / "releases" / label
    (release_dir / "bin").mkdir(parents=True, exist_ok=True)
    shutil.copy2(target_dir / "release" / "wxcd-worker", release_dir / "bin" / "wxcd-worker")
    shutil.copy2(target_dir / "release" / "wxcd-supervisor", release_dir / "bin" / "wxcd-supervisor")
    shutil.copytree(state.repo_root / "plugin", release_dir / "plugin", dirs_exist_ok=True)
    shutil.copytree(
        state.repo_root / "sidecars" / "webex-ws-sidecar",
        release_dir / "sidecars" / "webex-ws-sidecar",
        dirs_exist_ok=True,
    )
    validate_release_dir(release_dir)
    return release_dir


def write_wxcd_files(
    state: RunState,
    bot: BotCredentials,
    control_room_id: str,
    data_room_id: str,
    release_a: Path,
    plugin_instance_id: str,
    plugin_release_id: str,
) -> tuple[Path, Path, Path, Path]:
    wxcd_state_dir = state.test_root / "wxcd-state"
    cbth_home = state.test_root / "cbth-home"
    plugin_home = cbth_home / "plugins" / PLUGIN_NAME
    config_path = state.test_root / "wxcd.toml"
    env_path = state.test_root / "wxcd.env"
    ingress_socket = worker_ingress_socket_path(plugin_instance_id, plugin_release_id, wxcd_state_dir)
    lifecycle_socket = lifecycle_socket_path(plugin_instance_id, plugin_release_id, plugin_home)
    config = f"""socket_path = "{state.test_root / "wxcd.sock"}"
state_dir = "{wxcd_state_dir}"
session_title_prefix = "{state.prefix}"
approval_policy = "never"
sandbox_mode = "read-only"
snapshot_interval = 5

developer_instructions = \"\"\"
You are operating through the wxcd W7 live E2E harness.
Keep replies short and deterministic.
\"\"\"

[cbth_plugin]
enabled = true
plugin_home = "{plugin_home}"
plugin_instance_id = "{plugin_instance_id}"
plugin_release_id = "{plugin_release_id}"
manifest_path = "{release_a / "plugin" / "manifest.json"}"

[[repos]]
name = "codex-webex-connector"
path = "{state.repo_root}"
"""
    write_private_text(config_path, config)
    env_lines = [
        f"WEBEX_BOT_TOKEN={bot.token}",
        f"WEBEX_BOT_EMAIL={bot.email}",
        f"WEBEX_CONTROL_ROOM_SPACE_LINK={control_room_id}",
        f"WEBEX_DATA_ROOM_SPACE_LINK={data_room_id}",
        f"WEBEX_ALLOWED_USER_EMAILS={state.args.developer_email}",
    ]
    if bot.display_name:
        env_lines.append(f"WEBEX_BOT_DISPLAY_NAME={bot.display_name}")
    write_private_text(env_path, "\n".join(env_lines) + "\n")
    state.record(
        "wxcd",
        {
            "config_path": str(config_path),
            "env_path": str(env_path),
            "state_dir": str(wxcd_state_dir),
            "ingress_socket": str(ingress_socket),
            "lifecycle_socket": str(lifecycle_socket),
        },
    )
    return config_path, env_path, ingress_socket, lifecycle_socket


def write_cbth_registry(
    state: RunState,
    cbth_home: Path,
    release_a: Path,
    config_path: Path,
    env_path: Path,
    plugin_instance_id: str,
    plugin_release_id: str,
) -> Path:
    registry_path = cbth_home / "plugins" / "registry.json"
    registry = {
        "schema_version": 1,
        "plugins": [
            {
                "name": PLUGIN_NAME,
                "executable_path": str(release_a / "bin" / "wxcd-supervisor"),
                "args": ["run"],
                "enabled": True,
                "release_id": plugin_release_id,
                "capabilities": [
                    "plugin-rpc-v1",
                    "diagnostics",
                    "standalone-compatible",
                    "plugin-lifecycle-v1",
                    "plugin-handoff-v1",
                ],
                "environment": {
                    "WXCD_CONFIG_PATH": str(config_path),
                    "WXCD_ENV_PATH": str(env_path),
                    "WXCD_NODE_PATH": state.args.node_bin,
                    "WXCD_CODEX_PATH": state.args.codex_bin,
                    "WXCD_RELEASE_DIR": str(release_a),
                    "WXCD_PLUGIN_HOME": str(cbth_home / "plugins" / PLUGIN_NAME),
                    "WXCD_PLUGIN_INSTANCE_ID": plugin_instance_id,
                    "WXCD_PLUGIN_RELEASE_ID": plugin_release_id,
                },
            }
        ],
    }
    write_private_json(registry_path, registry)
    state.record("cbth_registry", str(registry_path))
    return registry_path


def run_live(state: RunState) -> None:
    ensure_opt_in(True)
    token_file = Path(state.args.token_file).expanduser().resolve()
    bot_env_file = Path(state.args.bot_env_file).expanduser().resolve()
    ensure_untracked(token_file, state.repo_root)
    ensure_untracked(bot_env_file, state.repo_root)
    release_a, release_b = prepare_release_dirs(state)
    state.record("release_dirs", {"release_a": str(release_a), "release_b": str(release_b)})
    cbth_home = state.test_root / "cbth-home"
    preflight_upgrade_command(state.args, release_a, release_b, cbth_home, state.prefix, state.repo_root)
    run_cbth_service_upgrade_smoke(state)

    developer = parse_token_file(token_file)
    state.developer_token = developer
    resolve_developer_email(state.args, developer)
    bot = parse_bot_env_file(bot_env_file)
    state.bot_credentials = bot
    state.record("developer_token", developer.redacted_summary)
    state.record("bot_credentials", {"email": bot.email, "token_len": len(bot.token)})

    dev_api = WebexApi(developer.bearer)
    bot_api = WebexApi(bot.token)
    developer_me = dev_api.get_me()
    bot_me = bot_api.get_me()
    ensure_person_email_matches(developer_me, developer.email, "developer")
    ensure_person_email_matches(bot_me, bot.email, "bot")
    bot_id = json_pointer_str(bot_me, "/id")
    bot_email = bot.email.lower()
    bot_display_name = bot.display_name or bot_me.get("displayName") or bot_email
    state.bot_credentials = BotCredentials(bot.token, bot_email, bot_display_name)
    state.record(
        "people_me",
        {
            "developer_email": developer.email,
            "bot_email": bot_email,
            "bot_display_name": bot_display_name,
        },
    )

    control_title = f"{state.prefix} control"
    data_title = f"{state.prefix} data"
    control_room = dev_api.create_room(control_title)
    state.add_room("control", json_pointer_str(control_room, "/id"), control_title)
    data_room = dev_api.create_room(data_title)
    state.add_room("data", json_pointer_str(data_room, "/id"), data_title)
    for room_id in state.created_rooms.values():
        dev_api.create_membership(room_id, bot_email)

    plugin_release_id = state.args.release_a_id
    plugin_instance_id = f"w7-{state.prefix.lower().replace('_', '-').replace(' ', '-')}"
    state.record(
        "plugin_identity",
        {
            "plugin_instance_id": plugin_instance_id,
            "release_a_id": state.args.release_a_id,
            "release_b_id": state.args.release_b_id,
        },
    )
    config_path, env_path, ingress_socket, lifecycle_socket = write_wxcd_files(
        state,
        BotCredentials(bot.token, bot_email, bot_display_name),
        state.created_rooms["control"],
        state.created_rooms["data"],
        release_a,
        plugin_instance_id,
        plugin_release_id,
    )
    write_cbth_registry(
        state,
        cbth_home,
        release_a,
        config_path,
        env_path,
        plugin_instance_id,
        plugin_release_id,
    )

    start_process(
        state,
        "cbth-service",
        [state.args.cbth_bin, "--home", str(cbth_home), "service", "run"],
    )
    wait_until(
        "worker health",
        state.args.startup_timeout_seconds,
        lambda: health_check(ingress_socket).get("healthy") is True,
    )
    state.record("worker_health_before_upgrade", health_check(ingress_socket))
    state.record("worker_active_before_upgrade", active_check(ingress_socket))

    thread_id = state.args.thread_id or create_local_codex_thread(state)
    state.thread_id = thread_id
    state.record("thread_id", thread_id)

    send_control_command(
        dev_api,
        state.created_rooms["control"],
        "/help",
        bot_id,
        bot_display_name,
    )
    wait_for_bot_message(dev_api, state.created_rooms["control"], bot_email, ["Control room commands:"], 60)
    send_control_command(
        dev_api,
        state.created_rooms["control"],
        "list local",
        bot_id,
        bot_display_name,
    )
    wait_for_bot_message(dev_api, state.created_rooms["control"], bot_email, [thread_id], 90)
    send_control_command(
        dev_api,
        state.created_rooms["control"],
        f"resume local {thread_id}",
        bot_id,
        bot_display_name,
    )
    attach_reply = wait_for_bot_message(
        dev_api,
        state.created_rooms["control"],
        bot_email,
        ["Attached local thread", thread_id],
        180,
    )
    session_id, session_title = parse_attached_session_reply(attach_reply)
    state.session_id = session_id
    state.record("session_id", session_id)
    session_room_id = wait_for_room_title(dev_api, session_title, 60)
    state.session_room_id = session_room_id
    state.add_room("session", session_room_id, session_title)
    assert_membership(dev_api, session_room_id, developer.email)
    assert_membership(dev_api, session_room_id, bot_email)
    wait_for_bot_message(dev_api, session_room_id, bot_email, ["Imported local Codex history", thread_id], 90)

    send_control_command(dev_api, session_room_id, "/history", bot_id, bot_display_name)
    wait_for_bot_message(dev_api, session_room_id, bot_email, history_page_needles(1, thread_id), 120)
    send_control_command(dev_api, session_room_id, "/history page 2", bot_id, bot_display_name)
    allow_missing_page_two = state.args.thread_id is not None or state.args.history_turns <= 10
    wait_for_bot_message_any(
        dev_api,
        session_room_id,
        bot_email,
        history_page_response_alternatives(2, thread_id, allow_missing_page_two),
        120,
    )

    marker = f"{state.prefix}-session-turn-{secrets.token_hex(3)}"
    send_control_command(
        dev_api,
        session_room_id,
        f"Reply with exactly: {marker}",
        bot_id,
        bot_display_name,
    )
    wait_for_bot_message(dev_api, session_room_id, bot_email, [marker], 240)
    state.record("session_turn_marker", marker)

    run_delivery_smoke(state, ingress_socket, thread_id, session_id, "before_upgrade")
    new_ingress_socket = worker_ingress_socket_path(
        plugin_instance_id,
        state.args.release_b_id,
        state.test_root / "wxcd-state",
    )
    new_lifecycle_socket = lifecycle_socket_path(
        plugin_instance_id,
        state.args.release_b_id,
        cbth_home / "plugins" / PLUGIN_NAME,
    )
    webex_release_upgraded = run_upgrade_smoke_or_block(
        state,
        release_a,
        release_b,
        cbth_home,
        lifecycle_socket,
        ingress_socket,
        new_lifecycle_socket,
        new_ingress_socket,
    )
    active_ingress_socket = new_ingress_socket if webex_release_upgraded else ingress_socket

    state.record("worker_health_after_upgrade_smoke", health_check(active_ingress_socket))
    post_upgrade_marker = f"{state.prefix}-post-upgrade-smoke-turn-{secrets.token_hex(3)}"
    send_control_command(
        dev_api,
        session_room_id,
        f"Reply with exactly: {post_upgrade_marker}",
        bot_id,
        bot_display_name,
    )
    wait_for_bot_message(dev_api, session_room_id, bot_email, [post_upgrade_marker], 240)
    state.record("post_upgrade_smoke_session_turn_marker", post_upgrade_marker)
    run_delivery_smoke(state, active_ingress_socket, thread_id, session_id, "after_upgrade_smoke")
    state.record("result", "passed")


def create_local_codex_thread(state: RunState) -> str:
    app_server = CodexAppServer(state.args.codex_bin, state.logs_dir / "codex-app-server.err.log")
    state.codex_app_server = app_server
    app_server.initialize()
    thread_id = app_server.create_thread(
        state.repo_root,
        "You are validating wxcd W7 live E2E. Reply exactly when asked.",
    )
    state.thread_id = thread_id
    state.record("thread_id", thread_id)
    for index in range(1, state.args.history_turns + 1):
        marker = f"{state.prefix} turn {index:02d} ok"
        app_server.turn_start_and_wait(
            thread_id,
            state.repo_root,
            f"{state.prefix} turn {index:02d}. Reply with exactly: {marker}",
            state.args.turn_timeout_seconds,
        )
    return thread_id


def send_control_command(
    api: WebexApi,
    room_id: str,
    text: str,
    bot_id: str,
    bot_display_name: str,
) -> None:
    markdown = f"<@personId:{bot_id}|{bot_display_name}> {text}"
    api.create_message(room_id, text, markdown)


def wait_for_bot_message(
    api: WebexApi,
    room_id: str,
    bot_email: str,
    needles: list[str],
    timeout_seconds: int,
) -> dict[str, Any]:
    return wait_for_bot_message_any(api, room_id, bot_email, [needles], timeout_seconds)


def wait_for_bot_message_any(
    api: WebexApi,
    room_id: str,
    bot_email: str,
    alternatives: list[list[str]],
    timeout_seconds: int,
) -> dict[str, Any]:
    def probe() -> dict[str, Any] | None:
        for message in api.list_messages(room_id, 50):
            author = str(message.get("personEmail", "")).lower()
            text = str(message.get("text") or message.get("markdown") or "")
            if author == bot_email.lower() and any(
                all(needle in text for needle in needles) for needles in alternatives
            ):
                return message
        return None

    return wait_until(f"bot message matching {alternatives}", timeout_seconds, probe)


def parse_attached_session_reply(message: dict[str, Any]) -> tuple[str, str]:
    text = str(message.get("text") or message.get("markdown") or "")
    match = re.search(r"session `([^`]+)` in room `([^`]+)`", text)
    if not match:
        raise HarnessError(f"failed to parse attached session reply: {text}")
    return match.group(1), match.group(2)


def wait_for_room_title(api: WebexApi, title: str, timeout_seconds: int) -> str:
    def probe() -> str | None:
        for room in api.list_rooms(100):
            if room.get("title") == title:
                return str(room["id"])
        return None

    return wait_until(f"room title {title}", timeout_seconds, probe)


def assert_membership(api: WebexApi, room_id: str, email: str) -> dict[str, Any]:
    memberships = api.list_memberships(room_id)
    for membership in memberships:
        if str(membership.get("personEmail", "")).lower() == email.lower():
            return membership
    raise HarnessError(f"{email} is not a member of room {room_id}")


def run_delivery_smoke(
    state: RunState,
    ingress_socket: Path,
    thread_id: str,
    session_id: str,
    phase: str,
) -> None:
    event_id = f"w7-delivery-{phase}-{secrets.token_hex(8)}"
    envelope = {
        "kind": "async_notification",
        "event_id": event_id,
        "session_id": session_id,
        "thread_id": thread_id,
        "summary": f"{state.prefix} delivery smoke",
        "payload": {"marker": event_id, "prefix": state.prefix},
        "created": dt.datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    ack = send_worker_ingress(ingress_socket, envelope, timeout_seconds=120)
    if not ack.get("ok"):
        raise HarnessError(f"delivery smoke was rejected: {ack}")
    delivery_smoke = state.manifest.setdefault("delivery_smoke", {})
    delivery_smoke[phase] = {"event_id": event_id, "ack": ack, "ingress_socket": str(ingress_socket)}
    write_private_json(state.manifest_path, state.manifest)


def run_upgrade_smoke_or_block(
    state: RunState,
    release_a: Path,
    release_b: Path,
    cbth_home: Path,
    lifecycle_socket: Path,
    ingress_socket: Path,
    new_lifecycle_socket: Path,
    new_ingress_socket: Path,
) -> bool:
    command_template = upgrade_command_template(state.args)
    if not command_template:
        state.record(
            "webex_release_upgrade",
            {
                "status": "skipped",
                "reason": "no Webex-specific release upgrade command configured",
                "cbth_service_upgrade_smoke": "passed",
            },
        )
        return False
    command = expand_upgrade_command(
        command_template,
        release_a,
        release_b,
        state.args.release_a_id,
        state.args.release_b_id,
        cbth_home,
        state.prefix,
    )
    before = active_check(ingress_socket)
    try:
        subprocess.run(
            command,
            cwd=state.repo_root,
            stdin=subprocess.DEVNULL,
            check=True,
            env=upgrade_command_env(cbth_home),
            timeout=state.args.upgrade_timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise HarnessError(
            f"Webex release upgrade timed out after {state.args.upgrade_timeout_seconds}s: {redact_command(command)}"
        ) from error
    except (OSError, subprocess.CalledProcessError) as error:
        raise HarnessError(f"Webex release upgrade failed: {redact_command(command)}") from error
    wait_until(
        "old worker ingress socket to stop accepting connections",
        state.args.startup_timeout_seconds,
        lambda: not socket_accepts_connections(ingress_socket),
    )
    wait_until(
        "old lifecycle socket to stop accepting connections",
        state.args.startup_timeout_seconds,
        lambda: not socket_accepts_connections(lifecycle_socket),
    )
    after = wait_until(
        "worker health after cbth upgrade",
        state.args.startup_timeout_seconds,
        lambda: health_check(new_ingress_socket).get("healthy") is True,
    )
    state.record(
        "webex_release_upgrade",
        {
            "status": "passed",
            "command": redact_command(command),
            "old_lifecycle_socket": str(lifecycle_socket),
            "old_ingress_socket_inactive": True,
            "old_lifecycle_socket_inactive": True,
            "new_lifecycle_socket": str(new_lifecycle_socket),
            "active_before": before,
            "health_after": after,
        },
    )
    return True


def expand_upgrade_command(
    template: str,
    release_a: Path,
    release_b: Path,
    release_a_id: str,
    release_b_id: str,
    cbth_home: Path,
    prefix: str,
) -> list[str]:
    values = {
        "{plugin}": PLUGIN_NAME,
        "{release_a}": str(release_a),
        "{release_b}": str(release_b),
        "{release_a_id}": release_a_id,
        "{release_b_id}": release_b_id,
        "{cbth_home}": str(cbth_home),
        "{prefix}": prefix,
    }
    parts = shlex.split(template)
    expanded = []
    for part in parts:
        for placeholder, value in values.items():
            part = part.replace(placeholder, value)
        expanded.append(part)
    if not expanded:
        raise HarnessError("empty Webex release upgrade command")
    return expanded


SENSITIVE_COMMAND_TERMS = ("token", "bearer")
BARE_SENSITIVE_COMMAND_KEYS = frozenset(SENSITIVE_COMMAND_TERMS)


def redact_command(command: Iterable[str]) -> list[str]:
    redacted = []
    redact_next = False
    for item in command:
        if redact_next:
            redacted.append("<redacted>")
            redact_next = False
            continue
        key, separator, _value = item.partition("=")
        key_lower = key.lower()
        if separator and any(term in key_lower for term in SENSITIVE_COMMAND_TERMS):
            redacted.append(f"{key}=<redacted>")
        elif item.startswith("-") and any(term in item.lower() for term in SENSITIVE_COMMAND_TERMS):
            redacted.append(item)
            redact_next = True
        elif item.lower() in BARE_SENSITIVE_COMMAND_KEYS:
            redacted.append(item)
            redact_next = True
        elif any(term in item.lower() for term in SENSITIVE_COMMAND_TERMS):
            redacted.append("<redacted>")
        else:
            redacted.append(item)
    return redacted


def cleanup_owned_test_root(state: RunState) -> None:
    original_manifest_path = state.manifest_path
    preserved_manifest_path = state.test_root.with_name(f"{state.test_root.name}-{RUN_MANIFEST}")
    try:
        write_private_json(preserved_manifest_path, state.manifest)
        shutil.rmtree(state.test_root)
    except Exception as error:  # noqa: BLE001
        preserved_manifest_path.unlink(missing_ok=True)
        state.manifest_path = original_manifest_path
        state.record_cleanup_error("cleanup_error_test_root", error)
        return
    if state.test_root.exists():
        preserved_manifest_path.unlink(missing_ok=True)
        state.manifest_path = original_manifest_path
        state.record_cleanup_error("cleanup_error_test_root", "test root still exists after cleanup")
        return
    state.manifest_path = preserved_manifest_path
    state.manifest["success_manifest_preserved"] = True
    state.manifest["deleted_test_root"] = str(state.test_root)
    write_private_json(state.manifest_path, state.manifest)


def cleanup_live(
    state: RunState,
    developer: DeveloperToken | None = None,
    bot: BotCredentials | None = None,
) -> bool:
    if state.codex_app_server and state.thread_id:
        try:
            state.codex_app_server.archive(state.thread_id)
            state.record("codex_thread_archived", True)
        except Exception as error:  # noqa: BLE001
            state.record_cleanup_error("codex_thread_archive_error", error)
    if state.codex_app_server:
        state.codex_app_server.close()
    stop_processes(state)
    developer_token = developer or state.developer_token
    developer_api = WebexApi(developer_token.bearer) if developer_token is not None else None
    bot_credentials = bot or state.bot_credentials
    bot_api = WebexApi(bot_credentials.token) if bot_credentials is not None else None
    if developer_api is not None or bot_api is not None:
        for label, room_id in list(state.created_rooms.items()):
            room_record = state.manifest.get("rooms", {}).get(label, {})
            title = str(room_record.get("title") or "")
            if not title.startswith(state.prefix):
                state.record(f"cleanup_skip_{label}", "room title does not match prefix")
                state.cleanup_failed = True
                continue
            api = bot_api if label == "session" and bot_api is not None else developer_api
            if api is None:
                state.record(f"cleanup_skip_{label}", "no token available for room owner")
                state.cleanup_failed = True
                continue
            try:
                api.delete_room(room_id)
                state.mark_room_deleted(label)
            except Exception as error:  # noqa: BLE001
                state.record_cleanup_error(f"cleanup_error_{label}", error)
        prefix_scan_known: set[str] = set()
        if developer_api is not None:
            prefix_scan_known.update(cleanup_untracked_prefix_rooms(state, developer_api, "developer"))
        if bot_api is not None:
            cleanup_untracked_prefix_rooms(state, bot_api, "bot", prefix_scan_known)
    elif state.created_rooms:
        for label in state.created_rooms:
            state.record(f"cleanup_skip_{label}", "no token available for room owner")
        state.cleanup_failed = True
    if (
        state.owns_test_root
        and not state.args.keep_root
        and state.manifest.get("result") == "passed"
        and not state.cleanup_failed
    ):
        cleanup_owned_test_root(state)
    return not state.cleanup_failed


def cleanup_untracked_prefix_rooms(
    state: RunState,
    api: Any,
    owner_label: str = "developer",
    additional_known_room_ids: set[str] | None = None,
) -> set[str]:
    if not SAFE_PREFIX_SCAN_RE.fullmatch(state.prefix):
        state.record(
            f"cleanup_prefix_scan_skipped_{owner_label}",
            {
                "reason": "prefix is not a generated W7 E2E prefix",
                "prefix": state.prefix,
                "owner": owner_label,
            },
        )
        return set()
    known_room_ids = set(state.created_rooms.values())
    if additional_known_room_ids:
        known_room_ids.update(additional_known_room_ids)
    deleted: list[dict[str, str]] = []
    try:
        rooms = api.list_rooms(100)
    except Exception as error:  # noqa: BLE001
        state.record_cleanup_error(f"cleanup_prefix_scan_error_{owner_label}", error)
        return set()
    for room in rooms:
        room_id = str(room.get("id") or "")
        title = str(room.get("title") or "")
        if not room_id or room_id in known_room_ids or not title.startswith(state.prefix):
            continue
        try:
            api.delete_room(room_id)
            deleted.append({"id": room_id, "title": title})
        except Exception as error:  # noqa: BLE001
            state.record_cleanup_error(f"cleanup_error_prefix_scan_{owner_label}_{fnv1a_hex(room_id)}", error)
    if deleted:
        state.record(f"cleanup_prefix_scan_deleted_rooms_{owner_label}", deleted)
    return {room["id"] for room in deleted}


def run_dry_run(args: argparse.Namespace, repo_root: Path) -> None:
    ensure_opt_in(False)
    token_file = Path(args.token_file).expanduser()
    bot_env_file = Path(args.bot_env_file).expanduser()
    prefix = args.prefix or default_prefix()
    dry_run = {
        "mode": "dry_run",
        "prefix": prefix,
        "repo_root": str(repo_root),
        "token_file": str(token_file),
        "bot_env_file": str(bot_env_file),
        "cbth_c8_merge_commit": CBTH_C8_MERGE_COMMIT,
        "cbth_c8_pr": CBTH_C8_PR_URL,
        "cbth_service_upgrade_smoke_required": True,
        "webex_release_upgrade_command_configured": bool(upgrade_command_template(args)),
        "webex_release_upgrade_check_command_configured": bool(upgrade_check_command_template(args)),
        "live_requires": [
            "WXCD_LIVE_E2E=1",
            "cbth C8 service upgrade-smoke support",
            "untracked developer token file",
            "untracked bot env file",
            "real Webex network access",
            "cbth service plugin mode",
        ],
    }
    print(json.dumps(dry_run, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="Run the real Webex/cbth smoke.")
    parser.add_argument("--token-file", default="token.txt", help="Developer token file.")
    parser.add_argument("--bot-env-file", default=".env", help="Bot credential env file.")
    parser.add_argument("--repo-root", default=None, help="Repository root.")
    parser.add_argument("--test-root", default=None, help="Task-scoped root for live artifacts.")
    parser.add_argument("--prefix", default=None, help="Unique Webex room title prefix.")
    parser.add_argument("--developer-email", default=None, help="Expected developer email override.")
    parser.add_argument("--cbth-bin", default=os.environ.get("CBTH_BIN", "cbth"), help="cbth binary.")
    parser.add_argument("--codex-bin", default=os.environ.get("WXCD_CODEX_PATH", "codex"), help="codex binary.")
    parser.add_argument("--node-bin", default=os.environ.get("WXCD_NODE_PATH", "node"), help="node binary.")
    parser.add_argument("--release-a", default=None, help="Existing active release directory.")
    parser.add_argument("--release-b", default=None, help="Existing upgrade release directory.")
    parser.add_argument("--release-a-id", default="w7-a", help="Active plugin release id.")
    parser.add_argument("--release-b-id", default="w7-b", help="Upgrade plugin release id.")
    parser.add_argument("--no-build-release", action="store_true", help="Require explicit release dirs.")
    parser.add_argument("--thread-id", default=None, help="Use an existing local-only Codex thread.")
    parser.add_argument("--history-turns", type=int, default=11, help="Local thread history turns to create.")
    parser.add_argument("--turn-timeout-seconds", type=int, default=180, help="Per-turn local Codex timeout.")
    parser.add_argument("--startup-timeout-seconds", type=int, default=90, help="Service startup timeout.")
    parser.add_argument(
        "--cbth-service-upgrade-smoke-root",
        default=None,
        help="Task-scoped root for cbth C8 service upgrade-smoke artifacts.",
    )
    parser.add_argument(
        "--cbth-service-upgrade-smoke-timeout-seconds",
        type=int,
        default=30,
        help="cbth C8 service upgrade-smoke timeout.",
    )
    parser.add_argument("--upgrade-timeout-seconds", type=int, default=180, help="Optional Webex release upgrade command timeout.")
    parser.add_argument("--cbth-upgrade-command", default=None, help="Optional Webex release upgrade command template.")
    parser.add_argument("--cbth-upgrade-check-command", default=None, help="Side-effect-free Webex release upgrade check command template.")
    parser.add_argument("--keep-root", action="store_true", help="Keep test root after success.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve() if args.repo_root else Path(__file__).resolve().parents[1]
    if not args.live:
        run_dry_run(args, repo_root)
        return 0
    try:
        ensure_opt_in(True)
        preflight_cbth_service_upgrade_smoke(args, repo_root)
        preflight_upgrade_command(args, cwd=repo_root)
    except BlockedError as error:
        print(json.dumps({"status": "blocked", "reason": str(error)}, sort_keys=True))
        return 78

    owns_test_root = not args.test_root
    test_root = Path(args.test_root).expanduser().resolve() if args.test_root else Path(tempfile.mkdtemp(prefix="wxcd-w7-live-"))
    try:
        validate_test_root(test_root, repo_root, explicit=bool(args.test_root))
    except BlockedError as error:
        print(json.dumps({"status": "blocked", "reason": str(error)}, sort_keys=True))
        return 78
    prefix = args.prefix or default_prefix()
    logs_dir = test_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    state = RunState(
        args=args,
        repo_root=repo_root,
        test_root=test_root,
        prefix=prefix,
        logs_dir=logs_dir,
        manifest_path=test_root / RUN_MANIFEST,
        owns_test_root=owns_test_root,
    )
    state.record(
        "run",
        {
            "prefix": prefix,
            "started_at": dt.datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "test_root": str(test_root),
        },
    )
    try:
        run_live(state)
        cleanup_ok = cleanup_live(state)
        if not cleanup_ok:
            state.record("result", "failed")
            print(json.dumps({"status": "failed", "reason": "cleanup failed", "manifest": str(state.manifest_path)}, sort_keys=True))
            return 1
        print(json.dumps({"status": "passed", "manifest": str(state.manifest_path)}, sort_keys=True))
        return 0
    except BlockedError as error:
        state.record("result", "blocked")
        cleanup_live(state)
        print(json.dumps({"status": "blocked", "reason": str(error), "manifest": str(state.manifest_path)}, sort_keys=True))
        return 78
    except Exception as error:  # noqa: BLE001
        state.record("result", "failed")
        state.record("error", str(error))
        cleanup_live(state)
        print(json.dumps({"status": "failed", "reason": str(error), "manifest": str(state.manifest_path)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
