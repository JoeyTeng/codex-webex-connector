---
id: 20260525-w7-live-upgrade-e2e-c2f3fe5
title: W7 Live Upgrade E2E Harness
status: active
created: 2026-05-25
updated: 2026-05-25
branch: codex/w7-live-upgrade-e2e
pr:
supersedes: []
superseded_by:
---

# W7 Live Upgrade E2E Harness

## Summary
- W7 adds an opt-in live E2E harness for real Webex credentials, isolated rooms, cbth C8 `service upgrade-smoke`, task-scoped cbth service/plugin mode, optional Webex release upgrade command smoke, delivery smoke, cleanup, and diagnostics.
- The harness is Webex-specific and keeps cbth generic lifecycle/release-manager ownership outside this repository.
- Live mode requires `WXCD_LIVE_E2E=1` and fails closed with exit code `78` when the selected `cbth` binary does not expose the C8 `service upgrade-smoke` command from PR #99 / merge commit `ee76fdd5937ca57e8156631c32509be12d3cf4c2`.

## Current State
- `scripts/w7_live_upgrade_e2e.py` defaults to dry-run and prints only non-secret execution prerequisites.
- Live mode preflights `WXCD_LIVE_E2E=1` and `cbth service upgrade-smoke --help` before reading credentials, creating Webex rooms, or starting the Webex connector.
- Live mode runs cbth C8 `service upgrade-smoke` in a task-scoped smoke root with a scrubbed child environment before reading Webex credentials, verifies `ok=true`, `system_mutation_performed=false`, `release_upgrade.handoff_performed=true`, and the C8 conservative release event contract as an ordered event subsequence, and records PR #99 / merge commit `ee76fdd5937ca57e8156631c32509be12d3cf4c2` in `manifest.json`.
- Live mode reads untracked developer and bot credential files, validates Webex `/people/me` owner emails, creates prefixed temporary rooms with bounded retry/backoff for Webex room propagation on membership/message writes, starts `cbth service run` with a task-scoped registry, and drives the existing Webex session smoke path.
- Task-scoped private files are written through `0600` temporary files so bot tokens are not briefly exposed before chmod.
- The default build/stage path ensures Webex sidecar runtime dependencies before staging release directories; explicit release directories must include those dependencies.
- The cbth safe-smoke, cbth service, supervisor, optional upgrade, and sidecar child environments are stripped of parent `WEBEX_*`, `WXCD_*`, and `CBTH_*` variables before task-scoped values are injected.
- Delivery smoke injects a Webex `async_notification` through the worker ingress socket so the existing supervisor broker forwards W4 `delivery.enqueue`.
- Optional Webex release A/B upgrade is delegated to `WXCD_E2E_CBTH_UPGRADE_CMD` or `--cbth-upgrade-command`; custom commands must provide `WXCD_E2E_CBTH_UPGRADE_CHECK_CMD` / `--cbth-upgrade-check-command`, and the script expands release/cbth placeholders, resolves repo-relative executables against the configured repo root, runs the external command with closed stdin, private stdout/stderr logging, and a bounded timeout, validates the cbth registry now points to release B's supervisor, release dir, release id, and manifest path, and then verifies old release socket shutdown, the post-upgrade release-scoped worker ingress, a real Webex session turn, and delivery smoke before passing. If omitted, W7 records `webex_release_upgrade.status=skipped` and relies on the mandatory C8 safe harness for generic upgrade ordering.
- Cleanup records all created rooms/processes in `manifest.json`, deletes only rooms whose titles match the run prefix, uses the bot token for bot-created session room deletion, follows Webex room pagination for session-room lookup and prefix-scanned untracked temporary rooms, stops child processes, archives the dedicated local Codex thread when possible, preserves the test root for failed, blocked, cleanup-error, or explicit `--test-root` runs, preserves a sibling `*-manifest.json` audit artifact before deleting a clean default success root, and rejects repo-internal explicit test roots unless they are under ignored `.codex-tmp/`.

## Next Steps
- Run the W7 live smoke with real Webex credentials and a `cbth` binary built from or newer than `ee76fdd5937ca57e8156631c32509be12d3cf4c2`.
- If a future cbth API exposes a product-specific Webex connector release upgrade command, wire it through the existing optional command hook; do not move generic upgrade logic into webex-connector.

## Evidence
- C8 dependency:
  - `git show -s --format=%H%x09%D%x09%s ee76fdd5937ca57e8156631c32509be12d3cf4c2` in `codex-background-task-handler`
  - `cargo run --quiet -- service upgrade-smoke --allow-task-scoped-mutation --smoke-root /private/tmp/cbth-c8-service-upgrade-smoke-w7-check --startup-timeout-ms 10000 --json` from an `ee76fdd5937ca57e8156631c32509be12d3cf4c2` source snapshot
- Dry-run:
  - `python3 scripts/w7_live_upgrade_e2e.py`
- Compatibility dry-run:
  - `/usr/bin/python3 -B scripts/w7_live_upgrade_e2e.py`
- Focused validation:
  - `python3 -B -m py_compile scripts/w7_live_upgrade_e2e.py scripts/tests/test_w7_live_upgrade_e2e.py`
  - `python3 -B -m unittest scripts.tests.test_w7_live_upgrade_e2e`
  - `/usr/bin/python3 -B -m unittest scripts.tests.test_w7_live_upgrade_e2e`
  - `env WXCD_LIVE_E2E=1 python3 -B scripts/w7_live_upgrade_e2e.py --live --token-file /tmp/definitely-missing-w7-token --cbth-bin /tmp/definitely-missing-cbth`
- Local delivery gates:
  - `bash -n scripts/smoke-test.sh`
  - `shellcheck scripts/smoke-test.sh`
  - `git diff --check`
  - `uv run --no-project /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
  - `cargo fmt --check`
  - `bash scripts/smoke-test.sh`
  - `cargo clippy --workspace --all-targets -- -D warnings`
- Helper-backed offline review:
  - `isolated_review stateful wait --state-dir .codex-tmp/isolated-review-gx7ocgie` produced `LGTM`
