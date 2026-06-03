---
id: 20260603-w8-c9-upgrade-hook-b2959bc
title: W8 C9 Upgrade Hook
status: completed
created: 2026-06-03
updated: 2026-06-03
branch: codex/w8-c9-upgrade-hook
pr: https://github.com/JoeyTeng/codex-webex-connector/pull/17
supersedes: []
superseded_by:
---

# W8 C9 Upgrade Hook

## Summary
- W8 connects the W7 live Webex release A/B upgrade hook to the merged cbth C9 `plugin upgrade` operator command from PR #103 / merge commit `87ebc8e3a39558daa5441c40d9bd8d7cffb3ca06`.
- The default command template is `{cbth_bin} --home "{cbth_home}" plugin upgrade {plugin} --release-id "{release_b_id}" --release-dir "{release_b}" --manifest-path "{release_b}/plugin/manifest.json" --json`.
- `{cbth_bin}` expands from the resolved `--cbth-bin` executable so W9 can point the harness at a cbth binary that includes C8 and C9 without relying on `PATH`.

## Current State
- `scripts/w7_live_upgrade_e2e.py` now defaults the Webex release upgrade hook to the C9 command template while preserving `--cbth-upgrade-command` / `WXCD_E2E_CBTH_UPGRADE_CMD` as overrides.
- The harness infers a side-effect-free check from the C9 command shape as `{cbth_bin} --home "{cbth_home}" plugin upgrade --help`; custom non-`plugin upgrade` commands still need an explicit safe check command.
- Release-dir validation now reads `plugin/manifest.json` and fails closed unless it is a JSON object with `enabled=true`, matching C9 `PluginManifest` requirements.
- `plugin/manifest.json` now includes `enabled=true`, and the existing packaging metadata test asserts that field.
- The harness remains Webex-neutral: it templates and invokes cbth C9, then verifies Webex product behavior and the task-scoped registry state after cbth service-side promote; it does not copy cbth release-manager logic or edit cbth registry directly.

## Next Steps
- W9 should run the real live smoke with Webex credentials and a `--cbth-bin` built from or newer than C8 `ee76fdd5937ca57e8156631c32509be12d3cf4c2` and C9 `87ebc8e3a39558daa5441c40d9bd8d7cffb3ca06`.
- Do not run W9 live smoke from W8; W8 only lands the harness, runbook, tests, and project-record wiring.

## Evidence
- C9 dependency:
  - cbth PR #103
  - merge commit `87ebc8e3a39558daa5441c40d9bd8d7cffb3ca06`
- PR:
  - https://github.com/JoeyTeng/codex-webex-connector/pull/17
- Focused validation:
  - `python3 -B -m py_compile scripts/w7_live_upgrade_e2e.py scripts/tests/test_w7_live_upgrade_e2e.py`
  - `python3 -B -m unittest scripts.tests.test_w7_live_upgrade_e2e`
  - `python3 -B scripts/w7_live_upgrade_e2e.py`
  - `bash scripts/smoke-test.sh`
  - `cargo fmt --check`
  - `git diff --check`
  - `python3 /Users/hoteng/.codex/personal-sync/overlays/private/releases/7ead1a9818db266b4d3768514cc817270d9aeaf7/personal_codex/skills/project-journal/scripts/project_journal.py validate --repo .`
