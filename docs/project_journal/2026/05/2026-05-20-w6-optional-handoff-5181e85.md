---
id: 20260520-w6-optional-handoff-5181e85
title: W6 Optional Handoff
status: completed
created: 2026-05-20
updated: 2026-05-20
branch: codex/w6-optional-handoff
pr: https://github.com/JoeyTeng/codex-webex-connector/pull/15
supersedes: []
superseded_by:
---

# W6 Optional Handoff

## Summary
- W6 adds optional `plugin.handoff_export` and `plugin.handoff_import` support for the Webex connector lifecycle socket.
- Handoff remains an optional acceleration after W5 quiesce/drain. The conservative W5 lifecycle path remains correct without handoff.
- Export captures the durable bridge snapshot, recent Webex event-id cursor, in-flight session/approval summary, and sidecar deferred/drain metadata.
- Import validates plugin instance and installation identity, applies the local mirror/cursor without external Webex or Codex side effects, materializes missing deferred sidecar ingress records, and persists the imported mirror in plugin home.

## Current State
- `plugin-handoff-v1` is advertised by the manifest, supervisor hello, and worker doctor hello path.
- The worker rejects handoff export unless Webex work admission is quiesced and runtime drain is empty.
- The worker rejects handoff import unless it is in pre-active quiesced admission and the snapshot matches the same plugin instance and installation identity.
- Imported state does not advance Data Space, submit delivery, emit Webex REST calls, or start Webex listeners before promote/unquiesce.

## Remaining Dependencies
- C8/W7 live upgrade smoke still needs real Webex credentials and a cbth service/plugin upgrade path.

## Evidence
- Dependency baseline: W5 merge commit `5181e852933320182a431876875cb8149b3ed90c`.
- Focused local validation:
  - `cargo test -p wxcd-worker handoff_`
  - `cargo test -p wxcd-cbth-rpc handoff`
  - `cargo test -p wxcd-supervisor plugin_hello_advertises_lifecycle_handoff_capability`
- Full local validation:
  - `cargo fmt --check`
  - `git diff --check`
  - `cargo test`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `pnpm --dir sidecars/webex-ws-sidecar check`
  - `node --check sidecars/webex-ws-sidecar/index.cjs`
  - `bash scripts/smoke-test.sh`
  - `uv run --no-project /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
- Helper-backed local review found and W6 fixed two import hardening issues before commit:
  - `plugin.handoff_import` now requires pre-active quiesced admission rather than any quiesced worker.
  - `plugin.handoff_import` no longer claims unowned legacy sessions that lack current installation evidence.
