---
id: 20260505-webex-bridge-history-05ee1a8
title: Webex Bridge Completed History
status: completed
created: 2026-05-05
updated: 2026-05-06
branch: master
pr: https://github.com/JoeyTeng/codex-webex-connector/pull/1
supersedes: []
superseded_by:
---

# Webex Bridge Completed History

## Summary
- The macOS-first `wxcd` bridge was implemented, locally validated, deployed, documented, and live-E2E tested.
- Completed work from the former top-level trackers is archived here; the exact pre-migration tracker text is preserved in `docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`.

## Completed Work
- Implemented the Rust workspace, shared config, Webex REST client, event log support, worker, supervisor, and Node websocket sidecar.
- Deployed the bridge under `~/Library/Application Support/codex-webex-connector` with `launchd` managing `wxcd-supervisor`, `wxcd-worker`, and the Webex websocket sidecar.
- Validated the critical live path: Webex Mercury websocket delivery, worker health socket, new Codex session execution, and final answer capture.
- Moved Data Space startup replay to the bot-owner 1:1 direct room after validating that group-room history replay was unavailable with the current bot token.
- Added control-room discovery and attachment for local-only Codex threads: `list local`, `list local page <n>`, `list all`, and `resume local <thread_id>`.
- Added `attach <session_id>` so users can rejoin existing bridge session rooms without local shell access.
- Added session-room history browsing with automatic newest-page import for `resume local <thread_id>`, `/history`, and `/history page <n>`.
- Added a Mercury watchdog so unrecoverable Webex realtime disconnects force sidecar restart instead of leaving the bridge falsely healthy.
- Fixed ingress deduplication to use unique Webex webhook/message ids instead of non-unique SDK event labels.
- Hardened reliability in release `2026-05-03T17-25-10`: UTF-8-safe checkpoints, visible membership failures, transient `Invalid roomId` retry, failed-room/thread cleanup, paginated Data Space replay, bounded ingress dedupe, and strict workspace clippy.
- Documented the isolated live Webex E2E runbook in `docs/WEBEX_E2E_TEST_PLAN.md`.
- Passed isolated live Webex E2E on release `2026-05-03T20-26-06` with prefix `WXCD-E2E-20260503-c46m0szu`, dedicated thread `019def4e-faf9-7bd3-a0cc-0fedbe0224c1`, and session `ses_20260503_3nfbb665w7`.
- Fixed session-room mention-prefix slash command handling using bot display-name and bot-email local-part matching.
- Added session recovery cleanup in release `2026-05-03T23-53-10`: failure metadata, startup reconcile categories, `diagnose`, `cleanup failed`, and explicit destructive `purge archived <session_id> confirm`.
- Documented user-facing commands in `docs/COMMANDS.md`.

## Validation History
- Local Codex probe accepted `initialize`, `thread/start`, and `turn/start` over `codex app-server --listen stdio://`.
- Mercury self-test observed a bot-posted control-room message through `messages.listen()`.
- Local execution proof: session `ses_20260408_w6sw4yrhvq` completed with final answer `master`.
- Deployment proof: `launchctl print gui/501/com.example.wxcd.supervisor` showed the agent running, and `/tmp/wxcd.sock` returned `{"ok":true,"healthy":true,"detail":null}`.
- Direct-room replay proof: `GET /v1/messages?roomId=<direct_room_id>` returned user-originated `/help` and bot-authored `WXCD/V1 EVENT ...` frames; after moving aside `bridge-state.json`, restarting produced fresh `session_updated` Data Space events at `2026-04-09T06:29:02Z` through `2026-04-09T06:29:04Z`.
- Local-thread attach proof: synthetic `resume local 019d6f03-3ed0-7163-95ff-c8c547af7525` created `ses_20260409_zgstv4bkto`, reached `idle`, and Webex memberships included `hoteng@cisco.com` plus `codex-webex-connector@webex.bot`.
- History import and paging proof: release `2026-04-09T08-06-40` attached `019aa68d-8a8f-7ca1-812e-674711f9cf60` as `ses_20260409_3xo2dzciw5`; Webex messages confirmed the import banner and `/history page 2` response `Showing turns 89-98 of 108`.
- Mercury watchdog proof: on `2026-04-17`, sidecar stderr showed Mercury `connection_failed` / `ERR_INVALID_ARG_TYPE` while the control-room `/help` path was dead; release `2026-04-17T21-25-02` restored real user `/help`.
- Dedupe proof: release `2026-04-17T21-37-54` switched control-room dedupe to `payload.id` / `payload.data.id` after REST replay showed consecutive commands could collapse on `payload.event`.
- Reattach proof: release `2026-04-17T21-56-08` accepted `attach ses_20260408_w6sw4yrhvq`; memberships still included the user and bot, and a duplicate direct membership add returned HTTP `409`.
- Reliability hardening proof: `cargo test`, `cargo clippy --workspace --all-targets -- -D warnings`, and `bash scripts/smoke-test.sh` passed on `2026-05-03`; helper-backed `codex-review` findings were fixed; release `2026-05-03T17-25-10` was installed; production health returned `{"ok":true,"healthy":true,"detail":null}`; Webex `/v1/people/me` returned HTTP `200`.
- Isolated live E2E proof: release `2026-05-03T20-26-06` verified `/help`, `list local`, `resume local`, imported history, `/history`, `/history page 2`, ordinary session turn, `attach`, `/archive`, cleanup, and production health before/after.
- Recovery cleanup proof: `cargo test`, `cargo clippy --workspace --all-targets -- -D warnings`, and `bash scripts/smoke-test.sh` passed on `2026-05-03`; helper-backed review findings were fixed; isolated recovery E2E on release `2026-05-03T23-45-56` verified `diagnose sessions`, `cleanup failed`, archived-session diagnosis no-mutation, purge preview, confirmed purge, and final `list`; release `2026-05-03T23-53-10` was installed and production health stayed healthy.

## Related Docs
- Migration PR: https://github.com/JoeyTeng/codex-webex-connector/pull/1
- Commands reference: `docs/COMMANDS.md`
- Isolated live E2E runbook: `docs/WEBEX_E2E_TEST_PLAN.md`
- Original design pointer from legacy tracker: `plan.md`
- Complete pre-migration tracker snapshot: `docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`
