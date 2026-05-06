---
id: 20260505-webex-bridge-followups-05ee1a8
title: Webex Bridge Follow-ups
status: active
created: 2026-05-05
updated: 2026-05-05
branch: master
pr:
supersedes: []
superseded_by:
---

# Webex Bridge Follow-ups

## Summary
- The deployed bridge is functional, documented, and covered by isolated live E2E evidence.
- Remaining work is concentrated in recovery behavior and Webex platform reliability choices.

## Current State
- Production uses the bot-owner 1:1 direct room for Data Space replay because the current bot token cannot list group-room history.
- Session recovery cleanup is deployed: `diagnose`, `cleanup failed`, and two-step `purge archived ... confirm` are documented in `docs/COMMANDS.md`.
- Live E2E coverage exists in `docs/WEBEX_E2E_TEST_PLAN.md` for temporary rooms, local-only thread creation, `resume local`, history paging, session turns, `attach`, recovery cleanup, and cleanup.

## Next Steps
- Use `diagnose sessions` and `cleanup failed <session_id>` on existing production failed sessions that should no longer appear active.
- Improve recovery for previously created Codex threads that are not reloaded by `thread/read` after process restart.
- Decide whether the long-term Data Space should stay on the bot-owner 1:1 direct room or move to a credential model that can replay shared/group rooms.
- Investigate whether Webex overview cards can be refreshed reliably, or replace them with a safer update strategy.
- Decide whether to root-cause the Mercury SDK/service `url`-undefined disconnect regression, or keep watchdog restart as the long-term mitigation.

## Blockers
- Webex bot tokens still cannot replay group-space Data Space history via `GET /messages?roomId=...`.
- Webex rejects overview card updates with `Invalid roomId`; plain session/final messages and approval cards continue to work.
- `codex app-server` did not successfully reload some earlier threads by `thread/read`, leaving older sessions failed after restarts.

## Evidence
- Current command surface: `docs/COMMANDS.md`
- Isolated live E2E procedure: `docs/WEBEX_E2E_TEST_PLAN.md`
- Completed historical proof bundle: `docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`
- Complete pre-migration tracker snapshot: `docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`
