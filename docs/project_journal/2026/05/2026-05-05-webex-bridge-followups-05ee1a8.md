---
id: 20260505-webex-bridge-followups-05ee1a8
title: Webex Bridge Follow-ups
status: active
created: 2026-05-05
updated: 2026-05-16
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
- W1 state authority split records a stable local installation identity under `state_dir`, treats Data Space as an index/audit log, and uses the local snapshot/mirror plus readable Codex thread as the executable authority for default control lists.
- W1 current-writer local snapshots still require explicit current-installation `authority` or `local_mirror` evidence before claiming remote legacy sessions; authority-less legacy snapshot records require listed local Codex thread evidence.
- If `installation-identity.json` is missing but a W1 local snapshot records a writer installation, startup recovers and persists that writer identity before minting any new installation id.
- W1 local snapshot mirror merge preserves snapshot-only current sessions, and missing identity recovery tolerates malformed fallback snapshots by warning and minting a fresh identity.
- W1 preserves existing installation identity lookup errors instead of minting replacement IDs, and failed session rooms only accept recovery/status commands while staying routable.
- W1 persists local mirror merge results back to Data Space snapshots and merges newer local snapshot session fields plus current-installation pending approvals after successful replay.
- W1 remote replay tombstones prevent stale local mirror snapshots from resurrecting purged sessions, resolved approvals, or remotely archived sessions.
- Failed session rooms remain routable for in-room recovery commands, `/list all` includes archived sessions for purge discovery, and legacy claim evidence includes archived Codex threads without making archived sessions executable.

## Next Steps
- Use `diagnose sessions` and `cleanup failed <session_id>` on degraded sessions whose local Codex thread is missing, unreadable, or not probeable.
- Plan the next recovery/handoff layer separately from W1.
- Decide whether the long-term Data Space should stay on the bot-owner 1:1 direct room or move to a credential model that can replay shared/group rooms.
- Investigate whether Webex overview cards can be refreshed reliably, or replace them with a safer update strategy.
- Decide whether to root-cause the Mercury SDK/service `url`-undefined disconnect regression, or keep watchdog restart as the long-term mitigation.

## Blockers
- Webex bot tokens still cannot replay group-space Data Space history via `GET /messages?roomId=...`.
- Webex rejects overview card updates with `Invalid roomId`; plain session/final messages and approval cards continue to work.
- `codex app-server` can still fail to reload some earlier threads by `thread/read`; W1 now classifies those sessions as degraded instead of treating Data Space as sufficient executable authority.

## Evidence
- Current command surface: `docs/COMMANDS.md`
- Isolated live E2E procedure: `docs/WEBEX_E2E_TEST_PLAN.md`
- W1 PR: https://github.com/JoeyTeng/codex-webex-connector/pull/2
- W1 validation: `cargo test`
- Completed historical proof bundle: `docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`
- Complete pre-migration tracker snapshot: `docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`
