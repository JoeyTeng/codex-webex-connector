# Project State

## Current State
- The macOS-first `wxcd` bridge is implemented, locally validated, and deployed under `~/Library/Application Support/codex-webex-connector`.
- `launchd` now manages `wxcd-supervisor`, which in turn starts `wxcd-worker` and the Node Webex websocket sidecar.
- Live validation succeeded for the critical path: Webex Mercury websocket delivery works, the worker health socket responds, and a newly created session executed a Codex turn and captured the final answer `master`.
- The deployed instance now uses the bot-owner 1:1 Webex direct room as the Data Space, and startup replay from that room was validated against the live worker.
- The control room now supports local-thread discovery and attachment: `list local`, `list local page <n>`, `list all`, and `resume local <thread_id>` are implemented and deployed.
- Session rooms now support history browsing: `resume local <thread_id>` imports the newest history page automatically, and `/history` plus `/history page <n>` can page back through older turns on demand.
- The deployed sidecar now has a Mercury watchdog: if Webex realtime ingress falls into an unrecoverable disconnect state, the sidecar exits non-zero so supervisor restarts the bridge instead of leaving it falsely "up but deaf".

## Active Handoff
- Phase: deployed
- Summary: The bridge can create Webex session rooms, start Codex threads, attach existing local-only threads, and page through prior Codex turn history inside the session room. Data Space recovery no longer depends on the old group room; the deployed launch agent now replays state from the bot-owner 1:1 direct room, and the Webex sidecar now self-recovers from the observed Mercury disconnect failure mode.
- Next Steps:
  - Verify one real user-originated `/history` or `/history page <n>` command from Webex proper, now that synthetic ingress plus user-token room replay confirmed the full path.
  - Decide whether to keep or archive the earlier failed recovery sessions in the existing control/session spaces.
  - Improve session recovery for previously created threads that `codex app-server` does not reload by `thread/read`.
- Blockers:
  - Webex bot tokens still cannot replay group-space Data Space history via `GET /messages?roomId=...`; the working deployment now relies on a 1:1 direct room instead.
  - Webex rejects overview card updates with `Invalid roomId`; overview refresh is best-effort, while plain session/final messages and approval cards continue to work.
- Evidence:
  - Design: `plan.md`
  - Local Codex probe: `codex app-server --listen stdio://` accepted `initialize`, `thread/start`, and `turn/start`
  - Mercury self-test: bot-posted control-room message was observed back through `messages.listen()`
  - Local execution proof: session `ses_20260408_w6sw4yrhvq` completed with final answer `master`
  - Deployment proof: `launchctl print gui/501/com.example.wxcd.supervisor` shows the agent running; `/tmp/wxcd.sock` health check returns `{\"ok\":true,\"healthy\":true,\"detail\":null}`
  - Direct-room replay proof: `GET /v1/messages?roomId=<direct_room_id>` returned both the user-originated `/help` and bot-authored `WXCD/V1 EVENT ...` frames; after moving aside `bridge-state.json`, restarting the worker produced fresh `session_updated` Data Space events at `2026-04-09T06:29:02Z` through `2026-04-09T06:29:04Z`, proving startup state came from the direct room rather than the local snapshot.
  - Local-thread attach proof: a synthetic control ingress `resume local 019d6f03-3ed0-7163-95ff-c8c547af7525` created session `ses_20260409_zgstv4bkto`, reached `idle`, and Webex `GET /v1/memberships?roomId=<session_room_id>` returned both `hoteng@cisco.com` and `codex-webex-connector@webex.bot`.
  - History import + paging proof: after deploying release `2026-04-09T08-06-40`, a synthetic control ingress `resume local 019aa68d-8a8f-7ca1-812e-674711f9cf60` created session `ses_20260409_3xo2dzciw5`; using a temporary user token for `hoteng@cisco.com`, `GET /v1/messages?roomId=<session_room_id>` confirmed both the auto-import banner `Imported local Codex history... Showing latest 10 of 108 turns.` and a later `/history page 2` response `Showing turns 89-98 of 108`.
  - Mercury watchdog recovery proof: on `2026-04-17`, deployed bot token `GET /v1/people/me` still returned `HTTP/2 200`, but sidecar stderr showed repeated Mercury `connection_failed` / `ERR_INVALID_ARG_TYPE` (`url` undefined) and the control-room `/help` path was dead. After deploying release `2026-04-17T21-25-02` with a sidecar watchdog that exits on `connection_failed` / `offline.permanent`, a real user-originated `/help` succeeded again.

## Recent Updates
- Replaced the broken aggregate `webex` package path with modular `@webex/*` SDK packages and added the missing `@babel/runtime-corejs2` runtime dependency.
- Added `WXCD_ENV_PATH`, `WXCD_NODE_PATH`, and `WXCD_CODEX_PATH` deployment plumbing so `launchd` can find secrets, `node`, and `codex`.
- Added retry-on-`Invalid roomId` for immediate `create_message()` calls after room creation and switched overview refresh failures to warnings instead of hard failures.
- Updated the installer to use Cargo's reported `target_directory`, copy `.env` into the deployed config directory, and create the launch-agent logs directory.
- Switched `WEBEX_DATA_ROOM_SPACE_LINK` to the raw room id of the bot-owner 1:1 direct room after confirming that the current bot token can list direct-room history but not group-room history.
- Hardened `wxcd-eventlog` replay so malformed `WXCD/V1` frames are skipped with a warning instead of aborting the entire Data Space replay.
- Added control-room support for `list local`, `list all`, and `resume local <thread_id>` by paging `codex app-server` `thread/list`, filtering out already attached bridge sessions, and attaching a selected local-only thread to a newly created Webex room.
- Added session-room history browsing with `/history` and `/history page <n>`, backed by `thread/read` extraction of prior user/final-answer turns and newest-first paging.
- Changed `resume local <thread_id>` so the newly created session room immediately receives the newest history page plus hints for `/history page <n>`.
- Added a Mercury watchdog to the Node sidecar so unrecoverable realtime disconnects force a non-zero exit and let supervisor restart the bridge.

## Next Steps
- Confirm one real Webex `/history` or `/history page <n>` command against a deployed session room, now that synthetic ingress and direct room replay are covered.
- Decide whether to add cleanup or archival handling for stale failed sessions left behind during bring-up.

## Risks Or Open Questions
- The current bot token still appears unable to list group-room messages for Data Space replay, so the stable deployment shape is currently tied to a bot-owner 1:1 direct room unless the credential model changes.
- `codex app-server` did not successfully reload earlier created threads by `thread/read`, leaving older sessions in a failed state after restarts.
- Overview message updates are still unreliable in Webex, even though initial card creation and normal text replies work.
- The underlying Mercury SDK failure (`ERR_INVALID_ARG_TYPE` while handling `connection_failed`) is only mitigated by watchdog restart today; the root SDK/service regression is still not explained.
