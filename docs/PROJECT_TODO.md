# Project TODO

- [done] Implement the Rust workspace, shared config, Webex REST client, event log support, worker, supervisor, and Node websocket sidecar.
- [done] Deploy the bridge with `launchd` and verify the managed supervisor/worker/sidecar processes start successfully.
- [done] Validate the live control path far enough to create a session, execute a Codex turn, and capture the final answer `master`.
- [done] Validate that the bot-owner 1:1 direct room can serve as Data Space storage and startup replay with the current bot token.
- [done] Add control-room discovery of local-only Codex threads and attachment of a selected local thread to a new Webex room.
- [done] Add session-room history browsing so `resume local <thread_id>` can expose prior turns and `/history page <n>` can page back through older history.
- [done] Add a sidecar watchdog so unrecoverable Webex Mercury disconnects restart the local bridge instead of leaving control-room commands offline.
- [done] Fix control-room ingress deduplication so consecutive commands are keyed by unique Webex message/webhook ids instead of the SDK event label.
- [done] Add control-room `attach <session_id>` so a user can rejoin an existing bridge session room after leaving it.
- [pending] Verify one real user-originated `/history` or `/history page <n>` command against the deployed launchd-managed session room.
- [pending] Decide how to handle stale failed sessions left behind during bring-up.
- [pending] Improve recovery for previously created Codex threads that are not reloaded by `thread/read` after process restart.
- [pending] Decide whether to keep the 1:1 direct room as the long-term Data Space shape or switch to a credential model that can replay a shared/group room.
- [pending] Investigate whether Webex overview cards can be refreshed reliably, or replace them with a safer update strategy.
- [pending] Decide whether to root-cause the underlying Mercury SDK/service `url`-undefined disconnect regression, or keep watchdog restart as the long-term mitigation.
