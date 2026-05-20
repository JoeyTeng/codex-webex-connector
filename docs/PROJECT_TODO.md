# Project TODO

- [done] Split session authority so missing or unreadable local Codex threads become degraded sessions hidden from the default active list while remaining diagnosable/cleanable.
- [done] Add W2 plugin packaging, manifest, compatible cbth plugin RPC hello client, explicit plugin-mode config, and doctor diagnostics without changing standalone runtime behavior.
- [done] Add W3 cbth-managed app-server lease usage for explicit plugin mode, without changing standalone/direct Codex app-server startup.
- [done] Add W4 delivery enqueue routing for async/background notifications without changing normal Webex user-message forwarding.
- [done] Add W5 conservative lifecycle hooks for C7 quiesce, drain, shutdown, unquiesce, health checks, durable plugin-home mirror persistence, and startup replay/reconcile gating.
- [pending] Add W6 optional handoff export/import for Webex cursor, in-flight handler state, and sidecar restart metadata without claiming that behavior in W5.
- [pending] Decide whether to keep the 1:1 direct room as the long-term Data Space shape or switch to a credential model that can replay a shared/group room.
- [pending] Investigate whether Webex overview cards can be refreshed reliably, or replace them with a safer update strategy.
- [pending] Decide whether to root-cause the underlying Mercury SDK/service `url`-undefined disconnect regression, or keep watchdog restart as the long-term mitigation.
- [done] Archive completed historical work in `docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`.
- [done] Preserve complete pre-migration tracker contents in `docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`.
