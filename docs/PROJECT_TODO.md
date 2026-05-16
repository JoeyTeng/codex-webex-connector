# Project TODO

- [done] Split session authority so missing or unreadable local Codex threads become degraded sessions hidden from the default active list while remaining diagnosable/cleanable.
- [pending] Decide the next recovery/handoff layer for degraded sessions after W1, without folding cbth RPC, app-server lease, delivery enqueue, lifecycle hooks, or plugin packaging into W1.
- [pending] Decide whether to keep the 1:1 direct room as the long-term Data Space shape or switch to a credential model that can replay a shared/group room.
- [pending] Investigate whether Webex overview cards can be refreshed reliably, or replace them with a safer update strategy.
- [pending] Decide whether to root-cause the underlying Mercury SDK/service `url`-undefined disconnect regression, or keep watchdog restart as the long-term mitigation.
- [done] Archive completed historical work in `docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`.
- [done] Preserve complete pre-migration tracker contents in `docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`.
