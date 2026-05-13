# Project State

## Current State
- Webex-Codex Bridge 已部署在 macOS launchd 管理形态下，当前能力包括 session 创建、local thread attach、history paging、user reattach、failed-session diagnosis、soft archive，以及 archived room purge。
- GitHub pull requests run `codex/review-gate` through the repository workflow.
- 详细历史、验证证据和迁移前 tracker 原文已移入 `docs/project_journal/`：
  - 当前后续事项：`docs/project_journal/2026/05/2026-05-05-webex-bridge-followups-05ee1a8.md`
  - 已完成历史：`docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`
  - Legacy tracker snapshot：`docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`

## Active Handoff
- Phase: deployed
- Summary: 生产 bridge 依赖 bot-owner 1:1 direct room 作为 Data Space；Webex Mercury 断连由 sidecar watchdog 自恢复；session-room slash commands 已支持 Webex mention prefixes；recovery cleanup commands 已部署并记录在 `docs/COMMANDS.md`。
- Next Steps:
  - 使用 `diagnose sessions` 和 `cleanup failed <session_id>` 处理仍应 soft-archive 的生产 failed sessions。
  - 改进旧 Codex threads 在 process restart 后无法通过 `thread/read` reload 的恢复路径。
- Blockers:
  - 当前 Webex bot token 仍不能 replay group-room Data Space history。
  - Webex overview card refresh 仍可能返回 `Invalid roomId`，目前仅作为 best-effort。
- Evidence:
  - Commands reference: `docs/COMMANDS.md`
  - Live E2E runbook: `docs/WEBEX_E2E_TEST_PLAN.md`
  - Historical proof bundle: `docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`

## Recent Updates
- Session recovery cleanup commands and user-facing command documentation are current.
- Isolated live Webex E2E passed for `resume local`, `/history`, ordinary session turns, `attach`, recovery cleanup, and cleanup of temporary rooms/processes/root.
- Top-level trackers were migrated to short entrypoints; complete pre-migration contents are preserved in the legacy snapshot journal.

## Next Steps
- Work from `docs/project_journal/2026/05/2026-05-05-webex-bridge-followups-05ee1a8.md`.

## Risks Or Open Questions
- Long-term Data Space shape still depends on the credential model.
- Older sessions can remain failed if the underlying Codex thread cannot be reloaded by `thread/read`.
- Overview card updates and the underlying Mercury SDK/service disconnect regression remain open reliability questions.
