# Project State

## Current State
- Webex-Codex Bridge 已部署在 macOS launchd 管理形态下，当前能力包括 session 创建、local thread attach、history paging、user reattach、failed-session diagnosis、soft archive，以及 archived room purge。
- W1 state authority split 已落地：bridge 会持久化本安装 identity，将 Data Space 作为 index/audit log，并用本地 snapshot/mirror 与可读 Codex thread 决定默认可控 session。
- W2 plugin packaging/RPC client foundation 已落地：standalone legacy mode 仍是默认路径，显式 cbth plugin mode 支持 manifest、`plugin.hello` UDS readiness check 和 `wxcd-worker doctor`。
- W3 cbth-managed app-server usage 已落地：显式 cbth plugin mode 由 supervisor 通过 C3 `app_server.ensure/refresh/stop` 管理 loopback Codex app-server 租约，worker 通过 managed `ws://` endpoint 连接；standalone/direct mode 仍默认直接启动 `codex app-server --listen stdio://`。
- W4 delivery enqueue routing 已落地：Webex async/background notifications 通过 supervisor-owned broker 调用 cbth C5 `delivery.enqueue` delivery-owned `codex_app_server` target；普通 Webex user-message forwarding 仍走 W3 direct app-server path。
- W5 lifecycle hooks 已落地：显式 cbth plugin mode 提供 C7 `plugin.health_check`、`plugin.quiesce`、`plugin.drain`、`plugin.shutdown`、`plugin.unquiesce` 的保守实现；quiesce/shutdown 后拒绝新的 Webex 外部 ingress，drain 等待已接收 handler 与 live sidecar callback/retry backlog 完成并在 cbth plugin home 持久化 local session mirror 后返回。
- W6 optional handoff 已落地：显式 cbth plugin mode 提供 `plugin.handoff_export` / `plugin.handoff_import`，在 W5 quiesce/drain 之后可交接 durable bridge snapshot、recent Webex event-id cursor、in-flight session/approval summary 和 sidecar deferred/drain metadata；pre-active import 只更新本地 mirror/cursor，不产生 Webex/Codex 外部副作用。
- W7 opt-in live upgrade E2E harness 已落地：`scripts/w7_live_upgrade_e2e.py` 默认 dry-run，live 模式先执行 cbth C8 `service upgrade-smoke` safe harness，再隔离真实 Webex rooms、task-scoped cbth service/plugin home、session turn、delivery smoke、pagination-aware cleanup/manifest；真实 Webex release A/B upgrade command 是 optional hook，并会验证 cbth registry 已切到 release B。
- GitHub pull requests run `codex/review-gate` through the repository workflow.
- 详细历史、验证证据和迁移前 tracker 原文已移入 `docs/project_journal/`：
  - W7 live upgrade E2E harness：`docs/project_journal/2026/05/2026-05-25-w7-live-upgrade-e2e-c2f3fe5.md`
  - W6 optional handoff：`docs/project_journal/2026/05/2026-05-20-w6-optional-handoff-5181e85.md`
  - W5 lifecycle hooks：`docs/project_journal/2026/05/2026-05-19-w5-lifecycle-hooks-ab986d1.md`
  - W4 delivery enqueue routing：`docs/project_journal/2026/05/2026-05-19-w4-delivery-enqueue-41d6ec5.md`
  - W3 cbth-managed app-server：`docs/project_journal/2026/05/2026-05-17-w3-cbth-managed-app-server-4b1a3db.md`
  - W2 plugin packaging/RPC client：`docs/project_journal/2026/05/2026-05-13-w2-plugin-packaging-rpc-client-1989c19.md`
  - 当前后续事项：`docs/project_journal/2026/05/2026-05-05-webex-bridge-followups-05ee1a8.md`
  - 已完成历史：`docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`
  - Legacy tracker snapshot：`docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-05ee1a8.md`

## Active Handoff
- Phase: deployed
- Summary: 生产 bridge 依赖 bot-owner 1:1 direct room 作为 Data Space；Webex Mercury 断连由 sidecar watchdog 自恢复；session-room slash commands 已支持 Webex mention prefixes；recovery cleanup commands 已部署并记录在 `docs/COMMANDS.md`。
- Next Steps:
  - 使用 `diagnose sessions` 和 `cleanup failed <session_id>` 处理 degraded / missing-local-thread sessions。
  - 用支持 C8 PR #99 merge commit `ee76fdd5937ca57e8156631c32509be12d3cf4c2` 的 `cbth` binary 执行 W7 live smoke；harness 会先运行 `cbth service upgrade-smoke`，再进入真实 Webex/cbth plugin-mode delivery smoke。
- Blockers:
  - 当前 Webex bot token 仍不能 replay group-room Data Space history。
  - Webex overview card refresh 仍可能返回 `Invalid roomId`，目前仅作为 best-effort。
- Evidence:
  - Commands reference: `docs/COMMANDS.md`
  - Live E2E runbook: `docs/WEBEX_E2E_TEST_PLAN.md`
  - Historical proof bundle: `docs/project_journal/2026/05/2026-05-05-webex-bridge-history-05ee1a8.md`

## Recent Updates
- Session recovery cleanup commands and user-facing command documentation are current.
- W3 adds cbth-managed app-server leases for explicit plugin mode while preserving standalone/direct Codex app-server startup as the default.
- W4 routes async/background notifications through cbth delivery-owned `delivery.enqueue` while preserving normal user-message forwarding on the W3 direct app-server path.
- W5 adds conservative cbth lifecycle hooks while preserving W3/W4 forwarding and delivery routing semantics.
- W6 adds optional handoff export/import while preserving W5 conservative lifecycle fallback semantics.
- W7 adds the opt-in live upgrade E2E harness and keeps cbth generic upgrade orchestration outside this repo.
- W2 added plugin packaging metadata, explicit cbth plugin config, C1-compatible hello client tests, and doctor diagnostics without requiring Webex credentials.
- Isolated live Webex E2E passed for `resume local`, `/history`, ordinary session turns, `attach`, recovery cleanup, and cleanup of temporary rooms/processes/root.
- Top-level trackers were migrated to short entrypoints; complete pre-migration contents are preserved in the legacy snapshot journal.

## Next Steps
- Work from `docs/project_journal/2026/05/2026-05-05-webex-bridge-followups-05ee1a8.md`.

## Risks Or Open Questions
- Long-term Data Space shape still depends on the credential model.
- Older sessions with missing or unreadable local Codex threads are now intentionally degraded and hidden from the default active list, while remaining visible to diagnose/cleanup.
- Overview card updates and the underlying Mercury SDK/service disconnect regression remain open reliability questions.
