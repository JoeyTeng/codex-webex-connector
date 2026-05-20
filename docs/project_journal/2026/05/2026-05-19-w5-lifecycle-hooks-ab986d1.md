---
id: 20260519-w5-lifecycle-hooks-ab986d1
title: W5 Lifecycle Hooks
status: completed
created: 2026-05-19
updated: 2026-05-20
branch: codex/w5-lifecycle-hooks
pr:
supersedes: []
superseded_by:
---

# W5 Lifecycle Hooks

## Summary
- W5 为 C7 增加保守的 Webex plugin lifecycle hooks：`plugin.health_check`、`plugin.quiesce`、`plugin.drain`、`plugin.shutdown` 和 `plugin.unquiesce`。
- Worker 在显式 cbth plugin mode 下提供独立 lifecycle control socket；进入 quiesce/shutdown 后停止接收新的 Webex 外部 ingress，等待已接收 handler 与 sidecar callback/retry backlog 完成后才允许 drain 完成，并在报告 drain/shutdown 完成前把 durable local session mirror 持久化到 cbth plugin home。
- 启动路径仍先 replay Data Space，再合并 durable local mirror 并执行本地 Codex thread reconcile；这些步骤完成后才把 listener health 标记为 ready 并接受 Webex ingress。
- W4 路由语义保持不变：普通 Webex user messages 仍走 direct cbth-managed app-server path；async/background notifications 仍走 `delivery.enqueue` delivery-owned target mode。

## Current State
- `wxcd-cbth-rpc` 已镜像 C7 lifecycle method constants、`plugin-lifecycle-v1` capability，以及 typed request/response shapes。W5 只保留 optional `plugin-handoff-v1` 类型用于协议兼容，不 advertise handoff capability，也不实现 handoff RPC 行为。
- `wxcd-worker` 在 plugin mode 下优先绑定 `WXCD_CBTH_LIFECYCLE_SOCKET`，否则使用 `CBTH_PLUGIN_HOME/lifecycle.sock`；默认 fallback 只会清理不可连接的 stale socket，遇到仍可连接的 live socket 会拒绝启动并要求提供 release-scoped socket path。
- `WXCD_CBTH_PRE_ACTIVE=1` 会让 worker 以 quiesced admission 启动，便于 pre-active health checks 验证外部 Webex work 已被 fence；unquiesce 会先 claim 当前 lifecycle token，并在 startup reconcile 期间保持 activating/fenced 状态，避免已取消的 activation 产生 replay/reconcile side effect。
- Plugin-mode durable mirror snapshot 位于 `CBTH_PLUGIN_HOME/bridge-state.json`；如果 plugin-home mirror 还不存在，启动时会 fallback 到 legacy `state_dir/bridge-state.json`。
- `plugin.shutdown` 成功时会写入按 plugin instance/release hash scoped 且文件名限长的 supervisor shutdown marker；supervisor 会先校验 marker instance/release，匹配时正常退出而不是重启旧 worker，避免 upgrade shutdown 被 supervisor 误判为可恢复 worker crash。
- Sidecar 现在解析 worker ingress ACK，不再把 worker 拒收当作投递成功。Webex callback 入口会记录 `sidecar_received_at`，并在 `CBTH_PLUGIN_HOME/webex-sidecar-drain-state/` 下写入按 plugin instance/release/pid scoped 的 drain-state mirror；worker drain/shutdown 只读取当前 instance/release 的 live sidecar backlog 并计入 in-flight，忽略并清理已退出 sidecar 的 stale drain-state 文件，只放行首次 quiesce/shutdown cutoff 前已进入 sidecar 的 callback，cutoff 后的新 Webex work 会被拒绝且不会在 sidecar 内无限 retry。Sidecar 在无法持久化 drain-state 时会 fail closed，不继续处理缺少可见 drain-state 的 Webex callback；在 lifecycle rejection race 中会把已取到的 Webex ingress envelope 持久化到 `CBTH_PLUGIN_HOME/webex-sidecar-deferred-ingress/`，恢复 active 后按原始 `created` / `deferred_at` 顺序先 replay 这些 envelope，再恢复监听。

## Remaining Dependencies
- W6 仍负责 optional production handoff export/import，包括 Webex cursor、in-flight handler state 和 sidecar restart metadata。
- C8/W7 live upgrade smoke 应在 W5/W6 都可用后，用真实 Webex credentials 验证完整 cbth service/plugin upgrade path。

## Evidence
- Dependency baseline: W4 merge commit `ab986d17aea388f1935761d4e01fbb5ff92bd0e2`。
- cbth dependency API: C7 merge commit `d58400f259f94c7d0fb9a645592ff90379d5188b`。
- Local validation:
  - `cargo fmt --check`
  - `git diff --check`
  - `cargo test`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `bash scripts/smoke-test.sh`
  - `node --check sidecars/webex-ws-sidecar/index.cjs`
  - `pnpm --dir sidecars/webex-ws-sidecar check`
  - `uv run --no-project /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
- Local helper review:
  - pre-commit `codex-review` found shutdown ownership, quiesced ingress ACK, and shutdown-timeout admission recovery issues; all three were fixed before this journal was finalized.
