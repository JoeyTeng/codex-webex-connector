---
id: 20260517-w3-cbth-managed-app-server-4b1a3db
title: W3 Cbth Managed App Server
status: completed
created: 2026-05-17
updated: 2026-05-17
branch: codex/w3-cbth-managed-app-server
pr: https://github.com/JoeyTeng/codex-webex-connector/pull/9
supersedes: []
superseded_by:
---

# W3 Cbth Managed App Server

## Summary
- W3 接在 W2 plugin packaging/RPC client foundation 之后，让显式 cbth plugin mode 通过 C3 `app_server.ensure` / `app_server.refresh` / `app_server.stop` 租约使用 cbth-managed loopback Codex app-server。
- standalone legacy mode 仍保持默认行为：未启用 cbth plugin 时，worker 继续直接启动 `codex app-server --listen stdio://`。
- Webex message forwarding、approval card handling、session state 和 recovery 行为仍保留在 webex-connector 内部；本 workstream 不实现 `delivery.enqueue`、lifecycle hooks、release handoff 或 Webex handoff。

## Current State
- supervisor 在显式 cbth plugin mode 下作为 cbth-managed plugin 进程完成 `plugin.hello`，调用 C3 app-server lease RPC，向 worker 传递 managed `ws://` loopback endpoint，并负责周期 refresh 与 shutdown stop。
- worker 只在 cbth plugin mode 下接受 supervisor 注入的 managed endpoint；standalone 配置忽略该内部环境变量并保持 direct stdio app-server 路径。
- `wxcd-cbth-rpc` 对齐 C3 请求/响应 shape；`wxcd-codex` 新增 loopback WebSocket JSON-RPC transport，用于连接 cbth-owned app-server。

## Evidence
- Dependency baseline: webex `origin/master` head `4b1a3db`.
- cbth dependency API: C3 PR #88 merge commit `8241b68d58663045fd23d045d95d38a6921d45ec`.
- PR: https://github.com/JoeyTeng/codex-webex-connector/pull/9
- Local validation: `cargo fmt --check`, `cargo test -p wxcd-supervisor`, `cargo test`, `cargo clippy --workspace --all-targets -- -D warnings`, `bash scripts/smoke-test.sh`, project journal validation, and `git diff --check`.
- Local review: final `codex-readonly` LGTM in `.codex-tmp/isolated-review-9v4c7gyg`.
