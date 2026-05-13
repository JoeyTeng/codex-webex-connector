---
id: 20260513-w2-plugin-packaging-rpc-client-1989c19
title: W2 Plugin Packaging RPC Client
status: completed
created: 2026-05-13
updated: 2026-05-13
branch: codex/w2-plugin-packaging-rpc-client
pr:
supersedes: []
superseded_by:
---

# W2 Plugin Packaging RPC Client

## Summary
- W2 在 W1 state authority split 之上补齐 webex-connector 的 cbth plugin packaging 基础：plugin manifest、显式 plugin-mode 配置、C1-compatible `plugin.hello` UDS client，以及无需真实 Webex 凭据即可运行的 `wxcd-worker doctor` 诊断面。
- 运行时仍保持 standalone legacy mode 默认行为，worker 仍直接启动 Codex app-server；cbth-managed app-server、delivery enqueue 和 lifecycle handoff/drain 均留给后续 W3/W4/W5/W6。

## Current State
- 默认配置为 standalone；只有显式配置 `cbth_plugin.enabled = true` 或设置 `WXCD_CBTH_PLUGIN` 时才进入 cbth plugin diagnostics/RPC readiness 路径。
- `plugin/manifest.json` 描述当前 release 的 entrypoint、capabilities、config schema 和 doctor command。
- `wxcd-cbth-rpc` 复制并隔离 C1 `plugin_rpc.rs` 的 v1 frame/type shape，后续共享 crate 出现后可集中替换。

## Evidence
- Dependency baseline: W1 head `1989c19f45d1c08e20b9bf221e3c538ef10c59d9`.
- cbth dependency API: C1 PR #78 head `39ae8fe49ba25615385b292cdd6ed1e6628ba460`.
- Validation: `cargo test` passed locally on 2026-05-13.
- Review: helper-backed `codex-review` on `1989c19f45d1c08e20b9bf221e3c538ef10c59d9..ba9601f` found timeout, installed manifest path, and macOS UDS test-path risks; the follow-up commit fixed all three.
- Final review: helper-backed `codex-review` on `1989c19f45d1c08e20b9bf221e3c538ef10c59d9..30ec21f` found missing runtime handshake and missing `CBTH_*` env support; the follow-up fix adds supervisor startup hello and cbth env derivation.
