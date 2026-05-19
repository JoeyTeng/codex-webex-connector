---
id: 20260519-w4-delivery-enqueue-41d6ec5
title: W4 Delivery Enqueue Routing
status: completed
created: 2026-05-19
updated: 2026-05-19
branch: codex/w4-delivery-enqueue
pr: https://github.com/JoeyTeng/codex-webex-connector/pull/12
supersedes: []
superseded_by:
---

# W4 Delivery Enqueue Routing

## Summary
- W4 routes Webex async/background notifications that should enter Codex through cbth `delivery.enqueue` using C5 delivery-owned `codex_app_server` target mode.
- Normal Webex user messages remain on the W3 direct cbth-managed app-server forwarding path, and Webex approval cards, turn forwarding, room/session state, Data Space, local mirror, reconcile, and outbound final summaries remain Webex-owned behavior.
- The worker does not call cbth plugin RPC directly. It sends delivery enqueue frames to a supervisor-owned local broker, and the supervisor forwards them over its authenticated cbth plugin RPC connection.

## Current State
- `wxcd-cbth-rpc` carries the merged C5 delivery request shape and a `delivery.enqueue` client method.
- In explicit cbth plugin mode, `wxcd-supervisor` exposes the local delivery broker only when the cbth service advertises the C5 `delivery-owned-codex-app-server-target-v1` capability; otherwise W3 normal forwarding can still run and async notifications fail with an explicit broker-unavailable ACK.
- The broker only accepts same-user local clients and delivery-owned `codex_app_server` enqueue requests, strips worker-supplied target assertions, injects the supervisor-selected Codex binary, rejects explicit lease targets, and keeps accepting after transient local accept errors.
- The broker binds a short runtime socket under `/tmp` keyed by plugin/runtime identity, so long configured state directories do not prevent supervisor startup.
- The supervisor clears inherited delivery broker socket environment when no broker is available, preserving the explicit broker-unavailable async ACK path.
- `wxcd-worker` accepts `async_notification` ingress events, resolves them to an existing Webex session/thread, builds stable idempotent inline-payload delivery requests, and leaves `message_created` session turns on the existing direct app-server path.

## Evidence
- Dependency baseline: W3 merge commit `41d6ec5282893f05ba20e151e7877245c319f68b`.
- cbth dependency API: C5 squash merge commit `cedefb7058a3a8a41b25a5fd4793116652819849`.
- Local validation:
  - `cargo fmt`
  - `cargo test -p wxcd-cbth-rpc`
  - `cargo test -p wxcd-supervisor`
  - `cargo test -p wxcd-worker`
  - `cargo fmt --check`
  - `cargo test`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `bash scripts/smoke-test.sh`
  - `uv run --no-project /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
  - `git diff --check`
- Local review: helper-backed `codex-readonly` live-snapshot review returned `LGTM` in `.codex-tmp/isolated-review-j82jyj9l`.

## Next Steps
- Continue Wave 3 lifecycle/release/handoff workstreams separately; W4 does not implement lifecycle hooks, release handoff, or optional Webex handoff.
