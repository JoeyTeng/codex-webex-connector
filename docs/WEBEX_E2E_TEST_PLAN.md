# Webex 真机 E2E 测试计划

本文记录 Webex-Codex Bridge 的隔离真机 E2E 测试流程。目标是验证真实 Webex 事件、临时 bridge 进程、本机 Codex app-server、`resume local <thread_id>`、session room history、普通 turn、以及 `attach <session_id>` 重新加入能力。

## W7 live upgrade harness

W7 新增默认 dry-run 的 opt-in harness：

```bash
python3 scripts/w7_live_upgrade_e2e.py
```

该命令只做本地参数和执行计划输出，不读取真实 token、不访问 Webex、也不启动 cbth service。真实 live run 必须显式设置 `WXCD_LIVE_E2E=1`：

```bash
WXCD_LIVE_E2E=1 \
python3 scripts/w7_live_upgrade_e2e.py \
  --live \
  --token-file token.txt \
  --bot-env-file .env \
  --cbth-bin cbth \
  --cbth-service-upgrade-smoke-timeout-seconds 30
```

该路径依赖 cbth C8 已合并能力：PR #99 merge commit `ee76fdd5937ca57e8156631c32509be12d3cf4c2`，命令为 `cbth service upgrade-smoke`。W7 harness 会在读取 credential、创建 Webex room 或启动 Webex connector 前，先运行：

```bash
cbth service upgrade-smoke \
  --allow-task-scoped-mutation \
  --smoke-root "$test_root/cbth-c8-service-upgrade-smoke" \
  --startup-timeout-ms 30000 \
  --json
```

该 cbth C8 harness 只使用 fake plugin 和 task-scoped smoke root，并由 W7 通过清洗过的子进程环境启动，用于验证 cbth service 前台 supervisor、LaunchAgent plan 渲染、C7 pre-active health fence、quiesce、drain、handoff、promote 和 shutdown 顺序；它不包含 Webex token、Webex room、Data Space 或 delivery 行为。W7 harness 会校验输出中的 `ok=true`、`system_mutation_performed=false`、`release_upgrade.handoff_performed=true` 和关键 release event 的有序子序列。

如需额外验证真实 Webex connector 的 release A/B 切换，可显式提供 Webex-specific release upgrade command：

```bash
WXCD_E2E_CBTH_UPGRADE_CMD='tools/wxcd-release-upgrade --cbth-home "{cbth_home}" --plugin {plugin} --from {release_a_id} --to {release_b_id} --release-dir "{release_b}"' \
WXCD_E2E_CBTH_UPGRADE_CHECK_CMD='tools/wxcd-release-upgrade --help' \
WXCD_LIVE_E2E=1 \
python3 scripts/w7_live_upgrade_e2e.py --live --token-file token.txt --bot-env-file .env
```

这个 optional command 只展开 `{plugin}`、`{release_a}`、`{release_b}`、`{release_a_id}`、`{release_b_id}`、`{cbth_home}` 和 `{prefix}`，并以关闭 stdin、有界 timeout、stdout/stderr 写入私有 `logs/webex-release-upgrade.log` 的方式执行；webex-connector 不复制 cbth generic release manager。未提供 optional command 时，W7 会记录 `webex_release_upgrade.status=skipped`，但仍执行 cbth C8 upgrade smoke、真实 Webex session turn 和 delivery smoke。

W7 harness 覆盖：

- 在读取 credential、创建 Webex room 或启动 cbth 前预检 `WXCD_LIVE_E2E=1` 和 cbth C8 `service upgrade-smoke`；缺失时 fail closed。
- 先通过清洗过的子进程环境运行 cbth C8 `service upgrade-smoke` safe harness，并把 PR #99 / merge commit `ee76fdd5937ca57e8156631c32509be12d3cf4c2`、smoke root、release events 和 system-mutation 结果写入 manifest。
- 解析未跟踪的 developer token file 和 bot env file，校验 developer/bot `/people/me` email 与 credential 文件一致，只记录字段长度，不打印 bearer。
- 生成的 private config/env/manifest/registry 文件通过 `0600` 临时文件写入，避免 bot token 在 test root 中短暂暴露。
- 默认 build/stage release 路径会在创建 Webex 资源前确保 sidecar runtime dependencies；显式 `--release-a` / `--release-b` 目录也必须包含 sidecar `node_modules`。
- 创建带唯一 prefix 的临时 control/data/session rooms，并以该 prefix 作为 cleanup 安全边界；新建 room 后的 membership/message 写操作会对 Webex 短暂 `Invalid roomId` 传播延迟做有界 retry/backoff。
- 使用 task-scoped `CBTH_HOME` 写入 cbth plugin registry，并通过清洗过的子进程环境启动 `cbth service run` / `wxcd-supervisor run`，避免继承生产 `WEBEX_*`、`WXCD_*` 或 `CBTH_*` 配置。
- 通过真实 Webex 消息验证 `/help`、`list local`、`resume local <thread_id>`、history import、`/history`、`/history page 2` 和普通 session turn。
- 通过 worker ingress socket 注入 `async_notification`，验证 W4 delivery-owned `delivery.enqueue` broker 路径。
- 如果显式提供 Webex-specific release upgrade command，调用时关闭 stdin，并用 `--upgrade-timeout-seconds` 设置有界等待；命令成功后会验证 task-scoped cbth registry 已切到 release B 的 `release_id`、supervisor binary、`WXCD_RELEASE_DIR` 和 `WXCD_PLUGIN_MANIFEST_PATH`，再确认旧 release 的 ingress/lifecycle sockets 不再接受连接、验证新 release-scoped worker ingress health，并通过真实 Webex session turn 和 worker socket delivery smoke 分别验证升级后的 sidecar/Mercury 与 delivery 路径。未提供该 optional command 时，Webex live smoke 在当前 release 上继续跑 post-upgrade-smoke turn 与 delivery smoke，真实 upgrade ordering 由前置 C8 safe harness 覆盖。
- 写入 task-scoped `manifest.json`；失败、blocked 或 cleanup 失败时默认保留 test root 供诊断，成功且 cleanup 干净时会先保留一份同级 `*-manifest.json` 成功证据，再自动清理 harness 自建的临时 test root，显式传入的 `--test-root` 始终保留；如果显式 test root 位于 repo 内，必须放在 ignored `.codex-tmp/` 下，避免含 secret 的 `wxcd.env` 变成可提交文件。cleanup 会删除 manifest 中的 prefixed rooms，其中 session room 使用 bot token 删除，并且只在 prefix 符合生成的 `WXCD-W7-E2E-YYYYMMDD-<8 chars>` 形状时分别用 developer/bot token 按 Webex room pagination 补扫未记录的临时 rooms；session room title lookup 同样会跟随 room pagination。

## 范围

- 使用 `token.txt` 中的 developer auth 作为测试用户身份，但不得打印、复制到日志或提交任何 bearer token。
- 不修改现有 `launchd` 配置，不停止生产 `wxcd-supervisor`，不向生产 control/session/data spaces 发送测试消息。
- 所有 Webex 测试空间都使用唯一前缀，例如 `WXCD-E2E-20260503-<shortid>`，并在 manifest 中记录。
- 临时 worker 使用独立 socket、state dir、config、env、logs，不复用 `/tmp/wxcd.sock`。
- 测试结束必须清理临时 Webex rooms 和临时进程；harness 自建的临时文件在成功且 cleanup 干净时删除，但保留同级成功 manifest 供审计；显式 `--test-root` 或失败诊断目录保留并在报告中说明。若清理失败，必须报告 manifest 路径和未清理资源 id。

## 前置检查

1. 确认仓库没有待提交的代码改动，且 `token.txt` 是未跟踪文件。
2. 确认当前部署 release 路径：

```bash
readlink "$HOME/Library/Application Support/codex-webex-connector/current"
```

3. 用 raw JSON line 协议检查生产 worker health。不要用 HTTP `curl /health`，该 socket 不是 HTTP 服务。

```bash
ruby -rsocket -e 's=UNIXSocket.new(ARGV[0]); s.puts(%q({"kind":"health_check"})); puts s.gets' /tmp/wxcd.sock
```

4. 从 `token.txt` 读取 `email`、`bearer`、`ord_id`，只输出字段名和长度，不输出值。
5. 用 developer bearer 调用 Webex `/v1/people/me`，确认返回 email 与 `token.txt` 中的 `email` 一致。
6. 从当前部署 config/env 读取 bot token、bot email、bot Codex binary path、node path；不得输出 secret。
7. W7 live run 还需要可执行的 cbth C8 `service upgrade-smoke` command；缺失时报告 blocked，不手工模拟 release manager。

## 隔离环境

创建 task-scoped 临时目录，例如：

```bash
test_root="/private/tmp/wxcd-e2e-<shortid>"
mkdir -p "$test_root/logs"
```

在该目录中写入：

- `wxcd.toml`：配置 `socket_path`、`state_dir`、`session_title_prefix`、repo 列表等。
- `wxcd.env`：包含 bot token/email、临时 control/data room id、allowed test user email、Codex/Node path。
- `manifest.json`：记录 test prefix、created room ids、process ids、dedicated Codex thread id、bridge session id、cleanup status。

临时配置原则：

- `socket_path` 使用 `$test_root/wxcd.sock`。
- `state_dir` 使用 `$test_root/state`。
- `session_title_prefix` 使用唯一测试前缀。
- `WEBEX_CONTROL_ROOM_SPACE_LINK` 指向临时 control room id。
- `WEBEX_DATA_ROOM_SPACE_LINK` 指向临时 data room id。
- `WEBEX_ALLOWED_USER_EMAILS` 只包含测试用户 email。
- repo 配置只包含当前仓库。

## Webex 临时空间

使用 developer bearer 创建临时 Webex rooms：

- `<prefix> control`
- `<prefix> data`

随后把 bot email 加入两个 rooms。所有 room ids 写入 manifest。不得操作任何标题不匹配 `<prefix>` 的 room。

## 启动临时 bridge

使用当前 release 中的二进制和 sidecar，不修改生产 launch agent。

```bash
WXCD_CONFIG_PATH="$test_root/wxcd.toml" \
WXCD_ENV_PATH="$test_root/.env" \
"$release/bin/wxcd-worker" >"$test_root/logs/worker.out.log" 2>"$test_root/logs/worker.err.log" &

WEBEX_BOT_TOKEN="<redacted>" \
WEBEX_BOT_EMAIL="<redacted>" \
WXCD_SOCKET_PATH="$test_root/wxcd.sock" \
"$node_path" "$release/sidecars/webex-ws-sidecar/index.cjs" >"$test_root/logs/sidecar.out.log" 2>"$test_root/logs/sidecar.err.log" &
```

启动后用 raw JSON line 协议验证临时 worker health：

```bash
ruby -rsocket -e 's=UNIXSocket.new(ARGV[0]); s.puts(%q({"kind":"health_check"})); puts s.gets' "$test_root/wxcd.sock"
```

如果 health 不为 `{"ok":true,"healthy":true,"detail":null}`，停止临时进程并进入 cleanup。

## 专用 local-only Codex thread

使用独立 `codex app-server --listen stdio://` 创建一个只用于本次 e2e 的 local-only thread，cwd 指向当前 repo。测试 prompt 必须安全、短、可识别，不要求文件修改。

目标是创建 11 个短 turns，以便 `/history page 2` 覆盖真实第二页。每个 prompt 都带唯一 marker，例如：

```text
E2E_MARKER_<shortid> turn 01. Reply with exactly: E2E_MARKER_<shortid> turn 01 ok
```

把 `thread_id` 写入 manifest。该 thread 在 `resume local <thread_id>` 之前不得被 bridge 管理。

如果 11 个 turns 过慢，可以降低历史深度，但最终报告必须明确 `/history page 2` 是否覆盖真实第二页，还是只验证了明确 page response。

## E2E 场景

所有用户消息都通过 developer bearer 发送到临时 Webex rooms，等待 bot 真实 Webex 回复并用 Webex REST 读取确认。

通过 Webex REST 发 group-room 测试消息时，应同时发送 plain `text` 和带 bot mention 的 `markdown`。`markdown` 用于触发 bot realtime 事件，`text` 用于让 worker 看到不带 mention 前缀的原始命令或 prompt。Worker 会在启动时用 bot token 读取 Webex bot display name，并在 command 解析前接受 display-name 或 bot-email local-part 形式的 mention 前缀。

```json
{
  "roomId": "<room_id>",
  "text": "/history",
  "markdown": "<@personId:<bot_id>|<bot_display_name>> /history"
}
```

1. Control help：
   - 向临时 control room 发送 `/help`。
   - 期望 bot 回复包含 `Control room commands:` 和 `resume local <thread_id>`。

2. Local list：
   - 向临时 control room 发送 `list local`。
   - 期望回复包含 dedicated `thread_id`。

3. Resume local：
   - 向临时 control room 发送 `resume local <thread_id>`。
   - 期望回复包含 `Attached local thread`、新 `session_id` 和 session title。
   - 从 Webex rooms 或 Data Space/session state 中解析 session room id，并写入 manifest。

4. Session membership：
   - 查询 session room memberships。
   - 期望同时存在测试用户 email 和 bot email。

5. Imported history：
   - 查询 session room messages。
   - 期望看到 `Imported local Codex history from thread` banner。

6. Session history：
   - 向 session room 发送 `/history`。
   - 期望回复包含 `history page 1` 和 dedicated `thread_id`。
   - 向 session room 发送 `/history page 2`。
   - 期望回复包含 `history page 2`，或在历史不足时返回明确的 page response。

7. Session turn：
   - 向 session room 发送一个普通测试 turn，带唯一 marker。
   - 期望最终 summary 消息包含该 marker，证明 Webex -> sidecar -> worker -> Codex -> Webex 完整链路可用。

8. Attach rejoin：
   - 用 Webex REST 删除测试用户在 session room 中的 membership。
   - 确认 membership 不再包含测试用户。
   - 向临时 control room 发送 `attach <session_id>`。
   - 期望回复显示用户已加入或已在 room 中，并且 session room memberships 再次包含测试用户。

9. Session recovery cleanup：
   - 向临时 Data Space 写入一个指向不存在 Codex `thread_id` 的测试 session event，或用 runner 预置等价的本地 snapshot。
   - 启动临时 worker 后发送 `diagnose sessions`，期望回复列出该 failed session 和 missing/unreadable 原因。
   - 发送 `cleanup failed <session_id>`，期望该 session 被 soft-archive，Webex room 标题变为 `[ARCHIVED] ...`，且 `list` 中状态更新。
   - 发送 `purge archived <session_id>`，期望只返回 destructive preview，不删除 room。
   - 发送 `purge archived <session_id> confirm`，期望测试 session room 被删除，且该 session 不再出现在 `list`。

## 清理

清理顺序：

1. 优先向临时 control room 发送 `/archive <session_id>`，或直接用 Codex app-server 归档 dedicated thread。
2. 停止临时 sidecar 和 worker。
3. 删除 manifest 中记录且标题匹配 `<prefix>` 的 session/control/data rooms。
4. 删除临时 socket、env、state、logs；如果需要保留失败证据，则保留整个 `test_root` 并在报告中说明原因。
5. 再次检查生产 `/tmp/wxcd.sock` health，确认生产实例没有被干扰。

删除 Webex room 前必须同时满足：

- room id 存在于 manifest；
- room title 以本次 `<prefix>` 开头；
- room 是本轮测试创建的 control/data/session room。

## 结果报告

最终报告至少包含：

- Developer token `/people/me` 是否成功，email 是否匹配。
- 生产 worker 测试前后 health。
- 临时 worker health。
- 临时 control/data/session room 是否已删除。
- `list local` 是否包含 dedicated `thread_id`。
- `resume local <thread_id>` 创建的 `session_id` 和 session room 是否确认。
- Session room memberships 是否同时包含测试用户和 bot。
- `/history` 与 `/history page 2` 的 Webex 回复结果。
- 普通 session turn 的唯一 marker 是否出现在最终回复。
- `attach <session_id>` 是否在用户离开后成功重新加入。
- dedicated Codex thread 是否归档。
- 任何未清理资源的 id、标题、原因和建议处理方式。

## 已知风险

- 第二个临时 sidecar 使用同一个 bot token 监听 Webex。理论上生产 worker 会忽略不属于生产 control/session rooms 的测试事件；若发现 Mercury 状态异常或生产 health 失败，立即停止临时 sidecar。
- 临时 group room 用作 Data Space 只验证临时 worker 可运行，不改变当前长期部署依赖 bot-owner 1:1 direct room 的结论。
- Webex overview card 更新仍可能失败；本测试以普通消息、history 回复、membership 和 final summary 作为主要通过标准。
- `codex app-server` 对历史 thread 的 reload 仍有已知限制；本测试使用新建 dedicated local-only thread，避免复用旧失败 thread。
