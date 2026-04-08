下面是一份可以直接丢给 Codex 开工的 **v1 完整设计**。我按你的约束做了这些取舍：

* **一 个 Webex session space 绑定一个 Codex thread**
* **桥接层本地不落业务数据库**
* **业务真相尽量放在 Webex 远端**
* **本地只保留不可避免的执行态**：Codex 自己的线程持久化、当前发布版本、少量运行时缓存
* **无公网 IP 可用**
* **优先 Rust**
* **macOS 优先**
* **支持机器人通过 Webex 驱动本机 Codex 来更新本项目本身**

先说一个必须讲清楚的事实：
**你没法把“所有状态”都放到 Webex。** 因为 Codex app-server/CLI 自己就会把线程/转录持久化在本机；官方文档明确提供 `thread/list`、`thread/read`、`thread/resume` 和本地 transcript/resume 机制。桥接服务可以做到“无本地业务数据库”，但 **Codex 线程持久化仍然是本地执行基座的一部分**。这部分不应重复发明。([GitHub][1])

---

# 1. 目标

构建一个运行在 macOS 本机上的薄桥接器 `wxcd`，让 Webex 成为 Codex 的远程控制面：

* 在 **Control Space** 中创建/列出/归档 session
* 每个 **Session Space** 只绑定一个 Codex thread
* milestone / checkpoint / final 由 bot 发消息
* approval 通过 Adaptive Card 完成
* ongoing 仅做轻量状态标记
* 崩溃后可自动恢复
* 无公网 IP 环境可工作
* 可通过 Codex 自己修改、构建、滚动更新 `wxcd`

Webex Bot 可以在 1:1 或群组 space 中工作；Rooms、Messages、Memberships、Cards、Attachment Actions 都是官方支持的能力。([Webex for Developers][2])

---

# 2. 总体架构

## 2.1 组件

### A. `wxcd-supervisor`（Rust，极小、稳定）

职责：

* 由 `launchd` 拉起并常驻
* 启动/监控 `wxcd-worker`
* 做发布切换、健康检查、回滚
* 不参与业务逻辑
* 本地唯一“必须稳定”的控制进程

macOS 上适合用 `launchd` 管理常驻 agent/daemon；Apple 文档说明 `launchd` 负责管理 daemons/agents，可通过 `launchctl` 加载，适合做启动与保活。([Apple Support][3])

### B. `wxcd-worker`（Rust，主业务）

职责：

* 接收 Webex 事件
* 调用 Codex app-server
* 归并 agent 事件
* 发 milestone/final/approval
* 将业务元数据写回 Webex Data Space
* 启动时从 Webex Data Space 重建内存态

### C. `webex-ws-sidecar`（Node.js，极小）

职责：

* 只做一件事：用 **官方 Webex JavaScript SDK 的 websocket listener** 收消息/卡片提交事件
* 将事件转发到本机 `wxcd-worker`（Unix Domain Socket 或 localhost loopback）
* 不保存业务状态

之所以建议有这个 sidecar，是因为 Webex 官方当前把 websocket 监听能力放在 JS SDK 里，且明确说明它不需要 webhook 的 `targetURL`，适合没有公网 IP 的场景。官方博客还专门给出“监听 Webex 事件并转发到 localhost”的样例方向。([Webex for Developers][4])

### D. `codex app-server`（本地子进程）

`wxcd-worker` 通过 **stdio JSON-RPC** 与 `codex app-server` 通信。官方说明 app-server 的默认 transport 就是 `stdio`，并且它是为 rich client 提供认证、会话历史、审批、流式事件的正式接口。([OpenAI Developers][5])

---

# 3. 为什么不是 webhook

因为你的机器没有公网 IP。
Webex 的官方 websocket 方案正是为这种“应用在防火墙后面/没有可达 webhook URL”场景准备的。官方文档明确说 websocket listener 不需要 `targetURL`，事件通过 websocket 下发。([Webex for Developers][4])

因此 v1 设计结论：

* **入站**：Webex websocket
* **出站**：Webex REST API
* **不使用**：公网 webhook

---

# 4. 三类 Webex Space

## 4.1 Control Space

唯一控制面。命令：

* `new <repo> :: <task>`
* `list`
* `archive <session>`
* `help`
* `open <session>`（可选）

这里只做总览，不承载具体 agent 对话。

## 4.2 Session Space

每个 session 一个独立 space。
这里是用户真正与该 Codex thread 交互的地方：

* 普通文本 = 给该 thread 发新 turn
* `/status`
* `/resume`
* `/pause`
* `/stop`
* approval card
* milestone / final

## 4.3 Data Space

专门给 bot 存业务元数据。
这是桥接器的**远端真相源**，用于：

* room_id -> session_id -> thread_id 绑定
* 发布状态
* session 生命周期事件
* approval 决策日志
* checkpoint 摘要
* control space 索引
* 崩溃恢复时的重建输入

Webex Messages API 支持 list/create/update/delete，且消息可带文本、markdown、文件附件；Data Space 可以作为你的远端元数据日志。([Webex for Developers][6])

---

# 5. 真相模型

## 5.1 真相分层

### 远端权威（Webex Data Space）

用于桥接器业务语义：

* session 是否存在
* 哪个 session space 绑定哪个 Codex thread
* 当前 session 状态摘要
* 最近 checkpoint/final
* approval 是否待处理
* 发布版本意图
* 归档状态

### 本地权威（Codex）

用于 Codex 自身语义：

* thread transcript
* turn 历史
* agent 内部执行上下文
* thread persisted log

Codex app-server/CLI 已经提供本地 persisted thread 能力；`thread/list` / `thread/read` / `thread/resume` 是现成恢复接口。([GitHub][1])

### 本地极小执行态（Supervisor）

仅包括：

* 当前 release 目录
* `current` symlink
* worker pid / socket path
* 短暂健康检查文件或 socket
* 可选 rollback 指针

这不是业务数据库，只是程序运行必需状态。

---

# 6. Data Space 事件日志设计

采用 **append-only event log + 周期性 snapshot**。

## 6.1 格式

Data Space 中每条 bot 消息写成：

```text
WXCD/V1 EVENT <json>
```

json 示例：

```json
{
  "type": "session_created",
  "ts": "2026-04-08T16:00:00Z",
  "session_id": "ses_20260408_abc123",
  "control_room_id": "....",
  "session_room_id": "....",
  "thread_id": "thr_123",
  "repo": "/Users/me/src/foo",
  "owner_email": "user@example.com",
  "status": "idle"
}
```

再如：

```json
{
  "type": "approval_requested",
  "ts": "2026-04-08T16:03:00Z",
  "session_id": "ses_20260408_abc123",
  "thread_id": "thr_123",
  "turn_id": "turn_456",
  "approval_id": "apr_789",
  "kind": "command",
  "summary": "Run integration tests with network disabled"
}
```

## 6.2 Snapshot

每 N 个事件或每 M 分钟发一条：

```text
WXCD/V1 SNAPSHOT <json>
```

只保留最新几条 snapshot 即可。
worker 启动时：

1. 读取 Data Space 最近消息
2. 找到最新 snapshot
3. 回放其后的 events
4. 重建内存索引

## 6.3 为什么不用本地 SQLite

因为你的目标是：

* 崩溃恢复干净
* 尽量零本地业务状态
* 不想维护 migration / WAL / schema 升级

这个设计下，worker 可以做到“完全无本地业务库”。

---

# 7. Session 模型

## 7.1 业务主键

`session_id` 由桥接器生成，格式：

```text
ses_<yyyymmdd>_<6byte random base32>
```

## 7.2 绑定关系

一条 session 的核心关系：

```text
session_id
  -> session_room_id
  -> codex_thread_id
  -> repo_path
  -> owner_email
  -> status
```

## 7.3 状态机

```text
Creating
Idle
Running
WaitingApproval
Paused
Completed
Failed
Archived
```

状态变化只通过 Data Space 事件驱动。

---

# 8. Codex 接入协议

## 8.1 transport

使用 **stdio**。
理由：

* app-server 默认就是 stdio
* 单机通信最轻
* 不需要内部 websocket server
* Rust 侧直接管 child stdin/stdout 最省资源

官方 app-server 文档明确支持 `stdio` 和实验性的 websocket；v1 选默认 `stdio`。([OpenAI Developers][5])

## 8.2 初始化

worker 启动后：

1. spawn `codex app-server --listen stdio://`
2. 发送 `initialize`
3. 标记 `clientInfo.name = "wxcd-worker"`
4. 可开启 experimentalApi（若你要用更丰富 thread API 字段）

## 8.3 创建 session

在 Control Space 收到 `new` 后：

1. 创建 Session Space（Rooms API）
2. 邀请用户和 bot 进入 space（Memberships API）
3. 调用 `thread/start`
4. 记录 `thread.id`
5. 往 Data Space 写 `session_created`
6. 在 Session Space 发 overview 卡片

Rooms/Memberships 是官方 API；Rooms 用于创建 space，Memberships 用于邀请成员。([Webex for Developers][2])

## 8.4 继续 session

在 Session Space 中收到用户消息：

1. 由 `session_room_id` 查出唯一 `session_id`
2. 查出 `thread_id`
3. 若 thread 未加载，先 `thread/resume`
4. 再 `turn/start`

官方 app-server README 说明 `thread/start` 创建新线程，`thread/resume` 继续已有线程，`thread/read` 只读不恢复，`thread/list` 可做历史列表。([GitHub][1])

## 8.5 审批

当 app-server 发来 server-initiated approval request：

1. worker 生成 `approval_id`
2. Data Space 记一条 `approval_requested`
3. Session Space 发 Adaptive Card
4. 用户点击 approve/deny
5. sidecar 收到 `attachmentAction`
6. worker 调用 app-server 对应 approval response
7. Data Space 记 `approval_resolved`

官方 README 明确说明 approval 是 app-server 主动发给客户端的 JSON-RPC 请求，客户端必须回一个 decision；支持 `accept`、`acceptForSession`、`decline`、`cancel` 等。([GitHub][1])

---

# 9. milestone / ongoing / final 规则

## 9.1 ongoing

不刷消息流。只做两件事：

* 更新 session overview 卡片中的状态字段
* 仅在状态类别切换时发短消息

建议 emoji：

* `🟡` queued
* `🧠` planning
* `📖` reading
* `🛠` editing
* `▶` running
* `🧪` testing
* `🛑` approval
* `✅` completed
* `❌` failed
* `⏸` paused

## 9.2 milestone

符合以下条件才发消息：

* 从 planning 进入 concrete fix
* 发现 root cause
* 形成 edit plan
* 产生关键 diff
* 测试开始 / 结束
* 进入 checkpoint
* 需要 approval

格式：

```text
[S-xxxx] 🧠 Root cause confirmed: retry timer race in auth flow.
Next: patch timeout ownership and run targeted tests.
```

## 9.3 final

必须包含：

* 完成/失败
* 修改范围
* 测试结果
* 残余风险
* 下一步建议

---

# 10. Adaptive Card 设计

Webex Cards 要求发送卡片时带 fallback 文本；整条消息的 `text`/`markdown`/`attachments` 总长度有上限，卡片应保持小而明确。([Webex for Developers][7])

## 10.1 Session Overview Card

字段：

* Session ID
* Title
* Repo
* Thread ID
* State
* Last checkpoint
* Last updated
* Buttons:

  * Resume
  * Pause
  * Stop
  * Archive
  * Status

## 10.2 Approval Card

字段：

* Approval ID
* Requested action type
* Command preview / diff summary
* Why this is needed
* Scope selector:

  * Approve once
  * Approve for session
  * Deny
  * Cancel turn

## 10.3 Control List Card

列出最近 session：

* title
* owner
* state
* updated_at
* open room
* archive

---

# 11. 无公网 IP 的入站设计

## 11.1 推荐方案

`webex-ws-sidecar` 用 JS SDK：

* `messages.listen()`
* `attachmentActions.listen()`
* 可选 `rooms.listen()` / `memberships.listen()`

官方博客明确说明 websocket listeners 可以监听 messages、attachmentActions、rooms、memberships。([Webex for Developers][8])

## 11.2 sidecar 与 worker 的本地协议

使用 Unix Domain Socket：

* 路径：`/tmp/wxcd.sock`
* sidecar -> worker 发送 NDJSON
* worker 回复 200/ack

事件格式：

```json
{
  "source": "webex",
  "kind": "message.created",
  "event_id": "...",
  "room_id": "...",
  "person_email": "...",
  "message_id": "...",
  "text": "...",
  "ts": "..."
}
```

以及：

```json
{
  "source": "webex",
  "kind": "attachmentAction.created",
  "event_id": "...",
  "room_id": "...",
  "person_email": "...",
  "attachment_action_id": "...",
  "inputs": { ... },
  "ts": "..."
}
```

## 11.3 幂等

Webex 事件可能重试或重复到达；worker 必须以 `event_id` / `message_id` / `attachment_action_id` 去重。
因为你不想落本地 DB，去重集合只做短时内存缓存；长期幂等靠 Data Space 事件回放时的“已存在 session/approval 状态”检查。

---

# 12. 本地最小状态策略

## 12.1 允许存在的本地状态

只允许：

* Codex 自己的 thread persistence
* 当前运行 release
* supervisor/worker socket
* 短时内存缓存
* 日志文件（可选）

## 12.2 禁止存在的本地状态

不要有：

* 本地业务 SQLite/Postgres
* 本地 session registry
* 本地 approval ledger
* 本地 publish manifest 作为权威

## 12.3 重启恢复

worker 启动：

1. 连上 sidecar
2. 从 Data Space 拉最近消息
3. 找最新 snapshot
4. 重放事件，重建：

   * `room_id -> session_id`
   * `session_id -> thread_id`
   * `pending approvals`
   * `archived flags`
5. 对每个活跃 session：

   * 调用 `thread/read`
   * 必要时 `thread/resume`
   * 订阅后续事件
6. 更新 Session Space overview 卡片为“recovered”

`thread/read` 不会自动恢复线程，只读取 persisted thread；`thread/resume` 才加载回来。([GitHub][1])

---

# 13. repo / workspace 策略

v1 不做“一台机器上多 repo 并发调度器”，只做显式白名单。

配置文件：

```toml
[[repos]]
name = "wxcd"
path = "/Users/me/src/wxcd"

[[repos]]
name = "foo"
path = "/Users/me/src/foo"
```

每次 `new repo :: task` 时：

* 只允许命中配置白名单
* thread `cwd` 指向 repo path
* 每个 session 可选独立 git worktree

如果你要让多个 session 并行修改同一 repo，建议为 session 自动创建 worktree。Codex 官方桌面端明确强调 parallel threads 与 built-in worktree support；v1 桥接器虽然不必复刻全部能力，但这个隔离方向是对的。([OpenAI Developers][9])

---

# 14. 安全策略

Codex 本地默认网络关闭；审批/沙箱是两层控制。官方文档明确说明本地 CLI/IDE 默认使用 OS 级 sandbox，网络默认关闭，approval policy 控制需要停下来问用户的动作。([OpenAI Developers][10])

v1 建议：

* 默认 sandbox = workspace-write
* 默认 network_access = false
* approval policy = require approval for file writes outside workspace, network, dangerous commands
* 不启用“永远自动批准”
* approval card 仅支持：

  * once
  * for session
  * deny
  * cancel

---

# 15. macOS 部署设计

## 15.1 目录结构

```text
~/Library/Application Support/wxcd/
  releases/
    2026-04-08T16-30-00/
      wxcd-worker
      webex-ws-sidecar/
      static/
    2026-04-08T17-10-00/
      ...
  current -> releases/2026-04-08T17-10-00
  sockets/
  logs/
```

## 15.2 launchd

只让 `launchd` 盯住 `wxcd-supervisor`。

* `RunAtLoad = true`
* `KeepAlive = true`

Apple 文档说明 launchd 适合管理这类后台 agent/daemon，并通过 launchctl 装载。([Apple Support][3])

---

# 16. 滚动更新 / 自更新

这是关键。

## 16.1 原则

**不要让 worker 直接替换自己。**
采用 supervisor + release 目录 + 原子切换 symlink。

## 16.2 流程

在专门的 `wxcd-admin` Session Space 中，让 Codex 修改 `wxcd` repo。完成后：

1. 构建新版本到新 release dir
2. 运行最小 smoke test
3. 往 Data Space 写 `release_candidate_built`
4. supervisor 读取该事件
5. supervisor 启动新 worker（并保留旧 worker）
6. 健康检查通过后，切换 `current` symlink
7. 通知旧 worker drain 并退出
8. Data Space 写 `release_activated`
9. 若失败，回滚到旧 symlink，并写 `release_rolled_back`

## 16.3 健康检查

worker 启动后必须在 10 秒内完成：

* sidecar 连接正常
* Codex app-server initialize 成功
* Data Space 可读
* Control Space 可发一条内部 probe 或调用最小 API

## 16.4 为什么能自更新

因为 supervisor 不更新自己；被更新的是 worker 和 sidecar。
这使“系统通过自己更新自己”可行且稳定。

---

# 17. 命令设计

## 17.1 Control Space

### `new <repo> :: <task>`

行为：

* 创建 session room
* 邀请请求人 + bot
* `thread/start`
* 发 overview 卡
* Data Space 写 `session_created`

### `list`

行为：

* 从 Data Space 当前快照构造 session 列表
* 发列表卡片

### `archive <session_id>`

行为：

* `thread/archive`
* session room 改标题前缀 `[ARCHIVED]`
* Data Space 写 `session_archived`

app-server 支持 `thread/archive`；归档线程后不会出现在默认 `thread/list`，除非显式查 archived。([GitHub][1])

## 17.2 Session Space

### 普通文本

视为新 turn

### `/status`

读取：

* 内存摘要
* 必要时 `thread/read`

### `/resume`

若 thread notLoaded 或 paused：

* `thread/resume`

### `/pause`

v1 实现为：

* 若 turn 运行中，`turn/interrupt`
* 状态设为 Paused

### `/stop`

同 `/pause`，但额外在 Data Space 写 `session_stopped_by_user`

---

# 18. 推荐 repo 结构

```text
wxcd/
  Cargo.toml
  crates/
    wxcd-supervisor/
    wxcd-worker/
    wxcd-proto/
    wxcd-webex/
    wxcd-codex/
    wxcd-eventlog/
    wxcd-render/
  sidecars/
    webex-ws-sidecar/
  launchd/
    com.example.wxcd.supervisor.plist
  scripts/
    install-macos.sh
    smoke-test.sh
```

## 18.1 crate 说明

### `wxcd-proto`

共享类型：

* SessionState
* Event
* Snapshot
* ApprovalDecision
* RoomBinding

### `wxcd-webex`

REST client：

* create room
* create membership
* create/update message
* create card
* get attachment action details

### `wxcd-codex`

app-server JSON-RPC client：

* initialize
* thread_start
* thread_resume
* thread_read
* thread_list
* turn_start
* turn_interrupt
* approval_response

### `wxcd-eventlog`

Data Space 事件：

* append_event
* load_latest_snapshot
* replay_since_snapshot
* compact_snapshot

### `wxcd-render`

渲染：

* overview card JSON
* approval card JSON
* milestone text
* final summary text

### `wxcd-worker`

主状态机

### `wxcd-supervisor`

发布与保活

---

# 19. 事件驱动主循环

```rust
loop {
    select! {
        Some(webex_evt) = webex_inbox.recv() => handle_webex_event(webex_evt),
        Some(codex_evt) = codex_inbox.recv() => handle_codex_event(codex_evt),
        Some(timer_evt) = timers.recv() => handle_timer(timer_evt),
        else => break,
    }
}
```

## 19.1 `handle_webex_event`

* 判定 room 是 control 还是 session
* control:

  * parse command
* session:

  * parse slash command or plain text
* attachmentAction:

  * resolve approval

## 19.2 `handle_codex_event`

* 归并为高层状态
* 必要时发 milestone
* 若 approval request：

  * 生成卡片
* 若 turn complete：

  * 发 final / checkpoint
* 更新 Data Space

---

# 20. 恢复算法

## 20.1 冷启动

```text
1. start worker
2. load config
3. connect to sidecar UDS
4. init webex rest client
5. replay Data Space
6. spawn codex app-server
7. initialize app-server
8. reconcile active sessions:
   - thread/read
   - thread/resume if needed
9. rewrite overview cards
10. mark service healthy
```

## 20.2 reconcile 规则

如果 Data Space 说 session 活跃，但本地 `thread/read` 找不到：

* 将 session 标记 `Failed`
* 在 Session Space 发：

  * “remote metadata exists, but local Codex thread is missing”
* 这是唯一无法纯自动恢复的场景

这也是前面那句实话的含义：
**桥接层可以 remote-authoritative，但 Codex thread 本体不是 Webex 可替代的。**

---

# 21. 对“Data Space 当数据库”的边界判断

这个设计在 v1 是可行的，但要遵守三条约束：

1. **只存小型结构化元数据**
2. **append-only 为主，snapshot 辅助**
3. **不要把大日志/全量 diff/全量 turn transcript 塞进去**

原因：

* Webex Messages API 适合消息流和轻量元数据，不适合当大对象数据库
* 卡片消息有明确体积限制
* 你真正的大历史已经由 Codex 本地线程持久化承担了([Webex for Developers][7])

---

# 22. 实现优先级

## P0

* sidecar websocket ingress
* worker + codex app-server stdio
* Control Space / Session Space / Data Space
* `new` / `list` / plain message / approval / final
* Data Space replay recovery
* launchd + supervisor

## P1

* archive
* pause/stop
* overview card update
* self-update / rolling release
* worktree isolation

## P2

* snapshot compaction improvements
* multi-user moderation rules
* richer milestone heuristics
* web dashboard

---

# 23. 直接给 Codex 的实现任务单

把下面这段直接贴给 Codex 就行：

```text
Build v1 of a macOS-first Webex-to-Codex bridge called `wxcd`.

Constraints:
- One Webex session space maps to exactly one Codex thread.
- No local business database. Use a dedicated Webex Data Space as the authoritative event log for bridge metadata.
- Accept that Codex thread persistence itself remains local and should use Codex app-server/thread persistence directly.
- Support crash recovery by replaying Data Space snapshots/events and reconciling with local Codex threads.
- No public IP is available, so inbound Webex events must use Webex websocket listeners, not webhooks.
- Prefer Rust for all core components. A tiny Node.js sidecar is allowed only for Webex websocket ingress because official websocket support is in the Webex JS SDK.
- Use Codex app-server over stdio JSON-RPC.
- Support Control Space, Session Spaces, and one Data Space.
- milestone/checkpoint/final messages in Session Space.
- approval via Adaptive Cards.
- ongoing state should be lightweight, mostly status-card updates and occasional short messages.
- Must support rolling self-update on macOS via a tiny stable Rust supervisor plus replaceable worker releases.
- Must be deployable via launchd with KeepAlive/RunAtLoad.

Repository structure:
- crates/wxcd-supervisor
- crates/wxcd-worker
- crates/wxcd-proto
- crates/wxcd-webex
- crates/wxcd-codex
- crates/wxcd-eventlog
- crates/wxcd-render
- sidecars/webex-ws-sidecar

Implement:
1) typed event model for Data Space append-only log + snapshot
2) Webex REST client
3) Node sidecar using Webex JS SDK websocket listeners for messages and attachmentActions
4) Rust worker that consumes sidecar NDJSON over Unix socket
5) Rust Codex app-server client over stdio
6) control commands: new, list, archive
7) session commands: plain text -> turn/start, /status, /resume, /pause, /stop
8) approval flow end-to-end
9) recovery path from Data Space replay
10) supervisor-managed rolling upgrade with current symlink swap and rollback

Do not build a local sqlite/postgres business store.
Do not multiplex multiple sessions inside one Webex space.
Prefer simple, explicit code paths over abstraction-heavy design.
```

---

# 24. 我对这个 v1 的判断

这个方案的优点是：

* 架构简单
* 恢复路径清楚
* 无公网 IP 可行
* 本地状态极少
* approval 不容易串线
* 适合先做出能用的版本

它的主要限制是：

* Data Space 不是通用数据库，不能无限膨胀
* Codex thread 仍依赖本机持久化
* websocket ingress 现实上最好借 JS SDK，因此会有一个很薄的 Node sidecar

这是我认为最稳的工程折中。([Webex for Developers][4])

如果你要，我下一条可以继续把这个设计再压成 **更像 PRD + 接口文档** 的形式：包括 Data Space 事件 schema、Adaptive Card JSON、Rust trait 边界、以及 launchd plist 样例。

[1]: https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md "codex/codex-rs/app-server/README.md at main · openai/codex · GitHub"
[2]: https://developer.webex.com/docs/api/v1/rooms "Reference - Rooms | Webex for Developers"
[3]: https://support.apple.com/guide/terminal/script-management-with-launchd-apdc6c1077b-5d5d-4d35-9c19-60f2397b2369/mac?utm_source=chatgpt.com "Script management with launchd in Terminal on Mac"
[4]: https://developer.webex.com/blog/using-websockets-with-the-webex-javascript-sdk "Using Websockets with the Webex JavaScript SDK | Webex Developers Blog"
[5]: https://developers.openai.com/codex/app-server "App Server – Codex | OpenAI Developers"
[6]: https://developer.webex.com/docs/api/v1/messages "Reference - Messages | Webex for Developers"
[7]: https://developer.webex.com/docs/buttons-and-cards "Webex Messaging | Webex for Developers"
[8]: https://developer.webex.com/blog/using-websockets-with-the-webex-javascript-sdk?utm_source=chatgpt.com "Using Websockets with the Webex JavaScript SDK"
[9]: https://developers.openai.com/codex/app?utm_source=chatgpt.com "App – Codex | OpenAI Developers"
[10]: https://developers.openai.com/codex/agent-approvals-security "Agent approvals & security – Codex | OpenAI Developers"

