# Webex Commands

本文记录当前 Webex-Codex Bridge 的用户命令。群组空间里需要先 mention bot；1:1 或 session room 中可以直接发送命令。

## Control Room

- `help` 或 `/help`：显示命令帮助。
- `list` 或 `/list`：列出已由 bridge 管理的 session。
- `list local` 或 `/list local`：列出本机尚未 attach 到 Webex 的 Codex thread。
- `list local page <n>` 或 `/list local page <n>`：分页查看 local-only Codex thread。
- `list all` 或 `/list all`：同时列出 bridge session 和 local-only thread。
- `attach <session_id>` 或 `/attach <session_id>`：把发命令的用户重新加入指定 session room。
- `resume local <thread_id>` 或 `/resume local <thread_id>`：把已有本机 Codex thread attach 到新的 Webex session room。
- `new <repo> :: <task>` 或 `/new <repo> :: <task>`：在配置的 repo 中新建 Codex thread 和 Webex session room。
- `archive <session_id>` 或 `/archive <session_id>`：归档 session；healthy session 仍要求 Codex thread archive 成功，failed session 会 best-effort archive 本地 thread 后归档 bridge 状态。

## Recovery Cleanup

- `diagnose sessions` 或 `/diagnose sessions`：汇总 session 总数、failed 数量、archived 数量，并列出 failed session 的原因和建议操作。
- `diagnose <session_id>` 或 `/diagnose <session_id>`：重新探测单个 session 的本地 Codex thread 状态，并在确认 missing/unreadable 时更新 failure metadata。
- `cleanup failed` 或 `/cleanup failed`：预览所有可 soft-archive 的 failed session，不修改状态。
- `cleanup failed <session_id>` 或 `/cleanup failed <session_id>`：soft-archive 单个 failed session，保留 Webex room。
- `cleanup failed all` 或 `/cleanup failed all`：soft-archive 所有 failed session，保留对应 Webex rooms。
- `purge archived <session_id>` 或 `/purge archived <session_id>`：只显示删除预览和确认命令，不删除 Webex room。
- `purge archived <session_id> confirm` 或 `/purge archived <session_id> confirm`：删除已 archived session 的 Webex room，并从 bridge active state 中移除该 session。该命令不可批量执行。

## Session Room

- `help` 或 `/help`：显示命令帮助。
- 普通文本：向当前 Codex thread 提交一个新 turn。
- `/status`：显示当前 session 摘要。
- `/history`：显示最新一页历史 turn。
- `/history page <n>`：分页回看更旧的历史 turn。
- `/resume`：恢复当前 Codex thread。
- `/pause` 或 `/stop`：中断当前 active turn，并把 session 标记为 paused。

## Safety Notes

- `cleanup failed ...` 是 soft archive，不删除 Webex room。
- `purge archived ... confirm` 是 destructive 操作，只允许已 archived session，并且没有 `all` 形式。
- `diagnose <session_id>` 不会改写 archived session；已归档 session 只允许走 `purge archived <session_id>` 预览。
- 如果本地 Codex thread 只是暂时不可探测，启动 reconcile 不会因为 `thread/list` probe 失败而批量标记 session failed。
