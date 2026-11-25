# astrbot_plugin_activity_watcher

用户活动追踪插件：让 agent 感知目标用户在**所有群 + 私聊**的活动，沉淀记忆，并进行有时效性的主动关心。

## 功能

- **活动采集**：记录所有群聊/私聊消息文本（3 天滚动保留，只存本地 SQLite），
  支持按 `focus_users` 白名单限定统计/触发范围
- **prompt 注入**：`on_llm_request` 注入"近期活动观察"（24h 统计 + 最近 N 条 + 昨日摘要 + 进行中话题），
  带节流（每会话 10 分钟一次）；更早信息由 agent 按需调用工具查询
- **每日记忆总结**：每天定时（默认 04:30）用 LLM 总结当日重要信息，可选推送到
  livingmemory External API；顺带 LLM 复核进行中话题是否已自然结束
- **主动触发引擎**：
  - 深夜未睡（urgent）→ 30 分钟内触达，过期作废
  - 生病/不适（normal）→ 当日触达，默认每 24h 低频重复直到话题收口
  - 兴趣由头 → 放宽时间，触达 1-2 次收口
  - 触达内容走 provider 生成（含 persona_hint）；人设一致性建议参考下文
  - 已触达事件标记 `delivered`，同 kind 去重，防止反复触发
- **话题生命周期**：`topics` 表管理 open/closed，收口路径：关键词（"好多了"等）/
  每日 LLM 复核 / 管理员指令 / 7 天超时兜底；收口写摘要进记忆
- **上下文还原**：`query_user_activity` 工具可为每条发言附带前后 N 条对话语境，
  复读/刷屏自动合并（`text ×N`）
- **集成**：
  - livingmemory：每日/收口摘要经 External HTTP API 写入（需开 `external_api.enabled` + `/lmem apikey`）
  - private_companion：通过官方 `extension_api.register_memory_provider` 注册活动摘要 Provider
  - proactive_chat：无需配置，其主动会话中 agent 可调用本插件工具取"由头"
  - **注意**：private_companion 的主动引擎无第三方触发接口，时效性触达由本插件负责

## 指令

| 指令 | 权限 | 说明 |
|---|---|---|
| `/aw_status` | 所有人 | 查看自己的活动/话题/待触达统计 |
| `/aw_close [uid]` | 管理员 | 手动关闭话题 |

## Agent 工具

- `query_user_activity(user_id?, hours?, limit?, with_context?)` — 查询活动（可带语境）
- `suggest_topic(user_id?)` — 获取主动会话话题由头（含时效性标注）

## 安装

放入 AstrBot `data/plugins/astrbot_plugin_activity_watcher/` 后在 WebUI 重载即可，无额外依赖。

## 注意

- `qq_official`（QQ 官方开放平台）不支持主动发消息（`context.send_message` 限制），
  该平台下主动触达不可用，其余功能正常；建议使用 aiocqhttp（NapCat/Lagrange）接入。
- 隐私：为还原上下文会记录所有成员的文本消息（仅本地，3 天滚动删除）。
- 人设一致性：主动触达内容由 LLM 生成；若装有 private_companion，其人设体系经
  主链生效，本插件的 persona_hint 仅兜底。可在 `_generate_greeting` 中按需改为主链生成。
