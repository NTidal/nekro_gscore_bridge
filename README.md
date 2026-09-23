# GsCore 桥接 (nekro_gscore_bridge)

NekroAgent 插件：把 NA 已接入平台（OneBot v11 / SnowLuma 等）的消息**双向桥接**到
[gsuid_core / SayuCore](https://github.com/GsChiKit/gsuid_core)，让 core 侧的签到、体力查询、
图鉴等游戏工具箱插件在 NA 托管的群里照常工作，结果回传到 QQ。

```
[QQ] <-> [SnowLuma] <-OneBot v11-> [NekroAgent + 本插件] <-gscore WS-> [gsuid_core]
```

版本：**2.1.4**　作者：**NTidal**

> 为什么需要这个插件：SnowLuma 仅实现 OneBot v11 协议，不能直连 gscore 的 `/ws/<BOT_ID>`
> 端点（该端点要求 gscore `MessageReceive` 二进制帧，ob11 文本帧会让 core 抛
> `KeyError: 'bytes'` 后断开）。本插件在 NA 内完成协议翻译与消息段互转。

---

## 功能

- 🔁 **双向桥接**：平台消息 → gscore（`MessageReceive` 二进制帧上行），core 下发 → 平台（`MessageSend` 下行）
- 🧩 **消息段互转**：文本 / @ / 图片 / 文件 / 语音 / 视频 / 引用 / 合并转发 / 戳一戳
- 🖼️ **图片链路**：远程 URL 直传（core 可直连公网时）；NA 本地图片经**内置图床**托管给 core；
  引用消息与合并转发里的图片用 OneBot `get_msg` / `get_forward_msg` 重取 CDN 直链（规避预处理覆写）
- 🎙️ **语音直发**：core 下发的 `record` 语音经 OneBot v11 `record` 段直接发送为真正的语音消息
  （base64 内联，不依赖共享卷；NapCat / Lagrange 自动转 silk）
- 👤 **头像上报**：腾讯 qlogo 公开直链；GsCore 纯内网时可经内置图床中转
- 🔀 **两种工作模式**：全量桥接（所有消息转发给 core）/ 混合模式（仅指令前缀消息走 core，其余由 NA LLM 处理）
- 🔐 **权限映射**：NA `SUPER_USERS` 自动映射为 gscore `user_pm=1`，支持额外超级用户列表
- 🛠️ **LLM 运维工具**：`gscore_status`（查连接状态）、`gscore_reconnect`（改配置后重连），AI 可自主调用并向用户汇报
- 📡 **连接守护**：断线 5 秒自动重连（可配次数上限）、配置热生效（改地址/端口/token 后下次重连即生效，无需重启）

---

## 前置条件

1. 已部署 NekroAgent，并接入 OneBot v11 协议端（如 NapCat / Lagrange）。
2. gsuid_core / SayuCore 已运行，且 `config.json` 中 `HOST` 设为 `0.0.0.0`。
3. 网络可达：NA（容器）与 core 之间能互相访问——
   - NA 在 Docker、core 在宿主机：`GSCORE_HOST` 填宿主机 IP 或 `host.docker.internal`；
   - NA 在 WSL、core 在 Windows：`GSCORE_HOST` 填 WSL 网关 IP（在 WSL 执行 `ip route show default` 查看，
     **重启 WSL / Windows 后该 IP 可能变化**）。

---

## 安装

1. 将 `gscore_bridge/` 整个目录放入 NekroAgent 插件目录（`plugins/workdir/`），或在 NA WebUI 打包上传。
2. **完全重启 NekroAgent**。
3. 在插件配置中填写：
   - `GSCORE_HOST` / `GSCORE_PORT`：core 的地址与端口（默认 8765）；
   - `WS_TOKEN`：与 core `config.json` 的 `WsToken` 完全一致（core 未设则留空）；
   - `BOT_SELF_ID`：机器人 QQ 号（留空则自动从适配器获取）。
4. 重启后观察日志出现 `GsCore 连接成功: bot_id=...` 即接通；
   群里发送 core 支持的指令（如 `签到`）验证全链路。

> 提示：混合模式下不要把 `GSCORE_COMMAND_PREFIXES` 里的前缀填进 NA 系统配置的
> 『忽略的消息前缀』——那会在适配器入口直接丢弃消息，插件收不到。

---

## 配置项

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `ENABLED` | `true` | 总开关：关闭后不连接 core，也不转发消息 |
| `GSCORE_HOST` | `127.0.0.1` | gscore WS 地址（Docker/WSL 场景见前置条件） |
| `GSCORE_PORT` | `8765` | gscore WS 端口 |
| `GSCORE_BOT_ID` | `NekroAgent` | 连接路径标识（`ws://IP:PORT/ws/<BOT_ID>`），任意英文字符串，**不要填平台名** |
| `BOT_SELF_ID` | 空 | 上报给 core 的机器人自身平台 ID（QQ 号）；留空自动获取 |
| `WS_TOKEN` | 空 | 与 core `WsToken` 完全一致；留空不校验 |
| `MAX_RETRY` | `-1` | 最大重连次数；`-1` 表示无限重连 |
| `BRIDGE_ALL` | `true` | 自动桥接全部消息（core 前缀/关键词插件依赖此行为）；关闭后仅手动转发 |
| `BRIDGE_ONLY_TOME` | `false` | 仅转发 @机器人 / 私聊 / is_tome 的消息 |
| `BLOCK_LLM` | `true` | 转发的消息阻止 NA LLM 响应（由 GsCore 接管对话） |
| `HYBRID_MODE` | `false` | 混合模式：仅命中指令前缀的消息走 GsCore，其余放行给 NA LLM |
| `GSCORE_COMMAND_PREFIXES` | `["core"]` | 混合模式的指令前缀列表，如 `["core", "gs", "体力"]` |
| `HYBRID_BRIDGE_TOME` | `false` | 混合模式下 @机器人 / 私聊消息是否也转发 GsCore |
| `IMAGE_PASSTHROUGH_URL` | `true` | 远程图片 URL 直接传给 core（要求 core 能访问该 URL） |
| `IMAGE_BASE_URL` | 空 | 内置图床对外前缀；留空自动取本插件实际路由地址（跨机部署需填 core 可达的地址） |
| `AVATAR_VIA_IMAGE_HOST` | `false` | QQ 头像经内置图床中转（适用于 GsCore 无法出公网的场景） |
| `GSCORE_SUPER_USER_IDS` | 空 | 额外的 gscore 超级用户（`user_pm=1`）列表 |
| `MAP_NA_SUPER_USERS` | `true` | NA `SUPER_USERS` 自动映射为 gscore `user_pm=1` |

---

## 工作模式

**全量桥接（`BRIDGE_ALL=true`，默认）**

所有群/私聊消息都转发给 core，core 侧插件按各自前缀/关键词响应；
`BLOCK_LLM=true` 时这些消息不触发 NA 的 LLM，由 core 接管。

**混合模式（`HYBRID_MODE=true`）**

只有文本以 `GSCORE_COMMAND_PREFIXES` 任一前缀开头（或 `HYBRID_BRIDGE_TOME` 开启时的 @机器人/私聊）
才转发 core 并阻止 LLM；其余消息不转发、正常由 NA 大模型处理。适合不想让 core 干扰日常对话的场景。

**LLM 运维工具**

模型可自主调用两个工具（结果由 AI 向用户汇报）：

- `gscore_status()`：查询桥接状态（是否启用、运行中、已连接、地址）；
- `gscore_reconnect()`：重连 core（修改连接配置后调用）。

对 AI 说“看看 gscore 连上没有”即可触发。

---

## 常见问题

**连接超时（5 秒无响应）**
网络层不通，依次检查：① `GSCORE_HOST` 是否为 WSL 网关 IP（`ip route show default`，重启后可能变化）；
② core `HOST` 是否为 `0.0.0.0` 并已重启；③ Windows 防火墙是否放行 8765 入站（静默丢包表现为超时）。

**连接被拒绝（refused）**
core 未启动，或只绑定了 `127.0.0.1`；或 `GSCORE_HOST` 填成了 WSL 自身 IP（应为网关 IP）。

**403 / token 校验失败**
`WS_TOKEN` 与 core `config.json` 不一致。日志会输出 token 长度与 SHA-256 前 8 位指纹，
可在 core 侧用同算法核对两端是否一致，先对指纹再查封禁。

**core 收不到图片 / 识图插件报错**
图片优先以 URL 上行，要求 core 能访问该 URL。跨机部署时务必把 `IMAGE_BASE_URL`
填成 core 可达的本插件地址；core 无法出公网时开启 `AVATAR_VIA_IMAGE_HOST` 中转头像。

**语音发不出 / 发出后不能播放**
发送依赖 OneBot v11 协议端的 `record` 支持：NapCat / Lagrange 会自动把 mp3 转 silk；
老版 go-cqhttp 仅收 silk/amr，属协议端限制。

**桥接的消息 AI 不回复**
`BLOCK_LLM=true` 的设计行为——这些消息由 GsCore 接管。若希望 AI 也响应，关闭该开关或改用混合模式。

---

## 设计要点（给维护者）

- **上行必须二进制帧**：core `receive_bytes()` 只收二进制帧，文本帧会导致 `KeyError: 'bytes'` 断连；
- **日志回显包按内容识别**（`content[0].type` 以 `log` 开头），不能按 `bot_id` 判断——BOT_ID 误填平台名时正常回复会被当日志丢弃；
- **下行路由**：优先用上一条上行消息缓存的真实 `chat_key`，未命中时按 `adapter_key + target_type/target_id` 拼接 NA 标准 chat_key；
- **转发失败不吞消息**：core 不可达时放行给 NA LLM，避免用户消息凭空消失。

---

## 版权

MIT
