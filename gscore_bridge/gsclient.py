"""gsuid-core WS 客户端: 连接维护 + 上报队列 + 下发分发 + 路由缓存。

关键约定 (均为实测高频事故点):
- 发送**二进制帧** (model_dump_json().encode("utf-8")); core 端点
  receive_bytes() 只收二进制帧, 文本帧会导致 core 抛 KeyError: 'bytes'
  后断开 ("连上几秒就断")。
- 日志回显包按**内容**识别 (content[0].type 以 log 开头), 禁止用
  bot_id == 连接BOT_ID 判断——BOT_ID 误填平台名时正常回复会被当日志丢弃。
- 下行路由: 优先用上一条上行消息缓存的真实 chat_key; 未命中时按
  adapter_key + target_type/target_id 拼接 NA 标准 chat_key。
"""

import asyncio
import hashlib
from contextlib import suppress
from pathlib import Path
from typing import Awaitable, Callable, Optional
from urllib.parse import quote

from nekro_agent.adapters.interface.schemas.platform import PlatformSendRequest
from nekro_agent.adapters.utils import adapter_utils
from nekro_agent.core.logger import get_sub_logger
from websockets.asyncio.client import ClientConnection, connect as ws_connect
from websockets.exceptions import ConnectionClosed

from .converter import gs_to_platform_segments, materialize_voice
from .models import GsMessage, MessageReceive, MessageSend

logger = get_sub_logger("plugin.gscore_bridge")

RECONNECT_INTERVAL = 5  # 秒
RouteKey = tuple[str, str, str]  # (adapter_key, target_type, target_id)


def adapter_key_from_bot_id(bot_id: str) -> str:
    return "onebot_v11" if bot_id in {"onebot", "aiocqhttp"} else bot_id


def target_id_from_receive(message: MessageReceive) -> Optional[str]:
    return message.user_id if message.user_type == "direct" else message.group_id


def build_chat_key(adapter_key: str, target_type: Optional[str], target_id: str) -> str:
    """按 NA 标准格式拼 chat_key: onebot_v11-group_123 / onebot_v11-private_456。"""
    kind = "group" if target_type == "group" else "private"
    return f"{adapter_key}-{kind}_{target_id}"


class GsCoreSettings:
    """连接参数快照; 由插件从最新配置生成 (见 plugin._current_settings)。"""

    def __init__(self, *, bot_id: str, host: str, port: str, ws_token: str, max_retry: int) -> None:
        self.bot_id = bot_id
        self.host = host
        self.port = str(port)
        self.ws_token = ws_token
        self.max_retry = max_retry


class GsCoreClient:
    def __init__(
        self,
        *,
        settings_provider: Callable[[], GsCoreSettings],
        temp_dir: Path,
        media_url_hint: str = "",
    ) -> None:
        # 连接参数不再固化: 每次重连时通过 provider 读取最新配置,
        # WebUI 改完 host/port/token 保存后, 下一次重连自动生效 (无需重启)。
        self._settings_provider = settings_provider
        self.temp_dir = temp_dir
        self.media_url_hint = media_url_hint
        self._ws: Optional[ClientConnection] = None
        self._queue: asyncio.Queue[MessageReceive] = asyncio.Queue()
        self._supervisor: Optional[asyncio.Task] = None
        self._running = False
        self._route_map: dict[RouteKey, str] = {}

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.state is not None and not getattr(self._ws.state, "name", "").startswith("CLOSED")

    @property
    def is_running(self) -> bool:
        return self._supervisor is not None and not self._supervisor.done()

    def _settings(self) -> GsCoreSettings:
        """每次调用读取最新配置 (WebUI 保存后下一次重连即生效)。"""
        return self._settings_provider()

    async def start(self) -> None:
        if self.is_running:
            s = self._settings()
            logger.info(f"GsCore 客户端已在运行: {s.host}:{s.port}, bot_id={s.bot_id}")
            return
        self._running = True
        self._supervisor = asyncio.create_task(self._run())
        s = self._settings()
        logger.info(
            f"GsCore 客户端后台任务已启动: endpoint={s.host}:{s.port}, "
            f"bot_id={s.bot_id}, token_set={bool(s.ws_token)}, max_retry={s.max_retry}"
        )

    async def stop(self) -> None:
        s = self._settings()
        logger.info(f"GsCore 客户端停止中: bot_id={s.bot_id}")
        self._running = False
        if self._supervisor is not None:
            self._supervisor.cancel()
            with suppress(asyncio.CancelledError):
                await self._supervisor
            self._supervisor = None
        if self._ws is not None:
            with suppress(Exception):
                await self._ws.close()
            self._ws = None
        logger.info(f"GsCore 客户端已停止: bot_id={s.bot_id}")

    async def report(self, message: MessageReceive) -> None:
        if not self.is_running:
            logger.warning(f"GsCore 客户端未运行，消息丢弃: msg_id={message.msg_id}")
            return
        await self._queue.put(message)
        if not self.is_connected:
            logger.info(f"GsCore 尚未连接，消息已入队等待: msg_id={message.msg_id}, queue={self._queue.qsize()}")

    def remember_route(self, message: MessageReceive, chat_key: str) -> None:
        target_id = target_id_from_receive(message)
        if not target_id:
            return
        route_key = (adapter_key_from_bot_id(message.bot_id), message.user_type, str(target_id))
        self._route_map[route_key] = chat_key
        logger.debug(f"GsCore 路由缓存: {route_key} -> {chat_key}")

    # ------------------------------------------------------------------
    async def _run(self) -> None:
        retry = 0
        last_sig: Optional[GsCoreSettings] = None
        try:
            while self._running:
                s = self._settings()
                # 连接参数变化 (host/port/token/bot_id) 时重置退避计数
                if last_sig is None or (s.host, s.port, s.bot_id, s.ws_token) != (
                    last_sig.host, last_sig.port, last_sig.bot_id, last_sig.ws_token
                ):
                    retry = 0
                    last_sig = s
                try:
                    ws = await self._connect(s)
                except Exception as e:
                    retry += 1
                    if s.max_retry != -1 and retry > s.max_retry:
                        logger.error(f"GsCore 已达最大重试次数 ({s.max_retry})，停止重连")
                        break
                    logger.warning(f"GsCore 连接失败: {e}，{RECONNECT_INTERVAL}s 后重试 (第 {retry} 次)")
                    await asyncio.sleep(RECONNECT_INTERVAL)
                    continue
                retry = 0
                await self._serve(ws)
                if self._running:
                    logger.warning(f"GsCore 连接断开: bot_id={s.bot_id}")
                    await asyncio.sleep(RECONNECT_INTERVAL)
        finally:
            self._running = False
            logger.info("GsCore 连接守护任务退出")

    async def _connect(self, s: GsCoreSettings) -> ClientConnection:
        ws_url = f"ws://{s.host}:{s.port}/ws/{s.bot_id}"
        headers = None
        if s.ws_token:
            # token 必须 URL 编码: 含 #/空格/%/+ 等字符时裸拼会截断或变形;
            # query 与 Authorization header 双带, 兼容不同校验实现。
            ws_url += f"?token={quote(s.ws_token, safe='')}"
            headers = {"Authorization": f"Bearer {s.ws_token}"}
        # token 指纹: 不打印明文, 用长度+sha256前8位核对两端是否一致
        # (core 端可在 config.json 同算法计算比对; 403 时先核对指纹再查封禁)
        token_fp = hashlib.sha256(s.ws_token.encode("utf-8")).hexdigest()[:8] if s.ws_token else "-"
        logger.info(
            f"连接 GsCore: ws://{s.host}:{s.port}/ws/{s.bot_id}, "
            f"token_set={bool(s.ws_token)}, token_len={len(s.ws_token)}, token_fp={token_fp}"
        )
        # 连接前 TCP 预检: 把"网络不通"和"WS 协议层拒绝"分开诊断
        # (WSL NAT 场景下: 超时=防火墙静默丢包/网关IP变了/gscore 未监听 0.0.0.0;
        #  refused=gscore 未启动或只绑 127.0.0.1; TCP 通但 WS 失败=token/封禁问题)
        try:
            _, _writer = await asyncio.wait_for(
                asyncio.open_connection(s.host, int(s.port)), timeout=5
            )
            _writer.close()
            with suppress(Exception):
                await _writer.wait_closed()
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"TCP {s.host}:{s.port} 连接超时(5s无响应) —— 网络层不通: "
                "请检查 ① GSCORE_HOST 是否为 WSL 网关IP(在WSL执行 ip route show default 确认, "
                "重启WSL/Windows后该IP可能变化) ② gscore config.json 的 HOST 是否为 0.0.0.0 并已重启 "
                "③ Windows 防火墙是否放行 8765 入站(静默丢包表现为超时)"
            ) from None
        except OSError as e:
            raise RuntimeError(
                f"TCP {s.host}:{s.port} 不可达: {e} —— 请检查 gscore 是否启动、"
                "HOST=0.0.0.0、GSCORE_HOST 是否填成了 WSL 自身IP(应为网关IP)"
            ) from e
        ws = await ws_connect(
            ws_url,
            max_size=2**26,
            open_timeout=10,
            ping_timeout=20,
            additional_headers=headers,
        )
        self._ws = ws
        logger.info(f"GsCore 连接成功: bot_id={s.bot_id}")
        return ws

    async def _serve(self, ws: ClientConnection) -> None:
        send_task = asyncio.create_task(self._send_loop(ws))
        try:
            async for raw in ws:
                try:
                    await self._handle_packet(raw)
                except Exception as e:
                    logger.exception(f"处理 GsCore 下行包失败: {e}")
        except ConnectionClosed as e:
            logger.warning(f"GsCore WebSocket 已关闭: code={e.code}, reason={e.reason or '-'}")
        except Exception as e:
            logger.exception(f"GsCore 连接循环异常: {e}")
        finally:
            send_task.cancel()
            with suppress(asyncio.CancelledError):
                await send_task
            if self._ws is ws:
                self._ws = None

    async def _send_loop(self, ws: ClientConnection) -> None:
        while True:
            message = await self._queue.get()
            try:
                # 二进制帧! core receive_bytes() 不接受文本帧
                payload = message.model_dump_json().encode("utf-8")
                await ws.send(payload)
                logger.debug(
                    f"已上行 GsCore: msg_id={message.msg_id}, bot_id={message.bot_id}, "
                    f"user_type={message.user_type}, segs={len(message.content)}, bytes={len(payload)}"
                )
            except Exception:
                logger.exception(f"发送 GsCore 失败，消息重新入队: msg_id={message.msg_id}")
                await self._queue.put(message)
                return

    # ------------------------------------------------------------------
    async def _handle_packet(self, raw: "str | bytes") -> None:
        message = MessageSend.model_validate_json(raw)
        first_type = message.content[0].type if message.content else None
        logger.info(
            f"【GsCore 下行】bot_id={message.bot_id}, target={message.target_type}/{message.target_id}, "
            f"segs={len(message.content or [])}, first={first_type}, echo={bool(message.echo)}"
        )

        # 日志回显包: 按内容识别 (不能按 bot_id 判断!)
        if message.content and first_type and str(first_type).startswith("log"):
            log_level = str(first_type).split("_")[-1].lower()
            text = str(message.content[0].data or "")
            getattr(logger, log_level if hasattr(logger, log_level) else "info", logger.info)(
                f"[gsuid-core] {text}"
            )
            return

        # 控制包 (NA 通用链路暂不支持)
        if message.content and len(message.content) == 1:
            ctype = message.content[0].type
            if ctype in ("excute_delete_message", "excute_ban_user"):
                logger.warning(f"GsCore 控制包 {ctype} 暂不支持, 已忽略")
                return

        recall_id: Optional[str] = None
        try:
            recall_id = await self._send_to_platform(message)
        finally:
            if message.echo:
                await self._send_recall_receipt(message, recall_id)

    async def _send_to_platform(self, message: MessageSend) -> Optional[str]:
        if not message.target_id or not message.content:
            logger.warning(f"GsCore 下行缺少 target_id/content, 跳过: {message.target_id}")
            return None

        adapter_key = adapter_key_from_bot_id(message.bot_id)
        route_key = (adapter_key, str(message.target_type or ""), str(message.target_id))
        chat_key = self._route_map.get(route_key)
        if chat_key:
            logger.info(f"GsCore 下行命中路由缓存: {chat_key}")
        else:
            chat_key = build_chat_key(adapter_key, message.target_type, str(message.target_id))
            logger.info(f"GsCore 下行未命中缓存, 拼接 chat_key: {chat_key} (回复前需该会话存在)")

        segments = await gs_to_platform_segments(
            message.content, self.temp_dir, self.media_url_hint, adapter_key
        )

        has_record = any(c.type == "record" for c in (message.content or []))
        if not segments:
            # OneBot v11 的 record 段不进标准段, 语音-only 下行包在此直接发送
            if adapter_key == "onebot_v11" and has_record:
                if await self._send_onebot_voices(message, chat_key):
                    return "voice"
            logger.info(f"GsCore 下行转换为空, 跳过发送: {chat_key}")
            return None

        try:
            adapter = adapter_utils.get_adapter(adapter_key)
        except Exception as e:
            logger.error(f"NA 适配器 {adapter_key} 未就绪: {e}")
            return None

        # 引用段: gscore reply 段的 data 为我们上行的平台 msg_id
        ref_msg_id = None
        for seg in message.content:
            if seg.type in ("reply", "reply_id") and isinstance(seg.data, str) and seg.data.isdigit():
                ref_msg_id = seg.data
                break

        try:
            response = await adapter.forward_message(
                PlatformSendRequest(chat_key=chat_key, segments=segments, ref_msg_id=ref_msg_id)
            )
        except Exception as e:
            logger.exception(f"转发到 NA 平台失败 ({chat_key}): {e}")
            return None
        if not response.success:
            logger.error(f"NA 平台发送失败 ({chat_key}): {response.error_message}")
            return None

        # 语音在普通消息之后发送 (多数插件的卡片与语音本就是分开的两个下行包)
        if adapter_key == "onebot_v11":
            await self._send_onebot_voices(message, chat_key)
        return response.message_id

    async def _send_onebot_voices(self, message: MessageSend, chat_key: str) -> bool:
        """把 gscore record 段通过 OneBot v11 record 段发送为真正的语音消息。

        NA 标准发送结构只有 TEXT/AT/IMAGE/FILE, 无法表达语音; 故此处直接调用
        协议端 send_group_msg / send_private_msg。音频以 base64 内联 (与 NA
        发图片同机制), 不依赖共享卷路径, 任何 OneBot v11 协议端均可接收;
        是否能播放取决于协议端 mp3 支持 (NapCat/Lagrange 自动转 silk;
        老 go-cqhttp 仅收 silk/amr, 属协议端限制)。
        """
        records = [c for c in (message.content or []) if c.type == "record"]
        if not records:
            return False

        from nonebot.adapters.onebot.v11 import Message as OBMessage
        from nonebot.adapters.onebot.v11 import MessageSegment as OBMS

        from nekro_agent.adapters.onebot_v11.core.bot import get_bot

        is_group = str(message.target_type or "") == "group"
        try:
            target = int(str(message.target_id))
        except (TypeError, ValueError):
            logger.warning(f"语音发送跳过, target_id 非法: {message.target_id}")
            return False

        bot = get_bot()
        sent = False
        for rec in records:
            path = await materialize_voice(str(rec.data or ""), self.temp_dir)
            if not path:
                logger.warning("record 语音落盘失败, 跳过该语音段")
                continue
            # 与 NA 发送图片同一机制 (adapter._send_message 中
            # MessageSegment.image(file=path.read_bytes())): NoneBot 会把 bytes
            # 编码为 base64:// 内联在 WS 报文中, 协议端无需访问 NA 本地文件,
            # 因此不依赖共享卷/挂载路径 (Windows 独立部署、Docker 均通用)。
            try:
                audio_bytes = await asyncio.to_thread(Path(path).read_bytes)
            except OSError as e:
                logger.warning(f"语音文件读取失败 {path}: {e}")
                continue
            ob_message = OBMessage(OBMS.record(file=audio_bytes))
            try:
                if is_group:
                    await bot.call_api("send_group_msg", group_id=target, message=ob_message)
                else:
                    await bot.call_api("send_private_msg", user_id=target, message=ob_message)
                sent = True
                logger.info(f"OneBot 语音已发送: {chat_key}, file={Path(path).name}")
            except Exception as e:
                logger.exception(f"OneBot 语音发送失败 ({chat_key}): {e}")
        return sent

    async def _send_recall_receipt(self, message: MessageSend, recall_id: Optional[str]) -> None:
        await self.report(
            MessageReceive(
                bot_id=message.bot_id,
                bot_self_id=message.bot_self_id,
                user_id="",
                content=[GsMessage(type="recall_message_id", data={"echo": message.echo, "id": recall_id})],
            )
        )
