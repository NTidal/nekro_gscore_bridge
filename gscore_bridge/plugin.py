"""GsCore 桥接插件主入口: 配置 / 生命周期 / 消息钩子 / 图床 / 运维工具。

拓扑: [QQ] <-> [SnowLuma] <-OneBot v11-> [NekroAgent + 本插件] <-gscore WS-> [gsuid-core]
"""

import asyncio
import re
import shutil
import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import Field

from nekro_agent.adapters.utils import adapter_utils
from nekro_agent.api.plugin import ConfigBase, NekroPlugin, SandboxMethodType
from nekro_agent.api.schemas import AgentCtx
from nekro_agent.api.signal import MsgSignal
from nekro_agent.core.config import config as core_config
from nekro_agent.core.logger import get_sub_logger

from .converter import build_message_receive
from .gsclient import GsCoreClient, GsCoreSettings

logger = get_sub_logger("plugin.gscore_bridge")

plugin = NekroPlugin(
    name="GsCore桥接",
    module_name="gscore_bridge",
    author="NTidal",
    version="2.1.4",
    description="将 NA 已接入平台 (SnowLuma/OneBot v11 等) 消息桥接到 gsuid_core / SayuCore，并转发 core 下发消息",
    url="https://github.com/NTidal/nekro_gscore_bridge",
    support_adapter=[],
    sleep_brief="用于连接 gsuid_core / SayuCore，桥接游戏工具箱命令与结果",
)

GSCORE_PM_MASTER = 0
GSCORE_PM_SUPERUSER = 1
GSCORE_PM_GROUP_ADMIN = 3
GSCORE_PM_USER = 6


@plugin.mount_config()
class GsBridgeConfig(ConfigBase):
    """GsCore 桥接配置"""

    ENABLED: bool = Field(default=True, title="启用 GsCore 桥接", description="关闭后不连接 gsuid_core，也不转发消息")
    GSCORE_HOST: str = Field(
        default="127.0.0.1",
        title="GsCore 地址",
        description="gsuid_core WS Host。NA 在 Docker 填宿主机 IP / host.docker.internal；NA 在 WSL、core 在 Windows 填 WSL 网关 IP（ip route show default 输出）或镜像模式 127.0.0.1",
    )
    GSCORE_PORT: str = Field(default="8765", title="GsCore 端口", description="gsuid_core WS 服务端口")
    GSCORE_BOT_ID: str = Field(
        default="NekroAgent",
        title="连接路径 BOT_ID",
        description="固定连接标识 (任意英文字符串)，连接地址 ws://IP:PORT/ws/<BOT_ID>；不要填平台名",
    )
    BOT_SELF_ID: str = Field(
        default="",
        title="bot_self_id",
        description="上报给 GsCore 的机器人自身平台 ID (QQ 号)；留空则自动从适配器获取",
    )
    WS_TOKEN: str = Field(
        default="",
        title="WsToken",
        description=(
            "与 gsuid_core config.json 中的 WsToken 完全一致；留空不校验。"
            "明文显示以免遮罩下粘贴造成旧值拼接；修改时请先清空输入框再粘贴。"
        ),
    )
    MAX_RETRY: int = Field(default=-1, title="最大重连次数", description="-1 表示无限重连")
    BRIDGE_ALL: bool = Field(
        default=True,
        title="自动桥接全部消息",
        description="开启后所有群/私聊消息都转发给 GsCore (gscore 前缀/关键词插件依赖此行为)；关闭后仅 /sayu 命令手动转发",
    )
    BRIDGE_ONLY_TOME: bool = Field(
        default=False,
        title="仅桥接与机器人相关消息",
        description="BRIDGE_ALL 开启时生效：仅转发 @机器人/私聊/is_tome 的消息",
    )
    BLOCK_LLM: bool = Field(
        default=True,
        title="桥接消息阻止 NA 大模型响应",
        description="开启后转发的消息不触发 NA LLM 回复，由 GsCore 接管对话",
    )
    HYBRID_MODE: bool = Field(
        default=False,
        title="混合模式：仅 gscore 指令走 GsCore，其余消息放行给 NA LLM",
        description=(
            "开启后，只有命中 GSCORE_COMMAND_PREFIXES 前缀 (或 HYBRID_BRIDGE_TOME 开启时的 @机器人/私聊消息) "
            "才转发 GsCore 并阻止 LLM；其他消息不转发、正常由 NA 大模型处理。"
            "注意：不要在 NA 系统配置的『忽略的消息前缀』里填这些前缀，那会在适配器入口直接丢弃消息、插件收不到。"
        ),
    )
    GSCORE_COMMAND_PREFIXES: list[str] = Field(
        default_factory=lambda: ["core"],
        title="GsCore 指令前缀列表 (混合模式)",
        description='混合模式下，消息文本 (去除首尾空白后) 以其中任一前缀开头即视为 gscore 指令，如 ["core", "gs", "体力"]；留空列表表示不按前缀匹配',
    )
    HYBRID_BRIDGE_TOME: bool = Field(
        default=False,
        title="混合模式下 @机器人/私聊也转发 GsCore",
        description="开启后，混合模式中 is_tome 消息 (@机器人、私聊) 同样转发 GsCore 并阻止 LLM；关闭时 @机器人/私聊按普通消息放行给 LLM",
    )
    IMAGE_PASSTHROUGH_URL: bool = Field(
        default=True,
        title="远程图片 URL 直传",
        description="消息中的远程图片 URL 直接传给 GsCore (要求 core 能访问该 URL)；关闭或为本地图片时走内置图床",
    )
    IMAGE_BASE_URL: str = Field(
        default="",
        title="内置图床对外前缀",
        description="GsCore 可达的本插件地址，如 http://<NA主机IP>:8021/plugins/<本插件key>（留空自动取本插件实际路由地址，仅同机部署可直连）",
    )
    AVATAR_VIA_IMAGE_HOST: bool = Field(
        default=False,
        title="QQ 头像经内置图床中转",
        description=(
            "默认关闭：用户头像直接用腾讯 qlogo.cn 公开直链上报给 GsCore (要求 core 能访问公网)。"
            "开启后：NA 侧下载头像再经内置图床托管上报，适用于 GsCore 纯内网/无法出公网、"
            "但能访问 NA 图床 (IMAGE_BASE_URL) 的场景；下载失败自动回退为 qlogo 直链。"
        ),
    )
    GSCORE_SUPER_USER_IDS: list[str] = Field(
        default_factory=list,
        title="GsCore pm=1 超级用户 ID",
        description="平台用户 ID 列表 (QQ 号)，命中时上报 user_pm=1",
    )
    MAP_NA_SUPER_USERS: bool = Field(
        default=True,
        title="NA SUPER_USERS 映射为 GsCore pm=1",
        description="NA 配置的 SUPER_USERS 转发时 user_pm=1；pm=0 主人仍需在 GsCore masters 中配置",
    )


config: GsBridgeConfig = plugin.get_config(GsBridgeConfig)

_client: Optional[GsCoreClient] = None
_data_dir: Path = plugin.get_plugin_data_dir()
_temp_dir: Path = _data_dir / "temp"
_img_dir: Path = _data_dir / "img"
_avatar_cache: dict[str, str] = {}  # user_id -> 已托管头像 URL (进程内缓存, 头像极少变)


def _image_base_url() -> str:
    if config.IMAGE_BASE_URL:
        return config.IMAGE_BASE_URL.rstrip("/")
    return f"http://127.0.0.1:8021/plugins/{plugin.key}"


async def _host_image(local_path: str, file_name: str = "") -> Optional[str]:
    """把 NA 本地图片/文件复制进图床目录, 返回对外 http URL。"""
    try:
        src = Path(local_path)
        if not src.exists() or not src.is_file():
            return None
        ext = Path(file_name).suffix or src.suffix or ".jpg"
        name = f"{uuid.uuid4()}{ext}"
        _img_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, _img_dir / name)
        return f"{_image_base_url()}/img/{name}"
    except Exception as e:
        logger.warning(f"图床托管失败 {local_path}: {e}")
        return None


async def _host_avatar(user_id: str, avatar_url: str) -> Optional[str]:
    """下载远程头像 (qlogo) 落盘到图床目录, 返回 NA 图床 URL; 供内网 GsCore 取头像。

    结果按 user_id 进程内缓存 (头像极少变更); 失败返回 None, 由调用方回退直链。
    """
    if not avatar_url:
        return None
    cached = _avatar_cache.get(user_id)
    if cached:
        return cached
    try:
        import aiohttp

        from .converter import _suffix_for

        async with aiohttp.ClientSession() as session:
            async with session.get(avatar_url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    logger.warning(f"下载头像失败 HTTP {resp.status}: user_id={user_id}")
                    return None
                data = await resp.read()
        # 魔数校验, 防止落盘 HTML 错误页 (与图片链路同一教训)
        if not _suffix_for(data, ""):
            logger.warning(f"头像内容非图片, 放弃托管: user_id={user_id}, bytes={len(data)}")
            return None
        name = f"{uuid.uuid4()}{_suffix_for(data, '.jpg')}"
        _img_dir.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread((_img_dir / name).write_bytes, data)
        url = f"{_image_base_url()}/img/{name}"
        _avatar_cache[user_id] = url
        return url
    except Exception as e:
        logger.warning(f"头像图床托管异常 user_id={user_id}: {e}")
        return None


def _user_pm(user_ids: set[str]) -> int:
    if set(config.GSCORE_SUPER_USER_IDS) & user_ids:
        return GSCORE_PM_SUPERUSER
    if config.MAP_NA_SUPER_USERS and user_ids & {str(x) for x in core_config.SUPER_USERS}:
        return GSCORE_PM_SUPERUSER
    return GSCORE_PM_USER


async def _resolve_bot_self_id(message) -> str:
    if config.BOT_SELF_ID:
        return config.BOT_SELF_ID
    try:
        adapter = adapter_utils.get_adapter(message.adapter_key)
        info = await adapter.get_self_info()
        return str(info.user_id)
    except Exception as e:
        logger.warning(f"获取 bot self id 失败: {e}")
        return config.BOT_SELF_ID or "NekroAgent"


async def _ensure_client() -> Optional[GsCoreClient]:
    global _client
    if not config.ENABLED:
        return None
    _temp_dir.mkdir(parents=True, exist_ok=True)
    _img_dir.mkdir(parents=True, exist_ok=True)
    if _client is None:
        logger.info(
            f"创建 GsCore 客户端: {config.GSCORE_HOST}:{config.GSCORE_PORT}/ws/{config.GSCORE_BOT_ID}, "
            f"token_set={bool(config.WS_TOKEN)}"
        )

        def _settings() -> GsCoreSettings:
            # 每次连接/重连时读取最新配置: WebUI 改完 host/port/token 保存后,
            # 下一次重连 (5s 周期) 自动生效, 无需禁用插件或重启 NA。
            return GsCoreSettings(
                bot_id=config.GSCORE_BOT_ID.strip(),
                host=config.GSCORE_HOST.strip(),
                port=config.GSCORE_PORT.strip(),
                # strip 防粘贴带首尾空白/换行; core 端 secrets.compare_digest 精确比对
                ws_token=config.WS_TOKEN.strip(),
                max_retry=config.MAX_RETRY,
            )

        _client = GsCoreClient(
            settings_provider=_settings,
            temp_dir=_temp_dir,
            media_url_hint=_image_base_url(),
        )
    await _client.start()
    return _client


async def _stop_client() -> None:
    global _client
    if _client is not None:
        await _client.stop()
        _client = None


async def _forward(message, *, force: bool = False) -> bool:
    client = await _ensure_client()
    if client is None:
        return False

    user_ids = {str(x) for x in (message.platform_userid, message.sender_id) if x}
    receive = await build_message_receive(
        message,
        bot_self_id=await _resolve_bot_self_id(message),
        user_pm=_user_pm(user_ids),
        passthrough_remote=config.IMAGE_PASSTHROUGH_URL,
        host_image=_host_image,
        host_avatar=_host_avatar if config.AVATAR_VIA_IMAGE_HOST else None,
    )
    if receive is None:
        logger.info(f"消息转换为空, 跳过: {message.chat_key}")
        return False

    logger.info(
        f"上行 GsCore: chat={message.chat_key}, msg_id={receive.msg_id}, bot_id={receive.bot_id}, "
        f"user_type={receive.user_type}, user_pm={receive.user_pm}, segs={len(receive.content)}"
    )
    client.remember_route(receive, message.chat_key)
    await client.report(receive)
    return True


# ---------------------------------------------------------------------------
# 生命周期
# ---------------------------------------------------------------------------
@plugin.mount_init_method()
async def _init() -> None:
    logger.info(
        f"GsCore 桥接初始化: enabled={config.ENABLED}, endpoint={config.GSCORE_HOST}:{config.GSCORE_PORT}, "
        f"bot_id={config.GSCORE_BOT_ID}, bridge_all={config.BRIDGE_ALL}, 图床前缀={_image_base_url()}"
    )
    if config.ENABLED:
        await _ensure_client()


@plugin.on_enabled()
async def _on_enabled() -> None:
    logger.info("GsCore 桥接插件已启用")
    await _ensure_client()


@plugin.on_disabled()
async def _on_disabled() -> None:
    logger.info("GsCore 桥接插件已禁用")
    await _stop_client()


@plugin.mount_cleanup_method()
async def _cleanup() -> None:
    await _stop_client()


# ---------------------------------------------------------------------------
# 消息钩子
# ---------------------------------------------------------------------------
def _is_gscore_command(message) -> bool:
    """混合模式下判断消息是否应路由给 GsCore。

    判定 (任一命中即转发):
      1. 文本去首尾空白后以 GSCORE_COMMAND_PREFIXES 中任一前缀开头
      2. HYBRID_BRIDGE_TOME 开启且消息 is_tome (@机器人/私聊)
    """
    text = (getattr(message, "content_text", "") or "").strip()
    prefixes = [str(p).strip() for p in (config.GSCORE_COMMAND_PREFIXES or []) if str(p).strip()]
    if text and any(text.startswith(p) for p in prefixes):
        return True
    if config.HYBRID_BRIDGE_TOME and getattr(message, "is_tome", False):
        return True
    return False


@plugin.mount_on_user_message()
async def on_user_message(_ctx: AgentCtx, message):
    if not config.ENABLED or not config.BRIDGE_ALL:
        return None
    # 混合模式: 非 gscore 指令直接放行 (返回 None = CONTINUE, 不转发、不阻止 LLM)
    if config.HYBRID_MODE and not _is_gscore_command(message):
        logger.debug(f"混合模式放行 (非 gscore 指令): chat={message.chat_key}, text={message.content_text[:32]!r}")
        return None
    if config.BRIDGE_ONLY_TOME and not message.is_tome:
        return None
    try:
        forwarded = await _forward(message)
    except Exception as e:
        logger.exception(f"桥接消息失败: {e}")
        return None
    # 转发失败 (如客户端未就绪) 时不要误吞消息, 放行给 LLM
    if not forwarded:
        return None
    return MsgSignal.BLOCK_TRIGGER if config.BLOCK_LLM else None


# ---------------------------------------------------------------------------
# 运维工具 (LLM/沙盒可调)
# ---------------------------------------------------------------------------
@plugin.mount_sandbox_method(SandboxMethodType.AGENT, name="gscore_status", description="查询 GsCore 桥接连接状态（是否启用、运行中、已连接、地址）")
async def gscore_status(_ctx: AgentCtx) -> str:
    """查询 GsCore 桥接连接状态。无需参数。结果需要你向用户汇报。

    AGENT 方法：返回后请基于结果继续回复用户（连接正常与否、地址是否正确等）。
    """
    if not config.ENABLED:
        return "GsCore 桥接未启用"
    if _client is None:
        return "GsCore 客户端未创建"
    return (
        f"enabled=True, running={_client.is_running}, connected={_client.is_connected}, "
        f"endpoint={config.GSCORE_HOST}:{config.GSCORE_PORT}, bot_id={config.GSCORE_BOT_ID}"
    )


@plugin.mount_sandbox_method(SandboxMethodType.AGENT, name="gscore_reconnect", description="重连 GsCore（修改地址/端口/token 配置后调用）")
async def gscore_reconnect(_ctx: AgentCtx) -> str:
    """重连 GsCore 连接。无需参数。修改连接配置后调用以生效，结果需要你向用户汇报。

    AGENT 方法：返回后请基于结果继续回复用户（重连已启动/失败原因）。
    """
    if not config.ENABLED:
        raise RuntimeError("GsCore 桥接未启用")
    await _stop_client()
    client = await _ensure_client()
    return "GsCore 重连任务已启动" if client else "GsCore 客户端创建失败"


# ---------------------------------------------------------------------------
# 内置图床 (供 GsCore 及识图插件下载 NA 侧本地图片)
# ---------------------------------------------------------------------------
@plugin.mount_router()
def create_router() -> APIRouter:
    router = APIRouter()
    name_re = re.compile(r"^[0-9a-fA-F-]{36}\.[A-Za-z0-9]{1,8}$")

    @router.get("/img/{name}")
    async def serve_img(name: str):
        if not name_re.match(name):
            raise HTTPException(status_code=404)
        path = (_img_dir / name).resolve()
        if _img_dir.resolve() not in path.parents:
            raise HTTPException(status_code=404)
        if not path.exists():
            raise HTTPException(status_code=404)
        return FileResponse(path)

    return router
