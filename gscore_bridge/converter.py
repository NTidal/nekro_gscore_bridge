"""消息段转换: NA ChatMessage <-> gscore 报文段。

上行注意:
- 图片优先给 core 可达的 http URL (识图/OCR 插件直接 httpx 下载, 不支持
  base64 内联); 远程 URL 按配置直传, NA 本地图片经插件内置图床托管。
- @机器人自身的 at 段 data 必须等于 bot_self_id, core 才置 is_tome。

下行注意:
- NA onebot 适配器 IMAGE/FILE 发送只认本地文件 (file_path 需真实存在,
  读取字节上传), 因此 gscore 的 http/link/base64 媒体一律先落盘到 temp_dir。
"""

import asyncio
import base64
import re
import uuid
from pathlib import Path
from typing import Awaitable, Callable, Optional

import aiohttp
from nekro_agent.adapters.interface.schemas.platform import (
    PlatformAtSegment,
    PlatformSendSegment,
    PlatformSendSegmentType,
)
from nekro_agent.core.logger import get_sub_logger
from nekro_agent.schemas.chat_message import (
    ChatMessage,
    ChatMessageSegmentAt,
    ChatMessageSegmentFile,
    ChatMessageSegmentImage,
    ChatMessageSegmentType,
    ChatType,
)

from .models import GsMessage, MessageReceive

logger = get_sub_logger("plugin.gscore_bridge")

ImageHost = Callable[[str, str], Awaitable[Optional[str]]]
"""把本地文件托管为 http URL 的回调 (local_path, file_name) -> url 或 None。"""

AvatarHost = Callable[[str, str], Awaitable[Optional[str]]]
"""把远程头像 URL 下载后经内置图床托管的回调 (user_id, avatar_url) -> hosted_url 或 None。"""

_MAGIC = {
    b"\xff\xd8\xff": ".jpg",
    b"\x89PNG\r\n\x1a\n": ".png",
    b"GIF8": ".gif",
    b"BM": ".bmp",
}


def _suffix_for(data: bytes, fallback: str) -> str:
    for magic, suf in _MAGIC.items():
        if data.startswith(magic):
            return suf
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return fallback


async def _download(url: str, temp_dir: Path, suffix: str) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.warning(f"下载 gscore 媒体失败 HTTP {resp.status}: {url[:100]}")
                    return None
                data = await resp.read()
        suf = _suffix_for(data, suffix)
        path = temp_dir / f"{uuid.uuid4().hex}{suf}"
        await asyncio.to_thread(path.write_bytes, data)
        return str(path)
    except Exception as e:
        logger.warning(f"下载 gscore 媒体异常: {e}")
        return None


async def _materialize(data: str, temp_dir: Path, suffix: str) -> Optional[str]:
    """gscore 媒体数据 (http/link://base64) -> NA 可发送的本地文件路径。"""
    if not data:
        return None
    if data.startswith("link://"):
        data = data[len("link://"):]
    if data.startswith(("http://", "https://")):
        return await _download(data, temp_dir, suffix)
    raw = data[len("base64://"):] if data.startswith("base64://") else data
    if data.startswith("data:") and "," in raw:
        raw = raw.split(",", 1)[1]
    try:
        blob = base64.b64decode(raw)
    except Exception as e:
        logger.warning(f"gscore base64 媒体解码失败: {e}")
        return None
    path = temp_dir / f"{uuid.uuid4().hex}{suffix}"
    await asyncio.to_thread(path.write_bytes, blob)
    return str(path)


# ---------------------------------------------------------------------------
# 上行: NA -> gscore
# ---------------------------------------------------------------------------
def parse_chat_key(chat_key: str) -> tuple[bool, str]:
    """'onebot_v11-group_123' -> (is_group=True, '123'); 'xxx-private_456' -> (False, '456')。"""
    channel = chat_key.split("-", 1)[1] if "-" in chat_key else chat_key
    m = re.match(r"^(group|private|channel|sub_channel)_(.+)$", channel)
    if m:
        return m.group(1) == "group", m.group(2)
    return False, channel


def gscore_bot_id(adapter_key: str) -> str:
    return "onebot" if adapter_key == "onebot_v11" else adapter_key


async def _na_segments_to_gscore(
    message: ChatMessage,
    *,
    bot_self_id: str,
    passthrough_remote: bool,
    host_image: Optional[ImageHost],
) -> list[GsMessage]:
    out: list[GsMessage] = []

    # 引用: NA ext_data 保留被引用消息平台 id
    ext = message.ext_data or {}
    ref_id = ""
    if isinstance(ext, dict):
        ref_id = str(ext.get("ref_msg_id") or "")
    else:
        ref_id = str(getattr(ext, "ref_msg_id", "") or "")
    if ref_id:
        out.append(GsMessage(type="reply_id", data=ref_id))
        # 引用消息里的图片不在当前消息 content_data 中 (NA 入站只保留 ref_msg_id),
        # 必须用 OneBot 原语 get_msg 重取被引用消息的 CDN 直链, 否则 gscore 收不到图。
        if message.adapter_key == "onebot_v11":
            quoted = await _onebot_quoted_image_segments(
                ref_id, passthrough_remote=passthrough_remote, host_image=host_image
            )
            out.extend(quoted)

    for segment in message.content_data or []:
        stype = ChatMessageSegmentType(segment.type)

        if stype == ChatMessageSegmentType.TEXT:
            if segment.text:
                out.append(GsMessage(type="text", data=segment.text))

        elif stype == ChatMessageSegmentType.AT and isinstance(segment, ChatMessageSegmentAt):
            target = str(segment.target_platform_userid or "")
            if target:
                # @机器人自身改写为 bot_self_id (core 据此置 is_tome)
                out.append(GsMessage(type="at", data=target))

        elif stype == ChatMessageSegmentType.IMAGE and isinstance(segment, ChatMessageSegmentImage):
            msg = await _image_to_gscore(segment, passthrough_remote, host_image)
            if msg:
                out.append(msg)

        elif stype in (ChatMessageSegmentType.FILE, ChatMessageSegmentType.VOICE, ChatMessageSegmentType.VIDEO):
            if isinstance(segment, ChatMessageSegmentFile):
                remote = segment.remote_url or ""
                local = segment.local_path or ""
                data_url = ""
                if passthrough_remote and remote.startswith(("http://", "https://")):
                    data_url = remote
                elif local and host_image is not None:
                    data_url = (await host_image(local, segment.file_name or "")) or remote
                elif remote:
                    data_url = remote
                if data_url:
                    out.append(GsMessage(type="file", data=f"{segment.file_name or 'file'}|{data_url}"))

        elif stype == ChatMessageSegmentType.POKE:
            out.append(GsMessage(type="text", data="[戳一戳]"))

        elif stype == ChatMessageSegmentType.FORWARD:
            out.append(GsMessage(type="text", data=getattr(segment, "text", "") or "[合并转发消息]"))

    return out


async def _image_to_gscore(
    seg: ChatMessageSegmentImage,
    passthrough_remote: bool,
    host_image: Optional[ImageHost],
) -> Optional[GsMessage]:
    remote = seg.remote_url or ""
    local = seg.local_path or ""

    if passthrough_remote and remote.startswith(("http://", "https://")):
        return GsMessage(type="image", data=remote)

    if local and host_image is not None:
        url = await host_image(local, seg.file_name or "")
        if url:
            return GsMessage(type="image", data=url)
        logger.warning("图床托管失败, 尝试远程 URL 或 base64 兜底")
    if remote.startswith(("http://", "https://")):
        return GsMessage(type="image", data=remote)
    if local:
        # 最终兜底: base64 (仅 core 核心可解, 识图类插件不可用)
        try:
            b64 = base64.b64encode(await asyncio.to_thread(Path(local).read_bytes)).decode()
            return GsMessage(type="image", data=f"base64://{b64}")
        except Exception:
            return None
    return None


def _iter_image_datas(obj):
    """递归深扫 OneBot 报文 (dict / list / nonebot MessageSegment 对象双形态),
    产出所有图片段的 data dict; 同时产出合并转发段的 forward id。

    不信任预处理后的组件链: 真实图片可能埋在嵌套节点里, 故按段类型判定而非 URL 关键字。
    """
    forward_ids: list[str] = []

    def _walk(o):
        if isinstance(o, dict):
            seg_type = o.get("type")
            seg_data = o.get("data")
            if seg_type == "image" and isinstance(seg_data, dict):
                yield seg_data
            elif seg_type in ("forward", "node") and isinstance(seg_data, dict):
                fid = seg_data.get("id") or seg_data.get("resid")
                if fid:
                    forward_ids.append(str(fid))
            for value in o.values():
                yield from _walk(value)
        elif isinstance(o, (list, tuple)):
            for value in o:
                yield from _walk(value)
        elif o is not None and not isinstance(o, (str, bytes, int, float, bool)):
            seg_type = getattr(o, "type", None)
            seg_data = getattr(o, "data", None)
            if seg_type == "image" and isinstance(seg_data, dict):
                yield seg_data
            elif seg_type in ("forward", "node") and isinstance(seg_data, dict):
                fid = seg_data.get("id") or seg_data.get("resid")
                if fid:
                    forward_ids.append(str(fid))
            try:
                for value in o:
                    yield from _walk(value)
            except Exception:
                pass

    yield from _walk(obj)
    return forward_ids


async def _onebot_call(action: str, message_id):
    """调用 OneBot v11 动作; 延迟导入以保证平台隔离 (仅 onebot_v11 触发)。"""
    from nekro_agent.adapters.onebot_v11.core.bot import get_bot

    bot = get_bot()
    return await bot.call_api(action, message_id=message_id)


async def _onebot_quoted_image_segments(
    ref_id: str,
    *,
    passthrough_remote: bool,
    host_image: Optional[ImageHost],
) -> list[GsMessage]:
    """用 OneBot get_msg / get_forward_msg 重取被引用消息中的图片, 转 gscore image 段。

    框架预处理可能把引用链图片 URL 覆写为本地临时路径 (失效/被压码), 故必须用
    平台原语取回未改写的 CDN 直链; http 直链直接上行, 本地文件走内置图床。
    """
    payload_id: object = int(ref_id) if str(ref_id).isdigit() else ref_id
    responses: list[object] = []
    try:
        responses.append(await _onebot_call("get_msg", payload_id))
    except Exception as e:
        logger.warning(f"get_msg 重取引用消息失败 (id={ref_id}): {e}")
        return []

    # 深扫引用消息; 若内含合并转发, 再用 get_forward_msg 拉取节点内容
    seen_forward: set[str] = set()
    image_datas: list[dict] = []
    forward_ids: list[str] = []

    def _collect(o) -> None:
        gen = _iter_image_datas(o)
        try:
            while True:
                image_datas.append(next(gen))
        except StopIteration as stop:
            fids = stop.value or []
            forward_ids.extend(fids)

    for resp in list(responses):
        _collect(resp)

    for fid in forward_ids:
        if fid in seen_forward:
            continue
        seen_forward.add(fid)
        try:
            fwd_resp = await _onebot_call("get_forward_msg", int(fid) if fid.isdigit() else fid)
            _collect(fwd_resp)
        except Exception as e:
            logger.warning(f"get_forward_msg 重取合并转发失败 (id={fid}): {e}")

    out: list[GsMessage] = []
    for data in image_datas:
        url = str(data.get("url") or "")
        file_field = str(data.get("file") or "")
        if url.startswith(("http://", "https://")) and (passthrough_remote or not file_field.startswith("/")):
            out.append(GsMessage(type="image", data=url))
        elif file_field.startswith(("http://", "https://")):
            out.append(GsMessage(type="image", data=file_field))
        elif file_field and host_image is not None and Path(file_field).exists():
            hosted = await host_image(file_field, data.get("file_name") or "")
            if hosted:
                out.append(GsMessage(type="image", data=hosted))
        elif url.startswith(("http://", "https://")):
            out.append(GsMessage(type="image", data=url))
    logger.info(
        f"引用消息 {ref_id} 重取: 图片段 {len(image_datas)} 个, 合并转发 {len(seen_forward)} 个, "
        f"上行图片 {len(out)} 张"
    )
    return out


def _qq_avatar_url(adapter_key: str, user_id: str, size: int = 640) -> str:
    """OneBot v11 (QQ) 用户头像直链。

    用腾讯官方头像 CDN q1.qlogo.cn: 公开、实时、无需协议端额外 API
    (OneBot v11 标准无头像动作, NapCat/Lagrange 的 get_user_info 返回字段不统一)。
    仅当平台为 QQ 且 user_id 为纯数字 QQ 号时生成; 其他平台返回空串。
    gscore 侧 httpx 直接下载, 要求 core 能访问公网。
    """
    if adapter_key == "onebot_v11" and user_id.isdigit():
        return f"https://q1.qlogo.cn/g?b=qq&nk={user_id}&s={size}"
    return ""


async def build_message_receive(
    message: ChatMessage,
    *,
    bot_self_id: str,
    user_pm: int,
    passthrough_remote: bool,
    host_image: Optional[ImageHost] = None,
    host_avatar: Optional[AvatarHost] = None,
) -> Optional[MessageReceive]:
    content = await _na_segments_to_gscore(
        message,
        bot_self_id=bot_self_id,
        passthrough_remote=passthrough_remote,
        host_image=host_image,
    )
    if not content and message.content_text:
        content = [GsMessage(type="text", data=message.content_text)]
    if not content:
        return None

    is_group, channel_id = parse_chat_key(message.chat_key)
    try:
        is_group = ChatType(message.chat_type) == ChatType.GROUP
    except Exception:
        pass

    user_id = str(message.platform_userid or message.sender_id or "")
    avatar = _qq_avatar_url(message.adapter_key, user_id)
    if avatar and host_avatar is not None:
        # 内网 GsCore 无法访问公网 qlogo 时, 经 NA 内置图床中转; 失败回退直链
        hosted = await host_avatar(user_id, avatar)
        if hosted:
            avatar = hosted
    if avatar:
        logger.info(f"上行用户头像: user_id={user_id}, avatar={avatar}")

    return MessageReceive(
        bot_id=gscore_bot_id(message.adapter_key),
        bot_self_id=bot_self_id,
        msg_id=str(message.message_id or message.chat_key),
        user_type="group" if is_group else "direct",
        group_id=channel_id if is_group else None,
        user_id=user_id,
        sender={
            "nickname": message.sender_nickname or message.sender_name or "",
            "avatar": avatar,
        },
        user_pm=user_pm,
        content=content,
    )


# ---------------------------------------------------------------------------
# 下行: gscore -> NA
# ---------------------------------------------------------------------------
async def materialize_voice(data: str, temp_dir: Path) -> Optional[str]:
    """gscore record 段 (base64/http) -> 本地音频文件路径, 供直接调 OneBot 语音 API。"""
    return await _materialize(str(data), temp_dir, ".mp3")


async def gs_to_platform_segments(
    messages: list[GsMessage] | None,
    temp_dir: Path,
    media_url_hint: str = "",
    adapter_key: str = "",
) -> list[PlatformSendSegment]:
    segments: list[PlatformSendSegment] = []
    for msg in messages or []:
        if msg.data is None and msg.type != "at":
            continue
        if msg.type == "text":
            text = str(msg.data)
            if text.strip():
                segments.append(PlatformSendSegment(type=PlatformSendSegmentType.TEXT, content=text))
        elif msg.type == "at":
            segments.append(
                PlatformSendSegment(
                    type=PlatformSendSegmentType.AT,
                    content=str(msg.data),
                    at_info=PlatformAtSegment(platform_user_id=str(msg.data)),
                )
            )
        elif msg.type == "image":
            path = await _materialize(str(msg.data), temp_dir, ".png")
            if path:
                segments.append(PlatformSendSegment(type=PlatformSendSegmentType.IMAGE, file_path=path))
            else:
                segments.append(PlatformSendSegment(type=PlatformSendSegmentType.TEXT, content="[图片]"))
        elif msg.type == "file":
            raw = str(msg.data)
            name, _, payload = raw.partition("|") if "|" in raw else ("file", raw)
            path = await _materialize(payload, temp_dir, Path(name).suffix or ".bin")
            if path:
                segments.append(PlatformSendSegment(type=PlatformSendSegmentType.FILE, file_path=path))
        elif msg.type == "record":
            if adapter_key == "onebot_v11":
                # OneBot v11 由 gsclient 直接调 send_group_msg + record 段发送真正的语音,
                # NA 标准发送段无 VOICE 类型, 这里跳过避免降级成文件。
                logger.info("record 语音段交由 OneBot v11 直连 API 发送, 跳过标准段转换")
            else:
                path = await _materialize(str(msg.data), temp_dir, ".mp3")
                if path:
                    segments.append(PlatformSendSegment(type=PlatformSendSegmentType.FILE, file_path=path))
        elif msg.type == "video":
            path = await _materialize(str(msg.data), temp_dir, ".mp4")
            if path:
                segments.append(PlatformSendSegment(type=PlatformSendSegmentType.FILE, file_path=path))
        elif msg.type == "node" and isinstance(msg.data, list):
            for node in msg.data:
                if isinstance(node, dict):
                    segments.extend(
                        await gs_to_platform_segments(
                            [GsMessage.model_validate(node)], temp_dir, media_url_hint, adapter_key
                        )
                    )
        else:
            logger.info(f"gscore 下行未处理段类型: {msg.type}")
    return segments
