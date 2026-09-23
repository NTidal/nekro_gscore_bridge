"""GsCore 桥接插件: NA(已接入 SnowLuma/OneBot 等平台) <-> gsuid_core (SayuCore)。

SnowLuma 仅实现 OneBot v11 协议, 不能直连 gscore 的 /ws/<BOT_ID> 端点
(该端点要求 gscore MessageReceive 二进制帧; ob11 文本帧会触发 core
receive_bytes() 抛 KeyError: 'bytes' 后断开)。本插件在 NA 内做协议翻译。
"""

from .plugin import plugin

__all__ = ["plugin"]
