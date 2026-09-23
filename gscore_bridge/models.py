"""gsuid-core (SayuCore) WS 协议报文模型 (pydantic, NA 自带依赖)。

帧承载: UTF-8 JSON 二进制帧 (core 侧 receive_bytes() + msgspec.json;
pydantic model_dump_json().encode() 与其完全兼容, 无需 msgspec)。
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class GsMessage(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    type: str | None = None
    data: Any | None = None


class MessageReceive(BaseModel):
    """上行: 适配器 -> core。"""

    model_config = ConfigDict(coerce_numbers_to_str=True)

    bot_id: str = "Bot"
    bot_self_id: str = ""
    msg_id: str = ""
    user_type: Literal["group", "direct", "channel", "sub_channel"] = "group"
    group_id: str | None = None
    user_id: str | None = None
    sender: dict[str, Any] = Field(default_factory=dict)
    user_pm: int = 6
    content: list[GsMessage] = Field(default_factory=list)


class MessageSend(BaseModel):
    """下行: core -> 适配器。"""

    model_config = ConfigDict(coerce_numbers_to_str=True)

    bot_id: str = "Bot"
    bot_self_id: str = ""
    msg_id: str = ""
    target_type: str | None = None
    target_id: str | None = None
    content: list[GsMessage] | None = None
    echo: str | None = None
