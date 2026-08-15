from typing import Annotated, Any, Dict, Literal, Union, List

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


# Content payloads (discriminated union)
class TextContent(BaseModel):
    type: Literal["text"]
    text: str


class MediaContent(BaseModel):
    type: Literal["media"]
    media_type: Literal["image", "video", "audio", "document"] = "image"
    url: HttpUrl | str
    caption: str | None = None
    filename: str | None = None
    mime_type: str | None = None
    raw: Dict[str, Any] = {}


class StickerContent(BaseModel):
    type: Literal["sticker"]
    ref: str


class ContactContent(BaseModel):
    type: Literal["contact"]
    name: str
    phone: str
    org: str | None = None


class LocationContent(BaseModel):
    type: Literal["location"]
    latitude: float
    longitude: float
    name: str | None = None


Content = Annotated[
    Union[
        TextContent,
        MediaContent,
        StickerContent,
        ContactContent,
        LocationContent,
    ],
    Field(discriminator="type"),
]


# Unified message
class UnifiedMessage(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    channel: Literal["whatsapp", "telegram", "vk"]
    recipient_id: str
    sender_id: str | None = None
    sender_name: str | None = None
    content: Content
    attachments: List[MediaContent] = []
    raw: dict | None = None
