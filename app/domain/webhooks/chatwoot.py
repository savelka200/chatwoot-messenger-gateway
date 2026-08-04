from typing import List, Optional, Dict, Any

from pydantic import BaseModel


class ChatwootConversationMeta(BaseModel):
    channel: Optional[str] = None  # "whatsapp" | "telegram" | "vk"
    recipient_id: Optional[str] = None  # unified recipient id


class ChatwootConversation(BaseModel):
    meta: ChatwootConversationMeta = ChatwootConversationMeta()


class ChatwootAttachment(BaseModel):
    """Minimal model for Chatwoot message attachments."""
    id: Optional[int] = None
    type: Optional[str] = None  # "image", "video", "audio", "file", "sticker", "location", "contact"
    data_url: Optional[str] = None
    url: Optional[str] = None
    filename: Optional[str] = None
    mime_type: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    name: Optional[str] = None
    phone_number: Optional[str] = None
    org: Optional[str] = None


class ChatwootMessageCreatedWebhook(BaseModel):
    """Minimal model for Chatwoot event=message_created."""

    event: str
    message_type: Optional[str] = None  # "incoming" | "outgoing"
    private: Optional[bool] = None
    content: Optional[str] = None
    conversation: ChatwootConversation = ChatwootConversation()
    attachments: List[Dict[str, Any]] = []
